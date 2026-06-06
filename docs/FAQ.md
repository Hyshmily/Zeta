# FAQ: Common Questions

> Common questions about HotKey's architecture and design choices.

---

## Q1: Why both local HeavyKeeper AND a central Worker?

**Short answer:** Local detection is for _self-preservation_ (nanosecond-level); the central Worker is for _cluster coordination_ (100ms-level). They serve different time scales and purposes.

| Aspect            | Local HeavyKeeper                                    | Central Worker                                                      |
| ----------------- | ---------------------------------------------------- | ------------------------------------------------------------------- |
| **Response time** | Nanoseconds                                          | ~100ms–2s (report interval + sliding window accumulation)           |
| **Scope**         | Single instance                                      | Cluster-wide                                                        |
| **Purpose**       | Immediate hot-key TTL promotion to protect this node | Global consensus, cross-instance pre-warming, coordinated cool-down |
| **When it acts**  | On every `get()` call                                | After accumulating enough window data                               |

**How they cooperate in code:**

1. **Burst hits instance A:** `HotKeyCache.get()` L1 miss → `loadAndCache()` calls `hotKeyDetector.add(key, 1).isLocalHotKey()` → if hot, caches with `hotHardTtlMs` (1h default). Instance A is protected instantly.

2. **Meanwhile,** `get()` also calls `hotKeyReporter.record(key)` → `HotKeyReporter` accumulates in a Caffeine counter → every `reportIntervalMs` (100ms) flushes to RabbitMQ.

3. **Worker receives the report** → `ReportConsumer.onReport()` feeds `detector.addCount(key, count)` → `HotKeyStateMachine.evaluate()` tracks consecutive hot windows → after `confirmDurationMs` (2s) of sustained heat, broadcasts HOT to **all instances**.

4. **Instances B, C, D** receive HOT via `WorkerListener` → pre-warm Caffeine before traffic reaches them.

**Without the central Worker:** instance A survives, but traffic load-balanced to B moments later finds an empty cache. That's "hot-spot drift".

---

## Q2: The central Worker takes hundreds of milliseconds — isn't that too late?

**Short answer:** No. By the time the Worker confirms a hot key, the local HeavyKeeper has already protected the node that received the initial burst. The Worker delay only affects _cross-instance coordination_, which operates on a seconds-to-minutes timescale for sustained hot keys.

**Timeline of a real hot key (e.g., flash sale item):**

```
t=0ms    Key X first accessed on Node A
t=+1μs   Local HeavyKeeper: "looks hot" → cached with 1h hot TTL on Node A
t=+100ms HotKeyReporter flush → Worker receives first batch, key above threshold → tick 1
t=+2.0s   20 consecutive hot ticks (2000ms total, confirmCount=20 per WorkerProperties) → CONFIRMED_HOT
t=+2.0s   Broadcast sent to all nodes (AMQP, same tick)
t=+2.1s   Nodes B, C, D receive HOT → pre-warm from Redis → cached with hot TTL
```

The key insight: **a real hot key stays hot for minutes or hours**, not milliseconds. The ~2s Worker latency (default `confirmDurationMs=2000` / 100ms per evaluation tick = 20 ticks) is negligible compared to the total hot duration. Meanwhile, the node that got the first punch was already protected in microseconds.

**Measured latencies** (from `PropagationDelayIT`, 10 phases, 45k ops, 0 errors):

| Scenario | P50 | P95 | P99 |
|---|---|---|---|
| Worker Decision (w/o state machine) | **51.64 ms** | 97.75 ms | 104.16 ms |
| State Machine Pipeline (w/ state machine) | **1,983 ms** | 2,015 ms | 2,015 ms |
| Full Chain (w/o state machine) | **155.81 ms** | 202.14 ms | 212.69 ms |
| Full Chain w/ SM (20 confirm windows) | **2,038 ms** | 2,055 ms | 2,055 ms |

- **Without state machine** (Phase 7): Worker broadcasts HOT immediately after detection. P50=51.6ms includes `warmupJitterMs=100ms` + AMQP + L1 polling.
- **With state machine** (Phase 8): 2.0s is dominated by confirm windows (20 × 100ms = 2,000ms min). After confirmation, same AMQP path (~52ms). Configurable via `hotkey.worker.state-machine.*`.
- **Full Chain without SM** (Phase 9A): Full end-to-end path — local Caffeine miss → report aggregation (100ms batch) → AMQP report delivery → SlidingWindowDetector → AMQP decision broadcast → L1 promotion. Adds ~104ms over Phase 7 due to report batching + two AMQP hops.
- **Full Chain with SM** (Phase 9B): Same as 9A but with 20 confirm windows on the Worker side. Total latency dominated by the 2s confirm floor.

**What about a brief 100ms spike?** That's a local blip, not a cluster-level hot key. Local HeavyKeeper handles it with a longer TTL. Worker never needs to broadcast — that's why they're separate.

The `SingleFlight.java` layer provides additional safety: concurrent L1 misses for the same key share a single `CompletableFuture`, preventing a thundering-herd against the backend even during the brief window before local HeavyKeeper activates.

---

## Q3: If local HeavyKeeper already warms up the cache, what's the point of the central Worker broadcasting HOT to warm it again?

**Short answer:** Local warming protects _one node_. Central broadcasting protects _all nodes_ before traffic reaches them.

**The gap local warming cannot close:**

```
Time ──────────────────────────────────────────────────────────────>
      Node A gets hit → local HeavyKeeper → cached with hot TTL
                                                      ↓ Node A safe
      Load balancer shifts traffic → Node B ← NO CACHE → Redis hit!
```

Node A is safe, but Node B has no idea Key X is hot. When traffic swings to B, it finds an empty Caffeine cache. This is the **hot-spot drift** problem.

The Worker broadcast pre-warms **all nodes simultaneously**:

```
Worker detects Key X is globally hot → broadcasts HOT
  ├─ Node A: already hot (updates decisionVersion, extends TTL)
  ├─ Node B: receives HOT → loads from Redis → caches with hot TTL → ready
  ├─ Node C: same
  └─ Node D: same
```

**Beyond pre-warming, the central Worker also provides:**

- **Unified cool-down:** Only the Worker decides when a key is no longer hot. Without it, nodes would independently decide to cool down at different times, creating inconsistent cache states.
- **Decision version ordering:** Each HOT/COOL broadcast carries a monotonically increasing `decisionVersion` (`WorkerBroadcaster.java`). Receivers use this to discard out-of-order messages, ensuring total ordering per key.

- **Slow-heat detection via TopK:** The `TopKValidator` (`TopKValidator.java`) periodically scans the global frequency ranking. Keys that gradually accumulate high total counts (but never spike) are pre-warmed — something local HeavyKeeper cannot do.
---

## Q4: RabbitMQ has limited throughput (~10K–100K msg/s). Won't reporting traffic overwhelm it?

**Short answer:** No. The reporting channel uses three design measures to control traffic:

### 1. Batching dramatically reduces message count

`HotKeyReporter` (`HotKeyReporter.java`) does not send one message per access. It accumulates counts in a local Caffeine counter and flushes **batched** reports every `reportIntervalMs` (default 100ms). A single `ReportMessage` carries hundreds or thousands of key-count pairs.

**Formula:** Messages per second = `1000 / reportIntervalMs` = 10 msg/s _per shard_, regardless of access volume.

### 2. Consistent-hash sharding distributes load

Each key is routed to exactly one shard via `Math.abs(key.hashCode()) % shardCount` (`HotKeyReporter.java`). With `shardCount = N`, the reporting load is split across N queues/consumers. Each Worker consumer only sees its assigned keys.

### 3. The control channel is separate from the data channel

| Channel                       | Exchange                    | Traffic pattern                     | Volume                               |
| ----------------------------- | --------------------------- | ----------------------------------- | ------------------------------------ |
| **Report** (app → Worker)     | `hotkey.report.exchange`    | Batched counts, every 100ms per app | Stable, proportional to app count    |
| **Broadcast** (Worker → apps) | `hotkey.broadcast.exchange` | HOT/COOL decisions                  | Very low — only on state transitions |

Broadcasts are **control signals**, not data streams. A key transitions from COLD → CONFIRMED_HOT → PRE_COOLING → COLD at most once per lifecycle. The `HotKeyStateMachine` (`HotKeyStateMachine.java`) with its `confirmCount`/`coolCount` hysteresis ensures no flapping/bursts of broadcast messages.

**If your cluster is large enough to worry about MQ throughput:** increase `shardCount`, which reduces per-queue message volume proportionally. Each additional shard adds a consumer thread on the Worker.

---

## Q5: The TopKValidator pre-warms keys based on historical ranking — does this actually get used?

**Short answer:** It serves as a safety net for the 1% of cases the sliding window cannot catch. It is used infrequently — designed as a supplementary detection mechanism rather than a primary path.

**What it catches:** Some keys never "spike" but accumulate high total traffic over minutes or hours — e.g., a landing page that gradually attracts more visitors, a dashboard widget queried by many users at a steady rate.

These keys would **never** trigger the sliding-window threshold (default 1000 QPS in 1s window), but their total daily access count ranks in the global TopK. The `TopKValidator.validate()` (`TopKValidator.java`) runs every `validateIntervalMs` (default 60s) and checks:

1. Is this key consistently in the global TopK?
2. Has it appeared in the TopK for `preWarmMinAppearances` (default 2) consecutive validation cycles?
3. If yes → broadcast HOT → the key gets full hot-key protection.

**Why it is not the primary detection path:**

- The sliding window covers 99%+ of real hot keys (burst traffic pattern).
- TopK pre-warming adds 1–3 minutes of latency before a slow-heat key is promoted.
- Without it, that key might never be promoted and would rely on short TTLs indefinitely — a correctness gap the sliding window alone cannot fill.

**Configurable toggle:** The entire mechanism is controlled by `hotkey.worker.topk-validation.*` properties. Set `validate-interval-ms: 0` to disable it entirely.

---

## Q6: Could a brand-new key, never accessed before, hit ALL instances simultaneously faster than the Worker can broadcast HOT?

**Short answer:** Yes, this is theoretically possible. The framework is designed to minimize the risk but cannot guarantee absolute protection against every extreme edge case.

**What happens step by step:**

1. **t=0:** Key X, never seen before, suddenly receives concurrent requests on all 10 nodes simultaneously.

2. **t=+1μs:** Every node's `SingleFlight` (`SingleFlight.java`) activates. In stress tests, `singleFlight_extremeDedup` achieves **99.0% dedup** (100 threads → 1 execution) and `singleFlight_cacheStampede` achieves **91.5% dedup** (50 keys × 20 threads → 85 executions, data: `hotkey-stress-*.json`). The backend receives ~N concurrent requests (one per node), not `N × concurrent_requests`.

3. **t+~5ms to t+~20ms:** The first request on each node completes (L2 latency dependent). Data returned to all waiting callers. Value cached in L1 with **normal TTL** (5min default per `HotKeyProperties.java:87`) — local HeavyKeeper not yet triggered.

4. **t+100ms:** `HotKeyReporter` flushes the first batch of counts to the Worker (`reportIntervalMs=100` per `HotKeyProperties.java:148`). Stress test `reporter_highFrequency` confirms **3M ops/s throughput with zero data loss**.

5. **t+~2.1s:** After 20 consecutive hot evaluations (confirmCount=20 × 100ms per tick = 2000ms, per `WorkerProperties.java:74,124`), state machine transitions to CONFIRMED_HOT and broadcasts.

6. **t+~2.15s:** All nodes receive HOT (propagation benchmark shows P50=51.64ms, P95=97.75ms, P99=104.16ms, data: `propagation-delay-*.json`) → upgrade key to hot TTL (1h per `HotKeyProperties.java:91`) and enable soft expiration.

**Impact analysis:**

| Component          | Status                                                            |
| ------------------ | ----------------------------------------------------------------- |
| Backend (Redis/DB) | Safe — SingleFlight limits reads to ≤ N (proven 91.5-99.0% dedup) |
| L1 Caffeine        | Each node has the value cached with normal TTL (5min)             |
| p99 latency        | First-wave requests see single L2 latency. `timeoutContention` test: 0 timeouts under 50-thread load |
| Hot-key protection | W/o SM: ~74ms from Worker detection to L1 upgrade. W/ SM: ~2.1s (20 confirm windows). Normal TTL provides baseline protection during the gap |

**Without SingleFlight**, this scenario would send `N × concurrent_requests` to the backend, which could cause a cascading failure. SingleFlight is the critical safety net here — it is configured via `hotkey.local.inflight-*` properties (default: `max-size=50000`, `ttl=5s`, `timeout=3s`).

The Worker broadcast solves the _second_ and subsequent waves — ensuring that when traffic continues (as real hot keys do), all nodes have the optimal hot-key configuration.

When even the broadcast cannot arrive in time, the first wave relies solely on SingleFlight and normal TTL for protection. Subsequent waves receive full hot-key protection once the broadcast arrives.

This extreme scenario is rare in practice, typically requiring a traffic surge of extraordinary magnitude.
