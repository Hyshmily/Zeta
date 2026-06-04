# FAQ: Common Questions

> Before you start — the author answers common questions about HotKey's architecture and design choices.

---

## Q1: Why both local HeavyKeeper AND a central Worker? Isn't local detection enough?

**Short answer:** Local detection is for _self-preservation_ (nanosecond-level); the central Worker is for _cluster coordination_ (100ms-level). They serve different time scales and purposes.

| Aspect            | Local HeavyKeeper                                    | Central Worker                                                      |
| ----------------- | ---------------------------------------------------- | ------------------------------------------------------------------- |
| **Response time** | Nanoseconds                                          | ~100ms–2s (report interval + sliding window accumulation)           |
| **Scope**         | Single instance                                      | Cluster-wide                                                        |
| **Purpose**       | Immediate hot-key TTL promotion to protect this node | Global consensus, cross-instance pre-warming, coordinated cool-down |
| **When it acts**  | On every `get()` call                                | After accumulating enough window data                               |

**How they cooperate in code:**

1. **Burst hits instance A:** `HotKeyCache.get()` hits L1 miss → `loadAndCache()` calls `hotKeyDetector.add(key, 1).isLocalHotKey()` → if true, caches with `hotHardTtlMs` (1h default). Instance A is protected instantly.

2. **Meanwhile,** the same `get()` calls `hotKeyReporter.record(key)` → `HotKeyReporter` accumulates counts in a Caffeine counter → every `reportIntervalMs` (default 100ms) it flushes to RabbitMQ.

3. **Worker receives the report** → `ReportConsumer.onReport()` feeds `detector.addCount(key, count)` → `HotKeyStateMachine.evaluate()` tracks consecutive hot windows → after `confirmDurationMs` (default 2s) of sustained heat, broadcasts HOT to **all instances**.

4. **Instances B, C, D** receive the HOT broadcast via `WorkerListener` → pre-warm their local Caffeine with the key before traffic ever reaches them.

**Without the central Worker:** instance A survives, but traffic that load-balances to B moments later finds an empty cache. The Worker solves this "hot-spot drift" problem.

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

**What about a brief 100ms spike?** That's not a cluster-level hot key — it's a local blip. The local HeavyKeeper handles it with a slightly longer TTL, and the Worker never needs to broadcast. The author split the two precisely for this reason.

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

**Short answer:** No. The author did three things in the reporting channel to keep things under control:

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

**Short answer:** It is a safety net for the 1% case the sliding window cannot catch. It is used rarely — the author kept it as a fallback, not a primary path.

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

**Short answer:** Can this happen? Honestly — yes. The author can only do their best in the design to keep the cluster safe, but cannot fully guarantee the HotKey framework is bulletproof.

**What happens step by step:**

1. **t=0:** Key X, never seen before, suddenly receives concurrent requests on all 10 nodes simultaneously.

2. **t=+1μs:** Every node's `SingleFlight` (`SingleFlight.java`) activates. In stress tests, `singleFlight_extremeDedup` achieves **99.0% dedup** (100 threads → 1 execution) and `singleFlight_cacheStampede` achieves **93.7% dedup** (50 keys × 20 threads → 63 executions, data: `hotkey-stress-*.json`). The backend receives ~N concurrent requests (one per node), not `N × concurrent_requests`.

3. **t+~5ms to t+~20ms:** The first request on each node completes (L2 latency dependent). Data returned to all waiting callers. Value cached in L1 with **normal TTL** (5min default per `HotKeyProperties.java:87`) — local HeavyKeeper not yet triggered.

4. **t+100ms:** `HotKeyReporter` flushes the first batch of counts to the Worker (`reportIntervalMs=100` per `HotKeyProperties.java:148`). Stress test `reporter_highFrequency` confirms **3M ops/s throughput with zero data loss**.

5. **t+~2.1s:** After 20 consecutive hot evaluations (confirmCount=20 × 100ms per tick = 2000ms, per `WorkerProperties.java:74,124`), state machine transitions to CONFIRMED_HOT and broadcasts.

6. **t+~2.2s:** All nodes receive HOT (Worker decision benchmark shows propagation P50=62.6ms, data: `benchmark-worker-decision-*.json`) → upgrade key to hot TTL (1h per `HotKeyProperties.java:91`) and enable soft expiration.

**Impact analysis:**

| Component          | Status                                                            |
| ------------------ | ----------------------------------------------------------------- |
| Backend (Redis/DB) | Safe — SingleFlight limits reads to ≤ N (proven 93.7-99.0% dedup) |
| L1 Caffeine        | Each node has the value cached with normal TTL (5min)             |
| p99 latency        | First-wave requests see single L2 latency. `timeoutContention` test: 0 timeouts under 50-thread load |
| Hot-key protection | Delayed by ~2s, but normal TTL provides baseline protection       |

**Without SingleFlight**, this scenario would send `N × concurrent_requests` to the backend, which could cause a cascading failure. SingleFlight is the critical safety net here — it is configured via `hotkey.local.inflight-*` properties (default: `max-size=50000`, `ttl=5s`, `timeout=3s`).

The Worker broadcast solves the _second_ and subsequent waves — ensuring that when traffic continues (as real hot keys do), all nodes have the optimal hot-key configuration.

To be honest, when even the broadcast cannot make it in time, the author designed the framework to protect subsequent waves as much as possible, but cannot guarantee absolute safety for the very first wave.

As mentioned before, the author's ability is limited. A scenario this extreme is rare in practice — probably only a 12306-level traffic surge could trigger it. And even then, there is not much the author can do about it. The author cannot foresee every extreme case. The design tries its best, but cannot promise absolute safety.
