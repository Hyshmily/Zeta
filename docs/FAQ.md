# FAQ: Common Questions

> Common questions about HotKey's architecture and design choices.

---

First, a disclaimer: most issues can be fully resolved by tuning the parameters. The questions below only address extreme edge cases under default settings, and even those have reasonable protection built in by design.

## Q1: Why both local HeavyKeeper AND a central Worker? Isn't local detection enough?

**Short answer:** Local detection is for _self-preservation_ (nanosecond-level); the central Worker is for _cluster coordination_ (100ms–300ms level). They serve different time scales and purposes.

| Aspect            | Local HeavyKeeper                                    | Central Worker                                                      |
| ----------------- | ---------------------------------------------------- | ------------------------------------------------------------------- |
| **Response time** | Nanoseconds                                          | ~100ms–300ms (report interval + sliding window accumulation)        |
| **Scope**         | Single instance                                      | Cluster-wide                                                        |
| **Purpose**       | Immediate hot-key TTL promotion to protect this node | Global consensus, cross-instance pre-warming, coordinated cool-down |
| **When it acts**  | On every `get()` call                                | After accumulating enough window data                               |

**How they cooperate in code:**

1. **Burst hits instance A:** `HotKeyCache.get()` L1 miss → `loadAndCache()` calls `hotKeyDetector.add(key, 1).isLocalHotKey()` → if hot, caches with `hotHardTtlMs` (1h default). Instance A is protected instantly.

2. **Meanwhile,** `get()` also calls `hotKeyReporter.record(key)` → `HotKeyReporter` accumulates in a local Caffeine counter → every `reportIntervalMs` (100ms) flushes to RabbitMQ.

3. **Worker receives the report** → `ReportConsumer.onReport()` feeds `detector.addCount(key, count)` → `HotKeyStateMachine.evaluate()` tracks consecutive hot windows → after `confirmDurationMs` (300ms) of sustained heat, broadcasts HOT to **all instances**.

4. **Instances B, C, D** receive HOT via `WorkerListener` → pre-warm from Redis → cached with hot TTL before traffic reaches them.

**Without the central Worker:** instance A survives, but traffic load-balanced to B moments later finds an empty cache. That's "hot-spot drift".

---

## Q2: The central Worker takes hundreds of milliseconds — isn't that too late?

**Short answer:** No. By the time the Worker confirms a hot key, the local HeavyKeeper has already protected the node that received the initial burst. The Worker delay only affects _cross-instance coordination_, which operates on a seconds-to-minutes timescale for sustained hot keys.

**Timeline of a real hot key (e.g., flash sale item):**

```
t=0ms    Key X first accessed on Node A
t=+1μs   Local HeavyKeeper: "looks hot" → cached with 1h hot TTL on Node A
t=+100ms HotKeyReporter flush → Worker receives first batch, key above threshold → tick 1
t=+300ms  3 consecutive hot ticks (300ms total, confirmCount=3) → CONFIRMED_HOT
t=+300ms  Broadcast sent to all nodes (AMQP, same tick)
t=+360ms  Nodes B, C, D receive HOT → pre-warm from Redis → cached with hot TTL
(Setting confirm-duration-ms to 0 with report-interval-ms=1 and sliding-window=100/100 collapses the full-chain latency to ~9ms)
```

**A real hot key stays hot for minutes or hours**, not milliseconds. The ~300ms Worker latency (default `confirmDurationMs=300`, evaluated every 100ms = 3 ticks) is negligible compared to the total hot duration. Meanwhile, the node that got the first punch was already protected in microseconds.

**Measured latencies** (from `PropagationDelayIT`, 10 phases, 45k ops, 0 errors):

| Scenario                     | P50            | P95        | P99         |
| ---------------------------- | -------------- | ---------- | ----------- |
| Worker Decision Pipeline     | **56.38 ms**   | 99.21 ms   | 103.56 ms   |
| SM Confirm Pipeline (3 win)  | **246.46 ms**  | 295.00 ms† | 295.00 ms†  |
| Full Chain (SM 3 confirm)    | **298.19 ms**  | 351.50 ms† | 351.50 ms†  |

> † P95=P99=Max because these phases use only **10 keys** (10 samples). With N=10, P95=9.5th→10th=Max, P99=9.9th→10th=Max. Percentiles identical to max, not a measurement artifact.

- **Worker Decision Pipeline** (Phase 7): Worker detects hot key and initiates AMQP broadcast. P50=56.38ms includes `warmupJitterMs=100ms` + AMQP + L1 polling.
- **SM Confirm Pipeline** (Phase 8): ~246ms dominated by confirm windows (3 × 100ms = 300ms). After confirmation, same AMQP path (~56ms with jitter). Configurable.
- **Full Chain (SM 3 confirm)** (Phase 9): End-to-end — local Caffeine miss → report aggregation (100ms batch) → AMQP report delivery → SlidingWindowDetector → SM confirm (3 windows) → AMQP decision broadcast → L1 promotion. Total latency ~298ms P50.

> **Extreme parameter tuning** (PropagationDelayExtremeIT, same containers, report-interval-ms=1, warmup-jitter-ms=0, confirm-duration-ms=0, sliding-window=100/100) collapses these dramatically: Worker Decision Pipeline → **2.41ms P50**, SM Pipeline (0 confirm) → **7.71ms P50**, Full Chain (SM 0 confirm) → **9.23ms P50** (all with equal 10-key counts). The gap from default (3 windows, 298.19ms P50) to extreme (0 window, 9.23ms P50) is a **96.9% reduction**. See the [README extreme tuning section](../README.md#extreme-parameter-tuning--trading-reliability-for-latency) for trade-offs.

**What about a brief 100ms spike?** That's not a cluster-level hot key — it's a local blip. Local HeavyKeeper handles it with a longer TTL. Worker never needs to broadcast. They serve different purposes.

`SingleFlight.java` provides additional safety: concurrent L1 misses for the same key share a single `CompletableFuture`, preventing a thundering-herd against the backend even during the brief window before local HeavyKeeper activates.

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

- **Unified cool-down:** Only the Worker decides when a key is no longer hot. Without it, nodes independently decide to cool down at different times, creating inconsistent cache states.

- **Decision version ordering:** Each HOT/COOL broadcast carries a monotonically increasing `decisionVersion` (`WorkerBroadcaster.java`). Receivers use this to discard out-of-order messages, ensuring total ordering per key.

- **Slow-heat detection via TopK:** The `TopKValidator` (`TopKValidator.java`) periodically scans the global frequency ranking. Keys that gradually accumulate high total counts (but never spike) are pre-warmed — something local HeavyKeeper cannot do.

---

## Q4: RabbitMQ has limited throughput (~10K–100K msg/s). Won't reporting traffic overwhelm it?

**Short answer:** No. The reporting channel uses three design measures to control traffic:

### 1. Batching dramatically reduces message count

`HotKeyReporter` (`HotKeyReporter.java`) does not send one message per access. It accumulates counts in a local Caffeine counter and flushes batched reports every `reportIntervalMs` (default 100ms). A single `ReportMessage` carries hundreds or thousands of key-count pairs.

**Formula:** Messages per second = `1000 / reportIntervalMs` = 10 msg/s _per shard_, regardless of access volume.

### 2. Consistent-hash sharding distributes load

Each key is routed to exactly one shard via `Math.abs(key.hashCode()) % shardCount` (`HotKeyReporter.java`). With `shardCount = N`, the reporting load is split across N queues/consumers. Each Worker consumer only sees its assigned keys.

### 3. The control channel is separate from the data channel

| Channel                       | Exchange                    | Traffic pattern                     | Volume                               |
| ----------------------------- | --------------------------- | ----------------------------------- | ------------------------------------ |
| **Report** (app → Worker)     | `hotkey.report.exchange`    | Batched counts, every 100ms per app | Stable, proportional to app count    |
| **Broadcast** (Worker → apps) | `hotkey.broadcast.exchange` | HOT/COOL decisions                  | Very low — only on state transitions |

Broadcasts are **control signals**, not data streams. The `HotKeyStateMachine` (`HotKeyStateMachine.java`) with its `confirmCount`/`coolCount` hysteresis ensures no flapping or broadcast storms.

Finally, the state machine design fundamentally eliminates redundant broadcasts that would cause CPU overload on clients and self-inflicted congestion. In extreme tests, the SM-0-confirm path produced only 10 broadcasts, while the without-SM path produced 86 broadcasts for the same 10 keys.

**If your cluster is large enough to worry about MQ throughput:** increase `shardCount`, which reduces per-queue message volume proportionally. Each additional shard adds a consumer thread on the Worker.

---

## Q5: The TopKValidator pre-warms keys based on historical ranking — does this actually get used?

**Short answer:** It serves as a safety net for cases the sliding window cannot catch. It is used infrequently — designed as a supplementary detection mechanism rather than a primary path.

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

**Short answer:** This is theoretically possible (even under the extreme configuration that collapses full-chain latency to ~9.23ms). The framework is designed to minimize the risk but cannot guarantee absolute protection against every extreme edge case.

**What happens step by step under default settings:**

1. **t=0:** Key X, never seen before, suddenly receives concurrent requests on all 10 nodes simultaneously.

2. **t=+1μs:** Every node's `SingleFlight` (`SingleFlight.java`) activates. Stress tests confirm `singleFlight_extremeDedup` achieves **99.0% dedup** (100 threads → 1 execution) and `singleFlight_cacheStampede` achieves **91.5% dedup** (50 keys × 20 threads → 85 executions). The backend receives at most ~N concurrent requests (one per node), not `N × concurrent_requests`.

3. **t+~5ms to t+~20ms:** The first request on each node completes (L2 latency dependent). Data returned to all waiting callers. Value cached in L1 with **normal TTL** (5min default per `HotKeyProperties.java:87`) — local HeavyKeeper not yet triggered.

4. **t+100ms:** `HotKeyReporter` flushes the first batch of counts to the Worker (`reportIntervalMs=100` per `HotKeyProperties.java:148`). `reporter_highFrequency` stress test confirms **3M ops/s throughput with zero data loss**.

5. **t+~298ms:** Full chain completes — after 3 consecutive hot evaluations the state machine transitions to CONFIRMED_HOT and broadcasts; all nodes receive HOT (propagation benchmark P50=56.38ms, P95=99.21ms, P99=103.56ms) → upgrade key to hot TTL (1h per `HotKeyProperties.java:87`) and enable soft expiration. Measured full chain (SM 3 confirm) P50 = 298.19ms. The state machine guarantees each key broadcasts **only once per lifecycle** — extreme tests show SM-0-confirm path produces 10 broadcasts vs 86 for the without-SM path, demonstrating the SM's broadcast suppression capability. This is one of the framework's proudest design achievements.

The Worker broadcast solves the _second_ and subsequent waves — ensuring that when traffic continues (as real hot keys do), all nodes have the optimal hot-key configuration. The default 300ms confirm window is a deliberate stability choice; during those 300ms, the local HeavyKeeper, SingleFlight, and normal TTL form a three-layer protection barrier with zero request blocking.

For latency-critical scenarios, specific parameters can be tuned to collapse the full-chain delay to ~9.23ms — at the cost of confirm-window protection and the broadcast compression advantage of the state machine. This extreme case is rarely encountered in practice and typically requires a traffic surge of extraordinary magnitude.
