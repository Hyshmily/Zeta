# ADR-0071: Batch-End Semantics for PerKeyOrderedDispatcher (Disruptor borrow)

- Status: Accepted
- Date: 2026-09-19
- Related: ADR-0032 (global pending budget), ADR-0037 (log throttling / send-executor handoff), ADR-0066/0067 (sync plane versioning), LMAX Disruptor `BatchEventProcessor` / `SingleProducerSequencer` / `SleepingWaitStrategy` (prior art, upstream `disruptor-master` snapshot)

## Problem

Three concurrency idioms from LMAX Disruptor were evaluated for adoption into Zeta's sync and dispatch layers:

1. **BatchEventProcessor's `endOfBatch` + `onBatchStart`** — the consumer of a ring buffer receives `nextSequence == endOfBatchSequence` as a parameter, letting it amortize an expensive operation to once per batch instead of once per event.
2. **SingleProducerSequencer's cached gating sequence** — `next(n)` consults a cached copy of the (remote, monotonically advancing) consumer progress and only re-reads the true minimum when the wrap point is reached (`wrapPoint > cachedValue`).
3. **SleepingWaitStrategy's progressive backoff** — spin (100×) → `Thread.yield` (200×, configurable) → `parkNanos(100ns)` for waiting threads.

The Zeta-side claims to verify:

- `PerKeyOrderedDispatcher` batches up to `maxTasksPerCycle` tasks per key per pool slot, but the task contract is a bare `Runnable` — no "this is the last task of this key's batch" signal exists.
- `CacheSyncListener` submits one dispatch task per inbound sync message; `DefaultSyncDecisionHandler.handleRefresh` performs one Redis load per applied REFRESH message. A peer that writes the same key N times in quick succession broadcasts N version-increasing REFRESHes (each passes the sender-side dedup because the version strictly increases), so the receiver queues N same-key tasks and pays N sequential Redis loads for a final state only the last load defines.
- `PerKeyOrderedDispatcher.submitCore` evaluates `globalPendingUnits.sum()` (a full `LongAdder` traversal: `base` + all cells) on every submission, and the circuit breaker is state-checked per operation.
- Background threads (BroadcastBuffer flush, EvictStale scan, TimeSource ticker) were suspected of naive `Thread.sleep(fixed)` polling.

## Decision

**Adopted: the `endOfBatch` signal (borrowing #1).** The Disruptor consumer hint maps cleanly onto the dispatcher because the granted batch is already per-key — exactly the scope along which Zeta's sync-plane I/O repeats.

**1. `BatchAwareTask` contract** (`io.github.hyshmily.zeta.sync.dispatcher`):

```java
@FunctionalInterface
public interface BatchAwareTask { void run(boolean endOfBatch); }
```

Submitted via new `submitWithWeight(Object, BatchAwareTask, int[, long])` overloads. `runCycle` passes `endOfBatch == true` to the **last task of the granted batch**, `false` to all earlier tasks. Semantics copied deliberately from `BatchEventProcessor`:

- The flag is best-effort: a task granted alone always sees `true` (the common single-message case is byte-for-byte the old behaviour), and a submission racing the batch becomes the tail of a later cycle whose last task sees `true` again. Consumers must remain correct for any batch slicing; the signal only reduces I/O.
- `PendingTask` stores the task as `Object` and dispatches with one predictable `instanceof` — plain `Runnable` submissions keep the zero-allocation legacy path, no adapter object.
- `onBatchStart` was **not** adopted: its use case (pre-sizing per-batch accumulators) has no current Zeta consumer, and the flag-at-tail contract covers the amortization pattern. Re-add only with a concrete consumer.

**2. Sync-plane wiring.** `SyncDecisionHandler` gains `default void handleRefresh(SyncMessage, boolean endOfBatch)` delegating to the one-arg method, so existing custom implementations keep source compatibility and their exact behaviour; `CacheSyncListener` now submits a `BatchAwareTask` and threads the flag into the router. `DefaultSyncDecisionHandler` implements the amortization: a **non-final** REFRESH skips the Redis load, the compression and the L1 write, fires `onRefreshSkipped`, and returns; the batch-final REFRESH runs the full load-and-apply flow. The final applied state is identical to per-message processing in every same-key ordering (REFRESH bursts, INVALIDATE→REFRESH, REFRESH→INVALIDATE — the invalidation watermark and version guards are unchanged), because all tasks of a batch run back-to-back inside one dispatch cycle. The observable deltas are documented in the handler javadoc: intermediate versions never become visible in L1 and their per-message `afterRefresh` hooks do not fire.

**Net effect:** a same-key REFRESH burst of N messages costs 1 Redis load at the batch tail instead of N (best-effort; measured ≤ 2 for N = 3 under adversarial slicing in `handleSyncMessage_sameKeyRefreshBurst_shouldAmortizeLoadsToBatchTail`). The win is I/O count, not CPU — the dispatcher's batching already capped the pool-slot cost.

**Rejected: the cached gating sequence (borrowing #2).** Verified against the code and rejected for both claimed landing points:

- `globalPendingUnits.sum()` is indeed evaluated per submission, but the pattern does not transfer. Disruptor's cache is sound because the gated quantity is *monotonic* (consumer sequences only advance) and the producer's own contribution is exact local state (`nextValue`). Zeta's gate is a *non-monotonic, multi-producer* counter: discharges lower it, concurrent submitters raise it. A cache refreshed only on re-check is stale-low forever on a pure fast path (fast-path submissions never publish), so under sustained overload — precisely the broadcast-storm regime the budget exists for (ADR-0032) — the gate silently stops enforcing and pending memory grows without bound. A sound port needs per-thread exact local accumulators (Disruptor's `nextValue` generalization), i.e. a ThreadLocal lifecycle, new race surface and tests — to save at most a handful of volatile reads on a submission path whose next operation is a `ConcurrentHashMap.compute` bin lock that costs an order of magnitude more.
- The circuit-breaker check has nothing to amortize: the CLOSED fast path in `CircuitBreakerImpl.allowRequest` is already a single volatile read of `state`. The Disruptor trick pays off when the check aggregates many remote values (min over gating sequences / `LongAdder` cells); a one-field volatile read is already the floor.

**Rejected: the progressive backoff (borrowing #3).** A complete inventory of wait/poll sites found no naive `Thread.sleep(fixed)` polling loop to replace:

- `BroadcastBuffer` flush is event-driven (one-shot schedule on first record after a quiet period), not a polling thread; `EvictStaleTask` is a periodic scheduler scan (default 20 min), which parks between runs for free.
- `TimeSource`'s 5 ms ticker intentionally sleeps a fixed period — its entire job is to wake every 5 ms; spin/yield phases would burn CPU for nothing.
- `RedisLockProvider` already implements exponential backoff (`10·2^i` capped at 100 ms, `2·2^j` capped at 10 ms, 1 ms tight loop) — the natural habitat of the pattern.
- `WaveCounter` / `BufferedCounter` already implement the spin → yield → park ladder (`onSpinWait` → `Thread.yield` → `LockSupport.parkNanos(remaining >> 1)`).

Zeta's architecture routes all background work through a `ScheduledExecutorService`, so the "thread waiting for work" problem the `SleepingWaitStrategy` solves was designed away. Adding a configurable wait-strategy utility now would be speculative machinery without a consumer.

## Consequences

- New public-but-`@Internal` type `BatchAwareTask` and two `submitWithWeight` overloads; existing `Runnable` call sites (including `WorkerListener`) compile and behave unchanged.
- A custom `SyncDecisionHandler` that only implements `handleRefresh(SyncMessage)` keeps full per-message semantics (the default two-arg method delegates to it). Implementors who want the amortization override the two-arg form.
- The skipped-REFRESH hook contract: `onRefreshSkipped` fires for non-final messages; `afterRefresh` fires only for applied (batch-final) ones.
- Worker-side decision handling (`WorkerListener` → `DefaultWorkerDecisionHandler`) submits plain tasks and does not yet use the signal; its HOT-transition path is version-gated, so the same amortization is a follow-up opportunity, not a defect.
- Sender-side publication is unaffected: `BroadcastBuffer` already merges per key within the flush window, and collapsing N per-key REFRESH publishes into one AMQP message is a wire-protocol change (receivers WARN-drop unknown types, so rolling upgrades would lose refreshes) — noted as future work, out of scope here.

## Prior art

- **LMAX Disruptor `BatchEventProcessor.processEvents`** — the adopted design: `endOfBatchSequence = min(nextSequence + batchLimitOffset, availableSequence)`; `onEvent(event, sequence, sequence == endOfBatchSequence)`; `onBatchStart(batchSize, maxBatchSize)` per batch. Zeta's granted batch per key ↔ Disruptor's available-sequence window per consumer; the "last task of the granted batch" is the direct analogue of `sequence == endOfBatchSequence`.
- **LMAX Disruptor `SingleProducerSequencer.next`** — the rejected cache: `cachedValue` is safe there because gating sequences are monotonic and the producer's `nextValue` is exact single-thread local state; both preconditions fail for Zeta's pending-units gate.
- **LMAX Disruptor `SleepingWaitStrategy` / `PhasedBackoffWaitStrategy`** — the rejected ladder: parameters (`retries=200`, `sleepTimeNs=100`) are configurable for consumers that *block on a barrier*; Zeta's equivalent wait points are scheduler parks or already-optimized spin ladders.
