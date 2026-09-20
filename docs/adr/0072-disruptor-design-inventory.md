# 0072 — Disruptor Design Inventory: Remaining Borrow Candidates After ADR-0071

- Status: Accepted for D-1 (implemented, 2026-09-19); D-2 and D-3 proposed, awaiting a decision
- Date: 2026-09-19
- Related: ADR-0071 (batch-end hint — the one idiom already adopted), ADR-0003 / ADR-0007 / ADR-0013 / ADR-0024 / ADR-0032 / ADR-0037 / ADR-0063 / ADR-0065, upstream LMAX Disruptor `disruptor-master` (4.0.0-SNAPSHOT, Java 11 target)

## Context

ADR-0071 evaluated three Disruptor idioms (`endOfBatch`, the cached gating sequence, progressive
backoff), adopted the first and rejected the other two. It deliberately did not survey the rest of
the library. This ADR completes the sweep: every main source file of the upstream snapshot, plus
the user guide and the developer guide, is given an explicit verdict so that the next reader does
not have to re-open the library to discover that an element was already considered.

Snapshot provenance. The reviewed tree (`disruptor-master`, 71 main sources / 7,173 lines) is
byte-identical to the copy ADR-0071 used — a file-by-file MD5 comparison of all 292 tracked files
reports zero content differences (the two copies differ only in the enclosing directory names).
Upstream declares `version = 4.0.0-SNAPSHOT`, `sourceCompatibility = JavaVersion.VERSION_11`.

The central question for each element is not "is it good?" but "does Zeta have a site where the
element's *preconditions* hold?" Disruptor's designs are all conditioned on three properties of
its domain that Zeta only partially shares:

1. **One process, one address space.** The ring buffer's cursor is a plain field, so claim,
   publish and consume are ordered by memory fences alone. Zeta's "ring" spans AMQP, Redis and
   several JVMs — there is no shared cursor to advance, only version stamps that must be merged.
2. **Blocking is allowed.** Disruptor's producers park on the wrap point and its consumers park
   on the barrier. Zeta's producers must never block an application thread, so every capacity
   limit is a *drop* (ADR-0032), not a park.
3. **Data lives in the ring.** An event that leaves the ring before being processed is data loss,
   which is why `shutdown(timeout)` drains before halting. Zeta's sync-plane messages are
   *signals* whose payload is authoritative in Redis (ADR-0031), so loss is bounded by the next
   write or the next read (ADR-0007, ADR-0013) and drain-on-close buys latency, not correctness.

Every rejection below traces back to one of these three.

## Verdict summary

| #      | Disruptor element                                      | Zeta landing point                               | Verdict                    |
|--------|--------------------------------------------------------|--------------------------------------------------|----------------------------|
| 1      | `Sequencer.remainingCapacity()` / `isAvailable`        | `PerKeyOrderedDispatcher` (gate was all private) | **Adopted (D-1)**          |
| 2      | `ConsumerRepository.hasBacklog()` (end of chain)       | backlog predicate was absent                     | **Adopted (D-1)**          |
| 3      | `onBatchStart(batchSize, queueDepth)`                  | grant-time depth is computed then dropped        | **Decision (D-2)**         |
| 4      | `Disruptor.shutdown(timeout)` drain-then-halt          | `BroadcastBuffer` has no destroy hook            | **Decision (D-3)**         |
| 5      | `BatchEventProcessor` `endOfBatch`                     | adopted in ADR-0071                              | Adopted                    |
| 6      | `maxBatchSize` consumer window                         | `maxTasksPerCycle` = 64                          | Equivalent                 |
| 7      | Producer park on wrap point / `tryNext` throw          | drop at the gate (ADR-0032)                      | Rejected (by design)       |
| 8      | `MultiProducerSequencer.availableBuffer` flags         | per-key `dataVersion` on the L1 entry            | Rejected                   |
| 9      | `SingleProducerSequencer.cachedValue`                  | —                                                | Rejected (ADR-0071)        |
| 10     | Wait strategies (spin/yield/park, phased)              | `WaveCounter` / `RedisLockProvider` ladders      | Rejected (ADR-0071)        |
| 11     | `LiteBlockingWaitStrategy.signalNeeded`                | `BroadcastBuffer.rescheduleFlush` guard          | Equivalent                 |
| 12     | `SequenceBarrier.alert()` cooperative halt             | `closed` flag + per-task check                   | Equivalent                 |
| 13     | `SequenceGroup.addWhileRunning` seeding                | per-key L1 watermark, not a cursor               | Rejected                   |
| 14     | `Sequence` padding / `@Contended` discipline           | `LongAdder` cells, per-thread `Ceils`            | Rejected                   |
| 15     | `BatchRewindStrategy` replay family                    | at-most-once independent tasks                   | Rejected                   |
| 16     | `ExceptionHandlerSetting.with` + `alert()`             | ADR-0065 snapshot stamp, ADR-0003 gossip         | Equivalent                 |
| 17     | `FatalExceptionHandler` (rethrow, kill thread)         | never-throw chain (ADR-0029)                     | Rejected (by design)       |
| 18     | Handler `Throwable` → skip and advance                 | `ZetaExceptionHandler` + keep batch              | Equivalent                 |
| 19     | Stackless `AlertException.INSTANCE` singletons         | `ZetaBlockedException` (ADR-0063)                | Equivalent                 |
| 20     | `EventPoller` + `PollState`                            | dispatcher continuation cycle                    | Rejected (no consumer)     |
| 21     | DSL graph (`then` / `after` / `EventHandlerGroup`)     | linear 2-stage pipelines                         | Rejected (no consumer)     |
| 22     | `AggregateEventHandler` / `NoOpEventProcessor`         | no consumer                                      | Rejected (no consumer)     |
| 23     | `ProducerType` + `sameThread` assertion                | per-key exclusivity, pinned by tests             | Equivalent                 |
| 24     | `translateAndPublish` finally-publish                  | budget discharge + documented drop paths         | Equivalent                 |
| 25     | `publishEvents(batch, batchStartsAt, size)`            | `CacheSyncPublisher.BATCH_SIZE = 1000`           | Equivalent                 |

Verdicts: **Adopted** = implemented here, low risk, no semantic change. **Decision** = the landing
point is real but the change is a semantic trade-off that needs its own sign-off. **Equivalent** =
Zeta already has the mechanism (sometimes independently invented). **Rejected** = the preconditions
do not hold at any Zeta site; do not re-propose without new evidence.

## D-1 (Adopt) — Make the producer-facing gate a readable property

### Problem

Disruptor treats "how full is the structure / is the consumer behind" as a first-class readable
property of the data structure: `Sequencer.remainingCapacity()`, `getBufferSize()`,
`isAvailable(sequence)`, `getCursor()`, and — for the aggregate question —
`ConsumerRepository.hasBacklog(cursor, includeStopped)`, which asks only the consumers that are
*end of chain* whether the cursor has passed them (`ConsumerRepository.java:69-85`,
`EventProcessorInfo.isEndOfChain()`). A Disruptor operator can therefore distinguish "idle" from
"overloaded" from "one stage lagging" with no extra bookkeeping.

`PerKeyOrderedDispatcher` decides the opposite: `globalPendingUnits`, `dropCounter`,
`rejectedPerKeyCounter` and the key map are all `private` with no accessors
(`PerKeyOrderedDispatcher.java:130-162`), and no call site reads them
(`CacheSyncListener.java:125`, `WorkerListener.java:117` construct and `close()` only). The
consequence is that the two overload gates of the sync and decision planes are **invisible**:
the only observable signal is the throttled WARN emitted *after* the budget is exhausted
(`PerKeyOrderedDispatcher.java:374-382`, `:425-427`). There is no way to see the approach to the
cliff, and ADR-0032's deliberately asymmetric budgets (`zeta.sync.max-pending-units` = 50,000 vs
`zeta.worker-listener.max-pending-units` = 200,000) cannot be validated against production
traffic.

The reporter plane already does this correctly and is the in-repo precedent: `KeyReporter` exposes
`dispatcherDepth()` / `dispatcherCapacity()` / `dispatcherDropped()` / `dispatcherExpired()`
(`ZetaEndpoint.java:161-166`, gauges at `ZetaMicrometerAutoConfiguration.java:206-213`). The
inconsistency is that only one of the three per-key dispatchers is observable.

### Decision

`PerKeyOrderedDispatcher` gets an `@Internal` read-only surface, wired to both sinks, with names
and shapes copied from the Disruptor contract:

```java
// io.github.hyshmily.zeta.sync.dispatcher.DispatcherStats (immutable snapshot record)
pendingUnits()      // == globalPendingUnits.sum()             (Disruptor: cursor - min gating)
remainingUnits()    // max(0, maxPendingUnits - pendingUnits)  (Sequencer.remainingCapacity())
maxPendingUnits()   // configured budget                       (Sequencer.getBufferSize())
activeKeys()        // queues.size(): key workers holding work (estimate)
dropped()           // global gate drops (dropCounter)
rejected()          // per-key queue-full rejections
backlogged()        // pendingUnits() > 0                      (ConsumerRepository.hasBacklog)
```

`activeKeys()` over `ConcurrentHashMap.size()` is an estimate, which is the correct contract for a
gate metric (Disruptor's `estimatedSize`-class values are no different); `pendingUnits()` is exact
at the instant of the `sum()`, and `remainingUnits()` clamps at zero because a concurrent submitter
can push the gate past its budget after the snapshot is read.

Wiring (implemented):

- `PerKeyOrderedDispatcher.stats()` builds the snapshot; the surface is one method, so the
  dispatcher's submission path is untouched.
- `CacheSyncListener.dispatcherStats()` / `WorkerListener.dispatcherStats()` expose it per plane
  (`null` before their `@PostConstruct` initializer has run), keeping the dispatcher itself
  encapsulated.
- `ZetaEndpoint`: the existing `sync` section gains `dispatchPendingUnits` /
  `dispatchRemainingUnits` / `dispatchMaxPendingUnits` / `dispatchActiveKeys` /
  `dispatchBacklogged` / `dispatchDropped` / `dispatchRejected` via the shared
  `putDispatchStats` helper, and the `worker` section gains the same block from the decision
  plane's listener. Both listeners are injected as optional `ObjectProvider`s, matching the
  endpoint's existing null-safe construction, so a deployment mode without a plane simply omits
  the keys.
- `ZetaMicrometerAutoConfiguration`: gauges `zeta.dispatch.pending.units`,
  `zeta.dispatch.remaining.units`, `zeta.dispatch.active.keys`, `zeta.dispatch.backlogged`,
  `zeta.dispatch.dropped.total`, `zeta.dispatch.rejected.total`, each tagged `plane=sync` or
  `plane=worker`, following the existing `Gauge.builder("zeta.<area>.<name>", bean, fn)`
  convention. A plane whose dispatcher does not exist registers no gauges at all rather than a
  zeroed gate that would read as "saturated".
- Documentation: `docs/MONITOR.md` / `docs/MONITOR.zh.md` gain the endpoint fields in the sample
  response and the metric rows plus the "no traffic vs gate saturated" reading note.

Implementation note: the four touched test classes run green — 61 tests total
(`PerKeyOrderedDispatcherTest` 32 incl. 5 new gate tests, `ZetaEndpointTest` 15 incl. 2 new,
`ZetaMicrometerAutoConfigurationTest` 8 incl. 2 new, `ZetaActuatorAutoConfigurationTest` 6). The
full `common` suite is 2,189 tests with 13 failures, all in `StateMachineEndpointTest` and all
pre-existing (the working tree's in-flight `StateMachineEndpoint` parameter-validation change
against unsynchronised test expectations — the `coolCount > preCoolGraceCount` contract, unrelated
to this ADR).

### Consequences

- The ADR-0032 gate becomes diagnosable: `remaining.units` approaching zero explains a
  WARN-free burst of dropped sync messages, which today is indistinguishable from "no traffic".
- `hasBacklog`-shaped predicate exists for any future drain (see D-3) and for a health indicator;
  the sync plane currently has no lag signal at all, while the decision plane has
  `msSinceLastAnyHeartbeat`.
- Cost: a handful of field reads per scrape on a `LongAdder` / `CHM` — no hot-path change, since
  nothing is added to `submitCore` or `runCycle`.
- The counters are cumulative and typed as a rising value in a `Gauge`; this matches the existing
  `zeta.reporter.queue.dropped.total` convention, so the project's "gauges only" style is kept
  rather than introducing a `Counter` for one metric.

## D-2 (Decision) — Grant-time queue depth for load shedding

`BatchEventProcessor` hands its consumer both the batch size *and* the queue depth
(`onBatchStart(batchSize, queueDepth)`, `EventHandlerBase.java:43`; `endOfBatchSequence =
min(nextSequence + batchLimitOffset, availableSequence)`, `BatchEventProcessor.java:157`, the
hook invoked at `:161` with `availableSequence - nextSequence + 1` as the depth).
ADR-0071 rejected the flag because no consumer needed it — but the second argument is a different
offer: it is the signal that lets a consumer *shed optional work while the pipeline is behind*,
which is exactly the situation Zeta's gates currently handle by dropping the newest submission at
the door.

Today, under a broadcast storm, a REFRESH dropped by the per-key cap or the global budget leaves
that key's L1 stale until its TTL, even though the signal that was dropped is by definition the
most recent one. `PerKeyOrderedDispatcher.grantBatch` already knows the depth it is granting from
(`maxTasksPerCycle`, the worker's remaining queue) and throws it away.

The candidate: pass the grant-time depth (the worker's backlog, or the global remaining budget)
into the batch tail alongside `endOfBatch`, and let `DefaultSyncDecisionHandler` degrade a
*non-final* and, under pressure, even the final REFRESH from "load from Redis and write L1" to
"invalidate locally" — the ADR-0031 fallback path, which already exists for the "no Redis value"
case and is proven local-only and loop-free.

Why this is a Decision and not an Adopt: it changes the ADR-0071 guarantee that the batch-final
REFRESH always applies, trading eager freshness for a cheaper tail under overload; and it moves
load from the sync plane to the read path (an invalidated key pays a single-flight Redis load on
its next read). Which side wins depends on the write-burst key distribution, which is measurable
with the JMH harness (ADR-0070) but is not obvious a priori. Recommendation: do not implement
until D-1's gauges show production storms reaching the gate; if implemented, put it behind an
explicit property (default off) with the shed threshold as a fraction of `max-pending-units`.

## D-3 (Decision) — Drain-then-halt on graceful shutdown

`Disruptor.shutdown(timeout, unit)` busy-waits on `hasBacklog()` and only then calls `halt()`
(`dsl/Disruptor.java`), because a halt with entries still in the ring loses them. The same
asymmetry exists inside Zeta today:

- `WorkerBroadcastBuffer.shutdown()` (`worker/…/dispatch/WorkerBroadcastBuffer.java:166-169`)
  calls `sendExecutor.shutdown()`, which **drains** queued decisions before returning.
- `BroadcastBuffer` (the sync plane, `common/…/cachesupport/BroadcastBuffer.java`) has **no**
  destroy hook at all, and its bean has no `destroyMethod`
  (`ZetaAutoConfiguration.java:290-304`). On context close the pending map — up to
  `flushDelayMs` (500 ms) and, in a slow-writer case, `maxDeferMs` (2,000 ms) of merged
  `(key → version)` entries — is simply abandoned, and the scheduled flush task
  (`rescheduleFlush`, `:369-403`) is discarded with the shared `hotKeyScheduler`
  (`shutdownNow`, `ZetaFacadeAutoConfiguration.java:79`).

The honest bounds of the defect: a lost REFRESH does not corrupt anything. Peers keep serving the
stale value until the next write for that key or the L1 TTL/soft-expire (ADR-0007, ADR-0013), and
`CacheSyncPublisher`'s dedup watermark is advanced only on a successful send
(`CacheSyncPublisher.java` `sendDeduped`), so a lost broadcast does not poison later sends. The
gain is therefore a *shorter peer-staleness window on a rolling restart*, not correctness — and
ADR-0007 explicitly froze this plane with "do not enable publisher confirms; accept ephemeral
message loss".

Recommendation, split in two:

1. **Adopt the cleanup half unconditionally.** Mark the buffer closed on destroy (stop accepting
   records, cancel the scheduled flush). This removes a task that, in the current code, is
   scheduled against a scheduler that is being torn down, and makes the "records after close"
   behaviour explicit instead of incidental.
2. **Take the flush half only under an ordering contract.** A best-effort synchronous
   `flush()` with a short deadline (100–200 ms) is only correct if it runs *before* the AMQP
   resources are torn down; Spring's default destroy order does not guarantee that, and the
   flush would otherwise fail into the existing rate-limited WARN. Implementing it therefore
   requires an explicit phase (`SmartLifecycle` with `getPhase()` earlier than the AMQP
   container's, or `@DependsOn` on the connection factory) plus a bounded deadline, and it
   contradicts the spirit of ADR-0007 unless recorded as a deliberate exception to it.

Recommendation: implement (1) now; (2) only if operations report that rolling restarts visibly
extend peer staleness — and if (2) is implemented, the deadline must be a bound, not a drain
loop, since nothing on the sync plane may block a shutdown thread indefinitely.

## Rejected, with the precondition that fails

**#8 `MultiProducerSequencer.availableBuffer` / `getHighestPublishedSequence`.** The design lets
producers publish out of order and makes consumers read only the *contiguous* published prefix,
via a per-slot availability flag `(int)(sequence >>> indexShift)` stored at `sequence & indexMask`
(`MultiProducerSequencer.java:230-266`). The transfer condition would be "a consumer must not
apply version N+1 before N". Zeta fails it: sync messages carry no payload at all
(`SyncMessage` = `id, cacheKey, type, version, isVersionDegraded, rulesVersion`), REFRESH is a
"go reload from Redis" signal resolved at execution time, and the guard is per key against the
resident L1 entry's `dataVersion` (`VersionGuard.shouldSkipForRefresh`, `:214-225`). Skipping an
intermediate version is therefore not a lost update — the next signal or the next read reaches
the same final state. A publication-flag array would add a per-slot structure to defend an
invariant nobody depends on. Also note the flag trick's second half (avoid a shared sequence
object between publishers) is already handled on the sync plane by
`CacheSyncPublisher.sendDeduped`'s atomic claim via `ConcurrentHashMap.compute`
(`current >= version → skip`), which is version-ordered and allocation-free.

**#13 `SequenceGroup.addWhileRunning` seeding.** Adding a consumer to a *running* ring seeds its
sequence to the current cursor so it cannot "rewind" and re-read history. The sync plane has no
such rewind to prevent: the watermark is not a global cursor but the per-key L1 entry's version,
and `VersionGuard` treats an absent entry as "accept" (`:179-180`, `:215-216`) — which is correct
here, because the value source is authoritative Redis, not a history the newcomer must not
replay. Nor is there a stale-version storm on join: `RingManagerImpl.reconcileFromHealthView`
rebuilds the ring and touches no version state (`:77-93`), and `HealthView` has no warm-up gate
but does not need one for versions. The join-time risk is a *cold-start load volume* problem,
which is met by the existing warm-up/jitter path, not by seeding watermarks.

**#11 `LiteBlockingWaitStrategy`'s `signalNeeded`** is the "elide the notify when nobody is
waiting" pattern (an `AtomicBoolean` flipped by the waiter, read by the signaler). Zeta already
contains the same idea twice, independently: `BroadcastBuffer.rescheduleFlush`'s lock-free guard
returns early while a flush is still pending inside the deferral window (`:378-382`), and the
circuit breaker's CLOSED fast path is a single volatile read (ADR-0071). No further site needs
it.

**#10 wait-strategy family.** Already rejected in ADR-0071 and re-confirmed by the second sweep:
the additional strategies (`TimeoutBlocking` with its `onTimeout` idle hook,
`LiteTimeoutBlocking`, `PhasedBackoff`'s spin→yield→fallback ladder, `BlockingWaitStrategy`'s
`checkAlert()` inside the spin loop) all assume a thread that *blocks on a barrier*. Zeta's only
waiters are scheduler parks and the two spin ladders that already exist
(`WaveCounter.java:2329-2344`, `:2356-2365`, `BufferedCounter.java:531-533`, `:350-353`,
`:587-595`, `RedisLockProvider.java:254/275/452`). One genuine micro-observation worth keeping:
Disruptor's ladder is *step-count* based, so the spin phase performs no clock read at all,
whereas Zeta's ladders read `TimeSource` per iteration. That is not worth a change on its own —
it is noted so that a future hot-spin loop does not add a clock read "just to be safe".

**#14 `Sequence`'s padded field layout.** `Sequence extends RhsPadding` wraps one `long` in 128
bytes of padding to stop the cursor sharing a cache line with its neighbours
(`Sequence.java:7-42`), with `set` (release fence only) and `setVolatile` (+ `VarHandle.fullFence()`)
distinguished for the claim path. Zeta has no equivalent site: cross-thread counters are
`LongAdder` (self-padded cells) or thread-local (`WaveCounter.Ceils` is one array per thread, so
its adjacent slots are never written by two threads), and the JOL measurements recorded for
ADR-0069 confirm `CacheEntry` at 80 B with its fields already packed. Adding padding would cost
memory to defend against contention that the structure's ownership model already excludes.

**#15 the rewind family** (`RewindableEventHandler`, `BatchRewindStrategy`,
`SimpleBatchRewindStrategy`, `EventuallyGiveUpBatchRewindStrategy`,
`NanosecondPauseBatchRewindStrategy`; the batch restarts from `startOfBatchSequence`, replaying
already-processed events) exists for *transactional* batches: the documented use case is
"batch start → BEGIN; events → statements; batch end → COMMIT", where a mid-batch failure must
roll back and replay atomically (user guide §Batch Rewind). Zeta's batch is not a transaction:
tasks are independent, individually idempotent (version-`max` merges), at-most-once after the
ack (ADR-0004), and the batch tail is already the amortization point (ADR-0071). Replaying a
failed batch would re-execute tasks whose only effect on the tail is a merge that is already
idempotent, and a failed Redis load at the tail is handled by the ADR-0031 fallback plus the next
write — not by retrying the same load immediately.

**#16 config-change wakeups** (`ExceptionHandlerSetting.with` calls
`BatchEventProcessor.setExceptionHandler` and then `barrier.alert()` so an idle consumer picks the
change up at once). Zeta's config consumers are stateless per evaluation, so no wakeup is needed:
the rule memo is stamped with the exact snapshot array reference, so a rule change is honored on
the very next evaluation with no invalidation call and no storm (ADR-0065), and state-machine
parameters are version-gated on the heartbeat (ADR-0003, `configTimestamp` strictly newer). Worth
recording because "config change must wake the waiter" is an easy pattern to reach for and Zeta
does not need it.

**#17 `FatalExceptionHandler`** logs at ERROR and rethrows wrapped, which kills the consumer
thread. Deliberately opposite to `ZetaExceptionHandler`'s never-throw chain terminating in a WARN
(ADR-0029, ADR-0063). Nothing to borrow in either direction; noted so it is not "fixed" later.

**#20, #21, #22 `EventPoller` / the DSL graph / `AggregateEventHandler` / `NoOpEventProcessor`.**
No landing point: the pull-based poller exists to hand flow control to a thread the library does
not own, whereas Zeta's consumers are pool tasks driven by the dispatcher's continuation cycle;
the DSL composes a multi-stage topology, whereas both Zeta pipelines are two-stage
(listener → dispatcher → decision handler); `AggregateEventHandler` fan-ins several handlers onto
one thread, whereas Zeta's periodic tasks are independent and cheap; `NoOpEventProcessor` is a
test utility.

**#7 producer blocking / `tryNext` → `InsufficientCapacityException`.** Two producer flavors
(park until space vs fail fast) versus Zeta's single behavior (drop at the gate, ADR-0032). Zeta
cannot block an application thread, and the sync plane's loss envelope is already accepted
(ADR-0007/0013), so there is nothing to choose between. The observable part of this difference —
knowing which flavor happened — is D-1.

## Already equivalent (do not re-propose)

- **#5, #6, #12, #18, #19, #23, #24, #25.** The batch-end hint (#5, ADR-0071); the per-key batch
  cap (#6, `DEFAULT_MAX_TASKS_PER_CYCLE = 64` vs `maxBatchSize`); cooperative cancellation
  (#12, `closed` checked per task and per batch at `PerKeyOrderedDispatcher.java:503/516`);
  skip-and-advance on a handler `Throwable` (#18, `BatchEventProcessor.java:191-196` keeps the
  consumer alive after routing the error, which is exactly `runCycle`'s
  `catch (Throwable) → ZetaExceptionHandler` at `:521-527`); no-stacktrace control-flow
  exceptions (#19, `AlertException.INSTANCE` etc. vs `ZetaBlockedException`'s
  `writableStackTrace = false`, ADR-0063 — an independent arrival at the same trick);
  declared-single-threaded-producer with a debug assertion (#23, `ProducerType` + `sameThread()`;
  Zeta's invariant is per-key exclusivity, pinned by `PerKeyOrderedDispatcherTest`
  `submit_sameKey_shouldExecuteInFifoOrder` / `submit_differentKeys_shouldExecuteInParallel` /
  `submit_sameKeyBurst_shouldBatch`); publish-must-happen-even-if-translation-throws (#24,
  `translateAndPublish`'s `finally`; Zeta discharges budget per executed task and documents every
  drop path instead); batch publishing with an explicit sub-window (#25, `CacheSyncPublisher`
  chunks at `BATCH_SIZE = 1000`).
- **`EventHandlerBase.onStart` / `onShutdown`** ↔ `init()` / `@PreDestroy` on the listeners and
  the counters' `destroy()`.
- **`ExceptionHandlers.defaultHandler()`'s lazy holder** ↔ `ZetaExceptionHandler`'s
  thread-local → inheritable → default resolution (ADR-0063).
- **`Util.ceilingNextPowerOfTwo` / `log2` / index masking** ↔ `WaveCounter`'s power-of-2 slice
  masks; `Util.awaitNanos` has no analogue because Zeta has no `synchronized`-mutex waiter.
- **`DaemonThreadFactory` / `ThreadHints.onSpinWait`** ↔ `ZetaThreadFactory` and direct
  `Thread.onSpinWait()` calls. (`ThreadHints` is `@Deprecated` upstream for the same reason.)
- **`hasAvailableCapacity` as a non-claiming capacity probe** ↔ nothing needs to ask without
  acting; `remainingUnits()` in D-1 covers the read-only question.

## Where Zeta differs deliberately (recorded, not defects)

1. **Ordering by barriers vs ordering by stamps.** Disruptor serializes stages with
   `SequenceBarrier`s; Zeta has two independent planes (peer sync, worker decisions) writing the
   same L1 and orders them with version/epoch stamps (`VersionGuard.shouldSkipForSync` /
   `shouldSkipForRefresh` / `shouldSkipForWorker`) rather than a shared barrier. Stamps travel
   over transports that share no cursor, so barrier-style gating is not available — and the
   stamping choice is what makes out-of-order arrival tolerable (see #8).
2. **Drop instead of block, at the door.** Disruptor back-pressures producers; Zeta drops
   (ADR-0032) and counts. D-1 exists precisely because the counting was not observable.
3. **Loss-tolerant planes.** `Disruptor.shutdown` drains because the ring holds the only copy;
   Zeta's planes self-heal (ADR-0007/0013/0024). D-3 is therefore about latency, not data.
4. **A never-throw exception chain** where Disruptor has a fatal-by-default handler (#17).

## Consequences

- D-1 is implemented and touches no hot path: one accessor on the dispatcher, one per listener, an
  endpoint section, six gauges, and the tests for them. It makes ADR-0032's asymmetry measurable.
- Two decision items are captured with their trade-offs (D-2 semantic shedding, D-3 shutdown
  flush ordering) so they are not re-derived from scratch; neither is implemented, and D-3's flush
  half is explicitly in tension with ADR-0007.
- 25 element verdicts are on record. ADR-0071's three rejections are unchanged; this sweep adds
  no new performance claim, and nothing here should be quoted as a speedup without measurement
  (ADR-0070).
- Scope note: the sweep covers `src/main` (71 sources) plus the user and developer guides. The
  snapshot's other four source trees (`src/test` 63 files, `src/jcstress`, `src/jmh`,
  `src/perftest`, `src/examples`) are scaffolding, not library design. One item from them remains
  open and is *not* a design borrow: upstream runs its concurrency invariants under JCStress
  (`src/jcstress` + the `jcstress-quick` / `jcstress-manual` CI workflows) on top of JMH. Zeta
  landed the JMH half (ADR-0070, `benchmark/`) and deferred JCStress; this ADR does not change
  that, it only records that the deferral is still the whole outstanding delta in engineering
  practice.

## Prior art

- **`com.lmax.disruptor.Sequencer` / `ConsumerRepository`** — the adopted D-1 contract:
  `remainingCapacity()` = produced − consumed, `getBufferSize()`, `isAvailable(sequence)`, and
  `hasBacklog(cursor, includeStopped)` restricted to end-of-chain consumers.
- **`BatchEventProcessor.onBatchStart(batchSize, queueDepth)`** (via
  `EventHandlerBase.onBatchStart`) — the D-2 candidate: the second argument is the load signal
  ADR-0071 declined to adopt along with the flag.
- **`dsl.Disruptor.shutdown(timeout, unit)`** — the D-3 candidate: wait for the backlog, then
  halt, because halting a non-empty ring loses entries; Zeta's in-repo analogue that already does
  this is `WorkerBroadcastBuffer.shutdown()`.
- **`MultiProducerSequencer`'s `availableBuffer`** (index mask + availability flag, published
  out of order, consumed contiguously) — rejected for Zeta; the closest legitimate relative of
  the idiom in Zeta is version-ordered merge (`CacheSyncPublisher.sendDeduped`,
  `BroadcastBuffer.mergeVersion`) rather than slot flags.
