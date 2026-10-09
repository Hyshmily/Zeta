# Write-Path Ordering: Bump Before Drop, Commit-Thread Deferral, and a Dedicated Reader Pool

Three ordering and threading decisions on the write path, recorded together
because they share one root cause and because source comments in four files
(`HotKeyCache`, `ZetaAutoConfiguration`, `ZetaSpringCacheAutoConfiguration`,
`WorkerHeartbeatVerifier`) cite this ADR at their individual sites. Before this
document existed, those comments pointed at nothing.

## 1. Bump the data version BEFORE dropping L1

`invalidate`, `invalidateAfterPut`, and the batch invalidation paths each
remove the L1 entry and then tell peers. The version bump that orders those two
events could run before or after the removal, and the choice is not
interchangeable.

**Drop first, bump after** (the rejected shape) leaves this window:

1. instance drops `K` from L1 at `t0`
2. a reader misses at `t1`, reloads the **pre-invalidation** value, and caches it
   stamped with the still-old version `v` (ADR-0033 probes the version *after*
   the read, so the stamp is `v`)
3. the bump allocates `v+1` and the INVALIDATE carrying `v+1` is broadcast

Every receiver applies the plain `>=` matrix, so the step-2 entry (`v`) is
treated as **newer** than the invalidation (`v+1`) and the INVALIDATE is skipped.
The stale value is then pinned for its whole TTL with no watermark that could
unpin it — the instance broadcasts an invalidation it has itself just rejected.
ADR-0067's origin filter does not help: it drops only self-**REFRESH**, and the
self-INVALIDATE here is exactly the message that must apply.

**Bump first, drop after** inverts it: the racing reload is stamped `v+1`, so
the mutation's `v+1` is equal and the invalidation lands, and the next mutation's
`v+2` correctly supersedes it.

The bump is therefore **synchronous on the committing thread**. The alternative
(a synchronous bump, asynchronous send) still orders correctly, so the remaining
question is only about the send.

## 2. The send is synchronous too, not deferred to the executor

The bump-and-send used to be dispatched to `hotKeyExecutor`, which was
attractive: it kept a Redis round-trip and an AMQP publish off the caller's
thread. It was rejected for two reasons.

**Correctness.** On `RejectedExecutionException` the invalidation was dropped
outright, with only a WARN. A lost INVALIDATE leaves every peer on the stale
value for its full hard TTL, and there is no data-plane rebroadcast to correct it
(the rebroadcast in ADR-0024 covers *decisions*, not cache entries).

**Uniformity.** Three of the four invalidation paths had drifted to
bump-after-drop while the single-key path used bump-before-drop, so the same
logical operation had two orderings. Ordering is exactly the kind of property
that a shared helper should make unobservable: a single
`dropAndSendInvalidate(key, preAllocatedVersion)` helper is used by all four
call sites, so the ordering is a property of the call site rather than of the
author's care on the day.

The cost is one Redis RTT per invalidated key on the committing thread. That is
accepted deliberately: an invalidate is an explicit write-path operation, whereas
a *read* probe would land on every request. For the batch path this is one RTT
per key, which ADR-0066 already accepted when it moved batch invalidation from a
single unversioned `INVALIDATE_ALL` to per-key versioned messages.

## 3. After-commit deferral is registered before the caller's own synchronizations

`putThrough` and `invalidateAfterPut` run their body through
`TransactionSupport.runNowOrAfterCommit`, which runs inline when no transaction
is active. When one *is* active, the work is deferred to `afterCommit`.

The ordering contract: this registration happens **before** any synchronization
the caller registers afterwards, so Spring's after-commit sequence runs
write-then-invalidate in the order the caller intended. A `@CacheCondition` purge
registered by user code therefore sees the post-write state.

This is the narrow interaction ADR-0067 records around the `@CacheCondition`
purge race, and it constrains where the version bump may be allocated: it must
happen inside the deferred body, not before it, or the version would be consumed
for a mutation that has not committed yet.

## 4. The reader runs on a dedicated pool, not the caller

`InterruptingAsync` executes the miss-path reader on its own executor and the
caller blocks on the future. This is ADR-0030's lock-free-reader decision
applied to load, and the pool is dedicated rather than the shared
`hotKeyExecutor` for the same reason: a reader is caller-supplied code of
unbounded duration, and it must never occupy a slot that invalidation work needs.

The cost is a thread handoff (~1–3 µs) on every miss, which dominates the
surrounding L1 work by two to three orders of magnitude. Accepted: a miss already
pays a Redis round-trip for the version probe, and the alternative — running
reader code inline under the Caffeine bin lock — reintroduces the deadlock and
unbounded-lock-hold hazard ADR-0030 exists to remove.

## Consequences

- A mutation racing an invalidation reload can no longer pin a stale value for
  the full entry TTL. The residual window is a genuine same-key write conflict,
  resolved by the version matrix.
- Invalidations are no longer silently dropped on executor saturation; the only
  remaining loss mode is a Redis failure, which degrades the version (ADR-0019)
  and is visible via `zeta.version.degraded.total`.
- `invalidateAfterPut` and batch `invalidate` now block the committing thread for
  one Redis RTT per key. Operators who batch-invalidate large key sets on a
  latency-sensitive path should be aware of this.
- The reader-pool handoff remains the dominant miss-path cost; closing it would
  require per-caller execution, which is unsafe under the bin lock.

## Rejected

- **Asynchronous send with an unbounded retry** — reintroduces the dropped
  invalidation window and adds an unbounded queue on the failure path.
- **A fencing-style version stamp on the invalidation message** — the receiver
  matrix already handles ordering; a second stamp duplicates `dataVersion`.
- **Deferring the reader to the caller's thread when the loader is known cheap** —
  cheapness is not knowable at the seam, and one slow loader would then hold a
  Caffeine bin lock.