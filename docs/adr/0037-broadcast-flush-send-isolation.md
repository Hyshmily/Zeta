# Broadcast Flush Sends Leave the Shared Scheduler with Bounded Saturation Drop

`BroadcastBuffer.flush()` previously ran its per-key AMQP send loop synchronously on the shared `hotKeyScheduler` (pool size 4) and logged a per-key WARN with full stack trace on every failed send. During a RabbitMQ disconnect this both flooded logs (≈500 WARN/s at 1000 keys/window) and stalled peer tasks on the same pool (HeavyKeeper decay, expelled drain). We decided: (1) aggregate flush failures into one WARN per 10s window carrying the first exception — never per-key on the hot path; (2) move the send loop off the scheduler onto a dedicated single-threaded bounded executor, injected at construction, with drop-on-saturation reported through the same rate-limited WARN; a `null` executor keeps the legacy synchronous path so the fix ships in two independent parts.

## Status

accepted

## Context

`BroadcastBuffer.flush()` (BroadcastBuffer.java:152-173) swaps the pending map and then iterates it with a per-key try/catch: `log.warn("Failed to send refresh for key {}", key, e)` — full stack trace per key, per flush cycle. The flush runs on the `hotKeyScheduler` thread when triggered by the scheduled task, and synchronously on the calling thread in the rejection-fallback path (`record()` line 143, and HotKeyCache's "sync-flush fallback"). The scheduler is a `SafeScheduledExecutorService` with default pool size 4 (`zeta.scheduler-pool-size`, ZetaFacadeAutoConfiguration), shared with `cleanHotKeys` (decay, 20s cadence) and `drainExpelled` (10s cadence) via ZetaSchedulingConfiguration. A hanging AMQP send during connection recovery therefore blocks a quarter of the pool and delays the decay chain and expelled drain.

Loss tolerance already exists: ADR-0007 (no publisher confirms, fire-and-forget — lost broadcasts recovered by the next cycle), ADR-0013 (acceptable race fading — transient inconsistencies bounded by the next periodic cycle), `CacheSyncPublisher.sendDeduped` does not advance the `recentBroadcasts` dedup map on a failed send (so recovery re-sends cleanly and post-recovery duplicates are suppressed within the dedup window), and the receiver-side `VersionGuard.shouldSkipForSync` 4-case degraded comparison rejects stale late versions. There is also an existing connection-isolation precedent: the heartbeat plane already gets its own dedicated factory/template, isolated from the data plane (CONFIG.md "Connection isolation").

## Decision

**Failure logging contract:** per flush, count failed sends; emit at most one WARN per 10s window (`TimeSource.monotonicMillis()`, codebase convention) with the failed/total counts and the first captured exception. The rate-limit field is `volatile` — `flush()` is reachable from the scheduler thread, the caller thread (rejection fallback), and HotKeyCache's sync-flush fallback, so the field must be visible across threads. Per-key WARN on the hot path is removed.

**Send isolation:** the constructor gains an optional `@Nullable Executor sendExecutor`. When present, `flush()` performs the map swap on the calling/scheduler thread and hands the snapshot to the executor; `RejectedExecutionException` (queue saturated or shutdown) drops the batch and logs one rate-limited WARN (same 10s window). When `null`, the current synchronous send path runs unchanged. The executor is single-threaded and bounded: single-thread preserves send order = flush order, which keeps reasoning simple even though correctness does not depend on it (stale versions are rejected at the receiver); bounded avoids OOM under sustained disconnect; the achievable throughput (1000 keys per 500ms flush) is far below what one thread can publish.

**Wiring:** the executor is injected from the C2 thread-pool governance work (its bounded pool) when that lands; until then the bean passes `null` and only Part A is active. No new configuration property.

**Saturation drop is deliberate backpressure**, not loss of correctness: per ADR-0007/0013 a dropped REFRESH is re-sent by the next flush after recovery, and `sendDeduped` never advances its dedup map on failure.

## Considered Options

- **Logging fix only** (keep synchronous sends on the scheduler): removes the flood but leaves the decay chain and expelled drain stalled by a hanging send during disconnect. Rejected as incomplete — the scheduling impact is the worse failure mode of the two.
- **Multi-threaded send pool**: introduces cross-flush reordering (flush N+1 can publish before flush N). Correct at the receiver thanks to the 4-case version guard, but single-threaded is sufficient for the throughput and costs nothing to reason about. Rejected.
- **Unbounded executor**: unbounded queue growth under sustained disconnect. Rejected.
- **Send-side circuit breaker on the publisher** (short-circuit sends after consecutive failures): more moving parts; connection recovery is already handled by the AMQP layer, and drops are cheap under ADR-0007. Rejected for this ADR; may be revisited in the C2 thread-pool governance discussion.

## Consequences

1. **REFRESH messages can be dropped under sustained disconnect**, bounded by the executor's queue capacity — consistent with ADR-0007/0013. Peers converge on the next flush after recovery; the `recentBroadcasts` dedup window absorbs post-recovery duplicates.
2. **Log volume during disconnect drops from ≈500 WARN/s to 1 WARN/10s**, preserving the first exception for diagnosability. The "never WARN/INFO on the hot path" logging rule (AGENTS.md) is now formally enforced in BroadcastBuffer.
3. **`flush()` no longer guarantees synchronous delivery** when an executor is present. The rejection-fallback semantics in `record()` and HotKeyCache (sync-flush on scheduler rejection) are unchanged when the executor is `null`.
4. **Shutdown**: the executor must be closed with the context; in-flight or queued sends may be lost at shutdown — acceptable for fire-and-forget REFRESH (ADR-0007).
