# Stall-Cause Gauges (Why Is It Slow, Not Just How Slow)

The metric surface answered "how bad" (`zeta.reporter.queue.dropped.total`, `zeta.dispatch.backlogged`, `zeta.reporter.bbr.*`) but not "why": an on-call engineer seeing drops had to open logs and correlate WARN lines to attribute the loss to reporter backpressure, broadcast saturation, a degraded Redis, or a Worker partition. RocksDB solves the same problem with a `WriteStallCause × WriteStallCondition` ticker matrix (`db/write_stall_stats`) — each cause paired with each non-normal state gets its own ticker, and the normal state registers nothing. We adopt that pattern as a small gauge family, `zeta.stall.<cause>.<state>`.

## Status

accepted

## Context

An inventory of the four candidate stall causes found the raw signals scattered and incomplete:

| Cause | Signal | Pre-change state |
| --- | --- | --- |
| report_backpressure | dispatcher depth / dropped / expired | counters existed, some already gauged; no aggregated attribution |
| broadcast_storm | send failures, send-executor saturation drops | **no counters at all** — only the rate-limited WARN (ADR-0037) |
| redis_degraded | breaker open, dedup load timeouts | breaker state queryable via `SingleFlight.isBreakerOpen()`, timeouts uncounted |
| worker_partition | no alive Worker | only the 0/1 `zeta.worker.alive` health judgment; alive *count* not exposed |

Two additional design constraints came from the codebase: `ZetaCacheStats` is a pure L1 Caffeine snapshot (its record semantics are endpoint-published), so stall attribution must not be folded into it; and the metrics config already uses `ObjectProvider`-gated gauge registration, so a new cause must degrade to "not registered" when its component is absent (a missing component means the failure mode cannot occur in that deployment).

## Decision

- **Gauge family** (registered in `ZetaMicrometerAutoConfiguration.registerStallGauges`, each via `ObjectProvider` so absent components skip their causes):
  - `zeta.stall.report_backpressure.delayed` — dispatcher queue depth (clamped ≥ 0; `-1` pre-start reads as 0).
  - `zeta.stall.report_backpressure.stopped.total` — `dispatcherDropped + dispatcherExpired` (batches lost to a full queue or staleness).
  - `zeta.stall.broadcast_storm.stopped.total` — `BroadcastBuffer.sendFailures() + saturationDrops()`.
  - `zeta.stall.redis_degraded.stopped` — 1 while the breaker is open.
  - `zeta.stall.redis_degraded.timeouts.total` — new `SingleFlight.getLoadTimeoutCount()`.
  - `zeta.stall.worker_partition.stopped` — 1 while `getAliveWorkerIds()` is empty.
  The healthy/normal state registers nothing (RocksDB's convention) — an absent series means "not stalling".
- **Missing raw counters added at their sources** (attribution, not new semantics): `BroadcastBuffer` gains `sendFailureCounter`/`saturationDropCounter` beside the existing rate-limited WARNs; `SingleFlight` gains a timeout counter incremented at both single-key and batch timeout resolutions, exposed through a `default` interface method returning 0 for backward compatibility with custom beans (the established `invalidateAll()` precedent); no interface change for the alive count — `getAliveWorkerIds().size()/isEmpty()` suffices for a scrape-time gauge.
- **`dispatcherExpired` split.** The counter previously conflated two discard causes and its Javadoc said so ("deliberately not split"). The split is now load-bearing: dead-target discards are the *consumption-side evidence of a worker partition*, stale discards are the *report-backpressure* signal. `KeyReporter.expired()` keeps returning the sum (metric continuity); new `dispatcherExpiredDeadTarget()` / `dispatcherExpiredStale()` expose the halves, mirrored by `zeta.reporter.queue.expired.dead.total` / `.stale.total`. A batch that is both dead-target and stale is attributed to dead-target (the more actionable signal), documented at the counter.

## Considered Options

- **Fold causes into `ZetaCacheStats`:** pollutes a pure L1 snapshot record and couples the endpoint payload to report-plane internals. Rejected.
- **Enum-typed `StallCause`/`StallState` API:** the enum would have no logic behind it — the gauges are static registrations; a javadoc table scales the same and reads better. Rejected (YAGNI; the naming convention encodes the matrix).
- **Only add the missing counters, skip the gauge family:** leaves attribution as a manual cross-reference of five unrelated meters — the exact friction this ADR removes. Rejected.
- **Keep the expired conflation:** the Javadoc originally argued the two causes were "deliberately not split"; the stall family made the split load-bearing (worker_partition's only consumption-side signal), so the conflation became the defect. Adopted the split.

## Consequences

1. Dashboards can now answer "why is it slow" without log correlation; each series appears only while its cause is stalling (or has ever counted a loss, for the `.total` counters).
2. `ZetaMicrometerAutoConfigurationTest` was extended for the new gauges (including a `BroadcastBuffer` mock and `getAliveWorkerIds` stubbing — an unstubbed mock returning `null` would NPE the scrape, which is why the stub is explicit); `ZetaAmqpAutoConfigurationTest`/`ZetaMicrometerAutoConfigurationTest` call sites were adapted to the widened bean signatures.
3. Future causes follow one recipe: a source-side counter (or state query), one `ObjectProvider`-gated gauge in `registerStallGauges`, a table row. The matrix stays flat and greppable by the `zeta.stall.` prefix.
