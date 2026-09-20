# Reporter Watermark Auto-Tuning Rejected (Expose the Watermark, Don't Chase It)

The RocksDB-borrowing analysis (P0 item R5) proposed porting `GenericRateLimiter.TuneLocked`'s water-marking to the report dispatcher: every 100 flush cycles, compare the dispatcher queue's drain ratio against 50%/90% watermarks and ±5%-step the concurrency budget (BBR's floor or the consumer pool size) instead of dropping batches. Re-reading both sides before implementing showed the mapping is unsound: **the dispatcher's queue level is downstream throughput's *effect*, not a knob Zeta controls.** Rejected with the standard no-win documentation (ADR-0048/0056 precedent); the observable part that survived is a side effect of ADR-0076's stall gauges.

## Status

rejected

## Context

RocksDB's `TuneLocked` works because the rate limiter *is* the drain-rate controller: adjusting `rate_bytes_per_sec` directly changes how fast its own queue drains, and the hard bounds `[max/20, max]` keep the feedback loop inside a controlled range. `KeyReporterImpl.ReportDispatcher` is the mirror image:

1. **The queue drains at the broker's pleasure.** Publishes time out at 5 s and a Worker death discards batches (`expiredCount`); queue depth reflects RabbitMQ/Worker throughput, not App-side concurrency. Tuning an App-side multiplier cannot speed a slow broker — it can only push the same pressure somewhere else.
2. **The two candidate knobs fight each other or are inert.** (a) Raising `consumerCount` raises in-flight batches, which trips the BBR gate *sooner* — the "saved" batches are dropped one step earlier at the gate, the same data lost under a different counter. (b) BBR's `setMinInFlight` is a *floor* over the maxPASS×minRT budget (ADR-0011), usually inert when the budget dominates — and `onFlush` already overwrites it from the Worker node count on every topology change, so watermark writes would race that sync.
3. **The philosophy is self-protection, not capacity chasing.** ADR-0011 built the BBR gate precisely as "drop before overload"; ADR-0007 accepts lost report cycles as self-healing. Widening the consumer pool against a saturated broker is anti-flow-control.
4. **The wide middle is a no-op anyway.** Between the 50% and 90% watermarks every branch of the proposed tuner is a no-op; the only regimes it acts in are "healthy" (nothing to do) and "saturated" (already lost).

## Decision

- **No auto-tuning.** `ReportDispatcher` keeps its fixed bounded queue and consumer pool; BBR's floor stays owned by the Worker-topology sync.
- **The surviving observable:** dispatcher depth and drop/expiry attribution are exposed as `zeta.stall.report_backpressure.delayed` / `.stopped.total` plus the split `zeta.reporter.queue.expired.dead.total` / `.stale.total` (ADR-0076), so saturation is *diagnosable* — the honest version of the original goal.
- The genuine lever for sustained saturation is upstream load reduction (the compact report encoding, ADR-0074, shrinks bodies; the R2 admission protocol remains a future option in the borrowing report), or Worker/broker-side capacity.

## Considered Options

- **Tune the consumer pool at runtime** (the only physically coherent tuner): requires converting the fixed pool to a `ThreadPoolExecutor`, races the BBR in-flight budget, and moves drops from the queue to the gate. Rejected.
- **Tune BBR's `minInFlight` floor from the watermark:** adjusts a knob that is usually shadowed by the computed budget and is periodically overwritten by the topology sync. Rejected.
- **Log-only watermark alerting:** subsumed by ADR-0076's gauges; a throttle-limited WARN on saturation would duplicate `zeta.dispatch.backlogged` conventions without adding signal. Rejected.

## Consequences

1. None of the borrowing report's R5 code ships; the item is closed with this ADR and the analysis document records the verdict. The RocksDB constants (100-cycle cadence, 50/90 watermarks, ±5%, max/20 floor) are recorded here so a future re-proposal starts from evidence rather than rediscovery.
2. The dispatch-depth gauge (`zeta.stall.report_backpressure.delayed`) is the early-warning signal an operator would have tuned against; runbooks should alert on it and on the `.stopped.total` rate.
