# BBR + CPU Fusion for Reporter Self-Protection

The Reporter could saturate RabbitMQ and the Worker under high traffic when every `get()` call triggers a `record()` that eventually flushes to RabbitMQ — if the App or Worker is overwhelmed, reports pile up, latency spikes, and the whole system degrades. Zeta fused a BBR (congestion control) rate limiter with a CPU EMA monitor into the Reporter flush path: `BbrRateLimiter.tryAcquire()` runs before each flush cycle, and either admits or drops the batch based on Little's Law concurrency budget and the current CPU load.

The decision surface has two zones — below `cpuThreshold` (80%) the limiter is permissive (admits if concurrency is within budget OR if not in cooldown); above it, strict enforcement kicks in (only admits if concurrency is within budget). This prevents an already-loaded process from amplifying its own congestion while letting a healthy process tolerate brief concurrency bursts. CPU is smoothed via EMA (`cpuDecay=0.95`, 500ms poll interval) to avoid thrashing on transient spikes. BBR's sliding window (10s, 100 buckets) tracks max pass rate + min RT to compute the safe concurrency limit.

The net result is backpressure at the Reporter layer — batch drops are silently counted by the BBR rate limiter (`totalDropped` counter, exposed as `zeta.reporter.bbr.dropped` gauge), queue-full drops are logged at WARN level every 100th drop, and publish failures are logged at ERROR level. The system self-throttles before the bottleneck (RabbitMQ or Worker) gets overloaded. The feature is enabled by default (`zeta.local.reporter.enabled=true`) and introduces zero overhead when the system is healthy.

## 2026-09: Damped adaptive limiting (kernel writeback port)

The two-zone decision surface above had a smoothing gap: the budget was recomputed from the raw sliding-window extremes (`maxPASS()` returned the raw bucket max, the EMA caches were only fallbacks), so the limit jumped whenever a hot bucket slid out of the window, and measurement noise became admission action in a single step. The permissive/strict CPU switch was a hard discontinuity at exactly the load level where smoothness matters most.

`BbrRateLimiterImpl` now implements the three-layer structure of the kernel's writeback throttle (`mm/page-writeback.c`):

1. **Freerun band** (`dirty_freerun_ceiling`): at or below half the damped baseline the control loop does not restrict at all. A recent consumer drop (cooldown) bypasses the band — the budget is then strict.
2. **Position control** (`pos_ratio_polynom`): the admission budget is the damped baseline scaled by a cubic Q10 position ratio around the setpoint (= the baseline itself, clamped by the CPU-derated hard limit). The control band saturates at twice the setpoint (`setpoint + max(4, setpoint)`); a wide band out to the config ceiling would dilute the position error and stall the loop for small baselines. The kernel's `clamp(pos_ratio, 0, 2 << SHIFT)`, `| 1` divisor odd-ification and pre-cube overflow clamp are all preserved.
3. **Damped baseline** (`wb_update_dirty_ratelimit`, 200 ms cadence): a Little-Law estimate (`maxPass × minRt`, capped at the ceiling) moves the baseline through the direction gate (`:1436-1446` — the position-error direction decides whether only upward or only downward moves are allowed), a three-way clamp against `lastBalanced`/`est`/`task` outliers, and step-size decay (`:1453-1457`). `lastBalanced = est` (`:1465`), `max(…, 1)` and the `+1` ramp-up helper (`:1348`, `:1464`) are ported verbatim.

CPU is no longer a two-state switch: it derates the hard limit continuously over a ±20 pp ramp around `cpu-threshold` (no derating below the ramp start, ¼ of the ceiling at full load). This pulls the setpoint down smoothly instead of flipping a mode bit.

A new quasi-static config `zeta.local.reporter.bbr-max-in-flight-ceiling` (default 128) plays the role of the kernel's `thresh`: it caps the Little-Law estimate and the baseline (the analog of `balanced > write_bw → write_bw`), and CPU pressure derates it. It should exceed the expected Worker count so the `minInFlight` floor never overrides it.

Convergence under persistent gate drops: denied cycles never enqueue, so in-flight strictly drains; the maxPassCache ×0.99 decay lowers the estimate, and once in-flight overshoots the baseline far enough to engage the downward direction gate the baseline follows with a bounded step (≤ ~12.5% of the remaining gap per 200 ms update). Admission resumes without lock-up. The old maxPassCache-only convergence argument is superseded by this path.

Observability: `BbrRateLimiter` exposes `getBalancedInFlight()` (damped baseline), `getCurrentMaxPass()` / `getCurrentMinRt()` (the estimate inputs) alongside `getCurrentMaxInFlight()` (the effective budget); `KeyReporter.bbrBalancedInFlight()` surfaces the baseline curve. Plotting balanced / maxPass / minRt together shows how much of the budget comes from measured throughput versus position/CPU pressure.

Contract change: the permissive/strict CPU switch documented in the original ADR is gone — `cpu-threshold` is now the center of a continuous derating ramp, and the concurrency budget is enforced above the freerun band even without a recent drop. Timebase guard: the 200 ms loop re-anchors (instead of integrating) on clock rollback or > 60 s jumps, PELT-style (`pelt.c:184-194`).
