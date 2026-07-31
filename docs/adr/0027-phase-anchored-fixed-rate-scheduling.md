# Phase-Anchored Fixed-Rate Scheduling with Missed-Tick Skipping

`SafeScheduledExecutorService`'s `scheduleAtFixedRate` originally degraded to fixed-delay semantics: the next run was scheduled `period` after the *completion* of the previous run (`gap = period + execution time`). A run that occasionally took longer permanently backslid the cadence — a heartbeat that is slow once stays slow forever, and the drift accumulates for every slow run after that.

Threadly's `RecurringRateTaskWrapper` (`AbstractPriorityScheduler.java`) anchors the cadence differently: `nextRunTime += period` after each run, so the schedule stays fixed to the original phase. Its literal behaviour, however, is *catch-up with bursts*: when a run overshoots its slot, the wrapper immediately re-executes back-to-back until it catches up with the schedule. For Zeta's consumers (heartbeats, circuit-breaker window slides, buffer flushes) that burst behaviour is worse than the drift it fixes — a missed heartbeat produces a burst of heartbeats that arrive no sooner and burn cycles.

## Decision

Give `scheduleAtFixedRate` **phase-anchored cadence with missed-tick skipping** (semantics C):

1. **Phase anchor.** The chain keeps an absolute `nextRunTimeNanos` slot, initialised to `now + initialDelay` and advanced by exactly one `period` per run. The delay for the next link is `nextRunTimeNanos - now`, so a run that finishes early lets the next run start at its phase slot instead of waiting out the full period from completion.
2. **Skip, don't burst.** If a run overshoots its slot (`nextRunTimeNanos - now <= 0`), the missed tick is dropped and the cadence re-anchors to `now + period`. There is never a back-to-back catch-up burst; the next run starts `period` after the slow run *completed* (matching the old semantics in that single overshoot case, but never accumulating drift across consecutive slow-but-under-period runs).
3. **Fixed delay unchanged.** `scheduleWithFixedDelay` keeps the historical Zeta semantics (gap measured from completion). The two methods now map cleanly onto the JDK names: `rate` = phase-anchored, `delay` = completion-relative.
4. **Validation.** Both methods reject non-positive periods/delays with `IllegalArgumentException` (matching JDK and preventing a busy self-rescheduling loop).

The chain mechanism (non-overlapping links, exception tolerance via `ZetaExceptionHandler`) is unchanged.

## Alternatives Considered

- **Threadly literal catch-up (semantics B):** after an overshoot the task re-runs immediately, repeatedly, until it catches up with the schedule. Rejected — produces bursts of heartbeats/window slides exactly when the system is already slow; the last execution in the burst is the only one that matters, so the burst is pure waste.
- **Keep the old completion-relative semantics:** rejected — a single slow run permanently shifts the phase; consecutive slow runs accumulate unbounded drift, which was the reported problem.
- **JDK `scheduleAtFixedRate` semantics:** rejected — the JDK's start-to-start rate overlaps executions when a run exceeds the period, which Zeta's chain design deliberately forbids (non-overlap is a documented invariant).

## Consequences

1. `scheduleAtFixedRate` semantics change for all callers: heartbeat producers, circuit-breaker window slides, buffer flushers, decay/drain tasks. Slow runs no longer push the cadence; they skip a tick and re-anchor.
2. Existing consumers were audited: all use `scheduleAtFixedRate` for cadence-sensitive work (heartbeats, slides, flushes) where skip-and-re-anchor is strictly safer than burst or drift. No consumer depends on the old `period + execution time` gap.
3. `fixedRate_shouldNotOverlap_whenTaskExceedsPeriod` and `fixedRate_missedTick_skipsNoBurst` pin the non-overlap and no-burst invariants; `fixedRate_phaseAnchored_noCumulativeDrift` pins the no-drift property.
4. The `SafePeriodicTask` chain now carries a `rateMode` flag and a `nextRunTimeNanos` phase slot; the class Javadoc documents the three-way distinction (rate vs delay vs JDK rate).
5. Borrowing provenance: semantics C is the Zeta-adapted form of Threadly's phase-anchored `RecurringRateTaskWrapper` (see Threadly `AbstractPriorityScheduler.java` L1002-1028), deliberately diverging from its catch-up burst behaviour.
