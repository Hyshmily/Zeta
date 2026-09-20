# Eviction Scan Cadence Split From the Staleness Threshold

`zeta.worker.state-machine.evict-interval-ms` was doing double duty: it was simultaneously the eviction **staleness threshold** (a key is evicted after this long without any report — a correctness knob that must stay `>= 2 × cool-duration-ms`) and, via `@Scheduled(fixedDelayString = "${zeta.worker.state-machine.evict-interval-ms:30000}")`, the cadence of the eviction **scan** (a housekeeping-cost knob). The two semantics share one knob, with three mutually contradictory defaults: the bound bean default was `1_200_000` (20 min), the `@Scheduled` fallback was `30000`, and `docs/CONFIG.md` documented `30000` while advising `>= cool-duration-ms × 2` — meaning the *documented default violated its own guidance* (30 s < 2 × 600 s), and a deployment that never bound the property explicitly evicted stale keys every 30 s while believing it had a 20-minute threshold. We decided to split the knob: a new `zeta.worker.state-machine.evict-scan-interval-ms` (default `1_200_000`, matching today's effective bound default) binds the `@Scheduled` cadence, and `evict-interval-ms` remains purely the staleness threshold.

## Status

accepted

## Context

`WorkerAutoConfiguration.EvictStaleTask.evictStale()` is scheduled with `fixedDelayString = "${zeta.worker.state-machine.evict-interval-ms:30000}"` but reads its staleness threshold from `WorkerProperties.StateMachine.evictIntervalMs` (bean default `1_200_000`). Consequences of the conflation:

1. **The documented default was wrong and self-contradictory.** `docs/CONFIG.md` listed the default as `30000` (the `@Scheduled` fallback, which only applies when the property is unset in the *bindable* sense — in practice the bound bean default `1_200_000` always wins for the threshold, while the schedule placeholder fallback `30000` can never engage because the same property resolves to the bound value). The same doc row advised `>= cool-duration-ms × 2` = 20 min.
2. **Tuning one semantic silently broke the other.** An operator raising `evict-interval-ms` to reduce staleness-threshold aggressiveness also *slowed* eviction scanning proportionally (delaying memory reclaim); an operator lowering it for aggressive cleanup *spammed* scans.
3. **Config-surface fragility.** Once released, renaming or re-purposing a bound property is hard to reverse — deployments pin values to whichever semantics they assumed.

Related stale claim fixed in the same change: `DefaultEvaluator.EVICT_CYCLE_MS` Javadoc asserted it "matches the default of `evict-interval-ms` (30000)" — false against the bound default (20 min) and moot after the split; the 30 s constant is the EMA decay tick and is deliberately independent of eviction cadence.

## Decision

- New property `zeta.worker.state-machine.evict-scan-interval-ms`, `WorkerProperties.StateMachine.evictScanIntervalMs`, default `1_200_000` — chosen to match **today's effective** bound default so the split is behavior-preserving for anyone relying on the bound default.
- `EvictStaleTask` binds `@Scheduled(fixedDelayString = "${zeta.worker.state-machine.evict-scan-interval-ms:1200000}")`; the fallback mirrors the bean default (the old `30000` fallback was unreachable-in-spirit and contradicted the threshold guidance).
- `evict-interval-ms` keeps its name, value space, and role: **staleness threshold only** (`>= 2 × cool-duration-ms` guidance unchanged).
- Configuration metadata (`worker/src/main/resources/META-INF/additional-spring-configuration-metadata.json`) gains the new property; `docs/CONFIG.md` / `docs/CONFIG.zh.md` updated (default value corrected to `1200000`, threshold/scan descriptions separated).

## Considered Options

- **Keep one knob, fix the docs to `1200000`:** leaves the two semantics coupled — every threshold retune also retunes the scan cadence, and the `@Scheduled` fixedDelay would be forced to 20-minute granularity, making threshold *lowering* (faster cleanup) impossible without also lowering the scan interval. Rejected.
- **Bind the schedule to a fixed constant:** removes operator control over scan cost entirely. Rejected.
- **Reuse `cool-duration-ms` as the scan cadence:** conflates a third semantic and makes scan cost depend on a detection-tuning knob. Rejected.

## Consequences

1. Deployments that explicitly set `evict-interval-ms` and *also* relied on it to pace the scan (i.e. noticed 30 s scans) must now set `evict-scan-interval-ms` to keep the old cadence — a one-line config addition at upgrade, called out in the ADR because the old `@Scheduled` fallback `30000` made short cadences observable.
2. Threshold tuning no longer moves the scan cadence and vice versa; the documented guidance (`evict-interval-ms >= 2 × cool-duration-ms`) no longer conflicts with any default.
3. `DefaultEvaluator.EVICT_CYCLE_MS` documentation no longer claims a (false) coupling to `evict-interval-ms`.

> **Note (2026-09-17).** `DefaultEvaluator.EVICT_CYCLE_MS` no longer exists in the
> codebase (the class now declares `CV_HISTORY_SIZE`, `MAX_TRACKED_CMS_KEYS` and
> `MAX_TREND_RATIO`), so the two references above name a symbol a reader cannot
> look up. They are kept as a historical record of the claim that was corrected;
> treat the 30 s EMA decay tick as unnamed if you go looking for it.
