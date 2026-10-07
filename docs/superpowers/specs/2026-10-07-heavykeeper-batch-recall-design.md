# HeavyKeeper Batch-Aware Recall Design

Date: 2026-10-07 | Status: accepted (option A) | Scope: `common` detection path only

## Context

`HotKeyDetector.add()` buffers every app read through `WaveCounter` (`HotKeyDetector.java:147`)
and flushes a batched `Map<String, Long>` to `HeavyKeeper.addDirect` roughly every 500 ms
(`WaveCounter.java:324`). The report path uses a second, faster `WaveCounter` at 50 ms
(`ZetaProperties.java:302`). Production `HeavyKeeper` therefore sees single-deliverer
batch increments (tens to thousands per key per tide), not per-read `+1`s.

Current production operating point: `topK=100 / width=50k / depth=5 / decay=0.92 /
minCount=10 / sketchWindowCount=3` (`ZetaProperties.java:58-132`), `fading` every 20 s
(`ZetaSchedulingConfiguration.java:96`), i.e. ~60 s of sketch memory.

## Agreed decisions

1. **Precision means detection capability** — recall / false-positive / burst latency of
   local TopK promotion, not sketch count error.
2. **SLO** — a single-key burst is promotable within 2 detection tides (~1 s), P99 <= 2 s.
3. **Canonical workload first** — single-key 0-to-high step on top of high-cardinality
   cold churn. Concrete parameters: burst key `2000` counts/tide starting at tide 5;
   background `5000` distinct cold keys x `1` count each per tide (seed-fixed
   `Random(42)`); run 20 tides. Capacity (`N > topK`), flat-distribution stability,
   and drift are explicitly second priority.
4. **Harness first** — deterministic tide-driven simulation in `mvn test`
   (following `WaveCounterSimulatorTest`'s reflection-driven `tide()` pattern),
   never wall-clock or JMH for the SLO assertion. ADR-0080 notes the tree has no
   recall/precision harness today; this fills that gap for the App-side detector
   (the Worker-side counterpart is `DetectionAccuracyTest`: SlidingWindowDetector
   → Evaluator → Bayesian SM ground-truth scoring — complementary layers).
5. **First knob** — batch-aware decay + admission thresholds
   (`PROTECTION_THRESHOLD / MAX_DECAY_RATIO / BATCH_DECAY_THRESHOLD /
   DIRECT_DECAY_THRESHOLD` in `HeavyKeeper.java:144-168`, plus `minCount=10` and its
   interaction with WaveCounter's `PROMOTION_FLOOR=10`). `width / depth / k / fading`
   cadence are second priority. Concurrency throughput work is frozen; correctness
   locks stay.

## Changes (option A)

1. **Harness** (`common/src/test`, new file, deterministic):
   wire `new WaveCounter(batch -> { keeper.addDirect(batch); })` with the
   no-scheduler constructor, feed per-tide counts via `counter.count(key, delta)`,
   drive tides via the private `tide()` method (same reflection pattern as
   `WaveCounterSimulatorTest.java:299`), assert per-tide `keeper.contains(burstKey)`.
   Workload parameters are section-agreed (burst 2000/tide from tide 5, 5000x1 cold
   background, 20 tides, seed 42).
   Asserts: time-to-hot in tides (must be <= 2), burst rank first under 5000:1 cold
   camouflage (rank, not occupancy — TopK membership is sticky by design and a
   100-slot TopK under churn is always full), TopK membership stability across the
   next 10 tides.
2. **Recalibration** (source change only if the harness proves it):
   sweep the decay three-tier sampling thresholds and the protection cap under
   batch increments, plus the `minCount` / `PROMOTION_FLOOR` double-threshold
   interaction. Selection rule: smallest change meeting the 2-tide SLO with no
   false-positive increase on the canonical workload. No new config surface:
   constants stay constants; `ZetaProperties` defaults change only with measured
   evidence.
3. **Freeze** (explicit non-change): `lockStripes` / `admissionLock` ordering
   (`HeavyKeeper.java:678`), the `AtomicInteger` max-raise + CAS-halving protocol
   (ADR-0020), `WaveCounter` 500 ms tide base and hot/cold routing. Documented here
   so future reviews do not re-litigate.

## Verification

- New harness green (SLO assertion as hard gate).
- Baseline measurement (2026-10-07, `HeavyKeeperBatchRecallTest`, 4 tests green):
  `2000/tide -> tide 5`, `200/tide -> tide 5`, `15/tide (1.5x minCount) -> tide 5`
  (burst start tide 5 in all cases — first-tide admission, 1 tide better than the
  2-tide SLO), burst rank first under 5000:1 cold camouflage, cold-only top counts
  at noise level. Verdict: thresholds meet the SLO with margin at all probed
  magnitudes; **no production source change** (selection rule applied — first-tide
  admission is dominated by the `minCount` comparison, collision decay never gets
  a chance to erase the margin in a single tide).
- Existing suites green: `HeavyKeeperTest`, `WaveCounter*Test` (incl. simulator),
  `ZetaDetectorTest`, full `common` suite (`mvn -pl common -am test` scope per AGENTS.md).
- `HotHitBenchmark` throughput shows no significant regression (numbers quoted with
  machine context per ADR-0080 convention); recall work must not cost per-hit latency.
- Code-taste pass after the change (AGENTS.md rule 10); docs skimmed for affected
  references (rule 9).

## Risks and follow-ups

- Overfitting to the single canonical workload. Mitigation: capacity / flat /
  drift workloads are recorded follow-ups, not this change.
- Batch-size variance across deployments (tide backlog adapts 50-1000 ms).
  Mitigation: harness covers a second batch magnitude (burst 200/tide) besides the
  canonical 2000/tide.
- No ADR: threshold recalibration is reversible via constants/config, so it fails
  the ADR hard-to-reverse test. Offer one only if a surprising, irreversible
  structural change emerges during recalibration.
