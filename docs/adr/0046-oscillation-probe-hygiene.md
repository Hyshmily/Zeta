# ADR-0046: Oscillation Probe Hygiene — Evidence-Gated Raise Steps + Ladder-Priced Collapse (WaveCounter)

A full-pipeline simulation (real request streams through a faithful port of `count()` → `tide()` → `promote()` → `MoonsTidalForce`) exposed defects in the ADR-0045 governor under oscillating/marginal workloads, and a 400-scenario randomized fuzz campaign (random phase scripts: hot/quiet/silence/oscillation/drift/burst, random hot-set sizes vs `hotLimit`, random noise scale, both cadences) validated the fixes and rejected one overfit candidate.

**Problem 1 — blind stepping.** The raise-walk's bold driver stepped on EVERY distressed tide with no reference to the earners' capacity: the floor reliably climbed past the hot set's earnings, the members' evidence decayed, and the empty-set collapse had to undo the probe (1s square wave: floor 10↔113, one probe per ~13 tides forever).

**Problem 2 — ladder-resetting collapse.** The empty-set collapse called `reset()`, which wipes both retry ladders — the oscillation probe loop (ARM → climb → COLLAPSE → ladder reset → ARM) repeated forever, the backoff throttle (4→8→16→32) defeated by the reset.

**Fix 1 (earner-gated step):** the walk steps only while `0 < renewal < target` — a renewal of 0 means the set is quiet or dead; climbing cannot help it.

**Fix 2 (density-gated step):** the walk additionally steps only while `hotColdRatio < 1` — a ratio ≥ 1 means the members are genuinely earning, and a step would push the threshold past the marginal earners and eat the walk's own confirmation (the self-eating step: a 0.43-renewal tide stepping 41→56 excludes the 55-56-count earners and turns a confirm into a crash).

**Fix 3 (ladder-priced collapse):** a collapse that kills an in-flight walk prices the walk's OWN ladder as FAILED, and ANY priced ladder state (a crash/fail price landing before the collapse, e.g. during the post-verdict retreat) survives the reset; only a collapse with no walk AND no priced ladder (a genuine regime change) gets the full ladder reset.

## Validation (sandbox, ADR-0045 methodology — simulated sequences before wiring)

300 randomized workloads (phase scripts: hot/quiet/silence/oscillation/drift/ramp/band-rich/dual/burst; random hot-set sizes vs hotLimit 16-1024; random noise scale; both cadences), paired per seed (mean±std [p25,p75], two-sided binomial sign test on the paired deltas):

| metric | baseline | fixed (f1+f2+f3) | paired |
|---|---|---|---|
| probe arms | 0.760 | 0.667 | 18 improved / 2 worse, p=0.0004 |
| collapses | 0.443 | 0.307 | — |
| floor max | 26.1±22.6 | 17.8±9.9 | 101 improved / 0 worse, p<0.0001 |
| mean floor excess | 2.01±5.47 | 1.24±2.60 | 88 improved / 15 worse, p<0.0001 |
| confirms (responsiveness) | 0.277 | 0.317 | 12 improved / 3 worse, p=0.035 |
| harmful over-filter (renewal≥0.5 ∧ blocked>0 ∧ floor>boundary) | 0.028 | 0.031 | 12 improved / 33 worse, p=0.0025 |
| admit latency (max unresolved ADMIT-gate streak) | 0.69 | 0.73 | both ≤ 1 tide |
| floor flicker (direction reversals) | 0.018 | 0.014 | 25 improved / 3 worse, p<0.0001 |

The fixed machine probes less, climbs lower, flicks less and confirms MORE (responsiveness preserved). The one axis where it is slightly WORSE: harmful over-filter (+0.003 = 0.3% of promoted tides, paired-significant) — the evidence gates hold the floor longer in some regimes with a non-empty band; the ADMIT latency metric shows the admit still fires within ~1 tide, and the absolute cost is bounded. Classification thresholds are robust to ±sweeps (overfilter thr 0.1-0.3, ratchet thr 20-30 shift the ok/overfilter/unresolved counts predictably, no category appears/disappears abruptly).

Hand scenarios (assertion-gated, exit code): 1s square wave @500ms becomes ARM→FAILED→backoff cycles (floor 10↔41, no collapses, backoff 4→8→16→32); burst cadence collapses in 2 tides; regime shift keeps ARM→CONFIRM→ADMIT; ramp-up (the raise's positive case) ends near the seed; marginal-earner and pollution-scale bound the floor ≤ 64; dual hot sets stay eventless; the RLS-CRASH and VETO branches (unreachable in the e2e corpus — verified) are covered by synthetic-signal scenarios instead.

## Rejected candidate

**Mid-walk admit-abort (healthy+blocked ends the walk with the ADMIT verdict, priced FAILED):** solved the "suspended walk blocks the parked admit" corner (alternating renewals + long silence leave the floor raised over a healthy set) and scored dramatically on the fuzz (mean excess 0.44, over-filter 0.000) — but it collapsed the CONFIRM mechanism by 90% (0.282→0.028): in ANY workload with a noise tail below the floor, the first healthy tide aborts the raise before the durable confirmation can fire, so the raise can never stick (the one-shot-key filtering the raise exists for is reduced to a 1-tide probe per growing backoff). The fuzz score was an artifact of the corpus lacking the one-shot-key regime the raise serves; by the no-overfitting criterion it was rejected. The suspended-walk corner is bounded (the walk resolves within its 16-sample budget once traffic resumes; a floor raised over a silent set is inert).

## Refinement (2026-08-13): deferred first arm step

The remaining fuzz regression was concentrated in short/noisy workloads: the ARM branch took the first 10→26 step on the SAME tide it armed, so a single distress sample could move the floor before the walk had produced a second sample. The first step is now deferred: ARM freezes the base and creates the walk, but `computeNextRaiseStep()` is not called on the arm tide; the first step happens on the next distressed tide that still passes the earner/ratio gates.

Paired 300-workload validation (fixed f1+f2+f3 vs +deferred-arm; lower is better except confirms):

| metric | fixed (f1+f2+f3) | + deferred arm | paired |
|---|---|---|---|
| arms | 0.667 | 0.637 | 16 improved / 8 worse, p=0.1516 |
| collapses | 0.307 | 0.107 | 52 improved / 1 worse |
| confirms | 0.317 | 0.403 | 41 improved / 17 worse, p=0.0022 |
| floor max | 17.78±9.89 | 13.17±7.69 | 82 improved / 4 worse |
| mean floor excess | 1.238±2.600 | 0.475±1.841 | 113 improved / 7 worse |
| harmful over-filter | 0.031 | 0.011 | 85 improved / 6 worse |
| floor flicker | 0.014 | 0.005 | 89 improved / 0 worse |
| floor end | 11.75±5.64 | 10.38±2.92 | 25 improved / 2 worse |
| admits | 0.270 | 0.113 | 41 improved / 4 worse |

The trade is that some walk endings shift from COLLAPSE to priced FAILED/CRASHED (fails 0.007→0.020, crashes 0.013→0.053) — the machine resolves a lower, less-damaging probe more often instead of ladder-resetting. Hand-scenario expectations adjusted with the behavior: the burst cadence, regime-shift and empty-collapse scenarios no longer produce the old COLLAPSE/ADMIT/VETO excursions because the floor never over-raises in the first place.

## Refinement (2026-08-13): raise step 16 → 8

After the deferred first arm step, a 1000-workload campaign plus adversarial scripts showed the remaining excess was still dominated by the first few raise steps being too large. `STEP_INITIAL` was lowered from 16 to 8. Paired 1000-workload validation (`+deferred-arm` at step 16 vs step 8):

| metric | step 16 | step 8 | paired |
|---|---|---|---|
| arms | 0.747 | 0.747 | 15 improved / 15 worse |
| floor max | 14.07 | 12.38 | 198 improved / 9 worse |
| excess | 0.530 | 0.347 | 166 improved / 39 worse |
| confirms | 0.471 | 0.465 | 20 improved / 25 worse, p=0.55 |
| harmful over-filter | 0.012 | 0.014 | 28 improved / 68 worse |
| flicker | 0.0061 | 0.0057 | 19 improved / 5 worse |
| floor end | 10.518 | 10.321 | 25 improved / 10 worse |

The one worsened axis is harmful over-filter fraction, but the absolute increase is 0.002 (0.2% of promoted tides), and step 8 wins or ties under every tested objective weight set once arms/crashes/fails are included. The more aggressive candidate `STEP_INITIAL=8 + TIDAL_CRASH_PERSISTENCE=2` improves the unweighted loss further but raises arms 0.747→0.847 and crashes 0.077→0.211; it loses under crash-heavy objective weights and was rejected as probe-cost overfitting.

## Consequences

- The raise-walk's verdict endings under oscillation are now priced FAILED/CRASHED instead of ladder-resetting COLLAPSE; the backoff throttles the probe loop as documented in ADR-0045.
- A raise cannot climb while the set is quiet (renewal 0) or while the members genuinely earn (ratio ≥ 1) — the walk holds and ends in a verdict.
- The collapse still cancels in-flight walks and resets all regime state; only priced ladder state survives.
- Known trade-off: in alternating one-shot-pollution regimes (keys earning once then zero), the evidence gates slow the climb, so the walk more often ends FAILED without confirming than the blind stepper did — the filtering is partial but bounded, and correctness is untouched (routing-only).
- Tests: `governor_raiseWalk_renewalZero_doesNotStep`, `governor_raiseWalk_ratioGate_holdsWhenSetEarns`, `governor_emptySet_collapse_pricesInFlightWalk` in `WaveCounterAdaptiveTest`; full doublebuffer + common suites green.
