# ADR-0051: Governor Probe-Machine Bounds (Confirm-Admit, Confirm Shield, Hard Walk Budget)

The promotion-floor governor's probe machine (ADR-0045) showed three gaps under the expanded random-workload corpus (`zeta-tidal-sim/fuzz_expand.py` — 22 phase families, 300+ seeds, zipfian skew, migration, pulsing volume, burst trains, rotating bands, per-key periodic earners) and under a direct state-machine fuzzer (`gov_fuzz.py`, Caffeine `WindowClimberFuzzer` methodology). We decided three bound refinements, all validated sandbox-first and paired per seed.

## 1. Confirm-Admit (C1)

A raise-walk CONFIRM that lands with a non-empty blocked band (`floor > boundary`, keys the histogram boundary would admit, set healthy) is over-filtering. The parked branch would ADMIT next tide; the confirmation now corrects to the boundary immediately, and the veto anchor plants at the corrected position (an anchor at the stale base would let a later distress veto the corrected floor back down — a move that changes nothing since the threshold is `max(floor, boundary)` — costing flicker). The walk still gets its durable 3-tide confirmation: the rejected mid-walk admit (ADR-0046) aborted on the FIRST healthy tide and collapsed the confirm mechanism (0.282→0.028); this fires only AT confirmation, so the confirm mechanism is preserved (paired 250-seed campaign: confirms 2 improved / 4 worse, p=0.69 — not significant). Wins: excess 34/250, harmful over-filter 34/250, admit latency 34/250 (all p<0.0001). Worst-case seed 154: admit latency 8→2, harmful 0.036→0.009; seed 36: floor end 32→10.

## 2. Confirm Shield (C42)

A confirmed raise left the raise ladder rewarded (`rung 1`, no wait), so the machine could re-arm on the very next distressed tide — the alternating-workload ratchet (arm at F, 3 at-target, confirm at F, immediate re-arm; each cycle that passes the step gates climbs the floor step by step until the empty-set collapse undoes the probe; seed 37: 11 arms / 9 confirms / floor max 39 / 2 collapses). The CONFIRM now leaves the RAISE ladder refractory for `CONFIRM_SHIELD = 8` tides (rung untouched; the release ladder is separate; Caffeine's park-shield analog is 32 samples — four times this). Shield sweep (s4/s6/s8, 250 paired workloads + 60 legacy): s8 wins the harm axes on both corpora — expanded excess -49% (0.140→0.071, 53/250, p<0.0001), harmful over-filter -25%, admit latency -30%, arms -14%, floor max -0.45; legacy excess 0.116→0.088 with confirms 0.367→0.383 (not worse). The confirm cost (expanded 0.784→0.648, 27/250) concentrates in the vacuous re-arm cycles the shield exists to suppress.

## 3. Hard Walk Budget (C45)

The raise walk's `TIDAL_WALK_BUDGET = 16` check lived only on the below-target verdict branch. The state-machine fuzzer surfaced the contract violation: an alternating walk (at/below cycles that never reach 3 consecutive of either) outlived the documented budget by up to ~1.5x (samples 17-18 with the budget at 16), holding the floor hostage in alternating regimes. The budget now binds on BOTH branches (an at-target tide at budget → FAILED). Paired campaigns show it net-neutral on the fuzz corpora (6/250 excess wins, 3/250 confirm losses at 250 seeds) while restoring the documented bound — a genuine contract fix.

## 4. Rejected: Flood Gate 2500 (C23)

`FLOOD_RATE_PER_SEC` 5000 → 2500 measured a modest win on the expanded corpus (collapses 0.092→0.032, excess -0.019 in the combined config) — but it broke the test-documented semantics: at 2500 the P2 signature also fires on LEGITIMATE quiet windows at 2500-5000 counts/sec — a raise-walk in flight over a set that briefly reads renewal 0 (the classic quiet-tide test scenario, 4000 counts/sec) — flood-collapsing the walk and locking the ARM on a transient, not a regime change. The 5000 gate reserves the instant collapse for genuinely high-volume stale floors; moderate stale floors keep the bounded self-healing (4-tide decay + empty-set collapse, still bounded in the corpus). Rejected; the gate stays 5000.

## Status

accepted (C1 + C42 + C45). Landed in `WaveCounter.MoonsTidalForce` with the sim defaults unchanged for reproducibility; the sim carries the candidate switches (`confirm_admit`, `confirm_shield`, `hard_budget`, `flood_rate`) with `final_validate.py` as the shipped-config mirror.

Final validation battery (`zeta-tidal-sim/final_validate.py`):

**Expanded corpus (fuzz_expand, 300 seeds, paired):** arms 1.587→1.420 (41/300 improved / 1 worse, p<0.0001), excess 0.198→0.136 (−31%, 61/300, p<0.0001), harmful over-filter 0.014→0.010 (−29%, 50/300, p<0.0001), admit latency 0.700→0.507 (−28%, 47/300, p<0.0001), floor max 14.58→14.17 (15/300, p=0.0001), crashes 0.317→0.287 (9/300, p=0.02), flicker (21/300, p=0.0025), collapses 0.120→0.103; confirms 0.830→0.693 (5/300 improved / 34 worse) — the designed shield trade, concentrated in the vacuous re-arm cycles; regressions 0/300; failure modes 293 ok (shipped 292).

**Legacy corpus:** 200 seeds — all axes equal or better (excess 0.252→0.220, harmful 0.009→0.007, admit latency 0.300→0.235, arms 0.625→0.590, confirms 0.370→0.345); 60-seed spot: excess 0.116→0.091, confirms tied 0.367.

**Headline checks:** earn1/miss2 coverage 92.8% unchanged; full-churn occupancy peak ≤ hotLimit.

**Hand scenarios (final vs shipped per-scenario regression gate):** 0 failures; the historical fs/m4 event expectations are stale for the shipped machine itself (they were written for the pre-deferred-arm "tidal" config and the pre-normalization m4-java baseline — e.g. the drift ARM@12 is a phase artifact the shipped reading eliminates), so the gate compares harm axes and event kinds per scenario; several scenarios IMPROVE (empty_collapse excess 3.91→0.53 in the m4-java comparison, pollution_scale harmful 0.300→0.150, stale_flood and regime_shift excess down, no new COLLAPSE/FLOOD anywhere).

**State-machine fuzzer (`gov_fuzz.py`, Caffeine WindowClimberFuzzer methodology):** 1.2M+ random synthetic readings, all invariants held; the fuzzer is what surfaced the C45 contract violation (walk samples 17-18 against the documented budget of 16).

## Considered / Rejected

- **Mid-walk admit-abort** (rejected in ADR-0046, re-confirmed here): collapses the CONFIRM mechanism in any workload with a noise tail.
- **RAISE_ARM_DELAY 2** (rejected ADR-0045 §IV): wins probe-cost axes but reduces confirms — the walk's verdict machinery is the designed noise filter.
- **Shield 4/6**: s4/s6 win less on the harm axes for the same confirm cost; s8 chosen.
- **Budget 12** (C28): converts pending confirms into FAILED (16 seeds worse at 200), rejected — the budget contract fix (C45) already bounds the walk.
- **Flood gate 2500** (C23): see §4 — fires on legitimate quiet windows; rejected.
- **P1 quiet / veto / anchor semantics**: unchanged (vetoes unreachable in both corpora, 0.000); the veto anchor plants at the base on a plain confirm and at the corrected position on a confirm-admit (a veto back below the corrected floor changes nothing — the threshold is `max(floor, boundary)` — and costs flicker).
