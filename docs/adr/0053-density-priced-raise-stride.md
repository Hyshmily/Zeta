# Density-Priced Raise Stride (Caffeine `DensityClimber.steer` analog)

The MoonsTidalForce raise-walk's movement law was audited against Caffeine's
`WindowClimber`; the fixed bold-driver step with decay (`step * 0.98` per
step, re-seeded on every parked healthy/refractory tide) was replaced by a
density-priced stride that is a pure function of the tide's hot/cold density
ratio.  The change was validated on the `zeta-tidal-sim` sandbox (expanded
corpus paired campaign at 300 seeds, legacy corpus, hand scenarios with
regression gates, gov_fuzz invariants) and the Java unit suite before being
shipped.

## Status

Adopted (ADR-0053).  Six sibling candidates evaluated in the same campaign
were rejected with zero-delta evidence (see Rejected).

## Motivation

The raise-walk's bold driver stepped a fixed, decayed magnitude
(`STEP_INITIAL = 8`, `STEP_DECAY = 0.98`) on every evidence-gated distressed
tide.  Two defects follow from a fixed step near the decision zone:

1. **The self-eating overshoot.**  The step gate already requires
   `hotColdRatio < 1` (the hot slots under-earn the cold reservoir), but a
   fixed 8-count step ignores *how far* below the gate the ratio sits: a set
   at ratio 0.9 (marginal earners just under-earning) is stepped by the same
   8 counts as a set at ratio 0.1, pushing the threshold past the marginal
   earners' capacity and eating the walk's own confirmation.
2. **No convergence.**  The step decayed only while a walk was stepping and
   was re-seeded to `STEP_INITIAL` on every parked healthy/refractory tide,
   so the step never converged toward the equilibrium — every re-probe after
   a healthy pause started at full stride again.  (The R3 stand-down re-seed
   evaluated in the same campaign cancels the parked re-seeds exactly, which
   is why the convergence experiment measured zero delta — see Rejected.)

Caffeine solves both with `DensityClimber.steer`: the window moves by a
stride proportional to the log density-ratio error, which self-converges as
the error shrinks and needs no step state at all.

## Design

The raise stride becomes a pure function of the tide's density ratio:

```
stride = clamp(round(STEP_INITIAL * min(1, -log2(hotColdRatio))), 1, STEP_INITIAL)
```

- `hotColdRatio = 0.5` (a factor-two shortfall) strides the full
  `STEP_INITIAL = 8` — identical to the legacy first step, so the existing
  unit-test choreography (ratio 0.5 drives every legacy raise-walk test)
  is unchanged.
- A ratio approaching the 1.0 step gate strides 1 — the walk samples the
  decision zone at fine granularity and cannot overshoot the marginal
  earners.
- A deeper shortfall saturates at `STEP_INITIAL` — the arm's escape velocity
  is unchanged.
- The retired step-state knob (`step`, `STEP_DECAY`, the four re-seed sites)
  was removed: the stride is deterministic in the reading, so there is
  nothing to converge, decay, or re-seed.  `STEP_INITIAL` survives in its
  second role as the release walk's first stride (unchanged) and as the
  raise stride's ceiling.

## Validation

Sandbox (`zeta-tidal-sim`, baseline = shipped Java config
`GOV_FIXES + confirm_admit + confirm_shield=8 + hard_budget`):

| corpus | axis | shipped | candidate | paired |
| ------ | ---- | ------- | --------- | ------ |
| expanded, 300 seeds | excess | 0.136 | **0.066** | 60/300 improved, p=0.0001 |
| expanded, 300 seeds | floor_max | 14.17 | **13.37** | 51/300, p=0.0005 |
| expanded, 300 seeds | confirms | 0.693 | 0.713 | n.s. |
| legacy, 100 seeds | excess | 0.262 | **0.213** | — |
| legacy, 100 seeds | floor_max | 13.34 | **12.57** | — |
| hand scenarios | square_500ms excess | 9.02 | **7.40** | 0 gate failures |
| hand scenarios | regime_shift excess | 1.73 | **0.35** | 0 gate failures |
| hand scenarios | pollution_ramp excess | 0.40 | **0.15** | 0 gate failures |

Headline checks (earn1/miss2 coverage 92.8%, churn peak 1024), all 24 hand
scenarios, the 100-seed legacy corpus and the state-machine fuzzer pass.
The only cost axis: `walk_dur_mean` +0.12 tides (p=0.0034) — walks take more
small steps near the decision zone, which is the designed behavior; no harm
axis (harmful over-filter, flicker, admit latency, unresolved walks)
regressed significantly (3/300 seeds trip the >20% regression rule, vs the
paired wins on both primary axes).

Java: `computeNextRaiseStep(hotColdRatio)`; the old multi-step assertions
(18→25→32) become (18→26→34) at ratio 0.5 — the legacy decay's sub-step
granularity disappears; a new unit test
`governor_raiseWalk_densityPricedStride` pins the 0.9→1 / 0.5→8 / 0.25→8
law.

## Rejected (with evidence)

Six sibling candidates from the same Caffeine audit measured **zero delta**
across the full battery (120 expanded + 100 legacy seeds, 24 hand
scenarios, fuzzer — every axis byte-identical to shipped), i.e. their
guarded scenarios are unreachable on the real corpora:

- **shift shield (Caffeine `Anchor.isShielded` gating of the R3 stand-down).**
  Zero delta: the 3-tide healthy streak that confirms a raise has already
  pulled `lastRenewal` level with the current reading, so a freshly
  confirmed position is not re-discarded on the real workloads.
- **release-confirm shield (the ADR-0051 `CONFIRM_SHIELD` symmetry for the
  release direction).** Zero delta: the full arm condition (distress +
  `ratio < 1` + blocked + no backoff) does not recur within the shield
  window after a release confirm.
- **anchor track (Caffeine `Anchor.track` resync/re-plant).** Zero delta:
  the veto path is defensive (vetoes = 0 on the corpora), and the resync
  band coincides with the veto's exclusion band by construction.
- **audit move decay (Caffeine `AuditClock.tick` decay-on-movement applied
  to the ADMIT/walk/retreat moves).** Zero delta: the release arm after an
  ADMIT move is one-tide-later at most, and the corpora never exercise the
  sequencing where it binds.
- **step convergence (no parked re-seed; stand-down re-seed).** Zero delta
  by construction: the R3 stand-down — which precedes nearly every re-probe
  on the corpora — re-seeds the step exactly where the parked re-seeds were
  removed, cancelling the experiment.
- **shift min-signal gate (`remain >= 16` for the R3 test).** Zero delta:
  the stand-down's resets (EMA pair, distress streak) are consumed only by
  the veto and the release stride, neither of which binds on the small-remain
  readings the gate filters.
- **commitment depth (Caffeine `Ladder.commitmentDepth`).** Not implemented:
  Caffeine's commitment gates the audit's *adjudication* (confirm), which
  the release direction does not have (its confirm is budget-bound); the
  crash-abort persistence is rung-independent by design, so the mapping
  does not transfer.

All six switches remain in `tidal_sim.py` (default-off) so the evaluations
stay reproducible.
