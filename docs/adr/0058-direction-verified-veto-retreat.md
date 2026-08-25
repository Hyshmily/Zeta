# Direction-Verified Veto Retreat (Caffeine direction-probe analog)

The MoonsTidalForce veto was audited for the same disease Caffeine's
`WindowClimber` solves with its audit walk's **direction probing**: the anchor
validates a position, but the DIRECTION of the next move (down toward the
anchor, or stay) can never be known a priori — it must be verified against the
live goal metric.  The shipped Java veto plants one anchor (`anchorFloor`)
and, on `VETO_STREAK` distress tides above it with a renewal below the
anchor's claim minus the noise margin, retreats STRAIGHT to it.  That is a
one-direction hypothesis: the confirmed raise is treated as fully wrong the
moment the veto fires, even though the 3-tide at-target confirmation
verified the raised floor was a good equilibrium moments earlier.

The composite fix (validated in the `zeta-tidal-sim` sandbox before porting)
combines the three candidate ideas — Caffeine's direction stride probe, the
dual/interval anchor, and a verification-gated re-plant:

1. **Interval anchor (`anchorConfirmFloor`)** — the raise CONFIRM plants a
   SHALLOW veto target at the position the 3-tide confirmation just verified
   (the raised floor), while the base anchor (`anchorFloor`, the position the
   walk left from) stays as the DEEP target.  The interval is
   `[base, confirmFloor]`; the release CONFIRM plants both at the descended
   position (no interval above it).
2. **Direction stride probe (`retreatStage` 1)** — the veto no longer commits
   to the retreat immediately: it HOLDS the veto position and samples the
   live renewal for `PROBE_STREAK` (2) tides.
3. **Verdict-gated decisions** — a recovery (renewal at or above
   `RENEWAL_TARGET` 0.5) within the window settles at the held position and
   RE-PLANTS the anchor there (Caffeine's "only keep a position on a clear
   verdict"); a persistent shortfall deepens to the interval target when the
   floor stands measurably above it, else to the base.

The re-plant is deliberately **verification-event-only** (at a probe verdict,
never the unconditional resync of the rejected ADR-0053 `anchor_track`
candidate, whose every-tide re-plant degraded the hand scenarios).

## Status

Adopted (ADR-0058).  Ported to `MoonsTidalForce` as shipped behavior (no
default-off switch — the same adoption pattern as ADR-0052/0053: a validated
governor fix ships directly).  The sim mirrors it with the default-off
`interval_anchor` + `veto_dir_probe` switches for reproducibility.

## Motivation

1. **The veto's direction is an untested hypothesis.**  The shipped machine
   plants the anchor at the raise's base — the position the walk LEFT FROM —
   while the floor it actually validated sits at the confirm floor,
   measurably above.  When distress later fires the veto, the machine
   retreats PAST the verified position to the base, throwing away the
   confirmation's evidence.  A dip-and-recover workload (scenario A) shows
   the failure: shipped retreats 18 → 10 unconditionally, while a
   direction-verified machine keeps the raise (the anchor re-plants at 18).
2. **The evaluation mechanism exists but cannot judge direction.**  The
   veto's margin (`MAX(MIN_TIDAL_EVIDENCE, 2*deviation)`) measures whether
   the current position under-earns the claim — it cannot distinguish "the
   raise was wrong" from "the workload dipped briefly".  The probe samples
   the same signal over a small window and lets the verdict gate the move.

## Design

```
VETO fires (distressTides > VETO_STREAK && floor > anchorFloor + ANCHOR_BAND
            && renewal < anchorRenewal - vetoMargin()):
  retreatTarget   = floor            // hold the veto position
  retreatStage    = 1                // direction probe live
  retreatProbeLeft = PROBE_STREAK    // 2 sampling tides

retreat() while retreatStage == 1:
  retreatProbeLeft--
  if renewal >= RENEWAL_TARGET:      // recovery: the raise was right
    floor = retreatTarget            // settle at the held position
    anchorFloor = floor              // re-plant: this is the new reference
    anchorRenewal = renewal
    anchorConfirmFloor = floor
    retreatStage = 0                 // probe consumed
  else if retreatProbeLeft <= 0:     // shortfall persists: direction correct
    retreatTarget = (anchorConfirmFloor > anchorFloor
                     && floor > anchorConfirmFloor + ANCHOR_BAND)
        ? anchorConfirmFloor         // shallow: stop at the last verified rung
        : anchorFloor                // deep: the base
    retreatStage = 0                 // legacy single-stage walk resumes
```

- `anchorConfirmFloor` plants at the raise CONFIRM (the raised floor; the
  confirmed-admit correction plants it at the corrected floor — a veto below
  it changes nothing, the ADR-0051 position) and at the release CONFIRM
  (the descended position, equal to the anchor: no interval).
- The probe samples `PROBE_STREAK = 2` tides — a single tide would let a
  noise blip settle a veto; the walk's own confirm costs 3, scaled down for
  the veto path (the veto already waited a 4-tide streak first).
- `retreatStage == 1` is armed ONLY by the veto; a walk undo (`undoWalk`)
  stays single-stage (a verdict-driven return never probes).
- `collapse()` and the R3 `standDown()` at-anchor branch reset the three new
  fields with the anchor (a regime change discards its interval too).

## Validation

### Sandbox (`zeta-tidal-sim`)

Baseline = shipped Java config (`GOV_FIXES + confirm_admit + confirm_shield=8
+ hard_budget`); candidate = interval_anchor + veto_dir_probe (the composite).
Run via `item_campaign.py`, 150 seeds (expanded) + 100 (legacy):

| corpus | config | vetoes | expanded paired delta (all axes) | regressions |
| ------ | ------ | ------ | -------------------------------- | ----------- |
| expanded 150 | D1+D2 | 0 | **byte-identical zero** | 0/150 |
| expanded 150 | D1 only | 0 | byte-identical zero | 0/150 |
| expanded 150 | D2 only | 0 | byte-identical zero | 0/150 |
| legacy 100 | D1+D2 | 0 | byte-identical zero | 0/150 |
| hand scenarios (26) | D1+D2 | — | all byte-identical | 0 failures |

That is the ADR-0053 lesson restated: **vetoes = 0 on the existing corpora,
so the veto path is unreachable and every anchor change must be zero-delta
there** — the anchor-direction question can only be judged on scenarios that
FORCE the veto.  `anchor_dir_scen.py` adds that family (the gap ADR-0053
left open):

| scenario | shipped | D1+D2 | verdict |
| -------- | ------- | ----- | ------- |
| A) dip, then recovery in-window | VETO → retreat 18→10, raise lost | `VETO-VERIFIED` keeps 18, anchor re-plants | probe direction correct |
| B) persistent collapse | VETO → 10 | VETO → 10 (probe fails, deepens) | probe never holds the floor hostage |
| C) floor above confirmFloor | contiguous 26→10 (parks on rung 18 for 1 tide) | staged: parks at 18 for 8 tides (verify), then 10 | interval target honored |
| C2) recovery while parked at 18 | already at 10 by t=15 | still at 18 at t=15 (audit window) | confirmed rung preserved |

Headline checks (earn1/miss2 coverage 92.8%, churn peak 1024) pass for all
three configs; gov_fuzz 20×4000 invariants hold on the D1+D2 config.  The
cost of direction verification is the probe's time-to-retreat: a genuine
collapse reaches the base a probe window (2 tides) later, and a deep retreat
stages through the verified rung (C scenario) before deepening — the price of
not throwing away confirmed evidence.

### Java

`MoonsTidalForce`: new fields `anchorConfirmFloor`, `retreatStage`,
`retreatProbeLeft`; `retreat()` now takes the renewal (the probe's verdict
signal) and handles the stage-1 probe; `distress()` arms the probe instead of
a direct retreat; both CONFIRM sites plant the interval; `collapse()` and
`standDown()` reset it; `undoWalk` forces single-stage.  The existing veto
unit choreography (16 distress tides, floor 13/18 band test) passes unchanged:
the probe deepens within the test windows, so `retreatTarget` assertions see
the same end state.  Two new tests pin the ADR-0058 behavior itself:
`governor_vetoProbe_recoveryKeepsRaiseAndReplantsAnchor` (recovery inside the
probe window keeps the raise and re-plants the anchor at the held position)
and `governor_vetoProbe_persistentShortfallDeepensToIntervalAnchor` (a
persistent shortfall with the floor above the confirm deepens to the interval
anchor, not the base).  All 101 `WaveCounter*` tests green and the full
1804-test common module suite green.

## Rejected (with evidence)

- **A single probe tide (`PROBE_STREAK = 1`).**  Not measured as a sibling
  (the veto already persists `VETO_STREAK` = 4 distress tides before firing;
  a 1-tide recovery sample at the probe moment is a noise blip, and a
  persistent shortfall deepens on the very next tide anyway — the extra tide
  only disambiguates a borderline recovery).
- **Unconditional anchor track (re-plant/resync every parked tide).**  Already
  rejected at ADR-0053 (`anchor_track`): NOISE (over-filter +0.0455,
  22/120 seeds worse, admits 0.008→0.158).  The ADR-0058 re-plant fires only
  at a probe verdict — the correction of that design, not a re-adoption.
- **Default-off switch in Java.**  Rejected for the shipping form (per the
  adoption request): ADR-0052/0053 fixes ship directly; the sim keeps the
  default-off switches so the paired evaluations stay reproducible.