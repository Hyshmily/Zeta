# Descent Slope-Verified Retreat (ADR-0059)

ADR-0058 made the VETO itself probe its direction (up: keep the raise / down:
retreat is only a hypothesis). What it left blind is the retreat's own
DESCENT: once the direction probe deepens to the interval/base target, the
stage-0 walk steps straight toward it without ever reading the live renewal
again — the interval and base claims were quantified at their positions when
they were planted (possibly many tides earlier), never re-measured on the way
down. A descent that passes through a healthy band walks PAST it to a stale
target; a descent that under-earns its own start keeps descending anyway,
piling the over-filtering on top of a wrong-first move.

The fix applies the same verification discipline one level deeper: the descent
itself is a slope probe. Every stage-0 stride reads the live renewal, and two
verification-event-only verdicts gate the move:

1. **Mid-descent recovery** — a rung on the way down that earns
   `RENEWAL_TARGET` (0.5) parks the retreat THERE and re-plants the anchor
   (floor + interval + claim) at the parked rung. The blind descent walked
   past the first healthy rung to a target that was never re-measured.
2. **Direction falsified (pivot)** — when the descent keeps under-earning its
   OWN start (the renewal sampled at the veto arm, refreshed by the last probe
   sample when the probe deepens, minus the veto noise margin) for
   `SLOPE_STREAK` (2) tides, the down direction is refuted BY THE SLOPE: the
   retreat stops at the current rung and PIVOTS — the stale anchor claim is
   discarded (WindowClimber's `Anchor.standDown`: a claim tested at its own
   position and found wrong), and the parked distress re-arms the RAISE
   direction (the reverse attempt) from the pivot rung.

Walk undos stay deliberately verdict-blind (ADR-0058: their ending — crash /
budget-spent-failed — already priced the direction, so a return is never a
direction probe).

## Status

Adopted (ADR-0059). Ported to `MoonsTidalForce` as shipped behavior (no
default-off switch — the ADR-0052/0053/0058 adoption pattern). The sim
mirrors it with the default-off `retreat_slope_probe` switch for
reproducibility (D3).

## Motivation

1. **A descent can pass a healthier position than its target.** The interval
   and base claims are frozen at plant time (a release-case re-plant, a probe
   re-plant, or a raise base from the last confirmed walk). The veto waited
   `VETO_STREAK` distress tides at the raised position, but the receiving end
   of the descent may have recovered meanwhile — the blind walk throws that
   recovery away (scenario F).
2. **The hole: judging retreat by health alone cannot solve
   direction.** "正确方向" (the correct direction) can be ABOVE the anchor:
   the deep anchor's claim may itself be stale (a release CONFIRM or a probe
   re-plant can freeze a claim at or above target), and the true optimum may
   sit above both the current position and the anchor. Health-gating only
   decides "is the current position bad" — never "is DOWN actually good".
   That second question is answered by the slope: if descending makes the
   renewal WORSE relative to the descent's own start, down is falsified —
   even when the anchor's claim looks healthy (0.7) and the veto fired
   against it (scenario G). This is Caffeine's "proactive reverse probe"
   idea (probe the reverse when the renewal refuses to recover during the
   retreat), re-contextualized to the repo's verification-event discipline.

## Design

```
VETO fires:
  retreatFromVeto = true
  retreatBaseRenewal = renewal        // the descent's own reference
  retreatWorseStreak = 0
  (then the ADR-0058 probe opens)

A veto probe deepens (stage 1 -> 0):
  retreatBaseRenewal = renewal        // refresh to the freshest reading at
                                      // the descent's starting position
  retreatStage = 0

retreat() while retreatStage == 0 && retreatFromVeto:
  if renewal >= RENEWAL_TARGET:                  // mid-descent recovery
    anchorFloor = floor                          // park at THIS rung
    anchorConfirmFloor = floor
    anchorRenewal = renewal
    retreating = false                           // descent stops here
  else if renewal < retreatBaseRenewal - vetoMargin():
    retreatWorseStreak++
    if retreatWorseStreak >= SLOPE_STREAK:       // down falsified by the slope
      anchorFloor = 0                            // discard the stale claim
      anchorRenewal = 0
      anchorConfirmFloor = 0
      retreating = false                         // PIVOT at the current rung
      distressTides = 0                          // parked: re-arm the reverse
  else:
    retreatWorseStreak = 0
  // otherwise: one budgeted stride toward retreatTarget, as before
```

- `SLOPE_STREAK = 2` — the descent-scale window, the same persistence the
  ADR-0058 probe uses; a single tide would let a noise blip pivot a retreat.
- `vetoMargin()` reuses the anchor veto's noise margin (`MAX(0.1, 2σ)`): a
  flat or gently drifting band never accumulates worse samples (scenario H —
  threshold-inclusive noise protection).
- The reference is the descent's OWN start (`retreatBaseRenewal`), NOT the
  anchor claim: "worse than where we began" is the falsification that needs
  no belief in any planted claim, stale or fresh.
- Pivot semantics: the descent was falsified → no claim survives it. The
  anchor discard is WindowClimber's standDown — a claim tested (by the
  descent) at its own position and found wrong — never the rejected ADR-0053
  `anchor_track` (the discard is event-gated, the re-plant stays
  verification-only).
- Which retreats slope-probe: the veto descent only (`retreatFromVeto`).
  `undoWalk` forces it off (a verdict already priced the direction); the
  recovery park and pivot clear it with the retreat.
- `collapse()` and the R3 `standDown()` at-anchor branch reset the three new
  fields with the anchor (a regime change discards the descent probe too).

## Validation

### Sandbox (`zeta-tidal-sim`)

Baseline = shipped Java config (GOV_FIXES + confirm_admit + confirm_shield=8 +
hard_budget — carries D1+D2); candidate = D3 (`retreat_slope_probe=True`).
`item_campaign.py`, 150 expanded + 100 legacy seeds + headline + hand
scenarios + gov_fuzz:

| gate                       | result                                                          |
| -------------------------- | --------------------------------------------------------------- |
| expanded 150 paired        | **byte-identical zero** on every axis (0/150 worse, sign_p=1.0) |
| legacy 100 paired          | byte-identical zero                                             |
| hand scenarios (27)        | all identical, 0 failures                                       |
| headline checks            | earn1/miss2 92.8%, churn peak 1024, both configs                |
| gov_fuzz on the D3 config  | 40×4000 = 160k decisions, all invariants held                   |
| A/B/C/C2 (ADR-0058 family) | D3 == D byte-identical regression                               |

The vetoes = 0 structure fact (ADR-0058) means the descent branch is
unreachable on the real corpora — the zero is not a claim of benefit, it is
the no-side-effect gate. The anchor-direction question is judged on the
forced-veto family (`anchor_dir_scen.py`), extended by three scenarios:

| scenario                                                                                            | shipped (blind descent)                       | D3 (slope probe)                                                                                                             | verdict                          |
| --------------------------------------------------------------------------------------------------- | --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| F) descent crosses a healthy band                                                                   | VETO → walks past to 18 (never re-measured)   | parks at the FIRST healthy rung 21, anchor re-plants (21, 0.6, 21), `VETO-VERIFIED`                                          | mid-descent recovery             |
| G) optimum ABOVE the veto position (anchor claim stale 0.7, renewal strictly rising with the floor) | VETO → VETO → dead-parks at the stale base 10 | descent falsified at 19 (`VETO-PIVOT`), stale claim discarded, reverse raise re-climbs → `CONFIRM+ADMIT`; min floor 19 vs 10 | direction falsified by the slope |
| H) flat regime-wide decay (claim 0.7, renewal 0.35 everywhere)                                      | —                                             | **byte-identical**: completes 26→18→10 exactly, no invented park/pivot, `retreatWorseStreak` stays 0                         | flat-band noise protection       |

The G choreography records a real discovery: with a fresh ladder (no
confirm-shield backoff), the ARM races the veto — `shouldArmRaiseWalk` fires
on the FIRST distressed tide while the veto needs a 4-tide streak. In the
real machine the pre-veto ARM suppression is the fresh-confirm shield /
ladder-backoff era; scenario G reproduces that by holding the ARM's own gates
(ratio ≥ 1, blocked = 0) until the pivot, then re-engaging them (ratio 0.5,
blocked > 0) for the reverse attempt — the ARM gate structure itself is
unchanged.

### Java

`MoonsTidalForce`: new fields `retreatBaseRenewal` (double),
`retreatWorseStreak` (int), `retreatFromVeto` (boolean) and the constant
`SLOPE_STREAK`; `distress()` arms the slope state with the veto; the probe's
deepen refreshes the base reference; `retreat()` reads the renewal before each
stage-0 stride (recovery park / falsification pivot); `undoWalk` clears the
slope state; `collapse()` and `standDown()` reset it. Three new tests pin the
behavior: `governor_retreatSlope_midDescentRecoveryParks` (F),
`governor_retreatSlope_persistentWorseningPivots` (G — including the reverse
raise re-confirming at 27), `governor_retreatSlope_flatBandCompletesDescent`
(H). All 101 `WaveCounter*` tests green and the full common suite (1804
tests) green.

## Rejected (with evidence)

- **Probing the reverse OF THE VETO RETURN itself (the literal Caffeine
  "reverse probe" placement).** The repo's walk-undo discipline (ADR-0058)
  already prices direction at the ending: a walk undo is a verdict-driven
  return, never a direction probe. Where a reverse probe is genuinely
  needed is where NOTHING has yet priced the direction — the veto's own blind
  descent — which is what ADR-0059 probes. The pivot stops at the falsified
  rung. and the ordinary parked ARM flow (unchanged machinery) supplies the
  reverse attempt.
- **A single worse tide (`SLOPE_STREAK = 1`).** The veto already persisted 4
  distress tides, but the descent re-reads a DIFFERENT position each stride
  (a fresh measurement every tide) — one sample could mix stride noise with a
  genuine near-threshold band; 2 samples cost one tide and match the
  ADR-0058 probe's own persistence scale.
- **Sloping against the anchor claim instead of the descent's own start.**
  The claim can be stale in EITHER direction (too high, scenario G, or too
  low, a release re-plant after a dip) — believing it on the way down would
  make the falsification depend on exactly the value being tested.
- **Default-off switch in Java.** Rejected for the shipping form, per the
  adoption pattern of ADR-0052/0053/0058: a validated governor fix ships
  directly; the sim keeps the default-off switch so the paired evaluation
  stays reproducible.

## Cost

A genuine collapse whose descent happens to cross a temporarily-healthy rung
now parks there (2-tide probe-style delay before the continued descent can
resume — bounded by the relapse gates that re-armed the veto). A descent that
would have dumped to a STALE claim now pivots: the pivot rung is retained
instead of the base, and the recovery lands via the ordinary ARM path — the
reverse attempt — rather than a free-fall return. On every real corpus, where
the veto never fires, the cost is exactly zero (measured byte-identical).
