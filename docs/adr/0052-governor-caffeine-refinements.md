# WaveCounter Governor Caffeine Refinements (P2/R1/R2/R3/R5 adopted; R4/A1/A2 rejected)

The WaveCounter promotion governor and routing beacon were audited against the
Caffeine codebase (`FrequencySketch`, `WindowClimber`, `Pacer`); five
refinements were adopted and three rejected, each validated on the
`zeta-tidal-sim` sandbox (expanded corpus paired campaigns, legacy corpus,
hand scenarios with regression gates, gov_fuzz invariants) and the Java unit
suite before being shipped.

## Adopted

- **P2 — EMA renewal deviation (Caffeine `Rates`).** The noise-adaptive veto
  margin and the release stride law price their scatter from an EMA of the
  per-tide renewal absolute deviation (`RENEWAL_SMOOTHING = 0.2`, seeded wide
  at `DEVIATION_SEED = 0.05`), replacing the 8-sample ring's population std:
  an O(1) fold per tide instead of a two-pass ring sweep per margin/stride
  read, and less outlier-sensitive (a single noisy renewal cannot inflate the
  margin for a whole ring window).  Sandbox: zero delta on every axis (150
  expanded + 100 legacy seeds, hand scenarios, fuzzer) — a pure
  simplification with identical corpus behavior; only hand-crafted edge
  sequences differ (the veto needs ~14 distress tides instead of the
  8-tide ring wash-out, the below-bar descent creeps one count higher).
- **R1 — anchor stable band (Caffeine `Reading.stableBand`).** The veto
  requires the floor to stand measurably above the last confirmed anchor
  (`ANCHOR_BAND = max(1, FLOOR_MAX/50) = 5` counts): a floor within the band
  of its anchor retreating into itself changes nothing and costs flicker.
  Sandbox: zero delta everywhere (the veto path is defensive and rarely
  reachable on the corpora); the band absorbs 1-2-count jitter around a
  confirmed position.
- **R2 — audit-clock escalation (Caffeine `AuditClock.reschedule`).** A
  completed FAILED release walk at the deepest ladder rung plants
  `deepFail`; the next audit re-test of the release direction waits the
  doubled interval (`AUDIT_WAIT_MAX = 64`) instead of the standard 8.  The
  rung's own backoff already throttles intermediate rungs (the audit run
  accumulates during the backoff, so the inter-attempt gap is
  `max(backoff, wait)` — the wait law binds exactly where the backoff
  cannot), and a crash keeps the cadence (a crash is priced as a workload
  shift — deferring re-exploration would starve it).  Sandbox: zero delta
  (deepest-rung failed releases are unreachable on the corpora) — defensive
  hardening.
- **R3 — workload-shift stand-down (Caffeine `Anchor.standDown` +
  `Rates.reset`).** A single-tide goal-metric move at or above
  `RESTART_THRESHOLD = 0.05` announces a regime change: the parked machine
  discards its references (the renewal EMA pair, the distress streak, and
  the veto anchor when the shift happened AT the anchor position) and
  re-learns from here instead of smoothing across the shift.  The floor is
  deliberately untouched, the audit clock survives (position stillness is
  orthogonal to the rate — the failing test `governor_auditTides_decaysOnMovement`
  caught an early version that reset it), and the walk/retreat machinery —
  which expects renewal swings — never stands down.  Sandbox: fired in 50/50
  seeds (~55 firings/seed) with zero net delta on every axis; the initial
  version re-seeded the step and cost +0.07 excess on the empty-collapse hand
  scenario (a one-count-higher re-probe) — the step re-seed was dropped (the
  step is a convergence knob, not a stale reference).
- **R5 — beatBase confirm (Caffeine `Walk.isConfirmed`).** The release
  walk's confirm additionally requires the goal metric to have matched or
  beaten the arming renewal at least once (inclusive test): a budget spent
  entirely below the arming level is a confirm against a colder reference —
  a false confirm — and is priced as a completed FAILED experiment (undo +
  ladder escalation) instead.  The new FAILED verdict exposed a latent
  defect: the release-walk switch had no `case FAILED` (it existed only in
  the raise direction), so a below-base budget left the walk as an
  unresolvable zombie; the unit test
  `governor_releaseWalk_budgetBelowBase_failsInsteadOfConfirming` caught it
  and the case was added (the sim's branch had the same gap and was fixed
  in lockstep).  Sandbox: zero delta (healthy release walks always match
  their start on the corpora) — a confirmation-quality guard.

## Rejected (with evidence)

- **R4 — deep-rung stride amplification + FAILED step re-seed (Caffeine
  `Ladder.strideScale`).** A deep raise-ladder rung (>= 2x the initial
  backoff) made the raise step stride 2x/4x wider, and a FAILED ending
  re-seeded the decayed step.  Sandbox: 4 hand-scenario failures — the
  square-wave `ARM -> COLLAPSE` self-healing cycle is a regime cycle, not a
  stray zone, and the wider strides amplified the over-filtering
  (`square_500ms` excess 9.02 -> 10.45, harmful 0.300 -> 0.308, ratchet
  failure-mode 1 -> 2); the re-seed alone measured identical to shipped on
  every scenario.  Rejected: Caffeine's stray-zone premise does not transfer
  to the WaveCounter collapse-driven cycles.
- **A1 — traffic-driven beacon decay (Caffeine `FrequencySketch`
  sampleSize aging).** The decay sweep also fired when the traffic since the
  last sweep crossed `10 x hotLimit` counts — a flood tide ages the beacon
  proportionally to the traffic it carried, so stale members leave within
  1-2 flood tides instead of 4.  Sandbox (120 expanded seeds on the
  M4-sweep baseline): significant wins on the churn axes (arms -0.44,
  crashes -0.43, collapses -0.11, flicker down, confirms up, p < 0.05-0.001)
  but NET worse on all three harm axes (excess mean +0.134, harmful
  over-filter +0.0055, admit latency +0.15), 12/120 seeds with >20%
  regressions, zombies 2 -> 5, and the legacy corpus shows the same pattern
  (arms 0.90 -> 0.71 but excess 0.139 -> 0.235).  The wins and the harms are
  the same mechanism: cleaner evidence (stale members clear fast) lets raise
  walks confirm more often, so confirmed raised floors persist longer and
  over-filter the next distribution.  Threshold tuning (5x/10x/20x) changes
  nothing — flood tides cross all thresholds in one tide.  Rejected: the
  harm axes are the repo's primary acceptance criterion and A1 loses all
  three.
- **A2 — expired-lane threshold compensation (Caffeine's odd-count
  correction).** The next volume threshold was reduced by the count-lanes
  the halving expired.  Sandbox: zero marginal delta vs A1 on every axis —
  the zeroed per-sweep accumulator has no drift for the compensation to
  correct (Caffeine's formula adjusts a HALVED counter; ours is reset).
  Rejected with A1 (zero-delta candidates are not adopted, per the
  ADR-0045 §IV precedent).

## Sandbox infrastructure added

The `zeta-tidal-sim` sim gained the M4 sweep-period gate
(`m4_sweep`, mirroring the Java `sweepCountdown` — bit-identical to the
MemSim M4 parity on identical request streams, verified), the candidate
switches (`ema_deviation`, `anchor_band`, `audit_deep_double`,
`shift_detect`, `beat_base`, `deep_rung_stride`, `traffic_decay`,
`traffic_compensate`), the generic paired-campaign runner
(`item_campaign.py`) and the A1 battery (`a1_campaign.py`).
