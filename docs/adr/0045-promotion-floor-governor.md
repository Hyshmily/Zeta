# ADR-0045: The Promotion-Floor Governor (MoonsTidalForce)

The promotion floor — the noise filter between the scale-free histogram boundary and the promotion
threshold — is adapted to the workload by a probe-governed controller. This ADR is the governor's
complete design AND evolution record, consolidated in TWO merge passes:

- **First merge (original consolidation):** the release-walk discipline (formerly ADR-0044), the
  evidence-based rework around the reachable signals (formerly ADR-0045), and the three corrections
  that followed — the reachable anchor-veto retreat and durable raise confirmation (formerly
  ADR-0046), the per-direction retry ladders (formerly ADR-0047) and the verdict-based walk endings
  with crash-run ladder pricing (formerly ADR-0048). Their content is folded into Part I.
- **Second merge:** the three later refinements of the same component — Oscillation Probe Hygiene
  (formerly ADR-0046), Volume-Gated Regime Switches (formerly ADR-0047) and the Phase-Normalized
  Governor Reading (formerly ADR-0050) — are folded in as Parts II-IV with their full content
  (decisions, validation data, rejected options) preserved. NOTE: the former ADR-0046/0047/0048
  numbers of the first merge and the later independent ADR-0046/0047/0050 documents are DIFFERENT
  decisions that reused the numbering; the section titles disambiguate them.

The surrounding WaveCounter tide controls live in ADR-0038; the histogram-boundary refinement in
ADR-0042; the lock-free hot-add protocol in ADR-0043; the beacon decay sweep period in ADR-0049.

## Status

accepted — consolidated twice: with the former ADR-0044/0046/0047/0048 (Part I) and with the former
independent ADR-0046/0047/0050 (Parts II-IV).

---

# Part I — Governor Design (original ADR-0045)

## Context

`MoonsTidalForce` (nested in `WaveCounter`) adapts the promotion floor to the workload. Each
promoted tide it measures `renewal` = active hot slots whose key earned at least the threshold,
divided by active slots (probed: healthy single-key set 1.0, one-drifter pair 0.5, fully drifted
0.0); below the target (0.5) the set is distressed. Six problems drove the design:

1. **Releases were unadjudicated.** A release step that pushed the floor below the signal-capable
   region was only corrected indirectly (the parked-distress veto or the next audit), with no
   memory of where the release started and no budget bounding a failed descent.
2. **Audits could be suppressed forever.** The audit run (`auditTides`) was reset to 0 by ANY floor
   move; a workload whose renewal pulses periodically kept resetting the counter and never
   audited. Caffeine's `AuditClock.tick` decays a moving sample's run instead of zeroing it.
3. **No experiment budget or retry ledger.** No bound on a release's duration, no "worked vs
   failed" verdict, no backoff against re-testing a just-failed direction.
4. **The machinery was structurally unreachable in the count domain.** `renewal < 0.5` implies
   `blockedKeys > 0` (members in the stale tail of the 2-tide beacon memory set the boundary), and
   with keys blocked the admit drop is clamped at the seed — a no-op. The floor could never rise,
   so release walks, the veto retreat and the audit were all dead: the floor was permanently pinned
   at `PROMOTION_FLOOR = 10`. The scan gate compounded it: `promote()` computed renewal only when
   slots were free, hiding the full-set stale-squatting state where promotion matters most.
5. **The veto retreat was dead code.** The distress-episode re-freeze re-anchored the reference to
   the raised floor before the check ran, so `floor > probeBase` never held (the test reached the
   branch only via reflection injection of `probeBase`). And the raise-walk confirmed on the FIRST
   healthy verdict — one lucky tide kept a raise — while its crash required 3 consecutive
   below-target tides: a permissive, asymmetric gate.
6. **The retry ladder was shared and every crash escalated.** A crashed raise delayed the
   corrective release (the anti-ratchet channel a failed raise needs most) and a crashed release
   delayed the re-probe; and every crash doubled the rung, pricing a single crash —
   indistinguishable from an exogenous workload shift — as a completed failure.

## Decision

**Saturated visibility.** The renewal numerator, the hot-set earnings and the blocked count are
computed on EVERY promoted tide (`distinctKeys >= MIN_PROMOTION_KEYS`), including saturated ones:
the saturated path enumerates the post-decay membership directly (one O(n) pass over the snapshot,
deliverer-only). The governor now sees the full-set distress the scan gate hid. Since 2026-08-12
(adversarial audit M2) the saturated numerator counts ONLY beacon members that earned the
threshold — snapshot keys that are not members are cold keys waiting for a slot, and counting them
inflated renewal (1024 stale members + 600 new earners read 0.586 ≥ target, hiding the squatting
signal); the corrected reading is ~0, and the set self-heals via the 2-tide decay.

**Renewal-disambiguated blockedKeys.** Admit-on-block lives on the HEALTHY branch and only drops:
`blockedKeys > 0 && floor > max(seed, boundary)` lowers the floor to the boundary in one move (the
renewing keys the boundary would admit). Under distress the same band is the stale tail and is
never admitted.

**Walks run with probe discipline (Caffeine's WindowClimber).** A walk is armed BEFORE the first
step; `baseFloor`/`baseRenewal` (position AND renewal) are frozen at the arm and are what the
endings judge against. Each walk tide computes a verdict — `WalkEnding`: `CONFIRMED`/`CRASHED`/
`FAILED`/`WALKING`, Caffeine's `ProbeEnding` — and the walk branch acts on it in a switch, keeping
the decision separate from the mutation.

- **Release-walk** (healthy saturated or audit-due, floor above the seed): the stride is priced
  per tide by the smoothed renewal — the ring-mean signal — against the walk's own crash bar:
  `stride = clamp(round(16 * (ringMean - bar) / max(0.1, margin)), 1, 32)` where `bar =
  baseRenewal - margin` — comfortably healthy sets descend at up to 2x the initial step,
  approaching the bar the stride shrinks toward 1 so the verdict samples the decision zone at
  fine granularity, and a below-bar descent creeps instead of plunging to the seed before the
  crash verdict fires.  The bold-driver decay (`step *= 0.98`) is retired for this direction, so
  the release walk no longer touches the raise-owned step state.  A tide below the anchor-memory
  crash bar (`baseRenewal - margin`, not the fixed target — a 0.6 renewal against a 0.85 base
  reads as below-bar) increments the crash streak; 3 consecutive below-bar tides CRASH the walk
  (budgeted return to `baseFloor` + backoff); 16 healthy samples CONFIRM it (the descended
  position is kept, becomes the new veto anchor).
- **Raise-walk** (distress with the hot set under-earning the cold reservoir, `hotColdRatio < 1` —
  the frozen-set signal): confirms only after the set holds the target across
  `TIDAL_CRASH_PERSISTENCE` consecutive at-target tides (durable — a single lucky tide must not
  keep a raise); 3 consecutive below-target tides CRASH it; spending the 16-tide budget without a
  verdict prices it as FAILED. The bold driver steps only while the set is still distressed —
  and since Part II only while SOME member still earns the threshold (`0 < renewal < target`):
  a renewal of 0 means the set is quiet or dead, and climbing cannot help it (the un-gated climb
  outran the earners on oscillating workloads and self-inflicted the empty-set collapse);
  at-target tides hold so the confirmation streak accumulates. The confirm plants the veto anchor
  at the position the raise left from, so a later distress can undo a raise that failed to recover
  the signal.

**Anchor-memory veto (WindowClimber's anchor veto).** The anchor is the position the last
confirmed walk settled on, planted at confirmation: a raise-walk confirm plants it at the position
the raise left from, a release-walk confirm at the descended position — a veto never undoes a
healthy-confirmed descent. The veto fires when distress survives `GOVERNOR_VETO_STREAK = 4` tides
while the floor is above the anchor and the current renewal earns less than the anchor's reference
minus the noise margin; the floor returns to the anchor in 8 budgeted strides. The margin is
`max(MIN_TIDAL_EVIDENCE = 0.1, 2σ)` over the last 8 renewals (a ring buffer — the WindowClimber
`Rates` deviation): noisy workloads need a wider evidence gap before a veto/undo fires, quiet ones
keep the floor margin.

**Per-direction retry ladders.** The raise and release directions own one `RetryLadder` each
(Caffeine's `Ladder`: rung + tides-left + crash run); an ending may only deepen the ledger of the
layer that produced it — a crashed raise must not delay the corrective release, nor a crashed
release the re-probe. The empty-set collapse resets both — except since Part II a collapse that
kills an in-flight walk prices the walk's OWN ladder (the backoff survives the reset, throttling
the oscillation probe loop; a genuine regime change with no walk keeps the full reset).

**Crash-run ladder pricing.** `RetryLadder.crashStreak` + `PROBE_CRASH_ESCALATION = 2`: `crash()`
holds the rung (the wait stays at `max(TIDAL_BACKOFF_INITIAL = 4, rung)`, refractory hold while
unpaid) — probe damage and an exogenous shift are indistinguishable on one crash, so it is not
priced as a failed experiment — and only a consecutive crash run doubles it (4→8→16→32).
`fail()` (budget spent without a verdict) always doubles and resets the run; `reward()` resets it.

**Movement decays the audit run.** Distress and release movement apply `auditTides = max(0, tides-1)`
instead of zeroing it, so one floor move per wait can no longer suppress audits forever. A healthy
audit (8 still tides) or saturation (≥ 90% of `hotLimit`) arms a release walk.

**Idle collapse and regime reset.** An empty hot set (`activeSlots == 0`) proves the floor exceeds
the whole distribution: the floor collapses to the seed immediately, any in-flight walk or budgeted
return is cancelled (a stale undo would drag the floor back toward its old base), and the full
regime state — distress history, audit run, step, veto anchor, both ladders, the renewal ring — is
reset so the new regime starts from a blank slate. Since Part II the ladder reset is conditional:
a collapse that killed an in-flight walk is the walk's own fault (the raised floor outran the
earners) and is priced as FAILED with the backoff restored after the reset. An unplanted anchor is
inert: renewal is never below `0 - margin`.

## Considered Options

- **Keep blind release steps.** Cheapest, but leaves releases unadjudicated; the veto retreat
  would remain the only brake on a failed descent. Rejected.
- **Full Caffeine density verdict (hits-per-capacity density).** WaveCounter has no per-region
  density signal analogous to the W-TinyLfu window/main split — the promotion floor is a 0/1
  filter, not an allocation between regions. The renewal share is the closest goal metric and is
  already computed. Rejected as inapplicable.
- **Scatter-priced exit bars.** More Caffeine-faithful, but adds a Rates-style smoother and a new
  tuning surface; the fixed goal target is the threshold the whole governor already uses. Deferred.
- **Delta-based noise band (first-difference deviation).** A level ring with the same margin
  formula was validated in the sandbox and is one less tuning surface. Deferred.
- **Park shield for the confirmed position.** Caffeine shields a freshly parked anchor from
  workload-shift stand-downs; WaveCounter's anchor equivalent is the planted veto anchor, which the
  veto already defends. Rejected as inapplicable.
- **Remove the governor entirely (constant floor).** Honest about the reachability result, but
  throws away the floor's real role (an absolute noise floor) and the machinery the
  saturated-visibility signal now feeds. Rejected.
- **Unconditional raise (the ADR-0044-era direction).** Ratchets and starves low-traffic
  workloads; the raise-walk's verdict/undo/backoff is the bounded alternative. Rejected.
- **Delete only the `floor > probeBase` guard.** Makes the branch "reachable" as a periodic no-op
  that re-anchors the reference to the current renewal — functionless. Rejected.
- **Keep the episode-start freeze.** The root cause of the dead veto: any raise inside an episode
  re-anchors the reference to the raised floor within one tide. Rejected.
- **Un-durable raise confirmation.** One lucky tide above the noisy target keeps a raise;
  combined with the missing veto this is the exact ratchet the design claims to close. Rejected.
- **Keep the shared backoff ladder.** The cross-block is real but bounded (4-32 tides); rejected —
  the delay lands exactly where the failure already made the position worst, and the split is a
  mechanical change.
- **Keep the inline walk endings (no verdict enum).** The two latent inconsistencies (cumulative
  raise crash streak; unconditional escalation) stayed invisible inline; the verdict split surfaced
  both. Rejected.
- **3-value enum (no `FAILED`).** Collapses budget-spent into `CRASHED` and loses the pricing
  distinction the enum exists to carry. Rejected once the consecutive-semantics fix made `FAILED`
  reachable.
- **Keep the cumulative raise `crashStreak`.** `FAILED` stays dead code and the constant's
  "consecutive" contract stays wrong. Rejected — the streak now resets on at-target tides, truly
  consecutive and symmetric with the release direction.
- **Keep the bold-driver decay for the release stride.** The pre-existing law (16 initial step,
  ×0.98 per tide) is time-based — bolder at walk start, cautious as it runs — but blind to the
  renewal signal: it descends at ~16/tide whether the set is fully healthy or already below the
  crash bar, so a failing descent plunges ~46 units before the 3-tide crash verdict fires.  The
  sandbox (`law_sim`, WindowClimber transfer) showed the fixed decay compounded over long runs
  collapses any signal-driven stride, while a signal-priced stride alone cannot traverse — it
  needs an external stop, which the walk budget and verdicts provide. Replaced by the noise-
  priced stride law.
- **Target-anchored stride (`renewal - 0.5`).** The fixed goal target is the wrong reference for
  the release direction: the crash bar anchors at `baseRenewal - margin` (≈0.9 against a 1.0
  base), so the walk crashes long before renewal approaches 0.5 — the 0.5 anchor never binds and
  the law would stride at the ceiling straight through the boundary. Rejected; the stride
  anchors at the walk's own crash bar, so the gain self-converges exactly where the verdict
  needs fine sampling.

## Consequences

1. **The floor can genuinely move and no direction ratchets.** Raise-walks, healthy admit-drops,
   release walks and the anchor veto all have reachable arming states, each evidence-based and each
   released by another branch (audit/admit/saturation/veto).
2. **A failed experiment is bounded and undone; a healthy one is confirmed, not assumed.** Every
   undo returns to the frozen base (or the anchor) in `RETURN_BUDGET = 8` strides; every
   confirm keeps the position and rewards the layer's ladder.
3. **Behavior changes are bounded.** No veto can fire before the first walk confirmation; an
   unplanted anchor is inert; the raise crash needs 3 consecutive below-target tides; the ladder
   doubles only on a consecutive crash run or a budget-spent failure.
4. **Sanity results preserved.** The simulator still reports `driftRotation floor=[10,10]` and
   `stableZipf avgRenewal=1.00` — healthy workloads never move the floor.
5. **Tests.** `WaveCounterAdaptiveTest` (26 tests, including the no-reflection retreat test, the
   per-direction backoff isolation, the crash-run escalation and the release-stride law) plus the
   full common module (1848 tests) are green. Design changes were validated first on simulated
   sequences in a desktop sandbox before porting.
6. **Incidental cleanups landed with the rework:** the four near-identical steal+putIfAbsent+add
   merge sites are one `mergeKey` helper; promotion candidates cache their avalanched hash; and
   `estimatedSizeOfKeysCount()` reads the O(1) counter instead of the CHM `size()`.
7. **The release descent is signal-priced.** Below-bar tides creep (the pre-crash descent is
   bounded at ~24 units vs ~46 under the bold driver), at-bar renewals converge the walk into the
   decision zone where the verdict samples at fine granularity (it confirms above the seed
   instead of plunging to it), and comfortably healthy sets descend at the 2x stride ceiling
   within the walk budget.

---

# Part II — Oscillation Probe Hygiene (formerly ADR-0046)

A full-pipeline simulation (real request streams through a faithful port of `count()` → `tide()` → `promote()` → `MoonsTidalForce`) exposed defects in the Part I governor under oscillating/marginal workloads, and a 400-scenario randomized fuzz campaign (random phase scripts: hot/quiet/silence/oscillation/drift/burst, random hot-set sizes vs `hotLimit`, random noise scale, both cadences) validated the fixes and rejected one overfit candidate.

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

### Refinement (2026-08-13): deferred first arm step

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

### Refinement (2026-08-13): raise step 16 → 8

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

## Consequences (Part II)

- The raise-walk's verdict endings under oscillation are now priced FAILED/CRASHED instead of ladder-resetting COLLAPSE; the backoff throttles the probe loop as documented in Part I.
- A raise cannot climb while the set is quiet (renewal 0) or while the members genuinely earn (ratio ≥ 1) — the walk holds and ends in a verdict.
- The collapse still cancels in-flight walks and resets all regime state; only priced ladder state survives.
- Known trade-off: in alternating one-shot-pollution regimes (keys earning once then zero), the evidence gates slow the climb, so the walk more often ends FAILED without confirming than the blind stepper did — the filtering is partial but bounded, and correctness is untouched (routing-only).
- Tests: `governor_raiseWalk_renewalZero_doesNotStep`, `governor_raiseWalk_ratioGate_holdsWhenSetEarns`, `governor_emptySet_collapse_pricesInFlightWalk` in `WaveCounterAdaptiveTest`; full doublebuffer + common suites green.

---

# Part III — Volume-Gated Regime Switches (formerly ADR-0047)

The promotion-floor governor's signals (renewal, hotColdRatio, blockedKeys) are all ratios and one absolute count-level (the boundary). Ratios are scale-free by design, but the FLOOR is an absolute quantity, and two regime cases left the absolute floor wrong with no fast correction: a long low-traffic regime (the floor's noise role is vacuous, yet keys earning 1-9 counts/tide route cold forever — and the ADR-0038 decay freeze can strand a stale raised floor behind a frozen set) and a stale RAISED floor onto which a high-volume new distribution arrives (the wrong-direction raise-walk parks on the renewal==0 gate, and recovery waits for the 2-tide decay + empty-set collapse). We decided to add two volume-gated regime switches to `MoonsTidalForce`, validated first on simulated sequences in the desktop sandbox (`zeta-tidal-sim`, 300-seed fuzz: 0 regressions vs the Part II config, collapses -0.153, excess -0.164, confirms preserved, wall-clock-debounced).

**P1 (quiet bypass).** When the volume EWMA (`vol`, alpha 0.5, folded once per non-empty tide by the deliverer) is below `QUIET_VOLUME` (50 counts/tide) AND the hot set is empty (`remain == 0`) AND that condition has accumulated `QUIET_CONFIRM_MS` (2000ms) of wall time, the floor drops to `QUIET_FLOOR` (1) so ANY key routes hot; the `MIN_PROMOTION_KEYS` scan gate is simultaneously lifted while `quietVolume()` holds, so sub-minimum quiet snapshots still promote (and the decay runs, letting a stale set reach remain==0). The seed floor re-engages after `REENGAGE_CONFIRM_MS` (500ms) above `QUIET_REENGAGE_VOLUME` (2x entry — hysteresis). The confirms are WALL TIME, not tides: at the 50ms burst cadence a 4-tide confirm is only 200ms, and volume swings must not bounce the floor between 1 and 10.

**P2 (flood collapse).** When the floor is above the seed AND no key of a high-volume snapshot earns the threshold (`renewal <= 0`, `blockedKeys > 0`, `floor > boundary`, `distinct >= FLOOD_MIN_DISTINCT`, volume rate >= `FLOOD_RATE_PER_SEC` = 5000 counts/sec, normalized by the tide's accumulation interval) the floor is stale from another regime, not a noise filter: it collapses to the seed in ONE tide via the empty-set collapse's full reset (in-flight walk priced FAILED, priced ladders preserved — Part II). The collapse plants a sticky `floodLock` that suppresses the ARM until the volume drops below the flood rate: a stray earner (renewal 0.1) briefly breaks the signature, and without the lock one ARM→FLOOD ping-pong cycle re-occurs per ladder backoff expiry.

**What does NOT change.** The seed floor under a high-volume quiet window is the noise filter's designed job (the collapse gate requires floor > seed); the boundary stays scale-free; moderate-volume stale floors keep the existing bounded self-healing (2-tide decay + empty-set collapse); the raise-walk's stale-tail filtering is untouched (its own renewal==0 step gate already declared that case unclimbable — Part II).

**Why volume and not more ratio machinery.** The floor is an absolute threshold; its correctness depends on the regime's absolute scale. Ratios tell the governor the SHAPE of the set; volume tells it the SCALE of the regime. The two are deliberately not fused into one scalar — volume acts only as a gate (confidence) and a trigger (regime change), renewal stays the goal metric.

**Sandbox evidence.** 4 hand scenarios (quiet regime, stale-floor flood, relaxed-then-burst, sub-minimum quiet) + 300-seed fuzz vs the Part II config: regressions 0/300; collapses 0.307→0.153, excess 1.238→1.074, harmful over-filter 0.031→0.028, admit latency 0.733→0.677, confirms 0.317→0.307 (preserved), flicker 0.014→0.013. Rejected during validation: `blocked >= fraction * distinct` as the flood trigger (the blocked signal only counts the boundary bucket, undercounting the band — the renewal==0 signature replaced it); a literal "route everything hot" bypass of the boundary (equivalent to floor=1 at quiet volume, and boundary-selection keeps the set sane at medium volume); mid-walk admit (already rejected in Part II).

---

# Part IV — Phase-Normalized Governor Reading (formerly ADR-0050)

ADR-0049's sweep period (decay on every 2nd promoted tide) made the routing beacon's membership evidence freeze on skip tides, which split the promotion-governor's `remain` signal into two alternating measurement bases — skip tides report the frozen occupancy (evidence ≥ 1, the evidence-1 "death row" zombies included), decay tides report the post-decay occupancy (the evidence ≥ 2 cohort). The resulting phase oscillation transiently depressed `renewal`/`hotColdRatio` during stale windows and inflated the veto margin (measured in `lunar_check.py`: 0.39 vs 0.30), costing the raise-walk one extra step in stale windows and deferring the empty-set collapse to the physical sweep schedule. We decided that on sweep skip tides the governor's reading is measured on the **decay-equivalent basis**, so the governor sees the same signal on both phases.

**The reading normalization.** In `promote()`, the tide's phase is captured at the decay gate (`sweepDecay`). On a skip tide the reading switches to the decay-equivalent set: the reading's `remain` is the post-scan count-lane evidence ≥ 2 active count (`countActive(2)` — one shift + popcount per word, the same cost shape as `countActive(1)`), and the saturated-branch enumeration uses `isBeaconMember(h, 2)` (both role evidences ≥ 2), the frozen-beacon analogue of the post-decay membership test. By construction the skip-tide reading then equals the adjacent decay-tide reading — both are "the set a halving decay + this scan would have left" (evidence-1 lanes that did not earn stay 1 and are excluded; re-promoted evidence-1 earners count exactly like the decay tide's activations) — so `renewal`, `hotColdRatio`, the saturation gate and the empty-set collapse are phase-invariant. The promotion scan gate keeps the honest (evidence ≥ 1) occupancy, so capacity never oversells.

**Validation (sandbox-first, `fuzz_m4.py` — 120 random workloads, paired per seed, M4 sweep under the shipped governor, fuzz_sandbox methodology).**

| metric | m4-java | m4r | paired (improved/worse) | sign p |
| --- | --- | --- | --- | --- |
| crashes | 0.675 | 0.325 | 35/5 | <0.0001 |
| floor_max | 13.43 | 11.95 | 22/2 | <0.0001 |
| mean floor excess | 0.285 | 0.178 | 29/7 | 0.0003 |
| flicker | 0.012 | 0.006 | 22/1 | <0.0001 |
| probe arms | 1.067 | 0.867 | 24/2 | <0.0001 |
| admit latency max | 0.308 | 0.158 | 13/4 | 0.049 |
| confirms (responsiveness) | 0.233 | 0.317 | 16/8 | 0.15 (not worse) |
| harmful over-filter frac | 0.009 | 0.006 | 13/6 | 0.17 (not worse) |

Regressions vs baseline: 5/120 at 120 seeds (the same failure-mode distribution the baseline already has), 12/300 at 300 seeds (baseline's own failure modes: 10 zombie-walk + 2 overfilter + 1 ratchet vs m4r's 8 zombie-walk + 2 overfilter — the baseline's single ratchet case is fixed). Confirmed at 300 seeds (paired, `fuzz_m4.py`): crashes 0.663→0.317 (91/9, p<0.0001), arms 1.157→0.943 (70/16, p<0.0001), excess 0.371→0.252 (73/29, p<0.0001), floor_max 13.9→12.8 (51/17, p<0.0001), flicker 0.012→0.008 (54/12, p<0.0001), admit latency 0.42→0.29 (39/17, p=0.005), confirms 0.283→0.357 (39/21, p=0.027 — responsiveness significantly BETTER), over-filter 0.012→0.009 (40/24, p=0.06, direction good); collapses 0.090→0.133 (13/22, p=0.18, not significant — the collapse fires on the decay-equivalent schedule, i.e. earlier, which is the designed timing restoration). Headline checks hold for the shipped config: earn1/miss2 coverage 92.8% unchanged (100% excluding the cold-start tide — ADR-0049), full-churn occupancy peak ≤ hotLimit (no M4S-style oversell). With the shipped reading, `gov_interaction.py`'s four canned scenarios (square wave, P1 quiet, P2 stale-flood, drift) are **M2/M4 bit-identical** — the sweep's routing gain is delivered with zero governor-behavior delta vs the legacy 2-tide memory.

**Rejected.**
- **RAISE_ARM_DELAY 1 → 2** ("arm2"): wins the probe-cost axes (arms/excess/floor_max, p < 0.01) but reduces confirms 0.233 → 0.200 (4 improved / 10 worse, p = 0.18) — the responsiveness axis the raise exists for. Part II's principle stands: the walk's own verdict machinery (3-tide crash/confirm, budgeted undo, backoff) is the designed noise filter, not the arm delay.
- **VETO_STREAK 4 → 6 and LUNAR_MEMORY 8 → 12** ("veto6"/"ring12"): zero measurable deltas across all 120 seeds — the veto and release-stride branches are not reached by the workload corpus, so there is no evidence either way; per the no-change-without-evidence discipline they stay.
- **M4S (skip-tide evidence ≥ 2 for the scan GATE too)**: rejected in the ADR-0049 follow-up audit — the honest gate is what keeps real occupancy ≤ hotLimit (the strict gate oversells to ~2×hotLimit under full churn); the reading-only normalization achieves the legacy timings without touching the gate.

**Consequences (Part IV).** Skip tides pay one extra beacon popcount pass on the deliverer (same cost shape as the sweep it replaces, once per skip tide, never per op). The empty-set collapse and the P1 quiet bypass engage on the decay-equivalent schedule (legacy timing) instead of waiting for the physical sweep. No per-op routing cost, no config surface, no change to the governor's own constants or branches. The `zeta-tidal-sim` campaign (`fuzz_m4.py`) carries the candidate switches (`norm_reading`, `arm_delay`, `veto_streak`, `lunar_memory`) with the shipped config as `GOV_FIXES` defaults, so the rejected candidates stay reproducible.
