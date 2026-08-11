# ADR-0045: The Promotion-Floor Governor (MoonsTidalForce)

The promotion floor — the noise filter between the scale-free histogram boundary and the promotion
threshold — is adapted to the workload by a probe-governed controller. This ADR consolidates the
governor's complete design: the release-walk discipline (formerly ADR-0044), the evidence-based
rework around the reachable signals (formerly ADR-0045), and the three corrections that followed —
the reachable anchor-veto retreat and durable raise confirmation (formerly ADR-0046), the
per-direction retry ladders (formerly ADR-0047) and the verdict-based walk endings with crash-run
ladder pricing (formerly ADR-0048). The surrounding WaveCounter tide controls live in ADR-0038; the
histogram-boundary refinement in ADR-0042; the lock-free hot-add protocol in ADR-0043.

## Status

accepted — consolidated with the former ADR-0044/0046/0047/0048 (their content is folded in here).

## Context

`MoonsTidalForce` (nested in `WaveCounter`) adapts the promotion floor to the workload. Each
promoted tide it measures `renewal` = active hot slots whose key earned at least the threshold,
divided by active slots (probed: healthy single-key set 1.0, one-drifter pair 0.5, fully drifted
0.0); below the target (0.5) the set is distressed. Six problems drove the design:

1. **Releases were unadjudicated.** A release step that pushed the floor below the signal-capable
   region was only corrected indirectly (the parked-distress veto or the next audit), with no
   memory of where the release started and no budget bounding a failed descent.
2. **Audits could be suppressed forever.** The audit run (`auditAge`) was reset to 0 by ANY floor
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
deliverer-only). The governor now sees the full-set distress the scan gate hid.

**Renewal-disambiguated blockedKeys.** Admit-on-block lives on the HEALTHY branch and only drops:
`blockedKeys > 0 && floor > max(seed, boundary)` lowers the floor to the boundary in one move (the
renewing keys the boundary would admit). Under distress the same band is the stale tail and is
never admitted.

**Walks run with probe discipline (Caffeine's WindowClimber).** A walk is armed BEFORE the first
step; `baseFloor`/`baseRenewal` (position AND renewal) are frozen at the arm and are what the
endings judge against. Each walk tide computes a verdict — `WalkEnding`: `CONFIRMED`/`CRASHED`/
`FAILED`/`WALKING`, Caffeine's `ProbeEnding` — and the walk branch acts on it in a switch, keeping
the decision separate from the mutation.

- **Release-walk** (healthy saturated or audit-due, floor above the seed): the bold driver steps
  the floor down per tide; a tide below the anchor-memory crash bar (`baseRenewal - margin`, not
  the fixed target — a 0.6 renewal against a 0.85 base reads as below-bar) increments the crash
  streak; 3 consecutive below-bar tides CRASH the walk (budgeted return to `baseFloor` + backoff);
  16 healthy samples CONFIRM it (the descended position is kept, becomes the new veto anchor).
- **Raise-walk** (distress with the hot set under-earning the cold reservoir, `hotColdRatio < 1` —
  the frozen-set signal): confirms only after the set holds the target across
  `PROBE_CRASH_PERSISTENCE` consecutive at-target tides (durable — a single lucky tide must not
  keep a raise); 3 consecutive below-target tides CRASH it; spending the 16-tide budget without a
  verdict prices it as FAILED. The bold driver steps only while the set is still distressed;
  at-target tides hold so the confirmation streak accumulates. The confirm plants the veto anchor
  at the position the raise left from, so a later distress can undo a raise that failed to recover
  the signal.

**Anchor-memory veto (WindowClimber's anchor veto).** The anchor is the position the last
confirmed walk settled on, planted at confirmation: a raise-walk confirm plants it at the position
the raise left from, a release-walk confirm at the descended position — a veto never undoes a
healthy-confirmed descent. The veto fires when distress survives `GOVERNOR_VETO_STREAK = 4` tides
while the floor is above the anchor and the current renewal earns less than the anchor's reference
minus the noise margin; the floor returns to the anchor in 8 budgeted strides. The margin is
`max(RENEWAL_VETO_MARGIN = 0.1, 2σ)` over the last 8 renewals (a ring buffer — the WindowClimber
`Rates` deviation): noisy workloads need a wider evidence gap before a veto/undo fires, quiet ones
keep the floor margin.

**Per-direction retry ladders.** The raise and release directions own one `RetryLadder` each
(Caffeine's `Ladder`: rung + tides-left + crash run); an ending may only deepen the ledger of the
layer that produced it — a crashed raise must not delay the corrective release, nor a crashed
release the re-probe. The empty-set collapse resets both.

**Crash-run ladder pricing.** `RetryLadder.crashStreak` + `PROBE_CRASH_ESCALATION = 2`: `crash()`
holds the rung (the wait stays at `max(PROBE_BACKOFF_INITIAL = 4, rung)`, refractory hold while
unpaid) — probe damage and an exogenous shift are indistinguishable on one crash, so it is not
priced as a failed experiment — and only a consecutive crash run doubles it (4→8→16→32).
`fail()` (budget spent without a verdict) always doubles and resets the run; `reward()` resets it.

**Movement decays the audit run.** Distress and release movement apply `auditAge = max(0, age-1)`
instead of zeroing it, so one floor move per wait can no longer suppress audits forever. A healthy
audit (8 still tides) or saturation (≥ 90% of `hotLimit`) arms a release walk.

**Idle collapse and regime reset.** An empty hot set (`activeSlots == 0`) proves the floor exceeds
the whole distribution: the floor collapses to the seed immediately, any in-flight walk or budgeted
return is cancelled (a stale undo would drag the floor back toward its old base), and the full
regime state — distress history, audit run, step, veto anchor, both ladders, the renewal ring — is
reset so the new regime starts from a blank slate. An unplanted anchor is inert: renewal is never
below `0 - margin`.

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

## Consequences

1. **The floor can genuinely move and no direction ratchets.** Raise-walks, healthy admit-drops,
   release walks and the anchor veto all have reachable arming states, each evidence-based and each
   released by another branch (audit/admit/saturation/veto).
2. **A failed experiment is bounded and undone; a healthy one is confirmed, not assumed.** Every
   undo returns to the frozen base (or the anchor) in `GOVERNOR_RETURN_BUDGET = 8` strides; every
   confirm keeps the position and rewards the layer's ladder.
3. **Behavior changes are bounded.** No veto can fire before the first walk confirmation; an
   unplanted anchor is inert; the raise crash needs 3 consecutive below-target tides; the ladder
   doubles only on a consecutive crash run or a budget-spent failure.
4. **Sanity results preserved.** The simulator still reports `driftRotation floor=[10,10]` and
   `stableZipf avgRenewal=1.00` — healthy workloads never move the floor.
5. **Tests.** `WaveCounterAdaptiveTest` (24 tests, including the no-reflection retreat test, the
   per-direction backoff isolation and the crash-run escalation) plus the full common module (1762
   tests) are green. Design changes were validated first on simulated sequences in a desktop
   sandbox before porting.
6. **Incidental cleanups landed with the rework:** the four near-identical steal+putIfAbsent+add
   merge sites are one `mergeKey` helper; promotion candidates cache their avalanched hash; and
   `estimatedSizeOfKeysCount()` reads the O(1) counter instead of the CHM `size()`.
