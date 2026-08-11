# Density-Refined Promotion Boundary and Incumbent-First Promotion

The WaveCounter's promotion boundary — the top-`hotLimit` cut of the cycle's log2-bucket count histogram — landed on a power of two, so the boundary bucket (all counts in `[2^b, 2^(b+1))`) could hold far more keys than the remaining `hotLimit` slots, and the promotion scan then picked the winners in HashMap iteration order: arbitrary under a flat distribution, and rotating a new random sample of the hot set every scan tide. The boundary is now refined by density — a linear 64-sub-bucket histogram over the boundary bucket lands the threshold at the actual ~k-th largest count — and when the refined boundary still cuts a tie-band wider than the remaining slots, promotion is incumbent-first: renewing members (captured from the pre-decay beacon state, the only last-tide membership memory) are re-promoted before any newcomer, so the capacity break never evicts a renewing key and the hot set stays stable under flat distributions.

## Status

accepted

## Context

`tide()` (WaveCounter.java) estimates the promotion boundary by accumulating log2 buckets top-down until `hotLimit` keys are covered; the boundary is the lower edge of the last bucket — a power of two. Three pathologies follow:

1. **Bucket-coarse boundary.** Keys at 130 and 200 counts tie in bucket `[128, 256)`; the boundary cannot distinguish them. The promotion scan admits the whole bucket, then cuts at `hotLimit` in snapshot iteration order — the winner set among ties is arbitrary (HashMap order, unrelated to the workload).
2. **Flat-distribution churn.** With a flat pool (e.g. 3000 keys all ≈ 25 counts, hotLimit 1024), every tide's boundary sits at the bottom of the distribution, all keys qualify, and the scan promotes a random 1024. The two-tide evidence cycle (promote → 2 → decay 1 → saturated skip → decay 0 → scan re-promotes) then re-promotes a NEW random sample every scan tide: ~half the hot set rotates in and out, paying decay writes, promotion RMWs, and hot/cold path flip-flops for keys that are all equally hot.
3. **The renewal signal is a coin flip too.** The governor's renewal numerator depends on which keys the iteration order picked — meaningless under a flat shape, where any set is as good as any other.

The boundary is routing-only (a wrong promotion costs performance, never count correctness — ADR-0020), so refining it is safe to probe.

## Decision

**Density refinement.** When the boundary bucket holds more keys than the remaining slots (`histogram[b] > need`, with the floor at or below the boundary — otherwise the floor does the selection and refinement is skipped), the bucket's count range `[2^b, 2^(b+1))` is re-bucketed into 64 LINEAR sub-buckets (single-count resolution for bucket floors below 64) and the boundary is refined to the lower edge of the sub-bucket where the top-down accumulation crosses `need` — the actual ~k-th largest count. Keys below the refined boundary are excluded entirely instead of being admitted in iteration order. The refine pass is one sweep over the candidate list with a shift per key (the boundary floor is a power of two, so the sub-index is a shift, never a division); only on overflowing tides.

**Incumbent-first promotion.** When the refined boundary still cuts a tie-band wider than the remaining slots (`subAccum > need`), the scan runs in two passes: pass 1 re-promotes every captured incumbent that still earns the threshold (the capture reads the pre-decay beacon state — the halving decay zeroes every member's evidence on a saturated set's scan tide, so post-decay membership is empty and pre-decay is the only last-tide memory); pass 2 fills the remaining capacity, checking capacity BEFORE promoting so a refilled set cannot grow past `hotLimit`. Renewals that survived the decay count toward the capacity budget exactly like the single pass (a saturated set renews fully and freezes — stable, not rotating); fallen incumbents (below the threshold) decay out and yield their slots to newcomers.

The two-pass runs only in the overflow case; the non-overflow path keeps the exact single-pass scan (zero regression on skewed workloads where the boundary bucket fits). Both refinements are deliverer-side: zero per-op cost, no volatile reads added (ADR-0041 intact), beacon size unchanged.

The gate `subAccum > need` is exactly "the tie-band is wider than the remaining slots" — the tighter condition under which a capacity break can cut renewing keys. (An earlier design gated the two-pass on overflow alone; the sub-accumulation makes the exact tie check free.)

## Considered Options

- **Quickselect on the boundary bucket (exact k-th largest):** exact selection but needs a values buffer sized to the bucket (up to the key universe) and ~40 lines of selection code; the sub-histogram achieves single-count resolution below 64 — the region where boundaries land in practice — with a reused int[64] and no buffer. Rejected.
- **Persistent last-promoted key set (hash set consulted at scan time):** avoids the pre-decay capture sweep but adds a long-lived 4-16 KB structure and set bookkeeping; the capture reuses the existing candidate/entry lists with zero new state. Rejected.
- **Write-thread self-promotion (mid-cycle, no one-tide lag):** bypasses the global boundary discipline (local hot ≠ global hot) and contradicts the conservative-adaptivity direction of ADR-0038/0040. Rejected — the one-cycle lag is routing-only and measured harmless.
- **Ghost tie-break (ARC-style memory for the boundary ties):** the natural future refinement (idea from the same review), but needs a third evidence role; the incumbent capture already provides last-tide membership memory, and the sub-histogram already bounds the tie-band. Deferred.
- **Governor flatness signal:** structurally redundant — in the refined path the floor-blocked band is empty (boundary ≥ floor), so the admit-on-block branch never fires, and incumbent-first keeps renewal ≈ 1.0 in flat cases, leaving the governor inert at the seed. Rejected.

## Consequences

1. **Boundary precision:** the promotion threshold is the actual ~k-th largest count (single-count resolution below 64) instead of a power of two; below-boundary keys are excluded rather than admitted in iteration order.
2. **Flat-distribution stability:** the promoted set renews in place; the scan tide no longer rotates a random sample. Churn (decay writes, promotion RMWs, hot/cold flip-flops) drops to the boundary fringe.
3. **Capacity invariant preserved:** pass 2 checks capacity before promoting (the single-pass break would overshoot by one when pass 1 refills the set); the active set never exceeds `hotLimit` — no ratchet.
4. **Costs:** deliverer-side only — at most one extra O(n) pass per tide, only on overflowing tides: the sub-histogram sweep and the incumbent capture are merged into a single sweep (one membership test per candidate), and the fill pass iterates the pre-split newcomer list, dropping its membership re-test (provably always false: pass 1 re-promotes every qualifying incumbent). On saturated skip tides the histogram pass runs before the gate fails (bounded wasted work, ~ms at 100k keys). Zero per-op cost; ADR-0041's plain-load beacon untouched.
5. **Governor signals improve:** memberKeys is now complete (no break-cut loss) and renewal is meaningful (≈ 1.0 under flat shapes instead of a coin flip); the floor stays inert where moving it would be noise.
6. **Tests:** `WaveCounterDensityBoundaryTest` (refinement exclusion, flat-distribution stability across scan tides, fallen-incumbent slot yield, non-overflow regression, floor-dominance gate). Full doublebuffer suite green.
