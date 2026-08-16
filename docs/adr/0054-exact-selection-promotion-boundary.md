# Exact Selection of the Promotion Boundary

The WaveCounter's promotion boundary is no longer estimated by the ADR-0042 two-stage histogram (log2 buckets + a linear 64-sub-bucket density refinement); the boundary VALUE is the exact k-th largest count of the cycle's snapshot, selected by quickselect within the log2-located boundary bucket. The log2 histogram survives only as a location table. Sandbox-validated on the zeta-tidal-sim paired corpora (300 expanded seeds + 100 legacy seeds + 26 hand scenarios): every harm axis significantly better, hand scenarios byte-identical, headline checks unchanged.

## Status

accepted

## Context

ADR-0042 refined the power-of-two log2 boundary with a linear 64-sub-bucket histogram over the boundary bucket, landing the threshold at the actual ~k-th largest count (single-count resolution for bucket floors below 64). Two residual defects remained, both traced to the boundary READING the governor consumes:

1. **The 2^b bucket edge underestimates the true top-k cutoff.** On non-overflowing tides (the bucket fits the remaining slots — the common case for skewed distributions) the refinement is skipped entirely and the boundary stays `2^b`, which can sit far below the actual k-th largest count (e.g. kth 40 vs boundary 32). The governor gates on `floor > boundary` (the stale-floor evidence of the flood signature, the admit condition and the veto band): a systematically LOW boundary makes "the floor is above the boundary" fire far more often than the truth warrants, arming probes over healthy states, inflating the blocked band, and costing excess, over-filtering, flicker and admit latency.
2. **The blocked band counts keys the top-k cutoff excludes anyway.** `blockedKeys` was `histogram[b] − candidateHistogram[b]` = keys in `[2^b, floor)`. When `kth > 2^b`, the keys in `[2^b, kth)` would NOT qualify at the honest cutoff — yet they inflate the blocked signal, making the governor drop the floor to a position (`2^b`) that still excludes the "blocked" keys: vacuous admits.

The ADR-0042 "Considered Options" had rejected quickselect (needs a values buffer sized to the bucket, ~40 lines of selection code) because the sub-histogram reached single-count resolution below 64 — the region where boundaries land in practice. The sandbox re-examination showed that claim was about PROMOTION precision (which the sub-histogram indeed solves) while the boundary READING was still quantized; the reading is what the governor acts on.

## Decision

**Exact k-th largest boundary, selected within the log2-located bucket.**

The promotion phase is now: `sweepHistogram` (log2 location histogram + packed (value, hash) arrays + candidate view, one pass) → `estimateBoundary` (top-down accumulation: boundary bucket `b`, `above`, `need = hotLimit − above` — unchanged) → `selectBoundary`:

1. **Bucket view.** One pass over the packed values collects the indices whose count falls in `[2^b, 2^(b+1))` into a reused index view.
2. **Exact selection.** If the bucket overflows the remaining slots (`bucketSize > need`), a Hoare-partition quickselect finds the `need`-th largest count within the bucket — the exact k-th largest of the snapshot (the `above` keys in higher buckets all qualify). If the bucket fits (`bucketSize <= need`, including every snapshot below `hotLimit` distinct keys), the k-th largest is the bucket minimum. The quickselect partitions the index VIEW only — the packed arrays keep their (value, hash) pairing for the split and the promotion scans.
3. **Boundary semantics (exact where ADR-0042 was quantized):** `boundary = kth`, `threshold = max(floor, kth)` — on EVERY scan tide, not only overflowing ones. `overflow = count(v >= kth) > hotLimit` (the tie band wider than the capacity; the ADR-0042 `subAccum > need` analog, now on the exact cutoff). `blockedKeys = count(v >= kth) − count(v >= floor)` = the keys the floor excludes that WOULD qualify at the top-k cutoff — `qualifying − candSize`, one subtraction, exact by construction (the ADR-0042 band `[2^b, floor)` counted keys the cutoff excludes anyway). The incumbent/newcomer split (ADR-0042, unchanged in role) runs on `v >= kth` instead of `v >= 2^b`, and the ordering contract is untouched: the split reads the pre-decay beacon state, before `decayGate`.

Below `hotLimit` distinct keys the boundary is the minimum positive count (the unquantized analog of the ADR-0042 lowest-bucket edge) — the sandbox's `exact_select` branch, byte-identical to the shipped behavior on the hand scenarios.

## Sandbox Validation (zeta-tidal-sim, `exact_select_campaign.py`)

Baseline = the shipped FINAL governor config; the variants differ only in `boundary_mode`. Shipped Java numbers reproduce exactly (the legacy baseline reads excess 0.262 / floor_max 13.340, identical to the ADR-0053 record) — the refactored pipeline is behavior-neutral.

- **Expanded corpus, 300 paired seeds (vs shipped):** arms 1.420→1.230, excess 0.136→0.042, floor_max 14.17→13.15, harmful over-filter 0.010→0.006, flicker 0.006→0.005, admit latency 0.507→0.373 — all p < 0.0001, 34-49 improved seeds vs 4-9 worse; fails p = 0.035; walk duration p = 0.019. The one measured cost: confirms 0.693→0.620 (p = 0.006) — fewer successful raise probes, consistent with the honest boundary removing false stale-floor evidence (the raises that do fire are more legitimate).
- **Hand scenarios (26, both fs and fuzz_m4 sets):** zero failures — every scenario byte-identical (excess, harmful, floor_end, event kinds).
- **Headline checks:** earn1/miss2 coverage 92.8% and churn peak 1024 unchanged.
- **Legacy corpus, 100 seeds:** excess 0.262→0.194, floor_max 13.34→12.44, crashes 0.66→0.57, floods 0.11→0.06, admit latency 0.30→0.21, harmful 0.008→0.005 — same direction as the expanded corpus.
- **Operation accounting (deliverer boundary phase, per tide):** the selection is cheaper than the refinement on flat overflow tides (107k vs 140k ops at 20k keys in one bucket — no per-entry histogram increment, split only on the kth tie band) and more expensive on skewed non-overflow tides (109k vs 62k — the full-snapshot quickselect; the log2-located bucket variant pays only the bucket-sized selection, the Java design). On the realistic corpus: 104k vs 74k total boundary ops over 178 tides — a microsecond-scale difference per tide, entirely off the hot path (the deliverer runs once per 50-500ms tide).

A second variant — `log2_select` (ADR-0042 refinement replaced by an in-bucket quickselect, boundary otherwise unchanged) — measured behavior identical to shipped on every corpus (all sign p = 1.0): the corpus boundaries land below 64 counts, where the sub-histogram is already single-count, and in the divergence zone (buckets ≥ 64) the quantized vs exact boundary differs by at most one sub-bucket edge, which the promotion set absorbs. Rejected: it pays selection for no behavioral change.

## Considered Options

- **ADR-0042 sub-histogram, kept (status quo):** quantized boundary reading (bucket edge below 64 counts is exact, above it the reading is a sub-bucket edge), vacuous blocked band; measured worse on every harm axis at 300 paired seeds. Rejected.
- **Full-snapshot quickselect (the sandbox `exact_select`):** simplest semantics, but pays ~2-3n comparisons even when the boundary bucket fits and a cheap histogram location is already paid. Rejected for Java — the log2-located bucket selection is mathematically identical (the k-th largest of the snapshot is the `need`-th largest of the boundary bucket) at a fraction of the cost.
- **FrequencySketch-style boundary (Caffeine's Count-Min Sketch as the boundary source):** wrong direction — the deliverer already holds EXACT per-cycle counts; a sketch would replace exactness with 4-bit estimates and pay per-op cost for nothing. The sketch's role (cheap per-op frequency memory) is already served by the routing beacon. Rejected.
- **Caffeine-style pairwise admission (candidate-vs-victim, no boundary at all):** does not map — the hot set is a hash-only routing beacon with no key retention, so there are no victims to compare; the per-tide snapshot is the only enumeration, and a threshold is required. Rejected.

## Consequences

1. **Honest governor evidence.** The boundary reading is the exact top-`hotLimit` cutoff on every scan tide; the "floor above boundary" gates (flood signature, admit, veto band) and the blocked band measure what they claim. Measured: fewer probes, less excess/over-filtering/flicker, lower floor maxima and admit latency; fewer confirms (the probes that remain are legitimate).
2. **Simpler machinery.** `candidateHistogram[64]` and `subHistogram[64]` and the shift-form sub-indexing / div-mod lower-edge math are deleted; the blocked signal is one subtraction. The location histogram, the packed arrays, the incumbent-first split and the two-pass scan are unchanged.
3. **Costs.** Deliverer-only: the bucket view pass + quickselect + tie/blocked pass on every scan tide with `distinct >= hotLimit` (O(bucket) expected, ~2-3n comparisons; the bucket is usually a small slice of the snapshot), plus the incumbent split on overflowing tides only. No per-op cost; beacon size and writer paths untouched.
4. **The ordering contract survives.** The split still captures last-tide membership pre-decay; `decayGate` and the phase-normalized reading are untouched.
5. **Tests:** `WaveCounterDensityBoundaryTest` semantics preserved (refinement exclusion, flat-distribution stability, fallen-incumbent yield, non-overflow path, floor dominance — re-derived for the exact threshold, all five pass unchanged); full doublebuffer suite green.

## Related ADRs

Supersedes the ADR-0042 density-refinement mechanism (the incumbent-first promotion and the ordering contract are retained). ADR-0045's governor consumes the boundary reading and was the measured beneficiary. ADR-0049 (sweep period), ADR-0043 (hot-path atomicity) and ADR-0048 (routing) are untouched.
