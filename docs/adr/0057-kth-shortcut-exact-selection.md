# Previous-Boundary Kth-Shortcut for the Exact Promotion Selection

The ADR-0054 exact promotion-boundary selection (quickselect over the log2-located boundary bucket) is accelerated on stable workloads by a sweep filter: the histogram pass already visits every snapshot entry, so one extra comparison per entry (`v >= lastKth`, the previous tide's exact boundary) builds a filtered list that provably contains every key that can rank in the top `hotLimit`. When that list holds at least `hotLimit` keys, the exact selection runs over the ~`hotLimit`-key list instead of the bucket — the flat-distribution worst case (a boundary bucket the size of the snapshot, e.g. 1M distinct keys in one log2 bucket) degrades from an O(bucket) quickselect plus O(bucket) tie pass to an O(hotLimit) selection, with outputs byte-identical to the reference path on every reachable input (proven, not sampled).

## Status

accepted

## Context

ADR-0054 replaced the ADR-0042 two-stage histogram with the exact k-th largest count of the cycle's snapshot, selected by Hoare quickselect within the log2-located boundary bucket. The remaining cost shape: on every scan tide with `distinct >= hotLimit` the deliverer pays the bucket-view pass (O(candidates), sequential), the quickselect (~2-3n index-chasing comparisons over the BUCKET — usually a small slice of the snapshot, but on flat or tie-heavy distributions the bucket can be the whole snapshot), and the `>= kth` tie pass (O(bucket)). On a 1M-distinct-key tide at the 50ms burst cadence this selection phase alone can measure tens of milliseconds, pushing the deliverer's duty cycle toward the tide interval (the promotion pass runs after the consumer on the same tide; the delay lands on the NEXT tide's schedule, but it still delays detection latency at the extreme).

The user-proposed alternative — sampling the bucket and falling back to exact selection on low confidence — was rejected before implementation: the boundary is not a routing decoration, it is a governor input. `TideReading.boundary`/`blockedKeys`/`overflow` drive the admit, flood, veto and ARM gates (ADR-0055 audited all 8 consumed fields as load-bearing), and on a flat tie band the boundary decides which keys count as incumbents vs newcomers (ADR-0042's incumbent-first split). An approximate kth would re-introduce the exact systematic-bias class ADR-0054 removed, plus randomness that breaks the sandbox's deterministic paired campaigns.

The decisive observation is that the sweep already pays O(n) per tide and packs every entry's (value, hash); the histogram and the candidate list are free riders on that pass. A list filtered by the previous tide's exact boundary is another free rider, and its size on a stable workload is ~`hotLimit` + the tie band — the exact selection can run over THAT list instead of the bucket, with a proof of exactness (below) that needs no confidence threshold and no fallback decision.

## Decision

**Sweep filter + conditional exact selection over the filtered list.**

1. **`lastKth` (new deliverer-only field, init `-1`)** — the previous scan tide's exact boundary (`pass.boundary`). Updated at the end of every `selectBoundary` that computes one; retained across `clear()` like the governor floor (an adaptive reference, never correctness). `-1` = never armed.

2. **`aboveKthIdx` (new reused index view)** — `sweepHistogram` collects the packed index of every entry with `v >= lastKth` (one comparison per snapshot entry, gated off on the first tide). Size counter `PromotionPass.aboveKthSize`.

3. **`selectBoundary` shortcut** — when `pass.aboveKthSize >= hotLimit`, the selection runs over the list: `kth = selectKthLargest(aboveKthIdx, hotLimit)` and `qualifying = count(v >= kth)` over the same list (no `pass.above` addition — the list already contains the `above` keys). Otherwise the reference bucket-view path runs unchanged.

**The lemma (why the list selection is exact, not approximate).** The shipped boundary is the exact `hotLimit`-th largest of the snapshot on EVERY reachable path: the quickselect branch by construction (the `above` keys all outrank the bucket, and `bucketSize > need` puts the `hotLimit`-th position inside the bucket), and the bucket-fits branch because the crossing invariant (the top-down accumulation stops at the first bucket that reaches `hotLimit`) makes `bucketSize < need` unreachable — a fitting bucket either holds exactly `need` keys, whose minimum IS the `hotLimit`-th largest, or the snapshot holds fewer than `hotLimit` keys, whose minimum IS the k-th largest by definition. Now: a list of `>= hotLimit` keys above the previous boundary cannot sit below this cycle's k-th largest (else fewer than `hotLimit` keys could qualify — contradiction), so the list contains every key that can rank in the top `hotLimit`, and its `hotLimit`-th largest equals the snapshot's. The `>= kth` count over the list equals the whole-snapshot count (the list holds the `above` keys; nothing below the kth can rank), so `qualifying`, `overflow`, `blockedKeys` and the `splitIncumbents` inputs are reproduced exactly.

**Failure modes are fallbacks, never wrong boundaries.** A drifted workload (previous boundary above this cycle's kth) empties the list below `hotLimit` and the reference path takes over; a stale seed (previous kth far below) merely widens the list — the gate self-validates the lemma every tide. The first tide pays the full path and arms the seed.

## Validation

- **`WaveCounterKthShortcutTest` (8 tests, new).** Byte-identical pass outputs (`boundary`/`threshold`/`overflow`/`blockedKeys`/`above`/`boundaryBucket`) between the shortcut path and the reference path on 200 random snapshots (seeded, 512-3512 keys, floor noise / boundary-region / power-of-two / long-tail values) plus adversarial corners: the flat single-bucket worst case (3000@25 — deterministic firing, overflow, exactness), multi-bucket overflow with `above > 0` (the order statistic must be the `hotLimit`-th largest of the SNAPSHOT, not the `need`-th), the tie-band overflow, the bucket-fits corner (crossing invariant: bucket min IS the kth), the off-bottom corner (sub-hotLimit snapshot), floor-dominance (blocked band parity), and stale seeds (wide/narrow). The boundary is additionally asserted equal to an independently computed `hotLimit`-th largest on every random tide.
- **Existing suite green.** All 99 `WaveCounter*` + `BufferedCounterTest` tests pass, including the flat-distribution stability test (`flatDistribution_hotSetStaysStableAcrossScanTides`), which now exercises the shortcut from tide 2 on (tide 1 arms the seed; tide 2's whole-snapshot list fires the shortcut; the incumbent-first split is byte-identical). Full `common` module suite green.
- **Sandbox.** The shortcut is output-identical by proof + parity tests, so the zeta-tidal-sim paired campaigns (`exact_select_campaign.py`, baseline = shipped FINAL) are expected bit-identical — confirmed by a recorded run (60 expanded seeds + headline checks + 26 hand scenarios + 100 legacy seeds): GATE PASSED, every hand scenario byte-identical, and the shipped baseline reproduces the ADR-0054 record exactly (legacy excess 0.262 / floor_max 13.340 / crashes 0.660 / floods 0.110 / admit latency 0.300). No sim changes: the sim mirrors the boundary READING, which is unchanged.
- **Unrelated working-tree cleanup.** A pre-existing uncommitted WIP (a `defensiveHashSeed` per-instance random seed making `mixHash` instance-based, added outside this change) broke 17 existing tests (their static-hash beacon helpers mismatched the seeded hash, routing every key cold) and was reverted on request before this change's verification — the parity suite above ran on the clean tree.

## Considered Options

- **Random sampling with confidence fallback (the original proposal).** Rejected: the boundary is a governor input (ADR-0055) — approximate kth on a flat tie band changes incumbent/newcomer membership and biases the reading; randomness breaks sandbox determinism; and the O(n) sweep is not avoided anyway, only the selection phase. Sampling as a quickselect PIVOT seed stays exact but is inert (the middle pivot is already fine on hash-arbitrary iteration order).
- **Reusing `lastKth` as an estimation hint with exact recount.** Rejected: "estimate then recount" is a biased-lag design; the list filter version is exact with no second pass.
- **Incremental across-tide histogram.** Rejected (as the original proposal conceded): the sweep does more than histogram (packing, candidates, the shortcut list); a cross-tide diff structure costs more than the single pass it would replace.
- **ADR-0042-style linear sub-histogram resurrected as a location table (recursive bucket split).** Exact and deterministic, but it only helps on drift tides (the bucket-fits/quickselect split stays); the list shortcut covers the dominant stable case and the fallback covers the rest. Not adopted.
- **Do nothing.** The pathological tide (1M-key flat bucket at 50ms cadence) leaves `selectBoundary` as a tens-of-ms phase — the exact problem this change removes.

## Consequences

1. **Cost profile.** Stable workloads: the selection + tie pass drop from O(bucket) to O(`hotLimit` + tie band) — the flat worst case (bucket ≈ snapshot) becomes a ~1024-key selection. Drift tides: reference cost unchanged. Every scan tide pays one extra comparison per snapshot entry (deliverer-side, off the hot path; the first tide pays nothing extra).
2. **Zero behavioral change.** Outputs byte-identical on every reachable input (lemma + parity tests); governor inputs, incumbent-first split, decay ordering and the phase-normalized reading are untouched; no config surface; no per-op cost.
3. **Memory.** One reused `int[]` view (grows by doubling to the list size, ≤ distinct keys — the same lifecycle as `candidateIdx`) plus one `long` field.
4. **Tests.** 8 new parity/exactness tests; the existing 99 doublebuffer tests remain green with the shortcut active.
5. **Determinism.** The shortcut is a pure function of the snapshot and the previous boundary — no randomness, sandbox campaigns stay reproducible.

## Related ADRs

Builds on ADR-0054 (exact boundary selection — the selection source this accelerates) and retains ADR-0042's incumbent-first split and ordering contract untouched. ADR-0055's governor input contract is the constraint that made the exact shortcut necessary (an approximate variant would have changed the consumed reading). ADR-0056 (logical hotLimit) is unrelated; the `defensiveHashSeed` reverted in this change's validation was an independent WIP.
