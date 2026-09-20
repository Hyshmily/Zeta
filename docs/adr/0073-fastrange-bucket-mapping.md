# FastRange Bucket Mapping for the Non-Power-of-Two Sketch Width

`HeavyKeeper.bucketIndex` used a bit-mask when `width` was a power of two and a sign-stripped modulo (`(hash & 0x7FFFFFFF) % width`) otherwise. The modulo wastes the top bit of the 32-bit fingerprint entirely and consumes only the low bits — the part of a quality hash least uniformly distributed — and pays an integer division per bucket lookup on the sketch hot path. We replace the non-power-of-two branch with the RocksDB `util/fastrange.h` mapping: the high half of the `width × uint32(hash)` product, one multiplication, all 32 bits of entropy consumed.

## Status

accepted

## Context

RocksDB's `FastRange32` (`(range * hash) >> 32`) is the bucket-mapping primitive behind its cache shards and block filters. The fastrange.h header documents two properties we rely on: the product consumes the *full* hash width (a modulo only sees the low bits), and the variant must match the hash's natural output width — a 32-bit hash fed to a 64-bit FastRange "gives extremely bad results, mostly zero". Zeta's sketch fingerprints are already 32-bit (`(int) fingerprint(key)`, ADR-0020's memory trims), so `FastRange32` is the matching variant; `Math.multiplyHigh` and a 64-bit variant would be over-engineering until a 64-bit-hash caller exists.

Scope audit before implementation (the borrowing analysis originally proposed wider replacement):

- `HeavyKeeper.bucketIndex` non-pow2 branch — the only true modulo on a hash→bucket path. The pow2 mask path stays (it is already optimal); `lockStripes[index & lockMask]` and `windowMask` index *within* power-of-two-sized arrays and must keep their masks, because ADR-0020's stripe-uniformity argument is built on them.
- `ConsistentHashRing` — misidentified in the analysis: the ring is a sorted-array binary search, not a modulo bucket map, and its `murmur3_fixed()` hash is pinned by ADR-0005 for cross-node consistency. Untouched.
- `WaveCounter` — every range (beacon rooms, local-map capacity, window count) is a power of two with mask indexing. Nothing to replace.

## Decision

- New `io.github.hyshmily.zeta.util.FastRangeUtil` with a single static method `fastRange32(int hash, int range)` and the variant-mismatch warning reproduced in its Javadoc. Deliberately public: the same mapping is the natural primitive for any future hash→bucket structure (e.g. the borrowing report's R11 DynamicBloom probe addressing).
- `HeavyKeeper.bucketIndex` non-pow2 branch becomes `FastRangeUtil.fastRange32(hash, width)`; the constructor's non-pow2 WARN text now names the FastRange mapping instead of "slow modulo" (the advice to prefer a power of two stands — the mask path is still the fastest).
- Sketch layout is node-local memory with no persistence or cross-node agreement, so a distribution change in the rare non-pow2 configuration carries no compatibility risk.

## Considered Options

- **Replace the pow2 mask paths too:** masks beat any arithmetic; replacing them is a regression dressed as consistency. Rejected.
- **Apply FastRange to the consistent-hash ring:** the ring is not a bucket map; changing its hash would violate ADR-0005's cross-node determinism. Rejected.
- **Add a `fastRange64` variant now:** no 64-bit-hash caller exists; the fastrange.h header itself warns 32-bit inputs collapse under it. YAGNI — documented in `FastRangeUtil` instead. Rejected.

## Consequences

1. Non-pow2 sketch widths map buckets with full fingerprint entropy and one multiply instead of a division; distribution is at least as uniform as before on quality hashes.
2. The change is exercised only when `autoAlignWidth=false` and `width` is not a power of two — a rare configuration (the constructor warns and ADR practice aligns widths), which is also why the risk is small.
3. `FastRangeUtilTest` pins the known values, range membership, full coverage on a non-pow2 width, and a 4σ distribution bound on 1023 buckets; the HeavyKeeper suite guards the sketch behavior.
