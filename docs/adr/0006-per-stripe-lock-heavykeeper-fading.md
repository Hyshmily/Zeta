# Per-Stripe Lock in HeavyKeeper Fading

`HeavyKeeper.fading()` rotates the sliding-window ring buffer under per-stripe locks (`synchronized (lockStripes[i & lockMask])`) to prevent concurrent `addDirect()` from observing torn `long` writes (JLS 17.17). The decay cycle has two distinct halves, each with its own concurrency story:

## Sketch half — rotating the window ring buffer

The sketch maintains, per slot, a ring buffer of `windowCount` time windows. `fading()` zeroes the now-stale window index for every slot, keeping the running `slotSums[index]` in sync. Three flat `long[]` arrays share the same stripe protection:

- `long[] windows` (flattened 1D ring buffer, indexed `slot * windowCount + w` — see ADR-0014 for the flatten rationale)
- `long[] slotSums` (per-slot O(1) running sum across all windows)
- `long[] fingerprints` (per-slot collision-verification fingerprint)

Without the per-stripe lock, a 32-bit JVM torn read on any of these three arrays can produce a value that is neither the old nor the new state — a corrupted intermediate that propagates through every subsequent cycle for that bucket until a coincidental zero-write resets it. The per-stripe lock is therefore mandatory for correctness, not optional.

We explicitly chose NOT to use `AtomicLongArray` for `windows` / `slotSums` / `fingerprints` because:

1. It would bloat every element (object header per cell on top of the array backing), and the arrays together are `depth * width * (windowCount + 1)` longs — non-trivial.
2. It would degrade `addToSketch()` cache locality (StarTech cache lines prefer dense plain arrays).
3. It does not actually remove the lock — the three arrays are read together in the fast path (`adjacent reads/writes`), and an atomic on one element does not prevent another atomic on an adjacent slot from creating an inconsistent trio (fingerprint vs slotSums vs windows). The compound update is what needs atomicity, and only a lock can provide that.

**Lock the compound update, not the individual counters.**

The contention cost is negligible because `fading()` runs once per decay interval (~30s) while `addDirect()` holds the same stripe lock for a single compound update lasting nanoseconds.

## TopK membership half — halving membership counts

The same `fading()` call also halves each TopK member's count (`node.count >> 1`), dropping members whose halved value falls to zero. This half is guarded by the separate `admissionLock` (ReentrantLock), not the sketch stripes. Lock order is *sketch stripes → admissionLock*, identical to the admission path, so no deadlock is possible with concurrent `addDirect` callers.

`Node.count` is now a `LongAccumulator(Long::max, 0)` (see ADR-0014) — a striped accumulator, not an `AtomicLong` — so `fading()` lowers its value via `reset()` + `accumulate(halved)` while holding `admissionLock` (which serialises all concurrent writes for that brief window). This preserves the original halving semantics under the new concurrency primitive.