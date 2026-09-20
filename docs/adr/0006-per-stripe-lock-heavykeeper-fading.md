# Per-Stripe Lock in HeavyKeeper Fading

> **Superseded in part (2026-09-17).** The lock *conclusion* below still holds, but two supporting facts are stale and are corrected here: the sketch arrays are now `int[]` (not `long[]`), so the "torn `long` write (JLS 17.17)" rationale no longer applies — a non-volatile `int` write is already atomic under the JMM, and the lock is required for *compound-update* consistency only; and the decay interval is a **hard-coded 20 s**, not ~30 s. The current treatment lives in [ADR-0020](./0020-heavykeeper-concurrency-and-locking.md#per-stripe-lock-in-heavykeeper-fading-from-adr-0006).

`HeavyKeeper.fading()` rotates the sliding-window ring buffer under per-stripe locks (`synchronized (lockStripes[i & lockMask])`) to keep the three sketch arrays mutually consistent while `addDirect()` updates them concurrently. The decay cycle has two distinct halves, each with its own concurrency story:

## Sketch half — rotating the window ring buffer

The sketch maintains, per slot, a ring buffer of `windowCount` time windows. `fading()` zeroes the now-stale window index for every slot, keeping the running `slotSums[index]` in sync. Three flat `int[]` arrays share the same stripe protection:

- `int[] windows` (flattened 1D ring buffer, indexed `slot * windowCount + w` — see ADR-0014 for the flatten rationale)
- `int[] slotSums` (per-slot O(1) running sum across all windows)
- `int[] fingerprints` (per-slot collision-verification fingerprint)

Without the per-stripe lock, a reader can observe a *trio* from two different instants — e.g. the new `fingerprint` beside the pre-rotation `slotSums` — and treat it as a consistent slot. No single element is ever torn (individual `int` writes are atomic), but the three-element update is not; the per-stripe lock is therefore mandatory for correctness, not optional.

We explicitly chose NOT to use `AtomicIntegerArray` for `windows` / `slotSums` / `fingerprints` because:

1. It would bloat every element (object header per cell on top of the array backing), and the arrays together are `depth * width * (windowCount + 1)` ints — non-trivial.
2. It would degrade `addToSketch()` cache locality (contiguous cache lines prefer dense plain arrays).
3. It does not actually remove the lock — the three arrays are read together in the fast path (`adjacent reads/writes`), and an atomic on one element does not prevent another atomic on an adjacent slot from creating an inconsistent trio (fingerprint vs slotSums vs windows). The compound update is what needs atomicity, and only a lock can provide that.

**Lock the compound update, not the individual counters.**

The contention cost is negligible because `fading()` runs once per decay interval — a hard-coded 20 s in `ZetaSchedulingConfiguration.scheduleTasks()` — while `addDirect()` holds the same stripe lock for a single compound update lasting nanoseconds.

## TopK membership half — halving membership counts

The same `fading()` call also halves each TopK member's count (`node.count >> 1`), dropping members whose halved value falls to zero. This half is guarded by the separate `admissionLock` (ReentrantLock), not the sketch stripes. Lock order is *sketch stripes → admissionLock*, identical to the admission path, so no deadlock is possible with concurrent `addDirect` callers.

`Node.count` is now `final AtomicInteger count` — the final step of the `LongAccumulator → AtomicLong → AtomicInteger` evolution (v1.1 → v1.1.55 → v1.1.56). It was reverted from `LongAccumulator` after v1.1.55 because `reset()` silently drops concurrent `accumulate()` calls, and then narrowed to `AtomicInteger` because the sketch's `int` saturation guard makes a count above `Integer.MAX_VALUE` unreachable. The hot path uses `accumulateAndGet(maxCount, Math::max)` for atomic max-raise (single CAS under no contention). `fading()` lowers each member's count via a CAS retry loop in `decayMembership()` under `admissionLock`: it reads `count`, halves it, and retries if a concurrent raise causes the CAS to fail — never losing a write.