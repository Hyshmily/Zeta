# HeavyKeeper Concurrency Model and Lock Hierarchy

Merges and supersedes ADR-0006, ADR-0014, and ADR-0017.

---

## Per-Stripe Lock in HeavyKeeper Fading (from ADR-0006)

`HeavyKeeper.fading()` rotates the sliding-window ring buffer under per-stripe locks (`synchronized (lockStripes[i & lockMask])`) to prevent concurrent `addDirect()` from observing torn `long` writes (JLS 17.17). The decay cycle has two distinct halves, each with its own concurrency story:

### Sketch half — rotating the window ring buffer

The sketch maintains, per slot, a ring buffer of `windowCount` time windows. `fading()` zeroes the now-stale window index for every slot, keeping the running `slotSums[index]` in sync. Three flat `long[]` arrays share the same stripe protection:

- `long[] windows` (flattened 1D ring buffer, indexed `slot * windowCount + w`)
- `long[] slotSums` (per-slot O(1) running sum across all windows)
- `long[] fingerprints` (per-slot collision-verification fingerprint)

Without the per-stripe lock, a 32-bit JVM torn read on any of these three arrays can produce a value that is neither the old nor the new state — a corrupted intermediate that propagates through every subsequent cycle for that bucket until a coincidental zero-write resets it. The per-stripe lock is therefore mandatory for correctness, not optional.

We explicitly chose NOT to use `AtomicLongArray` for `windows` / `slotSums` / `fingerprints` because:

1. It would bloat every element (object header per cell on top of the array backing), and the arrays together are `depth * width * (windowCount + 1)` longs — non-trivial.
2. It would degrade `addToSketch()` cache locality (contiguous cache lines prefer dense plain arrays).
3. It does not actually remove the lock — the three arrays are read together in the fast path (`adjacent reads/writes`), and an atomic on one element does not prevent another atomic on an adjacent slot from creating an inconsistent trio (fingerprint vs slotSums vs windows). The compound update is what needs atomicity, and only a lock can provide that.

**Lock the compound update, not the individual counters.**

The contention cost is negligible because `fading()` runs once per decay interval (~30s) while `addDirect()` holds the same stripe lock for a single compound update lasting nanoseconds.

### TopK membership half — halving membership counts

The same `fading()` call also halves each TopK member's count (`node.count >> 1`), dropping members whose halved value falls to zero. This half is guarded by the separate `admissionLock` (ReentrantLock), not the sketch stripes. Lock order is *sketch stripes → admissionLock*, identical to the admission path, so no deadlock is possible with concurrent `addDirect` callers.

`Node.count` is now `final AtomicLong count` — reverted from `LongAccumulator` after v1.1.55 because `reset()` silently drops concurrent `accumulate()` calls. The hot path uses `accumulateAndGet(maxCount, Math::max)` for atomic max-raise (single CAS under no contention). `fading()` lowers each member's count via a CAS retry loop in `decayMembership()` under `admissionLock`: it reads `count`, halves it, and retries if a concurrent raise causes the CAS to fail — never losing a write.

---

## HeavyKeeper Concurrency Data-Structure Choices (from ADR-0014)

**Note:** Node.count reverted to `AtomicLong` in v1.1.55 (see "Revised Decision" at end).

The per-Node counter is now an `AtomicLong` using `accumulateAndGet(maxCount, Math::max)` on the hot path and a CAS retry loop during `decayMembership()`. The `LongAccumulator` was originally chosen for its 2.63× same-key-contention throughput advantage, but `reset()` has no atomic equivalent — concurrent `accumulate()` calls between `get()` and `reset()` are permanently lost on every decay cycle.

The remaining decisions in this section (stripe locks, flattened window array, admission decomposition) are unchanged.

The hot path of `HeavyKeeper.addDirect(key, increment)` performs two operations: (1) sketch slot update behind per-stripe synchronization, and (2) TopK admission on a per-key Node counter. The two axes have different contention profiles:

- The sketch axis fans out across `width * depth` slots, so per-slot contention is bounded — the ***stripe-lock implementation*** dominates here.
- The admission axis collapses onto one Node per hot key. When many threads hammer the same key — exactly the "hot key" stress pattern — the ***counter primitive*** dominates.

A micro-benchmark (`HeavyKeeperBenchmark`, in `common/src/test/...`) was run with three realistic scenarios.

### Stripe locks: keep `synchronized(Object[])`

| Scenario B (sketch stripe contention) | ops/ms (median of 3) |
|---|---|
| `synchronized(Object[])` | **114 285** |
| `ReentrantLock[]` (non-fair) | 68 085 (0.60×) |
| `StampedLock[]` (write-lock) | 84 210 (0.74×) |

`synchronized` wins decisively. Modern JVM (17+) thin/biased-monitor fast paths are cheaper than the equivalent `ReentrantLock` acquiring/unacquiring bookkeeping under lightweight critical-section durations (single `long ++`). No replacement implementation beat the baseline, so the simplest was retained.

### Node.count: was `LongAccumulator(Long::max, 0)` (v1.1)

The constructor uses identity `0` (not the admission `count`) and immediately coerces the initial value:

```java
this.count = new LongAccumulator(Long::max, 0);
this.count.accumulate(count);  // raise to the admission maxCount
```

This ensures `reset()` + `accumulate(halved)` in `decayMembership()` correctly sets the count to `max(0, halved) = halved`. Using `count` as identity would cause `reset()` to return to the admission count, making the membership count permanently stuck above the decay target.

| Scenario A (same-key 16-thread contention, external maxCount) | ops/ms |
|---|---|
| `AtomicLong` CAS max-raise loop | 380 952 |
| `LongAdder` accumulate(1) | 800 000 (2.10×) |
| **`LongAccumulator` max-merge** | **1 000 000 (2.63×)** |

| Scenario C (mixed 80% hot, 20% cold) | ops/ms |
|---|---|
| AtomicLong + synchronized | 296 296 |
| LongAdder + synchronized | 96 385 (0.33×) |
| **LongAccumulator + synchronized** | **275 862 (0.93× — statistical tie)** |

`LongAccumulator(Long::max, 0)` was chosen over both alternatives because:
1. **It strips contention precisely like `LongAdder`** — uses the same `Striped64` engine at the cell level, so 16 threads on one key do not CAS-spin against each other (the failure mode of `AtomicLong`).
2. **It preserves the exact monotonic-max semantics of the original `AtomicLong` CAS loop** — the `Long::max` merger means `accumulate(maxCount)` *only* raises the stored value, never lowers it. `LongAdder` cannot express this: `add(delta)` is purely cumulative, so a collision-backoff that should *lower* the membership count leaves the `LongAdder` sum permanently inflated.
3. **2.63× throughput win on the worst-case same-key path** — TopK's defining workload is *concentrated hot keys*.
4. **Statistically tied with `AtomicLong` in mixed workloads** — 0.93× in scenario C falls within run-to-run noise.

Memory cost: each `LongAccumulator` carries a `Striped64` with one cell per CPU; per-Node memory rises ~16× compared to a single `long`. The cost is bounded by `k` (Top-K set size, typically 100).

### Window ring buffer: flatten `long[][] windows` to `long[] windows`

Decided unconditionally before benchmarking:
- Single allocation vs `depth*width` sub-array allocations + headers
- Contiguous cache lines (sketch slots are loaded together during `fading`; one cache line refills multiple slots)
- Index `windows[slot * windowCount + w]` is two int-multiply/add-loads — JIT strength-reduction translates to a single `add+load` after C2

No measurable downside.

### TopK admission: split into fast path, `admitOrEvict()`, and `findMinMember()`

The hot path stays lock-free for existing members — a single `LongAccumulator.accumulate` call. Admission of new candidates is decomposed into:
- **Fast path (`admit`):** lock-free `members.get()` + `accumulate()` for existing members; falls through to `admissionLock` for new candidates
- **`admitOrEvict()`:** acquires `admissionLock` only when the key is brand new and above `minCount`; delegates minimum-scan to `findMinMember()`
- **`findMinMember()`:** O(k) scan of the membership set, single-purpose helper

Lock order (`sketch stripes → admissionLock`) preserved — no new deadlock surface.

### Revised Decision (v1.1.55): Node.count reverted to `AtomicLong`

`decayMembership()` calls `reset()` then `accumulate(halved)` on each Node's `LongAccumulator`. However, `reset()` zeros every `Striped64` cell — including concurrent `accumulate()` writes that arrived between `get()` and `reset()`. These concurrent counts are permanently lost. The `admissionLock` does **not** prevent this because the fast path in `admit()` (lock-free `accumulate()` on existing members) executes without holding any lock.

The `LongAccumulator` API provides no `compareAndSet()` equivalent, so there is no way to atomically lower the stored value without a destructive `reset()`. This is a fundamental API limitation.

```java
// Fast path (admit): atomic max-raise — lock-free, single CAS under no contention
member.count.accumulateAndGet(maxCount, Math::max);

// Decay path: CAS retry loop
do {
    prev = n.count.get();
    halved = prev >> 1;
} while (halved > 0 && !n.count.compareAndSet(prev, halved));
```

| Scenario | LongAccumulator (old) | AtomicLong (new) |
|---|---|---|
| Same-key 16-thread (Scenario A) | 1 000 000 ops/ms | ~380 952 ops/ms (0.38×) |
| Mixed 80/20 (Scenario C) | 275 862 ops/ms | ~296 296 ops/ms (1.07×, noise) |

The 2.63× worst-case throughput is surrendered, but `AtomicLong.accumulateAndGet(max, Math::max)` is a single CAS under no contention — identical to `LongAccumulator`'s fast path. The regression only manifests under extreme same-key multi-thread contention. Given that this is a correctness fix (systematic count loss on every decay cycle), the trade-off is accepted.

Memory per `Node` drops from ~150–400 B (`LongAccumulator` + lazily-allocated Striped64 cells) to ~24 B (`AtomicLong`).

---

## Lock Hierarchy and Ordering (from ADR-0017)

Zeta uses three independent locking domains across the hot-path detection and state-machine layers. The ordering between them is implicit but critical: violating it causes deadlock.

### The Three Lock Domains

| Domain | Guard | Implementation | Scope |
|---|---|---|---|
| **HeavyKeeper admission** | TopK membership mutations (`admitOrEvict`, `decayMembership`) | `ReentrantLock` (`admissionLock`) | Global — one per HeavyKeeper instance |
| **HeavyKeeper sketch stripes** | Per-slot sketch state (`windows[]`, `slotSums[]`, `fingerprints[]`) | `synchronized(Object[])` (up to 4096 stripes) | Per-stripe — stripes are distinct objects |
| **State machine per-key** | Per-key hot/cold streaks and state transitions | `Striped.lock(4096)` (`keyLocks`) | Per-key — 4096 stripes via Guava |

### Lock Hierarchy (strict total order)

```
1. State machine per-key (keyLocks)          ← highest in acquisition order
2. HeavyKeeper sketch stripes (lockStripes)
3. HeavyKeeper admission (admissionLock)     ← lowest
```

**Rule:** Code may hold N locks only if all locks with smaller ordinal are released before acquiring any lock with larger ordinal. Equivalent to: acquire in descending ordinal order, release in ascending.

### Current Lock Acquisitions

#### HeavyKeeper.addDirect (sketch write + possible admission)

```
1. sketch stripes (synchronized)     ← acquired per-row, released after each row
2. admissionLock                     ← acquired only for new-member admission
```

The stripes are acquired and released per-row inside `addToSketch`, so at most one stripe is held when `admissionLock` is taken. Correct.

#### HeavyKeeper.fading (periodic window rotation)

```
1. sketch stripes (synchronized)     ← rotateSketchWindows: acquired one stripe at a time
2. admissionLock                     ← decayMembership: acquired after all stripes released
```

`rotateSketchWindows` acquires and releases stripes sequentially, so by the time `decayMembership` takes `admissionLock`, no stripe lock is held. Correct.

#### ZetaStateMachineImpl.evaluate (per-key evaluation)

```
1. keyLocks.get(key) (lock)          ← held for entire evaluate()
2. [no HeavyKeeper lock acquired]
```

`confidenceEvaluator.evaluate()` performs only a read of the HeavyKeeper sketch (`ctx.cmsCount()` was already pre-computed in the `EvaluationContext`). No HeavyKeeper lock is taken inside `keyLocks`. **Critical invariant: no code path inside `keyLocks` may acquire any HeavyKeeper-level lock.**

### What Would Violate the Hierarchy

- Adding a `HeavyKeeper.add()` call inside `ZetaStateMachineImpl.evaluate()` — would acquire `keyLocks` → sketch stripe, which is the reverse order of the add→admission path and creates deadlock with `fading()`.
- Adding a `confidenceEvaluator.evaluate()` that lazily computes `cmsCount` (if it acquires a sketch stripe internally).
- Taking `keyLocks` inside a HeavyKeeper callback or listener.

---

## References

- ADR-0006: original per-stripe lock decision (superseded)
- ADR-0014: original concurrency choices (superseded)
- ADR-0017: original lock hierarchy (superseded)
