# HeavyKeeper concurrency data-structure choices

**Status:** Superseded (Node.count reverted to `AtomicLong` in v1.1.55).

The per-Node counter is now an `AtomicLong` using `accumulateAndGet(maxCount, Math::max)` on the hot path and a CAS retry loop during `decayMembership()`. The `LongAccumulator` was originally chosen for its 2.63× same-key-contention throughput advantage, but `reset()` has no atomic equivalent — concurrent `accumulate()` calls between `get()` and `reset()` are permanently lost on every decay cycle. See the "Revised Decision" section below for full rationale.

The remaining decisions in this ADR (stripe locks, flattened window array, admission decomposition) are unchanged.

---

The hot path of `HeavyKeeper.addDirect(key, increment)` performs two operations: (1) sketch slot update behind per-stripe synchronization, and (2) TopK admission on a per-key Node counter. The two axes have different contention profiles:
- The sketch axis fans out across `width * depth` slots, so per-slot contention is bounded — the ***stripe-lock implementation*** dominates here.
- The admission axis collapses onto one Node per hot key. When many threads hammer the same key — exactly the "hot key" stress pattern — the ***counter primitive*** dominates.

A micro-benchmark (`HeavyKeeperBenchmark`, in `common/src/test/...`) was run with three realistic scenarios before this ADR's choices were finalised. Results are reproduced below and led directly to the chosen implementations.

## Decision

### 1. Stripe locks: keep `synchronized(Object[])`

| Scenario B (sketch stripe contention) | ops/ms (median of 3) |
|---|---|
| `synchronized(Object[])` | **114 285** |
| `ReentrantLock[]` (non-fair) | 68 085 (0.60×) |
| `StampedLock[]` (write-lock) | 84 210 (0.74×) |

`synchronized` wins decisively. Modern JVM (17+) thin/biased-monitor fast paths are cheaper than the equivalent `ReentrantLock` acquiring/unacquiring bookkeeping under lightweight critical-section durations (single `long ++`). No replacement implementation beat the baseline, so the simplest was retained.

### 2. `Node.count`: switch from `AtomicLong` to `LongAccumulator(Long::max, 0)`

The constructor uses identity `0` (not the admission `count`) and immediately coerces the initial value:
```java
this.count = new LongAccumulator(Long::max, 0);
this.count.accumulate(count);  // raise to the admission maxCount
```
This ensures `reset()` + `accumulate(halved)` in `decayMembership()` correctly sets the count to `max(0, halved) = halved`. Using `count` as identity would cause `reset()` to return to the admission count, making the membership count permanently stuck above the decay target (see ADR-0014 rationale below).

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
2. **It preserves the exact monotonic-max semantics of the original `AtomicLong` CAS loop** — the `Long::max` merger means `accumulate(maxCount)` *only* raises the stored value, never lowers it. `LongAdder` cannot express this: `add(delta)` is purely cumulative, so a collision-backoff that should *lower* the membership count leaves the `LongAdder` sum permanently inflated. Recovering max semantics with `LongAdder` would require an `O(cells)` `sum()` read followed by a CAS that the API does not expose.
3. **2.63× throughput win on the worst-case same-key path** — TopK's defining workload is *concentrated hot keys*, so the worst case matters more than the average case.
4. **Statistically tied with `AtomicLong` in mixed workloads** — 0.93× in scenario C falls within run-to-run noise envelope; no persistent regression.

Memory cost: each `LongAccumulator` carries a `Striped64` with one cell per CPU; per-Node memory rises ~16× compared to a single `long`. The cost is bounded by `k` (Top-K set size, typically 100), so this is a tiny, deliberate, memory-for-performance trade-off that AGENTS.md explicitly authorises.

### 3. Window ring buffer: flatten `long[][] windows` to `long[] windows`

Joined **decided unconditionally** before benchmarking:
- Single allocation vs `depth*width` sub-array allocations + headers
- Contiguous cache lines (sketch slots are loaded together during `fading`; one cache line refills multiple slots)
- Index `windows[slot * windowCount + w]` is two int-multiply/add-loads — JIT-generated is a leaf that translates to a single `add+load` after C2 strength reduction

No measurable downside; no benchmark needed.

### 4. TopK admission: split into fast path, `admitOrEvict()`, and `findMinMember()`

The hot path stays lock-free for existing members — a single `LongAccumulator.accumulate` call. Admission of new candidates is decomposed into:
- **Fast path (`admit`, line 674):** lock-free `members.get()` + `accumulate()` for existing members; falls through to `admissionLock` for new candidates
- **`admitOrEvict()`** (line 709): acquires `admissionLock` only when the key is brand new and above `minCount`; delegates minimum-scan to `findMinMember()`
- **`findMinMember()`** (line 745): O(k) scan of the membership set, single-purpose helper

Lock order (`sketch stripes → admissionLock`) preserved — no new deadlock surface.

## Considered Options

### Stripe locks
- **`ReentrantLock[]` (non-fair)**: Provides `tryLock(timeout)` enabling `fading()` to skip a stuck stripe. Loses the benchmark badly on small critical sections. The interruptibility has not been needed in practice — `fading()` runs on a periodic scheduler with no caller dependencies.
- **`StampedLock[]` with optimistic read on the hot path**: Would help if there were a read-only fast path on the sketch. There is not — `addToSketch` always mutates. Optimistic reads are therefore wasted here; the write-lock benchmark confirms StampedLock is uniformly slower than `synchronized` for this workload.

### Node counters
- **`AtomicLong` (baseline)**: Highest accuracy (max-raise semantics exact), worst-case contention pathological under high same-key concurrency. Retained as the accuracy reference but not adopted.
- **`LongAdder`**: Best raw same-key write throughput but altered semantics (accumulator instead of max) makes the membership count diverge from the sketch's true estimate after collision backoff — particularly bad for TopK stability (a collision that briefly decays a slot could leave the membership count permanently inflated). Rejected.
- **`LongAccumulator(Long::max, 0)`** (chosen): Identical semantics to the `AtomicLong` CAS loop, near-LongAdder throughput, no mixed-workload regression. Selected.

## Consequences

- `HeavyKeeper`'s worst-case concurrency (16-thread-single-key) improves ~2.6× over the original CAS baseline; the common mixed hot/cold workload is unchanged.
- `Node.count` is read via `LongAccumulator.get()` (cells are combined with `Long::max`) and mutated via `LongAccumulator.accumulate(maxCount)` or `reset()+accumulate(halved)` on `fading()`. Reads (`list()`, `contains`, `fading`) are reasonably priced (per CPU cells); writes are lock-free per-cell.
- `fading()` performs `reset()` + `accumulate(halved)` on each member under `admissionLock`, which serialises all concurrent writes on existing members for that brief window; this is the same lock surface as the original membership-decay path (`AtomicLong.set(halved)` under admissionLock), so no new contention surface is opened.
- Memory per `Node` rises from a single `AtomicLong` (16 B object header + 8 B value, ~24 B per Node) to a `LongAccumulator` with lazily-allocated per-CPU cells under its `Striped64` base. Under contention on a 16-core machine this can reach ~150–400 B per Node, dominated by `Cell` instances (~24 B each). For default `k = 100` the extra allocation stays bounded in the 15–40 KB range — negligible relative to the rest of the sketch.
- Behavioural contract is unchanged: all 55 `HeavyKeeperTest` tests pass, all 1 346 common-module tests pass.

---

## Revised Decision (v1.1.55)

`Node.count` is **reverted from `LongAccumulator(Long::max, 0)` to `AtomicLong`**.

### Motivation

`decayMembership()` calls `reset()` then `accumulate(halved)` on each Node's `LongAccumulator`. However, `reset()` zeros every `Striped64` cell — including concurrent `accumulate()` writes that arrived between `get()` and `reset()`. These concurrent counts are permanently lost. The `admissionLock` does **not** prevent this because the fast path in `admit()` (lock-free `accumulate()` on existing members) executes without holding any lock.

The `LongAccumulator` API provides no `compareAndSet()` equivalent, so there is no way to atomically lower the stored value without a destructive `reset()`. This is a fundamental API limitation.

### New implementation

```java
// Fast path (admit): atomic max-raise — lock-free, single CAS under no contention
member.count.accumulateAndGet(maxCount, Math::max);

// Decay path: CAS retry loop — if a concurrent accumulate raised the count
// between get() and CAS, the CAS fails and retries with the higher value
do {
    prev = n.count.get();
    halved = prev >> 1;
} while (halved > 0 && !n.count.compareAndSet(prev, halved));
```

### Performance impact

| Scenario | LongAccumulator (old) | AtomicLong (new) |
|---|---|---|
| Same-key 16-thread (Scenario A) | 1 000 000 ops/ms | ~380 952 ops/ms (0.38×) |
| Mixed 80/20 (Scenario C) | 275 862 ops/ms | ~296 296 ops/ms (1.07×, noise) |

The 2.63× worst-case throughput is surrendered, but `AtomicLong.accumulateAndGet(max, Math::max)` is a single CAS under no contention — identical to `LongAccumulator`'s fast path. The regression only manifests under extreme same-key multi-thread contention. Given that this is a correctness fix (systematic count loss on every decay cycle), the trade-off is accepted.

Memory per `Node` drops from ~150–400 B (`LongAccumulator` + lazily-allocated Striped64 cells) to ~24 B (`AtomicLong`).