# Lock Hierarchy and Ordering

Zeta uses three independent locking domains across the hot-path detection and state-machine layers. The ordering between them is implicit but critical: violating it causes deadlock. This ADR documents the hierarchy explicitly so future code changes respect it.

## The Three Lock Domains

| Domain | Guard | Implementation | Scope |
|---|---|---|---|
| **HeavyKeeper admission** | TopK membership mutations (`admitOrEvict`, `decayMembership`) | `ReentrantLock` (`admissionLock`) | Global — one per HeavyKeeper instance |
| **HeavyKeeper sketch stripes** | Per-slot sketch state (`windows[]`, `slotSums[]`, `fingerprints[]`) | `synchronized(Object[])` (up to 4096 stripes) | Per-stripe — stripes are distinct objects |
| **State machine per-key** | Per-key hot/cold streaks and state transitions | `Striped.lock(4096)` (`keyLocks`) | Per-key — 4096 stripes via Guava |

## Lock Hierarchy (strict total order)

```
1. State machine per-key (keyLocks)          ← highest in acquisition order
2. HeavyKeeper sketch stripes (lockStripes)
3. HeavyKeeper admission (admissionLock)     ← lowest
```

**Rule:** Code may hold N locks only if all locks with smaller ordinal are released before acquiring any lock with larger ordinal. Equivalent to: acquire in descending ordinal order, release in ascending.

## Current Lock Acquisitions

### HeavyKeeper.addDirect (sketch write + possible admission)

```
1. sketch stripes (synchronized)     ← acquired per-row, released after each row
2. admissionLock                     ← acquired only for new-member admission
```

The stripes are acquired and released per-row inside `addToSketch`, so at most one stripe is held when `admissionLock` is taken. This is correct per the hierarchy.

### HeavyKeeper.fading (periodic window rotation)

```
1. sketch stripes (synchronized)     ← rotateSketchWindows: acquired one stripe at a time
2. admissionLock                     ← decayMembership: acquired after all stripes released
```

`rotateSketchWindows` acquires and releases stripes sequentially, so by the time `decayMembership` takes `admissionLock`, no stripe lock is held. Correct.

### ZetaStateMachineImpl.evaluate (per-key evaluation)

```
1. keyLocks.get(key) (lock)          ← held for entire evaluate()
2. [no HeavyKeeper lock acquired]
```

`confidenceEvaluator.evaluate()` performs only a read of the HeavyKeeper sketch (`ctx.cmsCount()` was already pre-computed in the `EvaluationContext`). No HeavyKeeper lock is taken inside `keyLocks`. **Critical invariant: no code path inside `keyLocks` may acquire any HeavyKeeper-level lock.**

## What Would Violate the Hierarchy

- Adding a `HeavyKeeper.add()` call inside `ZetaStateMachineImpl.evaluate()` — would acquire `keyLocks` → sketch stripe, which is the reverse order of the add→admission path and creates deadlock with `fading()`.
- Adding a `confidenceEvaluator.evaluate()` that lazily computes `cmsCount` (if it acquires a sketch stripe internally).
- Taking `keyLocks` inside a HeavyKeeper callback or listener.

## Non-Goals

This ADR does not attempt to prove correctness of the locking within each domain (see ADR-0014 for HeavyKeeper concurrency, ADR-0006 for stripe-lock fading). It only documents the cross-domain ordering constraint.
