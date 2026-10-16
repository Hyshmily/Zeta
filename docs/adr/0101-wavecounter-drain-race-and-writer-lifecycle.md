# ADR-0101: WaveCounter — Drain/Writer Race Closure and Writer-Map Lifecycle

Date: 2026-10-16
Status: Accepted

## Context

A review of `WaveCounter` (the hot/cold per-key routing counter feeding
`HotKeyDetector`) found four classes of defect that the existing tests could
not see, because all of them live in the interleaving between the deliverer's
drain sweep and the owner writer's lock-free `add`:

1. **Slot teardown order (NPE).** The writer's lock-free `add` claims a slot
   on `tags[i] == 0` alone. `waveTo` cleared a merged slot in the order
   `tags[i] = 0; keys[i] = null; clearBit(w, j)`, so a writer probing in the
   window after the tag store reclaimed a slot the sweep was still tearing
   down; the sweep's trailing stores then wiped the new key and bit. The slot
   was left as `tags[i] != 0 && keys[i] == null`, and the next merge passed
   `null` into `mergeKey` → `ConcurrentHashMap.putIfAbsent(null, …)` → NPE.
   On the deliverer the NPE aborts the whole tide (`tide()` catches
   `Throwable`); on the writer's own `discharge` it escapes into the caller
   of `count()`.
2. **Non-atomic occupancy-bit set.** `add` used
   `occupied.setRelease(w, getAcquire(w) | shift)` while the sweep cleared
   bits in the same word with a CAS loop — a lost update resurrects bits the
   sweep had just cleared (phantom claim bits, which the sweep's 0-take
   branch never clears again).
3. **Instance-level retention after `destroy()`.** `Ceils` was a non-static
   inner class and `hotLocals` is a `ThreadLocal` that is deliberately never
   removed. That closes
   `Thread → ThreadLocalMap.Entry.value → Ceils → WaveCounter → hotLocals`,
   so the entry's `WeakReference` key can never be cleared and a destroyed
   counter (16 KB beacon, six 1024-entry scan arrays, registry, up to 256
   pooled ~7 KB maps) stays pinned for the lifetime of every writer thread.
4. **Unbounded writer registry under thread churn.** `ceilPool` caps reuse at
   256 and the registry is only reaped once per tide, so a deployment that
   churns writers at request rate (per-request threads; virtual threads —
   Java 17 has no `Thread.isVirtual()` to detect them) registers tens of
   thousands of `Thread` keys and ~7 KB maps inside one tide interval.

Two smaller items came along: the `memberKeys++` off-by-one in the
newcomer pass (the capacity `break` was checked after the increment, so the
renewal numerator counted a key that was never promoted), and a
documentation/semantics mismatch — `opMaxCount` was documented as "every N
increments" but implemented as "N distinct slots claimed".

## Decision

**Slot teardown publishes the empty tag LAST, with release ordering, and
every merge reads the key once.**

- New `Ceils#releaseSlot(int i)`: `keys[i] = null` → `clearBit(w, j)` →
  `TAGS.setRelease(tags, i, 0)`. `tags[i] == 0` is the writer's only
  empty-slot predicate, so it is published only once the slot is fully torn
  down; a writer that observes it can no longer have its fresh key/bit wiped
  by the tail of that sweep. `waveTo`, `reconcile` and `recoverZero` all go
  through it.
- `waveTo` and `reconcile` read `String k = keys[i]` BEFORE the
  `counts.getAndSet(i, 0)` take and merge the local copy. `k == null` (a
  slot the sweep is mid-teardown that the owner re-claimed) reclaims the
  slot without merging: unreachable after the reorder, and a null-key merge
  would NPE the tide rather than drop one count.
- The `add` post-check is now **unconditional on the stamp**: with the key
  possibly wiped, re-reading tag/key can no longer distinguish "the sweep
  merged it" from "the sweep clobbered my key", so any observed stamp change
  runs the tag-driven `reconcile` (which merges every resident entry exactly
  once). Paid a handful of times per writer per tide.

**Occupancy bits are set with a CAS loop.** `Ceils#setOccupied(w, shift)`
mirrors `clearBit`'s CAS; `AtomicLongArray` has no `getAndBitwiseOr`, and
`getAndUpdate` with a capturing lambda would allocate per call.

**`Ceils` is a static nested class and the owner is a parameter, never a
field.** `add`, `tryFastAdd`, `recoverZero`, `drainInto`, `tryDrainInto`,
`drainDead`, `reconcile` and `waveTo` take `WaveCounter owner` as their first
argument. `destroy()` additionally clears `hotRegistry` and `ceilPool`
(every residual has been merged by then).

**The writer registry is capped.** `MAX_HOT_WRITERS = 4096`: past the cap,
`acquireLocal()` returns `null` and the writer counts straight down the cold
path instead of allocating a private map. A thread that is about to die gains
nothing from local aggregation anyway (its data has to wait for a tide
regardless), so the trade is nil aggregation for an O(cap) memory bound.

**The batch trigger moved into `Ceils#add`**, next to the two places that
grow `size` (the lock-free claim and the locked claim), so neither the
fast-add hit nor the cold path reads `size` per op. The Javadoc now says
what the code does: a slot-count trigger, not an increment count.

**Newcomer pass counts only what it promotes**: the capacity `break` precedes
`memberKeys++`, so `renewal` is no longer inflated by the key that triggered
the break.

**`PaddedMergesInFlight` is deleted.** Its 240 padding bytes surrounded a
`final LongAdder` reference; the state written concurrently lives in
separately allocated `Striped64.Cell`s (already `@Contended`). The field is a
plain `LongAdder` now.

## Considered Options

- **Keep the tag-first teardown, guard only the merge**: rejected — the null
  guard turns the NPE into a silent count drop, and the review's measurement
  (the regression test below) shows the corrupted slots lose ~30% of the
  counts, not one.
- **Store the owner as a field** (`new Ceils(this)`): rejected — it restores
  the exact reference cycle the static refactor removes.
- **Detect virtual threads and route them cold** (`Thread.isVirtual()`):
  rejected — Java 21+; the module targets Java 17. The registry cap is
  version-agnostic and also covers per-request-thread deployments.
- **Shard writer maps over a fixed `Ceils[]`** (Striped64-style probe):
  rejected — several carriers would share one map, breaking the single-owner
  assumption that lets `add` run lock-free.
- **Reuse the kth-shortcut view for the incumbent split / promotion scan**
  (the review's O(N) → O(hotLimit) proposal): **adopted, with one
  correction.** `selectKthLargest` permutes the index view it selects on, and
  `aboveKthIdx` doubles as the scan view, whose ORDER decides which keys the
  capacity break cuts and therefore which keys the incumbent-first gate
  renews. Selecting on `aboveKthIdx` directly reshuffled that order and broke
  `WaveCounterDensityBoundaryTest#fallenIncumbents_yieldSlots_toNewcomers`
  (the incumbent set became completely disjoint across tides). The selection
  now runs on `selectIdx`, a reusable copy, so `aboveKthIdx` keeps snapshot
  order and the split/scan walk ~hotLimit entries instead of every
  floor-passing candidate.
- **Reuse quickselect's partition to skip the `>= kth` count**: rejected —
  Hoare's post-partition invariant does not make `[target, hi]` a clean `>=
  kth` band, and the saving is one O(~hotLimit) pass per tide.
- **Recycle `PromotionPass` across tides / reset `lastKth` in `clear()`**:
  rejected — one small allocation per promoted tide is free, reuse adds
  twelve fields of reset risk, and `lastKth` is documented as an adaptive
  reference retained across `clear()` (a stale seed only widens or narrows
  the shortcut filter, never breaks the lemma).

## Consequences

- The hot path stays exact: the new regression test
  (`WaveCounterDrainRaceTest#concurrentDrainAndAdd_shouldNeitherThrowNorLoseCounts`)
  runs four writers cycling over eight hot keys against a concurrent drainer
  and asserts zero loss over 160k counts; with the old teardown order it
  delivers 111,159 of 160,000. The drainer calls `discharge` reflectively
  rather than `tide()` on purpose — `tide()` catches `Throwable` and would
  swallow the very NPE this guards.
- Steady-state cost is unchanged: the release store on `tags[i]` is a plain
  store on x86 (TSO) and a release store on weak models, paid once per merged
  slot; `setOccupied`'s CAS is uncontended (a slot is claimed once per
  batch).
- `destroy()` is now sufficient to release everything the counter owns; a
  live writer's `ThreadLocal` entry is reclaimed by GC once the counter
  itself is unreachable, which the static `Ceils` makes possible.
- The registry cap is a behavioural change only for deployments above 4096
  concurrently counting threads: they lose local aggregation for the excess
  (counts still land in the shared table, cold-path precision) instead of
  paying unbounded memory.
- Full `common` suite: 2096 tests, 0 failures.
