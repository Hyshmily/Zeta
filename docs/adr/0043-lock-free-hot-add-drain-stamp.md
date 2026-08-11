# ADR-0043: Lock-Free Hot Add via Drain-Stamp Protocol + Atomic Slot Counts (WaveCounter)

The WaveCounter hot path replaced its per-map `ReentrantLock` lock/unlock pair (~25-40ns/op, ~40-50% of the hot add) with a seqlock-style drain-stamp protocol: a volatile even/odd `drainStamp` read, slot writes followed by a release bit-store, a post-check that recovers a raced entry tag-driven into the current table, and — after a measured race in the first variant — atomic per-slot counts (`AtomicLongArray.getAndAdd` / `getAndSet(0)`) with an exact 0-return residual recovery. Measured (16-thread interleaved A/B, JDK 21): hot path +60% vs the locked baseline (the first lock-free variant measured +67%; the atomic protocol costs ~8-10% on the pure hot path and buys exactness), skewed 80/20 +35-40%, cold path unchanged, 1742 common tests green, 0/60 on the racing-drain stress that caught the race.

## Status

accepted

## Context

`Ceils.add` took a `ReentrantLock` on EVERY hot increment, serializing the owner writer against the deliverer's periodic drain. The drain happens at most once per writer per tide (500ms) — the mutex was paid per-op for an event that occurs ~µs-scale once per ~millions of ops. A 16-thread interleaved micro-benchmark (pre-seeded beacon, pre-generated key arrays, clock sampled per 128 ops) attributed ~25-40ns of the ~55-95ns per-op hot-path latency to the lock/unlock pair. The previous evaluation only compared ReentrantLock vs monitor ("within run-to-run noise") — lock-free was never measured.

The correctness contract to preserve: the hot path is exact — a drain must never observe a torn entry, and a racing add must never be lost or double-counted. The locked design achieved this by mutual exclusion; the lock-free design must achieve it by ordering and recovery.

### The race the first variant introduced (and why the atomic protocol exists)

The first lock-free variant used a NON-ATOMIC `counts[i] += delta` on the update path. When a drain's take-and-clear landed between the writer's read and its write-back, the write-back fell into the just-cleared slot: the old value was already in the shared table, the slot's tag/key were cleared, and the update path had no post-check — the delta vanished silently. Measured on `WaveCounterRecyclingTest.tryLockSkip_shouldNeverLoseHotCounts` (8 writers × 5k counts racing a 5ms deliver loop): ~3e-6/op, i.e. exactly 1-2 counts lost per ~10-18% of test runs; the locked baseline was 0/20. A first repair (skip slots without clearing them; per-merged-slot bit clears instead of the wholesale clear) did NOT change the failure rate (4/40, 11/60) — the hole was the update path's read-modify-write, not the bit clearing. The fundamental issue: a non-atomic read-modify-write racing a take cannot tell whether its delta was taken, and the dead slot's residual cannot be split into "already-merged old value" + "un-merged delta" — an information gap no ordering of plain stores can close.

### The non-atomic variant, concretely (the measured alternative)

For reference — the rejected variant's exact mechanics, so a future reader understands what the atomic protocol replaced and what its 8-10% margin bought:

- **Slot state:** `counts` is a plain `long[]`; the occupied bitmap is an `AtomicLongArray` (release bit-store / acquire bit-read); `drainStamp` (volatile even/odd) gates the fast path.
- **Claim path** (new key): write `tags[i]` / `keys[i]` / `counts[i]` (plain stores), then `occupied.setRelease(w, getAcquire(w) | shift)` LAST; post-check `drainStamp` — if it moved and the entry is still resident (`tags[i] == tag`), the whole map is merged tag-driven into the current table (bounded one-tide delay).
- **Update path** (existing key): plain `counts[i] += delta` — NO post-check, NO bit re-store. This is the hole.
- **Drain (`waveTo`)** (bit-driven): for each set bit, read `counts[i]`; if non-zero, merge and clear the slot; if zero, skip WITHOUT clearing the slot (repair #1) — then ALWAYS wipe the whole word's bits at the end (`occupied.setRelease(w, 0L)`).
- **Reconcile** (tag-driven): merge every `tags[i] != 0 && counts[i] != 0` slot, clear it, then wipe all bits.
- **Loss interleaving:** drain reads `counts[i]` = old value v, merges v, clears the slot (tags/keys/counts = 0); writer's write-back of v+delta lands in the cleared slot; the trailing bit wipe removes the slot's mark; the update path never re-checks — the delta is nowhere. The slot's residual (v+delta) cannot be split into "v already merged" + "delta lost", so no post-check on the update path can recover it exactly.

The atomic protocol closes this by construction: the take (`getAndSet(0)`) and the add (`getAndAdd`) serialize per slot, a taken slot reads exactly 0, and the residual is always precisely the un-merged delta.

## Decision

**Drain-stamp protocol.** Each `Ceils` gains `volatile long drainStamp` (even = quiescent, odd = drain/reset in flight; written only by the deliverer's `tryDrainInto` and by `reset()`, read by every fast add). The fast add:

1. Reads `drainStamp`; if odd, falls back to the locked `addSlow` (a drain is actively sweeping — µs-scale, once per writer per tide).
2. Writes tag/key/count into its slot, then stores the occupied bit LAST via `AtomicLongArray.setRelease` — a drain that observes the bit (via `getAcquire`) observes a complete entry. A torn read is impossible: half-written entries are never marked.
3. Post-checks `drainStamp`: if it moved, a drain raced this add. If the entry is still resident (the sweep passed before the bit-store), the whole map is merged tag-driven via `WaveCounter.reconcile(Ceils)` into the CURRENT table — the count joins the next tide's snapshot (bounded one-tide delay, the same bound as the documented skip of a locked map). Never loss, never double.

**Atomic slot counts (the exactness fix).** `counts` became an `AtomicLongArray` and every mutation is atomic per slot, serializing the two contenders:

- **Writer update:** `counts.getAndAdd(i, delta)`. A return of 0 means a drain took the slot before us — the prior value is PROVABLY already in the shared table, so the residual in the slot is exactly our delta; `Ceils.recoverZero` takes it (`getAndSet(0)`) and merges it into the current table under the per-map lock (lock order reservoirGate → per-map lock, mirroring `discharge`, so no deadlock with the tide paths). A 0 take during recovery means the sweep already consumed it — nothing to merge.
- **Drain (waveTo / reconcile / drainDead):** `counts.getAndSet(i, 0)` — the atomic take. A 0 take means the slot was already consumed (or is a phantom mark) — skip WITHOUT clearing the slot or the bit, so a racing update's residual stays visible for its recovery or the next sweep. The merged slot's bit is cleared selectively (CAS), never wholesale.
- **Claim:** `counts.setRelease(i, delta)` — the release-store keeps the publish ordering with the trailing bit-store.

Because the take and the add are serialized per slot, the residual is always precisely the un-merged delta — the information gap of the non-atomic variant is gone by construction, and the recovery is exact in both interleavings.

The `ReentrantLock` remains as the slow path — serializing the deliverer's drains, the writer's batch discharge, the tag-driven reconcile, the residual recovery and `reset()` — and the deliverer still `tryLock()`-skips a busy map. `Ceils` became an inner class to reach `reconcile`/`mergeResidual`; `destroy()` merges via the tag-driven reconcile (a fast add can be mid-flight at shutdown).

## Considered Options

- **Status quo (per-op lock).** Correct and proven; pays ~25-40ns/op for a once-per-tide event. Rejected after measurement.
- **Lock-free add + spin-wait writers.** Writer spins while `drainStamp` is odd instead of post-checking. Analysis showed a sub-µs preemption in the read-even → write window can lose a count — a regression from the cold path's documented >1ms window. Rejected; the post-check + reconcile recovers the race instead of waiting it out.
- **First lock-free variant (non-atomic `counts[i] += delta`).** Measured +67% hot path, but the update-vs-take read-modify-write race lost ~3e-6/op (red on the tryLockSkip stress, 10-18% of runs). Rejected — the dead-slot residual cannot be split into "already-merged" + "un-merged delta", so no ordering of plain stores can recover it.
- **Repair #1: selective bit clearing + skip-without-clear.** Keeps the sweep from wiping marks it did not consume; does NOT touch the update path's write-back — failure rate unchanged (4/40, 11/60). Rejected as the primary fix (kept as defense in the final protocol).
- **Per-slot versioning (4th array).** Closes every window exactly but doubles the slot state and the sweep work. Rejected — the atomic take/recovery already closes it exactly.
- **Accept the ~3e-6/op loss as a documented approximation.** Evaluated: single-count random loss does not flip any hot-key decision (thresholds are tens-hundreds), but the loss has no hard bound (unlike the cold path's 1ms-preemption window), it clusters on the hottest keys (the update-vs-take window is where traffic is highest), it breaks the exact-count contract and CI's exact assertions, and the atomic protocol costs only ~8-10% of the hot path. Rejected.
- **Per-op in-flight counter.** Measured ~15ns/op when tried before (2026-08-08) — precisely the per-op cost the routing design exists to avoid. Rejected.

## Consequences

1. **Hot-path throughput +60% vs the locked baseline** (hotonly 550-657M ops/s vs 350-394M; the first lock-free variant measured +67% at 582-673M — the atomic getAndAdd costs ~8-10% on the pure hot path, skewed +35-40%, cold path neutral; interleaved A/B on 16 threads, JDK 21).
2. **Correctness model:** the hot path is exact — a racing fast add is recovered (tag-driven reconcile or 0-return residual recovery) into the current table, never lost, never double-counted; at most one tide of delay.
3. **The ordering is load-bearing:** bit-after-write + release/acquire + atomic slot takes. Future changes to the occupied bitmap, the slot writes, the stamp transitions or the counts' atomicity must preserve them.
4. **Reconcile/recovery cost:** a 256-slot tag scan per writer per racing tide + a per-writer 0-return recovery (µs-scale, bounded); `destroy()` pays one reconcile per writer.
5. **Verified:** all 1742 common-module tests green on the final variant; the racing-drain stress (`tryLockSkip_shouldNeverLoseHotCounts`) 0/60 vs 2/20 and 4/40/11/60 for the broken variants; benchmark harness removed after the run.

## Measurement: paradigm comparison (three paradigms table)

The paradigm table from the "next paradigm" issue, re-measured 2026-08-10 on the original CounterBench harness (JDK 26, 16 logical cores): the `Zeta double-buffer` (BufferedCounter) and `WaveCounter` (atomic variant) columns are medians of 13-27 interleaved rounds; `Simple double-buffer` is a historical prototype measurement. M ops/s:

| Workload | Simple double-buffer† | Zeta double-buffer | WaveCounter (atomic variant) | WC / ZDB | WC/ SDB |
| --- | ---: | ---: | ---: | ---: | ---: |
| A: 1 writer × 64 hot keys | 57.6 | 45.5 | 48.8 | 1.07x | 0.85x |
| B: 16 writers × 64 hot keys | 122.2 | 70.5 | 346.1 | 4.91x | 2.83x |
| C: 16 writers × 10k steady keys | 18.8 | 56.6 | 91.4 | 1.61x | 4.86x |
| D: 1M-key churn @ 100k cap | 12.6 | 21.3 | 68.8 | 3.23x | 5.46x |
| E: slow consumer (30ms), hot/cold mix | 26.3 | 31.7 | 37.1 | 1.17x | 1.41x |

† Historical prototype measurement, superseded code path — not re-measurable.

Reading: WaveCounter beats Zeta double-buffer 1.1-4.9x (the 16-writer hot workload B is the largest gap — zero shared access vs shared-structure contention); the simple double-buffer only wins on the single-writer hot workload A (a shared table with one writer has no contention to amortize). Workload B's bimodality (see the distribution table below) applies to both the WaveCounter columns here; the medians sit between the promotion modes.

## Measurement: interleaved distribution (variant vs atomic, original CounterBench harness)

Re-measured 2026-08-10 on the original `CounterBench` harness (JDK 26, 16 logical cores, real tide-driven promotion, 3s warmup + six 1s samples per round, median per round; 12-14 interleaved rounds per cell). M ops/s:

| Workload | Version | N | min | p25 | median | p75 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A: 1 writer × 64 hot | VARIANT (non-atomic) | 14 | 37.9 | 41.2 | 54.5 | 61.9 | 64.7 |
| | ATOMIC | 13 | 37.3 | 42.3 | 48.8 | 55.6 | 60.2 |
| B: 16 writers × 64 hot | VARIANT | 14 | 71.5 | 266.8 | 334.4 | 387.8 | 596.9 |
| | ATOMIC | 13 | 49.8 | 330.0 | 346.1 | 365.0 | 528.7 |
| C: 16 writers × 10k steady | VARIANT | 13 | 71.2 | 78.3 | 90.8 | 111.0 | 168.1 |
| | ATOMIC | 13 | 68.2 | 82.6 | 91.4 | 112.8 | 124.0 |
| D: 1M-key churn @ 100k cap | VARIANT | 12 | 50.9 | 55.3 | 66.2 | 82.3 | 96.8 |
| | ATOMIC | 13 | 60.1 | 64.9 | 68.8 | 75.4 | 78.8 |
| E: slow consumer (30ms) | VARIANT | 12 | 27.6 | 32.4 | 41.1 | 45.0 | 53.1 |
| | ATOMIC | 13 | 29.4 | 34.5 | 37.1 | 43.1 | 45.8 |

Reading: cold-path workloads are identical code in both variants and measure neutral (C median 90.8 vs 91.4 — a measurement sanity check). The atomic cost appears only on hot-path workloads: A −10.5%, E −9.7% (median). B is bimodal in BOTH variants (the 64 hot keys either fully promote or fall back to the cold path, 71-597M range) — the medians sit between the modes and the p25 gap (266.8 vs 330.0) suggests the atomic variant's promotion is marginally more stable, within noise. The ~10-12% hot-path margin is the price of the exact contract; the locked baseline sits ~40% below both (see consequence 1).
