# ADR-0102: Collision Rows Contribute No Estimate

Date: 2026-10-17
Status: Accepted

## Context

`HeavyKeeper.addToSketch` aggregates a cross-row maximum (`maxCount`) per
key and hands it to `admit`, which is the only gate between a key and the
TopK set. Four of the `depth` rows per key are independent estimates; a row
answers "how much has been counted here for <b>you</b>?" only when the slot's
fingerprint matches. The fingerprint is what makes the row an estimate of
_this_ key rather than an upper bound over the whole key space — the whole
reason HeavyKeeper keeps 32-bit fingerprints per slot.

The partial-decay collision branch contradicted that. After sampling the
survivors and decaying the occupant's windows, it returned
`Math.max(maxCount, slotSums[index])` — the <b>occupant's</b> decayed count,
under a fingerprint that does not belong to the caller. Consequences:

- Any cold key (delta=1 is enough) colliding in a row whose occupant is hot
  came out of `admit` carrying that hot key's count, cleared `minCount`,
  and entered the TopK set — evicting a real member, or displacing one, on
  the strength of someone else's traffic.
- It inverted the ranking: the estimate of a key that had contributed
  <em>zero</em> to a row was the largest value in that row.
- `estimatedCount` / `list()` reported that inherited number as the key's own
  frequency.

Collision probability is low in production geometry (width >= 1024 puts a
single row's collision rate near 0.1%), so this was a slow pollution of the
TopK, not a crash. It was nonetheless test-pinned: three tests
(`addDirect_withCollisionAndDecaySubtracts_shouldDecay`,
`addDirect_withCollisionAndCountAboveLookup_shouldUseMaxLookup`,
`addDirectMap_withCollisionCountAboveLookup_shouldUseMaxLookup`) used the
degenerate `width=1, depth=1` sketch and asserted that a colliding key _did_
get admitted off the shared slot — i.e. the bug was documented as behaviour.

## Decision

**A row the caller does not own contributes exactly zero to `maxCount`.**

`decayCollisionSlot` still applies the sampled decay — the occupant's window
losing mass is a real cost of the collision — but the surviving partial decay
returns `maxCount` unchanged instead of folding in the occupant's
`slotSums[index]`.

The two paths where the slot genuinely changes hands are untouched and keep
returning the newcomer's own `delta` through `takeoverSlot`:

- the size-aware takeover (`delta > cur`, ADR-0092), and
- the full-displacement lottery win (`decays >= cur`).

An empty slot (`updateEmptySlot`) and a fingerprint match (`applyIncrement`)
are likewise unchanged.

## Considered Options

- **Keep the fold and re-document it as an intentional CMS upper bound:**
  rejected — Count-Min Sketch does share every counter with every key, which
  is precisely the property the per-slot fingerprints exist to escape. Calling
  it an upper bound does not make it a usable admission signal: the estimate
  then has no lower bound tied to the key's own traffic, so `minCount` stops
  meaning "this key was seen N times".
- **Return the occupant's count only above `minCount`:** rejected — a
  threshold does not repair the semantics, it just moves the cliff; and it
  would admit exactly the largest polluters.
- **Credit the incoming `delta` into the shared slot** (the rejected option
  from ADR-0092): unchanged reasoning, still double-counts across keys.
- **Decouple the estimate from the sketch and track exact per-key counts:**
  rejected — unbounded memory; the sketch is the reason this fits in a fixed
  footprint.

## Consequences

- A colliding cold key is admitted only after its own traffic actually
  reaches `minCount` in a row it owns (or it takes the slot outright). The
  TopK set now holds keys that were themselves counted.
- Expelled-queue consumers see fewer spurious TopK departures: a stranger
  that used to enter on an inherited count could later be evicted on its own
  (much smaller) real count.
- Three tests were rewritten to assert the corrected contract, and one was
  added for the unchanged full-displacement path. The drift/burst harness
  (`HeavyKeeperBatchRecallTest`, 7 gates including ADR-0092's 10/10
  rotation admission) stays green — recall never depended on the borrowed
  estimate, because admission was already carried by takeover and by
  fingerprint-matched rows.
- Estimator semantics are now uniform with `takeoverSlot`: every contribution
  to `maxCount` is mass this key actually put into a row it owns.
