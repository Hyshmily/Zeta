# 0064. Slice-Gated addCount Fast Path (SlidingWindowDetector)

## Status

Accepted (2026-08-30)

## Context

`SlidingWindowDetector.addCount` ran a bin-locked `windows.compute` on **every** call:
the CHM bin lock serializes the timestamp RMW + stale-slice clearing against a
concurrent eviction's re-check-and-remove (the ADR-0061 merged-entry design).
But `clearStaleSlices` is a pure no-op whenever the key was last touched within
the *current* slice (`elapsedSlices == 0`), and hot keys are reported by many App
instances within one slice — so the vast majority of computes for the hottest
keys did nothing but pay the bin lock (plus contention across the 8 concurrent
report consumers) and re-write an unchanged timestamp.

## Decision

`addCount` takes a lock-free fast path: `windows.get(key)` first; the bin-locked
compute runs only when the entry is missing **or** its timestamp is at least one
slice old (`now - lastAccessTime >= timeMillisPerSlice`). The gate exactly
implies the slow path's `clearStaleSlices` would be a no-op, so counting is
byte-identical.

The eviction timestamp is now refreshed **at most once per slice** instead of per
call. Soundness:

- **Eviction cannot remove a window an arriving count is touching.** The gate
  routes any addCount on an entry ≥ 1 slice old through the slow path, which
  refreshes the timestamp and defeats the evictor's re-check — exactly the
  interleaving ADR-0061 pinned. The fast path only applies to entries < 1 slice
  old, which the evictor cannot target anyway.
- **Precondition:** the no-lost-count guarantee now assumes the eviction
  staleness threshold is at least one slice (`staleAfterMs >= timeMillisPerSlice`).
  Any real configuration satisfies this by four orders of magnitude (default
  5 min threshold vs 62 ms slice). A degenerate config with
  `staleAfterMs < sliceMs` loses the between-scan-and-remove protection for
  same-slice adds; the pinned test was re-timed to cross a slice so it exercises
  the production interleaving.

## Consequences

- Widely-hot keys pay one lock-free map read instead of the bin-locked compute
  on ~(reports per slice − 1) of their calls, and cross-consumer bin-lock
  contention on the hot keys drops accordingly.
- The W-1 pre-clear arithmetic (shared with `GlobalQpsEstimator`) moved to
  `SliceWindowMath` — one home for the invariant.
- Companion in the same pass: the per-key state-transition logs in
  `ZetaBayesianSM` moved from per-key INFO to DEBUG plus one aggregate INFO per
  10 s window (`TransitionLogThrottle`, the ADR-0037 log convention) — a
  mass-heat event (the ADR-0061 scenario) no longer emits thousands of INFO
  lines per batch.

## Considered Options

- **Incremental running sum** maintained per window — rejected: an exact running
  sum must be updated by the count add, which would force the add under the bin
  lock and destroy the deliberately lock-free add+sum path.
- **Plain (non-volatile) per-call timestamp write outside the lock** — rejected:
  a racy write with no benefit over the slice-granularity refresh.
- **Keep per-call compute** — rejected: measurable lock traffic and contention
  on exactly the keys that matter most, for a freshness the eviction threshold
  cannot observe.
