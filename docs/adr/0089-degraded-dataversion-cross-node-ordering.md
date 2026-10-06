# Degraded dataVersion Cross-Node Ordering (A4 Closure)

Legacy item A4 (FIX_REPORT 2026-10-05): while Redis is down, two App instances
write degraded versions (`Long.MIN_VALUE | Snowflake`, ADR-0019) ordered only
by wall-clock timestamps. Under clock skew the `VersionGuard` both-degraded
comparison orders them wrongly, and with no further writes nothing
re-broadcasts — the loser pins peers on its value until the next write or TTL.

The report parked A4 on "needs a wire-protocol change (origin instance ID in
the sync message) or a global sequence". The wire half of that objection is
now gone: every sync message already carries `HEADER_ORIGIN_INSTANCE`
(ADR-0067, set by `CacheSyncPublisher`, consumed for self-REFRESH filtering).
What remains is evaluated — and rejected — below.

## Status

Decided (2026-10-06). No code change: the current matrix stands, bounded and
observed as described under Decision.

## Decision

Keep the current 4-case `VersionGuard` matrix unchanged:

1. Within-outage cross-node degraded ordering has no truth source available
   to the receiver. Same-origin degraded versions are already monotonic
   (per-node Snowflake), so per-origin scoping would only relabel the one
   branch that is already correct; cross-origin arrival-order LWW does not
   recover truth either — it merely swaps a skew-ordered wrong answer for an
   arrival-ordered wrong answer at the cost of a new `versionNodeId` field on
   every entry and every write path.
2. The damage is bounded by construction: a degraded version never beats a
   normal one (case 2), so the outage window only ever arbitrates among
   degraded writers; the first normal write after recovery converges every
   peer (case 4), and silence converges at the entry TTL via reload.
3. The window is observed, not silent:
   `zeta.version.degraded.total` (cumulative fallback count) plus the
   `zeta.stall.redis_degraded.*` gauges mark exactly when the cluster is in
   the unordered regime.

## Considered Options

- **Per-origin degraded comparison** (store the writer's instance ID on the
  entry, compare only within an origin, arrival-LWW across origins).
  Rejected: needs an entry-schema field plus population on all five store
  paths (load/put/refresh/broadcast/sync) for a comparison that is still
  wrong across origins — complexity without correctness.
- **Global sequence for degraded writes** (Redis-INCR-equivalent without
  Redis). Rejected: a global order without the store *is* consensus; against
  the project's explicit best-effort sync semantics (ADR-0004/0007/0013).
- **Recovery reconciliation sweep** (on Redis recovery, bump + rebroadcast
  every degraded key). Rejected: requires tracking the degraded key set
  (unbounded cardinality) for an event whose natural heal (next write/TTL)
  already converges; the sweep itself would be a broadcast storm sized by the
  outage.

## Consequences

1. No receiver, entry-schema, or wire change ships for A4.
2. Operators watch `zeta.version.degraded.total` rising as the signal that
   cross-instance values are arrival-ordered, not truth-ordered, until it
   stops.
3. Reopen only with new evidence: a production incident where the
   degraded-vs-degraded window (not the degraded-vs-normal case, which is
   already correct) caused user-visible harm beyond one entry TTL.
