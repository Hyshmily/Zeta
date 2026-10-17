# ADR-0099: Rule Distribution Unified on Redis; Sync-Plane Wire Types Reduced

Date: 2026-10-13
Status: Accepted

## Context

Rules historically reached peer instances through two channels working in
parallel ("both/and"):

1. **Redis persistence** — every mutation wrote the versioned JSON wrapper to
   the `KEY_RULES` key through a Lua compare-and-set script (ADR-0022 TTL
   guard, ADR-0062 version authority).
2. **AMQP broadcast** — the same payload also rode the sync exchange as a
   `RULES_SYNC` message; receivers ran a version-authoritative full replace
   (ADR-0062, superseding ADR-0012's union merge).

Two channels for one payload meant two failure domains, a publisher seam on
`RuleMatcherImpl`, a receive branch in the sync plane, and a public facade
method (`Zeta.RuleAdmin.broadcastAllLocalRulesManually`) whose only purpose
was to re-drive the broadcast.

The same review pass found the data plane carrying two further rolling-upgrade
relics: the `INVALIDATE_ALL` wire type (its in-tree producer was already gone —
ADR-0066 routes batch invalidations as per-key versioned INVALIDATEs) and the
legacy unversioned union-merge branch of rule sync.

## Decision

**Redis is the single rule distribution channel.**

- Every local rule mutation persists through the existing Lua CAS (unchanged).
- A daemon poll inside `RuleMatcherImpl` (`zeta-rules-reload`, 30s cadence)
  reloads the persisted set and adopts it **only when the persisted version is
  at least the local version** — a reload racing a local mutation (version
  bumped, CAS not yet landed) can never clobber the newer in-memory snapshot,
  and the winning version always reaches Redis first, so the next tick picks
  it up.
- Removed: `RULES_SYNC` wire type, `SyncDecisionHandler.handleRulesSync`,
  `RuleRepository.syncRules`, `CacheSyncPublisher.broadcastAllLocalRules`,
  `Zeta.RuleAdmin.broadcastAllLocalRulesManually`, the
  `Optional<CacheSyncPublisher>` constructor seam on `RuleMatcherImpl`, and
  the record component `SyncMessage.rulesVersion` (no consumer remains).

Also removed in the same pass, for the same no-legacy-wire rationale:

- The `INVALIDATE_ALL` wire type end to end (sender `broadcastAll`, publisher
  batch path with its 1s payload dedup, listener case, handler, and the
  `SyncOperation.INVALIDATE_ALL` constant). Batch invalidation is per-key
  versioned INVALIDATE, per ADR-0066.
- The legacy unversioned union-merge branch and legacy array parse in the rule
  JSON codec (wrapper format only; a version-less broadcast is ignored — it
  carries no ordering information and adopting it would reset convergence to
  last-writer-by-arrival).

## Consequences

- **Convergence latency** changes from ~instant (broadcast) to ≤ 30s (poll).
  Rules are operator-driven access control, not request-path state: a
  sub-minute convergence window is acceptable, and the CAS/poll pair preserves
  the exact-delete convergence ADR-0062 delivered (a fresher persisted set
  still replaces the local list wholesale).
- One less message type on the sync plane: the fanout now carries only
  INVALIDATE / REFRESH / PUT. Payload-size-based dispatcher weighting stays
  (PUT payloads are the heavy bodies now).
- A peer's rule change is visible after a broker partition heals with no
  manual re-drive: the poll self-heals what the broadcast used to need
  `broadcastAllLocalRulesManually()` for.
- The `rulesVersion` restore-on-load contract (ADR-0062) is unchanged: a
  restart restores the persisted version so post-restart local changes are
  neither CAS-rejected nor clobbered by a stale reload.
- The reload adoption guard (`persisted >= local`) is the only new ordering
  rule; it is the poll-time mirror of the receiver-side strictly-fresher
  adoption it replaces.

## Rejected

- Keep broadcast + drop Redis: loses the durable backstop and the
  restart-recovery path.
- Redis keyspace notifications instead of polling: requires enabling notify
  config on every deployment's Redis (operational burden) for a 30s-tolerable
  latency.
- Keep `rulesVersion` on `SyncMessage` as a reserved field: a zero-value
  component with no reader is dead weight in every constructor call site.
