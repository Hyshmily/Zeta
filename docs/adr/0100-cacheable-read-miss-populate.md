# ADR-0100: @Cacheable Read-Miss Populate Split from Write-Through

Date: 2026-10-14
Status: Accepted

## Context

Spring's `CacheInterceptor` funnels two fundamentally different intents
through one adapter method: a `@Cacheable(sync = false)` miss-fill
(`Cache.get` → miss → method runs → `Cache.put`) and an explicit
`@CachePut` write. `ZetaSpringCache.put` treated both as a versioned
write-through (Redis INCR + cluster broadcast + transaction deferral), so
every cold read behaved like a cluster write — an O(N) Redis write plus
O(N²) broadcast fan-out on cold starts and mass expiries — while the miss
itself never fed hot-key detection, soft-TTL refresh, orphaned-HOT demotion,
or hit/miss stats.

## Decision

**A `@Cacheable` miss-fill is a local read, never a cluster write.**

- `CacheExtensionAspect` marks the thread while inside a `@Cacheable`
  advice (`IN_CACHEABLE_READ` stack; an inner `@CachePut` advice shadows it
  with `FALSE`, so an explicit write nested in a read stays a write).
- `ZetaSpringCache.put` under the marker routes to the new internal
  `populateFromReadMiss`: read-only version probe (ADR-0033, never an INCR),
  immediate version-guarded L1 store (equal applies, ADR-0066;
  Worker-regime preserving like `putLocal`), SingleFlight invalidation
  (ADR-0067) — no broadcast, no transaction deferral.
- `peekAndTag` counts every served read (hit and miss) for detection,
  reporting, and stats; the fill records nothing, so one logical read
  counts exactly once. Bare `@Cacheable` methods skip the aspect's key
  resolution (Spring owns it, including its null-key rejection) but still
  carry the marker.

## Considered Options

- **Keep broadcasting read fills** (status quo): rejected — reads must not
  masquerade as writes; the storm cost scales with the fleet, not the data.
- **Route fills through `putLocal`**: rejected — `putLocal` skips version
  stamping, so a fill could pin an unversioned entry that later broadcasts
  wrongly order against; the probe stamp keeps the dual-version-space
  invariant (ADR-0008/0033) intact on every L1 write.

## Consequences

- `@Cacheable` misses no longer appear in Redis version keys or the sync
  plane; peers converge through their own reads (or explicit `@CachePut`
  writes), which is the documented Spring read semantic.
- `stats().missCount()` now includes annotation-path misses (previously 0
  there); dashboards dividing on the old 0-miss baseline shift once.
