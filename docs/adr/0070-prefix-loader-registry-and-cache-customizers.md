# ADR-0070: Prefix Loader Registry (LoadingCache-style read-through) and L1 cache customizers

- Status: Accepted (registry half: **DEPRECATED**, removed 2026-10; customizer half: in force)
- Date: 2026-09-18
- Related: ADR-0030 (bin-lock discipline / SingleFlight), ADR-0031 (valueless refresh fallback), docs/extension-points-review.md

> **Partial deprecation (marked 2026-10-15) — the loader-registry half of this ADR is gone; only the cache-customizer half remains in force.** `ZetaLoaderRegistry`, `PrefixRoutedLoader`, `ZetaLoadingSpec`, and the no-reader `get` overloads they served were **removed from the codebase** (recorded in ADR-0096). The symbol-drift table below is superseded by that removal. The `ZetaCacheCustomizer` contract and its `ZetaAutoConfiguration#hotLocalCache` wiring are still live. Do not cite the registry as current architecture.

> **Symbol drift — read this before acting on the Decision text.** Several
> names below were renamed after acceptance; the behaviour decided here is
> unchanged, but the identifiers are not:
>
> | Written in this ADR | Current name |
> |---|---|
> | `ZetaCacheLoader<V>` (root SPI) | `CacheLoader<V>` (`cache/loader/CacheLoader.java`) — `ZetaLoadingSpec#loader()` returns it |
> | `RegistryAwareCacheLoader` | `PrefixRoutedLoader` |
> | `ZetaLoadingSpec.toPolicy(...)` | `ZetaLoadingSpec.toReadPolicy(...)` |
> | `RedisCacheLoader` / `hotKeyRedisLoader` bean | `RedisValueLoader` / `hotKeyClusterLoader` bean |
> | `reload(key, oldValue)` hook | removed — never had a consumer (see Consequences) |
>
> One behavioural correction is also folded in below: the no-reader `get` used
> to drop the spec's `stalePolicy`, so `softTtlMs` never armed a background
> refresh on that path. That was a contract violation, not a design choice, and
> it is fixed — the read path now honours `stalePolicy` on every overload.

## Problem

Zeta already implements Caffeine's `refreshAfterWrite` runtime semantics — soft TTL + `StalePolicy.SOFT_REFRESH` + `ExpireManager` background refresh, wrapped in SingleFlight deduplication, circuit breaking, and Worker reporting. But the reader is bound to the **call site** (`CachePolicy.reader`, `read(key).withPrimary(...)`): there is no way to register a loader once and read with `get(key)` alone.

The gap has a cost beyond ergonomics. The cluster paths load values exclusively through the internal `CacheLoader` (default `RedisCacheLoader`, a Redis GET):

1. `DefaultWorkerDecisionHandler.handleHot` — Worker says "make `user:42` hot"; if the application's source of truth is a database (no Redis value-channel pre-fill pipeline), the load returns `null` and the warm-up is silently skipped (`HotSkipReason.VALUE_NOT_FOUND`). Hot-key detection works, but the headline feature — cluster-wide warm-up — does nothing for DB-backed applications.
2. `DefaultSyncDecisionHandler.handleRefresh` — a peer's REFRESH broadcast falls back to local invalidation (ADR-0031) because no value can be fetched; the cluster loses its one-shot refresh semantics.

## Decision

**1. One loader hierarchy, two roles.**

- `ZetaCacheLoader<V>` is the root SPI — `load(key)` plus a Caffeine-shaped `default reload(key, oldValue)`. No checked exceptions: load failures are unchecked and flow into each consumer's existing failure handling.
- `ZetaLoadingSpec<V>` — immutable loader + per-namespace policy (`hardTtlMs` = expireAfterWrite equivalent, `softTtlMs` = refreshAfterWrite equivalent, `stalePolicy`, `nullCaching`, `reportEnabled`, `failOnError`), expressible as `CachePolicy.toPolicy(cacheKey)`; the constructor is the single validation point, so a spec with a null loader cannot exist (`of(null)` fails at build time, not at first load).
- The internal `CacheLoader` is a **role alias**, not a separate concept: it extends `ZetaCacheLoader<Object>` and marks the sync-plane injection point (the `hotKeyRedisLoader` bean, overridable via `@ConditionalOnMissingBean(CacheLoader.class)`). Unifying by inheritance (instead of keeping two same-shaped interfaces, or deleting the published `CacheLoader` type) removes the duplicated `reload` default and lets a `RedisCacheLoader` be dropped straight into a `ZetaLoadingSpec`.
- Overload semantics guard against reader conflicts: the `get(key, reader)` / `get(key, CachePolicy)` / `read(key).withPrimary(...)` overloads never consult the registry — an explicit call-site reader always wins; the registered loader applies only to the no-reader `get` overloads (and, through the composite, to the cluster paths).

**2. `ZetaLoaderRegistry` matches by longest key prefix.** `ConcurrentSkipListMap`-backed; `match()` uses a single `floorEntry` probe for the common case plus a descending prefix walk for the rare case where a non-prefix registration sorts between the matching prefix and the key (e.g. prefixes `"ab"`, `"abZ"`, key `"aba"`). Registrations may be replaced or removed at runtime. An empty registry is a no-op everywhere, so the feature is opt-in and zero-impact for existing deployments.

**3. Three consumption points, all delegating into the existing read path.**

- `Zeta.get(cacheKey)`, `Zeta.get(cacheKey, stalePolicyOverride)`, `Zeta.getWithSoftExpire(cacheKey)` resolve the winning spec, wrap its loader into a `CachePolicy`, and delegate to the existing `get(key, policy)` chain — SingleFlight, circuit breaker, background refresh, and Worker reporting apply unchanged, and a plain L1 hit never invokes the loader or the registry lookup is paid once per call (it is O(1) in the common case). A no-reader `get` on an unregistered key (or with no registry wired) fails fast with an actionable `IllegalStateException` — silent empty results would hide wiring mistakes.
- `hotKeyRedisLoader` becomes a composite `RegistryAwareCacheLoader` when a registry bean exists: registered prefixes load through the application loader, everything else falls back to Redis GET. `handleHot` / `handleRefresh` are unchanged and gain DB-source warm-up/refresh for free; raw-value semantics are preserved (the composite returns unwrapped values exactly like `RedisCacheLoader`, handlers still apply wrapping/compression).
- `hotLocalCache` applies `ZetaCacheCustomizer` beans (Spring Boot `*Customizer` convention) last, just before `build()`, so applications can add removal listeners, schedulers, or executors without replacing the whole `Cache` bean and re-implementing the `hardExpireAtMs` `Expiry` semantics.

**4. Executor rejection policy becomes configuration** (`zeta.local.executor-rejection = abort | caller-runs`, default `abort` preserving today's behavior) — deployments that prefer back-pressure over dropped async work can choose `caller-runs`.

## Consequences

- DB-backed applications can now get cluster-wide hot-key warm-up and peer refresh: every instance can resolve a registered key independently from the application data source. Load fan-out is bounded by the existing mechanisms (SingleFlight per key per instance, SRE rate limiter on HOT promotions, circuit breaker).
- `reload(key, oldValue)` is currently unused by the in-tree consumers — the background refresh path threads the reader (which calls `load`) and the sync plane has no cheap oldValue channel (L1 values are stored in the wrapped envelope; unwrapping them for a hook would need a public unwrap API). The hook ships for SPI shape parity and explicit use.
- Caffeine setters are single-use, so customizers can only add orthogonal settings (listeners/scheduler/executor/ticker); capacity and expiry knobs remain owned by `zeta.local.cache.*` — attempting to override them in a customizer throws.
- The sync-plane override condition stays `@ConditionalOnMissingBean(CacheLoader.class)` and must not be widened to `ZetaCacheLoader.class`: after the unification every loader (including per-spec application loaders) is a `ZetaCacheLoader`, so a widened check would let an unrelated spec-loader bean suppress the Redis fallback loader.
- Overriding `hotLocalCache` entirely remains possible and keeps its documented caveats; the customizer is the intended low-friction path.

## Verification

Current suite (names as they exist in the tree — see the symbol-drift table above):

- `ZetaLoaderRegistryTest` (12 cases): longest-prefix wins, floor fast path, the
  `ab/abZ/aba` fallback, empty registry, replace/unregister/clear, argument validation.
- `PrefixRoutedLoaderTest` (6 cases, formerly `RegistryAwareCacheLoaderTest`):
  registry hit skips the Redis fallback, miss delegates, unchecked loader
  exceptions propagate to the consumer's failure handling.
- `ZetaLoadingSpecTest` (16 cases): defaults, builder validation (null loader and
  negative TTLs rejected), `toReadPolicy` carries TTLs/knobs/reader and invokes
  `load` per call, stale-policy override (null rejected), `withLoader` copy
  semantics (source spec unchanged, knobs carried).
- `ZetaTest` no-reader cases: load through the registered spec (the policy carries
  the spec's TTLs and reader), forced `SOFT_REFRESH` via `getWithSoftExpire`,
  fail-fast without a registry / without a matching prefix, and batch grouping by
  spec (`getNoReaderBatch_shouldGroupBySpecAndMerge`).
- `ZetaTest.getNoReaderBatch_carriesNullCachingAndStalePolicy`: the batch path
  forwards the spec's `nullCaching` and `stalePolicy` — it used to hard-code
  `true` / `SOFT_REFRESH`, which silently ignored a `nullCaching(false)` spec.
- `ZetaReadQueryTest`: `withStalePolicy` flows into the built `ReadPolicy`;
  `null` rejected.
- Soft-expiry arming — the behaviour that makes `softTtlMs` real — is pinned in
  `HotKeyCacheConflictTest`: a `SOFT_REFRESH` read of a soft-expired NORMAL entry
  arms the background refresh on **both** `get` and `getWithSoftExpire`
  (`get_plainOverloadWithSoftRefreshPolicy_triggersBackgroundRefresh`), with
  `RETURN` as the no-refresh control. The plain-`get` case was broken when this
  ADR was written — the overload dropped the policy's stale policy — and is now
  the regression guard for it.
