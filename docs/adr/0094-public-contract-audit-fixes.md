# ADR-0094: Public-Contract Audit Fixes (contract vs. execution path)

- Status: Accepted
- Date: 2026-10-10
- Related: ADR-0018 (circuit breaker), ADR-0066 (strict-newer / versioned batch),
  ADR-0068 (appName isolation), ADR-0070 (prefix loader registry), ADR-0086
  (write-path ordering), ADR-0093 (explicit load outcomes)

## Problem

A user-facing audit traced every public entry point (`Zeta` facade, Spring
annotations, fluent API) down to `HotKeyCache` and the sync plane, then asked a
narrow question of each hop: **what does the published contract promise, and
what does the execution path actually do?** Twelve gaps survived that check.
They share one shape — the contract was either silently contradicted or never
stated, and nothing in the type system or the log told the caller.

The expensive ones, in the caller's own terms:

1. **A transient blip became a 10-second "this key does not exist".** A
   suppressed load (breaker open, dedup timeout, executor saturation) resolved
   through the same `resolveNullLoadOutcome` as a genuine `null` source and
   wrote a negative sentinel for `null-value-ttl`. The source had been asked
   nothing; the caller still got `Optional.empty()` on every read in that
   window. This is the opposite of what negative caching is for (ADR-0093 §1
   separated the outcomes; the storage decision had not followed).
2. **`softTtl` silently did nothing on the documented path.** `Zeta.get(key)`
   kept the spec's `SOFT_REFRESH` policy but the engine hard-coded
   `armSoftRefresh=false`, so the refresh gate never fired. ADR-0070's headline
   "background refresh applies unchanged" was false for the very overload it
   advertises (ADR-0070 §3).
3. **A degradation chain that degraded on first contact.** `ZetaReadQuery`'s
   fallbacks ran bare: one throwing link propagated and skipped every
   remaining link.
4. **`zeta.local.cache.max-value-size` was dead** — defined, documented in
   CONFIG as "values above this are not cached", read nowhere.
5. **`@Cacheable(cacheNames={"a","b"})` acted on `a` only.** Spring writes
   every named cache; the second namespace never joined detection and never
   got purged.
6. **The instance that issued an invalidation could be the one that kept the
   stale value.** ADR-0086 bumps before dropping, so a concurrent probe can
   stamp a reloaded entry with the same version the INVALIDATE carries; the
   origin filter drops self-REFRESH but not self-INVALIDATE, and the receiver's
   `>=` matrix skipped an equal version — pinning the origin's own pre-write
   value for the whole hard TTL.
7. **One breaker for the whole JVM.** A sick `user:` datasource silenced
   `product:` too. ADR-0018 solved "the wrong exception opens the breaker"; it
   could not solve "the right exception opens it for everyone".
8. **Two app-name keys that had to agree.** `zeta.local.app-name` (App) and
   `zeta.worker.routing.app-name` (Worker) diverge silently and break *both*
   planes: decisions are dropped and reports never reach a binding, so no
   decision is ever produced. ADR-0068 documented only the decision half.
9. `peek()` was advertised as side-effect-free but threw for blocked keys
   (undocumented) and served logically-expired entries.
10. `stats().hitRate()` counted Caffeine operations (internal `compute`s
    included), not requests, while the javadoc invited capacity decisions on it.
11. Batch `getAll` hard-coded `nullCaching=true` and `SOFT_REFRESH`, so a spec
    that opted out of negative caching still got it on the batch path.
12. `invalidateAfterPut(Map)` returned `void` — a partially applied batch was
    indistinguishable from a fully applied one.

## Decision

Fix all twelve as behaviour changes, not documentation. Where a fix is
observably different, the public contract is rewritten to match; nothing is
left as a "known quirk".

1. **Suppression and failure never write a negative sentinel**
   (`resolveNoAnswerOutcome`). Only `SOURCE_EMPTY` — the source explicitly
   answering "absent" — writes the sentinel. Breaker-open plus a hard-valid
   stale entry still serves stale (graceful degradation is untouched); the
   sentinel write is the only thing removed. Suppressed loads now also log at
   the same rate-limited level as failures — previously they were silent
   whenever `failOnError=false`. ADR-0093 separated the outcome *types*; this
   closes the storage half it left open (the code path is
   `HotKeyCache.resolveNoAnswerOutcome`).
2. **Delete the `armSoftRefresh` switch.** The hit path arms
   `refreshSoftExpire` whenever the policy carries `SOFT_REFRESH`, on every
   overload. `get` and `getWithSoftExpire` remain distinct entry points; the
   latter only *forces* the policy. ADR-0070's Verification is rewritten and
   its javadoc is corrected.
3. **Fallback chains swallow `RuntimeException` and continue**;
   `ZetaBlockedException` and `Error` still propagate (a policy rejection and a
   JVM fault are not source failures). Every link exhausted resolves empty.
4. **`maxValueSize` gates the three local write points** (load, `putThrough`,
   `putLocal`) using the existing `DefaultWeigher`, so the gate and
   `max-weight` can never disagree about what "big" means. Oversized values are
   served but not stored; broadcast is still emitted (peers apply their own
   gate).
5. **The annotation aspect iterates every cache name** — detection, the
   intercept gate, preload registration and `@CacheCondition` purges run per
   namespace.
6. **INVALIDATE uses `shouldSkipForRefresh`** (equal applies), matching the
   REFRESH receiver. Removal is idempotent: applying an equal version can only
   delete a key that was about to be deleted anyway, while skipping pins it.
   The store-side guard keeps `>=`.
7. **`CircuitBreakerRegistry` shards by key prefix** (the segment before the
   first `:`, prefix-less keys share one bucket), bounded by
   `zeta.local.circuit-breaker.max-prefixes` (default 64, LRU eviction) and
   sharing one slide scheduler. The single global breaker is the degenerate
   `maxPrefixes=1` case; ADR-0018 gains a consequence documenting the change.
8. **One key: `zeta.app-name`**, read by both sides. Report routing and the
   decision header are now the same value by construction, so the dual-plane
   outage is unrepresentable rather than merely logged. ADR-0068 rewritten.
9. **`peek()` documents `ZetaBlockedException`** and now applies the same
   logical-expiry check as the read path; its javadoc says "free of
   load/detection/report side effects, not free of guards".
10. **`HotKeyCache` counts its own request hits/misses** (`LongAdder`, one
    increment per served key on the load-capable read paths; a blocking
    REVALIDATE counts as a miss). `ZetaCacheStats.hitRate` is now literally the
    request hit rate; Caffeine's own stats remain available through the
    Actuator cache view for storage internals.
11. **Batch reads carry `nullCaching` and `stalePolicy`** from the spec
    (`ReadContext.of` gained both), and the batch hit path gained the
    `REVALIDATE` branch it could never reach while `SOFT_REFRESH` was
    hard-coded.
12. **`invalidateAfterPut(Map)` returns the keys it did not invalidate** —
    up-front rejections (blank / rule-excluded) plus mutation failures. The
    javadoc states the transaction caveat: inside an active Spring transaction
    the mutations run after commit, so only the up-front rejections are known
    at return time and later failures are logged.

## Consequences

- **Recovery time from a transient outage is now the outage.** Previously it
  was `null-value-ttl` per affected key, with no source call to prove the key
  was really absent. A deployment that relied on the sentinel absorbing a
  flapping datasource will see more source traffic during the flap — that is
  the point, and the breaker still caps it.
- **A registered `softTtl` now actually refreshes.** Expect a step change in
  background-refresh volume on deployments that registered specs with a soft
  TTL and (correctly) saw nothing happen.
- **The breaker can no longer silence unrelated keys.** The aggregate signals
  stay meaningful: `SingleFlight.isBreakerOpen()` (no key) now answers "is
  *any* shard open", which is what `zeta.stall.redis_degraded.stopped` reports,
  while every read-path decision uses the key's own shard.
- **Breaking surface.** `Zeta.invalidateAfterPut(Map, …)` changes return type
  (source-compatible: a `void` call site still compiles); `SingleFlight`
  gained per-key breaker methods with defaults; `CacheCoreSettings` gained
  `getMaxValueSize()`; `LockProvider` gained three counter defaults (see
  below). `zeta.local.app-name` and `zeta.worker.routing.app-name` are gone —
  both replaced by `zeta.app-name`, which keeps a shared default
  (`"default"`, flagged by a startup WARN) so single-application deployments
  are untouched — the failure mode removed here is *disagreement*, not absence.
- **Observability debt paid in the same pass:** `zeta.lock.acquired.total` /
  `contended.total` / `error.total` split the two meanings of a `null`
  `tryLock` (peer holds it vs. Redis is down), and
  `zeta.sync.send.failed.total` counts publish failures. Both are documented in
  MONITOR.md.
- **Documentation corrected, not widened:** `invalidate`'s javadoc now states
  that peer convergence is bounded by the entry's remaining hard TTL (ADR-0013's
  "<1s" covers Worker decisions only) and that the sync queue's
  `x-message-ttl = 60s` can discard a backlogged INVALIDATE;
  `invalidateAllLocal` and `CentralDispatcher.send(Collection,
  INVALIDATE_ALL)` state plainly that there is no cluster-wide flush and that
  the batch primitive is reserved with no production caller.

## Verification

- Suppression writes no sentinel (`ZetaCacheTest`): one test per
  `SUPPRESSED_*` status, plus the breaker-open/stale control and a
  `SOURCE_EMPTY` control proving the sentinel path still works.
- Soft-refresh arming (`HotKeyCacheConflictTest`): a `SOFT_REFRESH` read of a
  soft-expired NORMAL entry refreshes on the plain `get` overload, with
  `RETURN` as the no-refresh control.
- Fallback chain (`ZetaReadQueryTest`): a throwing middle link is skipped, all
  links throwing resolves empty, `ZetaBlockedException` still propagates.
- Equal-version INVALIDATE lands (`DefaultSyncDecisionHandlerTest`), with an
  older-version control proving the guard still skips.
- Multi-namespace `@Cacheable` (`ZetaCacheExtensionAspectTest`): both
  namespaces are preloaded / gated.
- Batch policy propagation (`ZetaTest`): `nullCaching(false)` and
  `REVALIDATE` reach the batch call.
- Lock metrics (`ZetaMicrometerAutoConfigurationTest`): the three
  `zeta.lock.*` gauges register with the mocked provider's values.
- Full suites green: common 2343 + worker 349.
