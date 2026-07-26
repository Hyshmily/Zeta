# Annotation Three-Layer Responsibility Model

The Spring Cache annotation integration (`@Cacheable` + Zeta companion annotations) had accreted into a state where annotation semantics were defined nowhere: `@NullCaching` was written to the context but consumed by nobody, soft-expire was silently gated on the presence of a `@CacheTTL` override, the `@Intercept` path documented a detector increment that was never implemented, and `ZetaSpringCache` carried a "fast path" that bypassed logical-expiry checks for null sentinels (permanent null poisoning). This ADR records the three-layer responsibility model that now defines the annotation system, and the semantic decisions made for each annotation.

---

## Decision 1: Three-Layer Responsibility Model

Every annotation concern is owned by exactly one layer:

| Layer | Role | Owns |
| --- | --- | --- |
| `CacheExtensionAspect` | **WHETHER** — pre-invocation decision point | `@Intercept` (trigger → fallback chain), `@Fallback` (exception fallback), `@Preload` (detector inflation), combination validation |
| `ZetaCacheContext` | **TRANSPORT** — dumb ThreadLocal carrier | Exactly one immutable `CachePolicy` per thread; push / current / snapshot / restore. No annotation knowledge, no SpEL, no consume-on-read semantics |
| `ZetaSpringCache` | **HOW** — storage-policy enforcement point | TTL resolution routing, null-caching decision, `@CacheCondition` purge, broadcast flag routing for put/evict |

The transport object is the immutable `model/CachePolicy` record (`LongSupplier hardTtlMs`, `LongSupplier softTtlMs`, `boolean nullCaching`, `boolean skipBroadcast`). It replaces the previous bag of ThreadLocal fields (`ContextValues` + two one-shot-consume supplier ThreadLocals, whose getter-consumes-once semantics were a latent trap).

### Lazy TTL contract

`CachePolicy` TTL suppliers are evaluated **at most once per cache call** and only when an entry is created, promoted, renewed, or a soft-expire refresh is scheduled — never on a plain NORMAL-entry hit. This preserves the "no SpEL evaluation on hits" optimization (originally commits `ec8f251` / `2da2d42`) without the `ZetaSpringCache` fast path that caused null poisoning, double map lookups, and `preGuard` bypass.

---

## Decision 2: `@NullCaching` is an opt-out, not an opt-in

Null caching is **globally ON by default**: every `null` loader result is stored as a `NullValue` sentinel with the short `zeta.local.null-value-ttl-seconds` TTL (penetration protection). `@NullCaching(false)` is the per-method opt-out: no sentinel is written and the next call re-invokes the loader.

Previously the annotation fed an `allowNull` context flag that had **zero consumers** — null was cached unconditionally and the annotation was a no-op. The rejected alternative (opt-in: default no null caching, `@NullCaching(true)` to enable) was judged too breaking: production deployments rely on the current always-cache-null behavior for penetration protection.

### Null-sentinel hits are served, and counted

All three read paths (`get`, `getWithSoftExpire`, `computeIfAbsent[WithSoftExpire]`) now behave identically on a valid null-sentinel hit: serve empty **without re-invoking the reader** (until the sentinel's short TTL expires), and still count the access for hot-key detection so penetration-prone keys can go hot. Previously the `get()`/`getWithSoftExpire()` paths re-invoked the reader on every sentinel hit (the sentinel provided no protection at all there), while `computeInLock` served the sentinel but skipped the access count.

---

## Decision 3: `@CacheCondition` keeps purge semantics, respects broadcast policy

`@CacheCondition(unless)` is evaluated after method execution and **evicts** the freshly stored entry (plus any stale one) when the condition holds. The eviction now respects `@SkipBroadcast`: it is broadcast cluster-wide by default instead of the previous hard-coded local-only invalidation, which silently split cluster state.

The annotation is deliberately **not** deprecated in favor of Spring's native `@Cacheable(unless=...)`, because purge semantics differ from Spring's skip-write semantics in two ways users rely on:

1. Spring's `unless` leaves a previously cached stale entry alive; `@CacheCondition` actively purges it.
2. `@CacheCondition` is also evaluated on cache hits (`#result` = cached value), enabling self-cleaning when the condition changes over time.

The store-then-evict race window is bounded and accepted per ADR-0013 (acceptable race fading).

---

## Decision 4: `@Intercept` feeds the local detector on interception

When an `@Intercept` rule triggers (any type: `FORCE`, `IS_LOCAL_HOT`, `QPS`, `CONCURRENT_THREADS`), the aspect calls `zeta.notifyLocalDetector(key)` — a local HeavyKeeper increment **without** a Worker report — before serving the fallback/peek value.

Previously the intercepted path only called `peek()` (side-effect-free), so an intercepted hot key's counts decayed until it fell out of the TopK, stopped being intercepted, executed the method, became hot again, and flapped. The `Zeta.notifyLocalDetector` facade method and its Javadoc ("Used by `@Intercept` path") plus CONTEXT.md documented this behavior, but the aspect never implemented it. The local-add sustains local hotness while traffic flows; when traffic stops, counts decay and the key cools normally. Worker-side COOL decisions for under-reported intercepted keys are tolerable per Local-First promotion (ADR-0021).

---

## Decision 5: TTL semantics — soft-expire decoupling and hot-TTL floor

**5a. Soft-expire is governed solely by global configuration.** The annotation read path always routes through `computeIfAbsentWithSoftExpire`, which self-degrades to plain `computeIfAbsent` when soft-expire is globally disabled. Previously the choice between the two facade methods was made on `hasTtlOverride`, so the globally-enabled soft-expire feature was silently inactive for annotated methods without `@CacheTTL`, and any `@CacheTTL` override (even hard-only) silently opted into soft-expire refresh.

**5b. Hot TTL is floored, not overridden.** `ExpireManager.resolveEffectiveHotHard/Soft` now return `max(override, hotDefault)` for positive overrides, so promotion to HOT never shortens an entry's lifetime. Previously a small `@CacheTTL(hardTtlMs)` override would replace the (usually much longer) hot TTL on promotion — promotion paradoxically shortened the entry's life.

---

## Decision 6: Lazy combination validation (WARN once per method)

The aspect validates annotation combinations the first time each method is intercepted and logs a WARN once per method:

- **R1**: `@Intercept(FORCE)` + `@CacheTTL`/`@NullCaching`/`@CacheCondition` — storage annotations are no-ops under FORCE (method body never runs, nothing cached).
- **R2**: read-path annotations on `@CachePut`/`@CacheEvict` — silently ignored; only `@SkipBroadcast` applies.
- **R3**: `@Tag` + `@Cacheable` on the same method — double-counts the key in HeavyKeeper.
- **R4**: `@CacheCondition` + Spring `unless=` on the same method — double condition evaluation with different semantics.

Lazy validation (over a startup `BeanPostProcessor` scan) was chosen for zero startup cost and natural fit with the existing per-method annotation caches. A fail-fast mode was rejected: a third-party bean's annotation problem must not prevent application startup.

---

## Decision 7: `@Tag` gains `cacheName` for namespace alignment

`@Tag(cacheName = "c")` prefixes the resolved key as `c + keySeparator + key`, aligning tagged keys with the `@Cacheable` key namespace. Empty (default) keeps the raw key namespace (backward compatible). Previously users had to hand-build the prefix inside the SpEL expression, undocumented.

---

## Consequences

- `ZetaSpringCache.get()` is a single routing call; the deleted fast path removes: permanent null poisoning (sentinel hits bypassed logical-expiry checks), double map lookups on hits, and `preGuard`/decompression bypass.
- `Zeta.computeIfAbsentWithSoftExpire(key, loader, CachePolicy, report)` and `get`/`getWithSoftExpire` CachePolicy overloads are new public API; all existing signatures delegate with `CachePolicy.of(h, s, true, false)` (null caching ON), preserving behavior for direct-API users.
- The fluent `ZetaReadQuery` primary path now returns raw loader values; null results are handled uniformly inside the cache layer with the short null TTL (previously the fluent wrapper cached `NullValue.INSTANCE` as a real value with normal TTLs). `notAllowNull()` is now truly honored on the primary path.
- Fallback-branch `putThrough`/`putLocal` of `NullValue.INSTANCE` in `ZetaReadQuery` is unchanged (uses per-call TTLs, not the short null TTL) — retained for compatibility, flagged as a future unification candidate.
- Batch read paths (`get(Iterable)`, `getWithSoftExpire(Iterable)`) keep the previous sentinel-reload behavior; aligning them is a follow-up.
- `ZetaCacheContext` API changed (`apply`/`applyLazy`/`ContextValues` removed → `push`/`current`/`snapshot`/`restore`). The class is `@Internal`; no public-API compatibility promise is broken.

## References

- ADR-0013: acceptable race fading (basis for the `@CacheCondition` store-then-evict window)
- ADR-0015: LZ4 compression (why raw-value peek paths must not bypass the orchestration layer)
- ADR-0021: cache promotion and graceful degradation (Local-First principle behind Decision 4)
- `CONTEXT.md`: resolved glossary entries for `@NullCaching`, `@CacheCondition`, `@Intercept`, `@Tag`, TTL Precedence
