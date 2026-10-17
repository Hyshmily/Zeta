# Zeta Annotation Integration

Annotation-driven entry point built on Spring's standard `@Cacheable` / `@CachePut` / `@CacheEvict`, extended by Zeta companion annotations. Enabled via:

```yaml
zeta:
  spring-cache:
    enabled: true
```

and `@EnableCaching` on a configuration class (required — without it Spring's `CacheInterceptor` is not registered and `@Cacheable` methods execute uncached).

For the design rationale behind the three-layer model below, see [ADR-0023](adr/0023-annotation-three-layer-responsibility-model.md).

---

## 1. Architecture: three layers, one policy object

```
@Cacheable method
   │
   ▼
CacheExtensionAspect      WHETHER the method body runs:
   │                      @Intercept (trigger→fallback), @Fallback (exception),
   │                      @Preload (detector inflation), combination validation
   │ builds ──► GetOptions (immutable: lazy TTLs) + skipSync flag
   ▼
ZetaCacheContext          TRANSPORT only — one ThreadLocal<Snapshot{GetOptions, skipSync, storeVeto}>
   ▼
ZetaSpringCache           HOW the result is stored:
   │                      TTL routing, @CacheCondition store veto,
   │                      sync routing for put/evict (skipSync)
   ▼
Zeta / HotKeyCache
```

`GetOptions` TTL suppliers are evaluated **at most once per cache call** and only on miss / promotion / refresh — never on a plain cache hit, so SpEL TTL expressions are free on the hit path.

---

## 2. Annotation reference

| Annotation        | Target | Operations   | Summary                                                                                                                                                                                                                                                                                                                                                                                    |
| ----------------- | ------ | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `@CacheTtl`       | M/T    | `@Cacheable` | Override hard/soft TTL. Static values and SpEL (`hardTtlSpEl`, `softTtlSpEl`). SpEL evaluated at most once per call, only on miss/promotion/refresh. Class-level annotation acts as fallback for all methods                                                                                                                                                                               |
| `@Intercept`      | M      | `@Cacheable` | Skip method body via trigger mode (`IS_LOCAL_HOT` / `FORCE` / `QPS` / `CONCURRENT_THREADS`). Mode-specific config through nested annotations: `@Intercept.QpsConfig` (`threshold`, `blockDurationMs`) and `@Intercept.ConcurrentConfig` (`threshold`). Fallback via `@Intercept.fallback()` → `@Fallback` → `peek()`. On interception the local detector is incremented (no Worker report) |
| `@Fallback`       | M      | `@Cacheable` | Fallback value (SpEL) or convention method (`{methodName}Fallback`) when intercepted / exception (a blacklist BLOCK never routes to fallback — it always propagates)                                                                                                                                                                                                                       |
| `@SkipSync`       | M      | all three    | Suppress cross-instance sync messages (local-only write/evict). `null` is never cached — there is no null-caching opt-out (v2 contract)                                                                                                                                                                                                                                                    |
| `@Preload`        | M      | `@Cacheable` | Pre-inflate HeavyKeeper counts for known hot keys (static `keys[]` or dynamic `keyExpr` SpEL)                                                                                                                                                                                                                                                                                              |
| `@CacheCondition` | M      | `@Cacheable` | SpEL `unless` with **pre-storage veto semantics** — when true, the result is never written to L1 and no cluster broadcast is sent; the expression is evaluated once per miss-fill put, never on a hit                                                                                                                                                                                      |
| `@Tag`            | M      | any          | Feed a SpEL-resolved key into detection/reporting without a cache lookup. `skipDetection` / `skipReport` suppress each side; `cacheName` aligns the key namespace with `@Cacheable`                                                                                                                                                                                                        |

M = method, M/T = method or type.

---

## 3. Operation applicability matrix

| Annotation        | `@Cacheable`    | `@CachePut`             | `@CacheEvict`           |
| ----------------- | --------------- | ----------------------- | ----------------------- |
| `@CacheTtl`       | ✅ TTL override | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |
| `@Intercept`      | ✅              | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |
| `@Fallback`       | ✅              | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |
| `@SkipSync`       | ✅              | ✅                      | ✅                      |
| `@Preload`        | ✅              | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |
| `@CacheCondition` | ✅              | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |

"Ignored" means: the annotation is silently a no-op today, and the aspect logs a one-time WARN (rule R2) so the mistake is visible.

**Detection on the annotation read path (all `@Cacheable` routes):** every `@Cacheable` read counts for hot-key detection, upholding the core invariant that every read triggers detection AND reporting. The non-sync route (`cache.get(key)` → `lookup`) serves the read — hit or miss — through the single rule-aware `peekAndTag` lookup (one hash lookup + one rule evaluation per read), which feeds the local HeavyKeeper detector and the Worker reporter on every served read (whitelist matches detect without reporting, a blocked key throws); the sync route (`get(key, Callable)`) enters the detecting compute path. On a miss the method body runs and the result is stored through the **local read-miss fill** (`populateFromReadMiss`: read-only version probe, no Redis INCR, no cluster broadcast, no transaction deferral — a read never masquerades as a cluster write; the fill itself records nothing, so one logical read counts exactly once). Only `@CachePut` takes the write-through broadcast path. Serving a non-sync hit from L1 only is intentional: the lookup never invokes a loader.

**Read-miss fill and version comparison:** the fill's version guard is the same envelope the load-capable store uses — an unstamped probe (fail-open, e.g. Redis unreachable) makes no version claim and therefore can neither win nor lose the comparison against a resident entry, nor replace its version with the default. A fill _is_ refused only when its probe was stamped, the resident is not past its hard TTL, and the resident already holds a version at least as fresh (equal applies, ADR-0066). Practically: a Redis blip no longer silently discards `@Cacheable` miss fills for keys that already have a versioned entry, and a hard-expired entry can always be replaced by its replacement. A refused fill stores nothing and therefore obsoletes no dedup load result — the SingleFlight invalidation fires only when the value actually landed.

**`@CachePut` cross-instance semantics:** `@CachePut` writes through (`putThrough`) and broadcasts a REFRESH sync message. Zeta itself never writes the cache-key namespace in Redis, so in the default wiring the receiver cannot load the new value — the REFRESH handler falls back to a **local invalidation** (ADR-0031): the peer's copy is evicted and the next read reloads through the application reader. If your integration maintains the value in Redis itself, the push path is used instead. Use `@SkipSync` for local-only writes.

---

## 4. Interaction rules

### 4.1 `@Intercept` × `@Preload`

`@Intercept(IS_LOCAL_HOT)` intercepts when the local TopK marks the key hot. On interception the aspect calls `zeta.notifyLocalDetector(key)` — local HeavyKeeper increment, **no** Worker report — so the key's hotness is sustained by traffic and cannot flap (intercept → decay → not hot → execute → hot → …). `@Preload` remains useful to make keys hot _before_ organic traffic accumulates, but is no longer required for stability.

### 4.2 `@Intercept(FORCE)` × storage annotations

`FORCE` returns before `proceed()`, so Spring's `CacheInterceptor` never runs and **nothing is ever cached**. `@CacheTtl`, `@CacheCondition` on the same method are no-ops (R1 warning).

### 4.3 `@Tag` × `@Cacheable`

Both on the same method → the key is counted twice per call (once by the tag path, once by the cache read path) — hotness is inflated ×2 (R3 warning). To boost a `@Cacheable` key deliberately, either remove `@Tag` or use `@Tag(value=..., cacheName="<same cache name>", skipReport=true)` with a distinct purpose. `@Tag(cacheName)` prefixes the key exactly like `@Cacheable` does (`cacheName + keySeparator + key`); without it, `@Tag` operates in the raw key namespace.

### 4.4 `@CacheCondition` × Spring `unless=`

Both are evaluated when present (R4 warning), now with **aligned semantics**:

|                                     | Spring `@Cacheable(unless=...)`  | Zeta `@CacheCondition(unless=...)`                                 |
| ----------------------------------- | -------------------------------- | ------------------------------------------------------------------ |
| Veto timing                         | post-invocation, **pre-storage** | post-invocation, **pre-storage** (aligned)                         |
| Effect on a previously cached entry | left alive (served on next hit)  | left alive (served on next hit)                                    |
| Evaluated on cache hits             | never                            | never — the expression runs once per miss-fill put, never on a hit |
| Cluster broadcast on veto           | n/a (never stored)               | none — nothing was stored, so nothing is invalidated               |

The veto travels from the advice into `ZetaSpringCache` through
`ZetaCacheContext` and is evaluated before the L1 write: a vetoed result
writes no entry, triggers no broadcast, and leaves no dirty-read window
(replacing the earlier store-then-evict "purge" design and its bounded race
window, ADR-0013).

### 4.5 Nested `@Cacheable` invocations

The aspect pushes the resolved `GetOptions` (plus the skipSync flag) unconditionally and restores the previous snapshot in `finally`, so an inner cached method never observes the outer method's policy. The transport is thread-bound: **do not** combine with `@Async` or any pattern that hops threads between the aspect and the cache adapter.

**Loader thread affinity:** on a cache miss the method body executes on the SingleFlight executor thread (`hotKeyExecutor`), not the caller thread — same as the direct `get()` path (ADR-0030). ThreadLocal state prepared by the caller (request context, transaction, security context) is **not visible** inside the method on a miss; the loader must not assume caller-thread affinity. On a hit the method body never runs.

---

## 5. TTL precedence chain

For a given read operation:

1. **NORMAL entry creation** — positive `@CacheTtl` (static or SpEL) override wins; 0 → configured default (`zeta.local.*`).
2. **Promotion to HOT / HOT renewal** — effective hot TTL = `max(override, hot default)`: an override may only raise the floor, so promotion never shortens an entry's lifetime. On a plain hit the override suppliers are not evaluated; promotion then uses the configured hot defaults.
3. **Soft-expire (stale-while-revalidate)** — governed solely by global configuration. The annotation `sync=true` read path enters via the soft-expire entry point, which self-degrades when the feature is globally disabled; the default non-sync path serves via detecting peek (`peekAndTag`, expiry-checked) without background refresh. TTL overrides never gate it.
4. **TTL jitter** — ±5% by default (`zeta.local.ttl-jitter-ratio`), applied to all expiry timestamps.

---

## 6. Null semantics (v2 contract: null is never cached)

- A `null` method result writes **no entry** on any path (loader miss, `@CachePut` returning null, non-sync `@Cacheable` stores, explicit Spring `NullValue` writes — Spring's own `NullValue` marker is never persisted). The next read re-invokes the method.
- There is no null-caching opt-out: the former `@NullCaching` annotation and the short-TTL null sentinel are removed (v2 null contract). Callers needing penetration protection apply it at their loader (a placeholder value + `shouldCache`).
- Spring's `allowNullValues` is forced to `false` on `ZetaSpringCache` regardless of configuration.
- **Sync-read failure semantics (`sync = true`)**: the delegated policy is forced to fail-fast, so a throwing method surfaces as Spring's `ValueRetrievalException` (never swallowed into a `null` miss) and a suppressed load (timeout / executor saturation with a closed breaker) surfaces as `ValueRetrievalException` wrapping `ZetaSuppressedException`. A breaker-open load with no serveable stale entry still resolves to `null` (graceful degradation wins over fail-fast there).

---

## 7. Validator warnings (once per method)

| Rule | Trigger                                               | Meaning                                              |
| ---- | ----------------------------------------------------- | ---------------------------------------------------- |
| R1   | `@Intercept(FORCE)` + `@CacheTtl`/`@CacheCondition`   | storage annotations are no-ops under FORCE           |
| R2   | read-path annotations on `@CachePut`/`@CacheEvict`    | only `@SkipSync` applies there                       |
| R3   | `@Tag` + `@Cacheable` on one method                   | double-counting the key in HeavyKeeper               |
| R4   | `@CacheCondition` + Spring `unless=` on one method    | double evaluation (both veto the store)              |
| R5   | `@Intercept(blockDurationMs=...)` with `type` ≠ `QPS` | blockDurationMs is silently ignored outside QPS mode |

---

## 8. Examples

```java
// Static TTL + intercept-on-hot with convention fallback
@Cacheable("products")
@CacheTtl(hardTtlMs = 60_000, softTtlMs = 10_000)
@Intercept @Fallback
public Product getProduct(String id) { ... }

// Dynamic TTL via SpEL (evaluated only on miss/promotion/refresh)
@Cacheable("users")
@CacheTtl(hardTtlSpEl = "#id.startsWith('vip') ? 600000 : 60000")
public User findUser(String id) { ... }

// Rate limiting with SpEL fallback (nested @Intercept.QpsConfig)
@Cacheable("orders")
@Intercept(type = InterceptType.QPS, qps = @Intercept.QpsConfig(threshold = 500), fallback = "'throttled'")
public Order getOrder(String id) { ... }

// Rate limiting with block duration (5s cooling-off after breach)
@Cacheable("flash-sale")
@Intercept(type = InterceptType.QPS, qps = @Intercept.QpsConfig(threshold = 100, blockDurationMs = 5000))
public Item getFlashItem(String id) { ... }

// Concurrency guard (nested @Intercept.ConcurrentConfig)
@Cacheable("reports")
@Intercept(type = InterceptType.CONCURRENT_THREADS, concurrent = @Intercept.ConcurrentConfig(threshold = 10), fallback = "'busy'")
public Report getReport(String id) { ... }

// Preload known flash-sale keys (stable hot from the start)
@Cacheable("items")
@Preload(keys = {"item-001", "item-002"})
public Item getItem(String id) { ... }

// Purge when the result is disabled
@Cacheable("configs")
@CacheCondition(unless = "#result == null || #result.disabled()")
public Config getConfig(String key) { ... }

// Local-only write (no cross-instance sync)
@CachePut("sessions")
@SkipSync
public Session updateSession(Session s) { ... }

// Tag a key into the @Cacheable namespace without a cache lookup
@Tag(value = "#id", cacheName = "products", skipReport = true)
public void touchProduct(String id) { ... }
```
