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
   │ builds ──► CachePolicy (immutable: lazy TTLs, nullCaching, skipBroadcast)
   ▼
ZetaCacheContext          TRANSPORT only — one ThreadLocal<CachePolicy>
   ▼
ZetaSpringCache           HOW the result is stored:
   │                      TTL routing, null-sentinel decision, @CacheCondition
   │                      purge, broadcast routing for put/evict
   ▼
Zeta / HotKeyCache
```

`CachePolicy` TTL suppliers are evaluated **at most once per cache call** and only on miss / promotion / refresh — never on a plain cache hit, so SpEL TTL expressions are free on the hit path.

---

## 2. Annotation reference

| Annotation | Target | Operations | Summary |
| --- | --- | --- | --- |
| `@CacheTTL` | M/T | `@Cacheable` | Override hard/soft TTL. Static values and SpEL (`hardTtlSpEl`, `softTtlSpEl`). SpEL evaluated at most once per call, only on miss/promotion/refresh. Class-level annotation acts as fallback for all methods |
| `@Intercept` | M | `@Cacheable` | Skip method body via trigger mode (`IS_LOCAL_HOT` / `FORCE` / `QPS` / `CONCURRENT_THREADS`); fallback via `@Intercept.fallback()` → `@Fallback` → `peek()`. On interception the local detector is incremented (no Worker report) |
| `@Fallback` | M | `@Cacheable` | Fallback value (SpEL) or convention method (`{methodName}Fallback`) when blocked / intercepted / exception |
| `@NullCaching` | M | `@Cacheable` | **Opt-out**: null results are cached by default (short-TTL sentinel, penetration protection); `@NullCaching(false)` disables caching for that method |
| `@SkipBroadcast` | M | all three | Suppress cross-instance AMQP sync messages (local-only write/evict, and local-only `@CacheCondition` purge) |
| `@Preload` | M | `@Cacheable` | Pre-inflate HeavyKeeper counts for known hot keys (static `keys[]` or dynamic `keyExpr` SpEL) |
| `@CacheCondition` | M | `@Cacheable` | SpEL `unless` with **purge semantics** — when true, the result is not kept AND any existing entry is evicted (broadcast unless `@SkipBroadcast`) |
| `@Tag` | M | any | Feed a SpEL-resolved key into detection/reporting without a cache lookup. `skipDetection` / `skipReport` suppress each side; `cacheName` aligns the key namespace with `@Cacheable` |

M = method, M/T = method or type.

---

## 3. Operation applicability matrix

| Annotation | `@Cacheable` | `@CachePut` | `@CacheEvict` |
| --- | --- | --- | --- |
| `@CacheTTL` | ✅ TTL override | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |
| `@Intercept` | ✅ | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |
| `@Fallback` | ✅ | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |
| `@NullCaching` | ✅ | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |
| `@SkipBroadcast` | ✅ | ✅ | ✅ |
| `@Preload` | ✅ | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |
| `@CacheCondition` | ✅ | ⚠️ ignored (R2 warning) | ⚠️ ignored (R2 warning) |

"Ignored" means: the annotation is silently a no-op today, and the aspect logs a one-time WARN (rule R2) so the mistake is visible.

---

## 4. Interaction rules

### 4.1 `@Intercept` × `@Preload`

`@Intercept(IS_LOCAL_HOT)` intercepts when the local TopK marks the key hot. On interception the aspect calls `zeta.notifyLocalDetector(key)` — local HeavyKeeper increment, **no** Worker report — so the key's hotness is sustained by traffic and cannot flap (intercept → decay → not hot → execute → hot → …). `@Preload` remains useful to make keys hot *before* organic traffic accumulates, but is no longer required for stability.

### 4.2 `@Intercept(FORCE)` × storage annotations

`FORCE` returns before `proceed()`, so Spring's `CacheInterceptor` never runs and **nothing is ever cached**. `@CacheTTL`, `@NullCaching`, `@CacheCondition` on the same method are no-ops (R1 warning).

### 4.3 `@Tag` × `@Cacheable`

Both on the same method → the key is counted twice per call (once by the tag path, once by the cache read path) — hotness is inflated ×2 (R3 warning). To boost a `@Cacheable` key deliberately, either remove `@Tag` or use `@Tag(value=..., cacheName="<same cache name>", skipReport=true)` with a distinct purpose. `@Tag(cacheName)` prefixes the key exactly like `@Cacheable` does (`cacheName + keySeparator + key`); without it, `@Tag` operates in the raw key namespace.

### 4.4 `@CacheCondition` × Spring `unless=`

Both are evaluated when present (R4 warning) with different semantics:

| | Spring `@Cacheable(unless=...)` | Zeta `@CacheCondition(unless=...)` |
| --- | --- | --- |
| Veto timing | post-invocation, **pre-storage** | post-storage, then evict |
| Effect on a previously cached entry | left alive (served on next hit) | **actively evicted** (purge) |
| Evaluated on cache hits | never | yes — `#result` is the cached value |
| Eviction broadcast | n/a (never stored) | broadcast unless `@SkipBroadcast` |

The store-then-evict race window is bounded and accepted per ADR-0013.

### 4.5 Nested `@Cacheable` invocations

The aspect pushes the resolved `CachePolicy` unconditionally and restores the previous one in `finally`, so an inner cached method never observes the outer method's policy. The transport is thread-bound: **do not** combine with `@Async` or any pattern that hops threads between the aspect and the cache adapter.

---

## 5. TTL precedence chain

For a given read operation:

1. **NORMAL entry creation** — positive `@CacheTTL` (static or SpEL) override wins; 0 → configured default (`zeta.local.*`).
2. **Promotion to HOT / HOT renewal** — effective hot TTL = `max(override, hot default)`: an override may only raise the floor, so promotion never shortens an entry's lifetime. On a plain hit the override suppliers are not evaluated; promotion then uses the configured hot defaults.
3. **Soft-expire (stale-while-revalidate)** — governed solely by global configuration. The annotation read path always enters via the soft-expire entry point, which self-degrades when the feature is globally disabled. TTL overrides never gate it.
4. **TTL jitter** — ±5% by default (`zeta.local.ttl-jitter-ratio`), applied to all expiry timestamps.

---

## 6. Null-caching semantics

- **Default (no annotation or `@NullCaching(true)`)**: a `null` result is stored as an internal `NullValue` sentinel with `zeta.local.null-value-ttl-seconds` (short TTL). Hits on a valid sentinel return `null` **without re-invoking the method**; the access is still counted for hot-key detection.
- **`@NullCaching(false)`**: a `null` result leaves no entry; the next call re-invokes the method.
- Identical semantics apply on all three read paths (`get`, `getWithSoftExpire`, `computeIfAbsent[WithSoftExpire]`) and in the fluent API (`read(key).notAllowNull()`).

---

## 7. Validator warnings (once per method)

| Rule | Trigger | Meaning |
| --- | --- | --- |
| R1 | `@Intercept(FORCE)` + `@CacheTTL`/`@NullCaching`/`@CacheCondition` | storage annotations are no-ops under FORCE |
| R2 | read-path annotations on `@CachePut`/`@CacheEvict` | only `@SkipBroadcast` applies there |
| R3 | `@Tag` + `@Cacheable` on one method | double-counting the key in HeavyKeeper |
| R4 | `@CacheCondition` + Spring `unless=` on one method | double evaluation, different semantics |

---

## 8. Examples

```java
// Static TTL + intercept-on-hot with convention fallback
@Cacheable("products")
@CacheTTL(hardTtlMs = 60_000, softTtlMs = 10_000)
@Intercept @Fallback
public Product getProduct(String id) { ... }

// Dynamic TTL via SpEL (evaluated only on miss/promotion/refresh)
@Cacheable("users")
@CacheTTL(hardTtlSpEl = "#id.startsWith('vip') ? 600000 : 60000")
public User findUser(String id) { ... }

// Rate limiting with SpEL fallback
@Cacheable("orders")
@Intercept(type = InterceptType.QPS, qps = 500, fallback = "'throttled'")
public Order getOrder(String id) { ... }

// Concurrency guard
@Cacheable("reports")
@Intercept(type = InterceptType.CONCURRENT_THREADS, concurrentThreads = 10, fallback = "'busy'")
public Report getReport(String id) { ... }

// Preload known flash-sale keys (stable hot from the start)
@Cacheable("items")
@Preload(keys = {"item-001", "item-002"})
public Item getItem(String id) { ... }

// Purge when the result is disabled
@Cacheable("configs")
@CacheCondition(unless = "#result == null || #result.disabled()")
public Config getConfig(String key) { ... }

// Explicitly refuse to cache nulls for this method
@Cacheable("maybe")
@NullCaching(false)
public Data findMaybe(String id) { ... }

// Local-only write (no AMQP sync)
@CachePut("sessions")
@SkipBroadcast
public Session updateSession(Session s) { ... }

// Tag a key into the @Cacheable namespace without a cache lookup
@Tag(value = "#id", cacheName = "products", skipReport = true)
public void touchProduct(String id) { ... }
```
