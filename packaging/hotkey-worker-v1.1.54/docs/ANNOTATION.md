# Annotation Reference

## Overview

HotKey provides a set of **companion annotations** that refine caching behavior. They are applied alongside standard `@Cacheable` / `@CachePut` / `@CacheEvict` annotations via the Spring Cache integration (`hotkey.spring-cache.enabled=true`).

## Prerequisites

| Requirement                            | Detail                                                                        |
| -------------------------------------- | ----------------------------------------------------------------------------- |
| `hotkey.spring-cache.enabled=true`     | Enable Spring Cache integration                                               |
| `spring-boot-starter-cache` on classpath | Provides Spring's caching abstraction                                       |
| `spring-boot-starter-aop` on classpath | Provides AspectJ weaver for the companion aspect                              |
| `@EnableCaching` on any `@Configuration` | Activates standard Spring Cache infrastructure                              |
| `-parameters` compiler flag            | Enables SpEL parameter name resolution (enabled by default in the parent POM) |

```yaml
hotkey:
  spring-cache:
    enabled: true
```

## Companion Annotations

Four companion annotations refine method-level caching behavior for `@Cacheable` methods.

### @Fallback

Declares a fallback value or method when the cache path is blocked or fails.

**Triggered in three scenarios:**

1. A blacklist rule blocks the cache key
2. The key is a local hot key and `@Intercept` is also present
3. The cache loader throws a `RuntimeException`

**Two resolution modes:**

| Mode              | Attribute               | Behavior                                                                         |
| ----------------- | ----------------------- | -------------------------------------------------------------------------------- |
| SpEL expression   | `value` non-empty       | Evaluated as SpEL; method parameters available as `#paramName`                   |
| Naming convention | `value` empty (default) | Looks for `{methodName}Fallback` with identical parameter types on the same bean |

```java
@Cacheable(cacheNames = "users", key = "#id")
@Fallback(value = "'fallback-user'")
public User getUser(Long id) { ... }
```

```java
@Cacheable(cacheNames = "users", key = "#id")
@Fallback  // empty value → naming convention
public User getUser(Long id) { ... }

// Must exist on the same bean:
public User getUserFallback(Long id) { return new User("guest"); }
```

### @Intercept

Controls when a `@Cacheable` read operation is intercepted and the cached/fallback value is returned instead of executing the method. The trigger mode is selected via `trigger()`:

| Trigger Mode                    | Behavior                                                     |
| ------------------------------- | ------------------------------------------------------------ |
| `IS_LOCAL_HOT` (default)        | Intercept when HeavyKeeper detects the key as a local hot key |
| `FORCE`                         | Always intercept — method is never executed                  |
| `QPS`                           | Intercept when per-key request rate exceeds `QPS()` threshold |

| Attribute  | Type              | Default           | Description                                                 |
| ---------- | ----------------- | ----------------- | ----------------------------------------------------------- |
| `trigger`  | `InterceptTrigger` | `IS_LOCAL_HOT`    | Condition that triggers interception                        |
| `QPS`      | `int`             | `0`               | Per-key QPS threshold (`trigger=QPS`). `0` disables check.  |
| `fallback` | `String`          | `""`              | SpEL fallback expression (takes precedence over `@Fallback`) |

When intercepted, fallback resolution order: `@Intercept.fallback()` (SpEL) → `@Fallback` → `peek()`.

```java
// IS_LOCAL_HOT mode (default)
@Cacheable(cacheNames = "users", key = "#id")
@Intercept
public User getUser(Long id) {
  // Skipped entirely when 'users:{id}' is a local hot key
  return userService.getById(id);
}

// FORCE mode — always skip method body
@Cacheable(cacheNames = "products", key = "#id")
@Intercept(trigger = InterceptTrigger.FORCE, fallback = "'from-cache'")
public Product getProduct(String id) { ... }

// QPS mode — throttle when exceeding 100 req/s per key
@Cacheable(cacheNames = "products", key = "#id")
@Intercept(trigger = InterceptTrigger.QPS, QPS = 100, fallback = "'too-fast'")
public Product getProduct(String id) { ... }
```

Only applies to `@Cacheable` methods (READ operations). Ignored on `@CachePut` and `@CacheEvict`.

### @HotKeyCacheTTL

Per-method override for hard and soft TTLs.

```java
@Cacheable(cacheNames = "users", key = "#id")
@HotKeyCacheTTL(hardTtlMs = 60000, softTtlMs = 5000)
public User getUser(Long id) { ... }
```

| Attribute   | Default | Description                               |
| ----------- | ------- | ----------------------------------------- |
| `hardTtlMs` | `0`     | Hard TTL in ms. `0` = use global default. |
| `softTtlMs` | `0`     | Soft TTL in ms. `0` = use global default. |

### @HotKeyPreload

Pre-inflates HeavyKeeper detection counts for known-hot keys, making them immediately eligible for long TTLs and `@Intercept` treatment without waiting for organic detection.

| Attribute | Type      | Default | Description                                                    |
| --------- | --------- | ------- | -------------------------------------------------------------- |
| `keys`    | `String[]` | `{}`    | Static cache keys to preload (inflated once)                   |
| `keyExpr` | `String`   | `""`    | SpEL expression for a dynamic preload key (evaluated per call) |
| `count`   | `int`      | `0`     | Inflated access count. `0` = `Integer.MAX_VALUE`               |

Static keys are inflated on first method invocation; duplicates are silently ignored via a bounded Caffeine cache (100k max, 1-hour TTL).

```java
@Cacheable(cacheNames = "products", key = "#id")
@HotKeyPreload(keys = {"flash-item-001", "flash-item-002"})
@Intercept
public Product getProduct(String id) { ... }
```

Only applies to `@Cacheable` methods. Ignored on `@CachePut` and `@CacheEvict`.

### @Broadcast

Controls whether cache write/evict operations broadcast sync messages to peer instances. By default (`@Broadcast(true)`), all cache mutations are broadcast via RabbitMQ. Place `@Broadcast(false)` to use local-only variants (`putLocal()` / `evictLocal()`).

| Attribute | Type      | Default | Description                                                       |
| --------- | --------- | ------- | ----------------------------------------------------------------- |
| `value`   | `boolean` | `true`  | Whether to broadcast. Set `false` to suppress cross-instance sync. |

```java
@CachePut(cacheNames = "users", key = "#user.id")
@Broadcast(false)
public User updateUser(User user) { ... }
```

Applies to `@Cacheable`, `@CachePut`, and `@CacheEvict` methods.

### @NullCaching

Opt-in to caching `null` return values. By default, `null` results from a cache loader are treated as "no value" and not stored. When `@NullCaching(true)` is present, `null` is stored as an internal sentinel (`NullValue`) and returned as `null` on subsequent lookups.

| Attribute | Type      | Default | Description                                                       |
| --------- | --------- | ------- | ----------------------------------------------------------------- |
| `value`   | `boolean` | `true`  | Whether to cache `null` return values. Set `false` to disable.    |

```java
@Cacheable(cacheNames = "users", key = "#id")
@NullCaching(true)
public User getUser(Long id) { ... }
```

Only applies to `@Cacheable` methods (READ operations). Ignored on `@CachePut` and `@CacheEvict`.

### Spring Cache Companion Aspect

When `hotkey.spring-cache.enabled=true`, `HotKeyCacheExtensionAspect` activates alongside standard Spring Cache annotations (`@Cacheable` / `@CachePut` / `@CacheEvict`). It applies the same companion annotations to `@Cacheable` methods:

| Annotation    | Role on `@Cacheable` / `@CachePut` / `@CacheEvict`                 |
| ------------- | ------------------------------------------------------------------ |
| `@HotKeyCacheTTL` | Override hard/soft TTL (`@Cacheable` only)                    |
| `@HotKeyPreload`  | Pre-inflate HeavyKeeper counts for known-hot keys (`@Cacheable` only) |
| `@Intercept`      | Skip method when key is local hot key / QPS exceeded; use `@Intercept.fallback()`, `@Fallback`, or `peek()` for stale value (`@Cacheable` only) |
| `@Fallback`       | Supply fallback value when blocked, intercepted, or on exception (`@Cacheable` only) |
| `@NullCaching`    | Opt-in to caching `null` return values (`@Cacheable` only)     |
| `@Broadcast`      | Suppress cross-instance sync messages                           |

The aspect runs at `@Order(HIGHEST_PRECEDENCE)` — before Spring's `CacheInterceptor` — allowing it to set TTL and null-caching context parameters that `HotKeySpringCache` reads during the `get(Callable)` call.

**Note:** SpEL key resolution is duplicated with `CacheInterceptor` for `@Cacheable` (acceptable overhead). For `@CachePut` and `@CacheEvict`, only the `@Broadcast` annotation is read.

```java
@Cacheable(cacheNames = "users", key = "#id")
@HotKeyCacheTTL(softTtlMs = 1000)
@Intercept
@Fallback
public User getUser(Long id) { ... }
```

## See Also

- [HotKey README](../README.md) — full framework documentation
- [CONFIG.md](CONFIG.md) — complete configuration reference
- [HotKeyCacheExtensionAspect.java](../common/src/main/java/io/github/hyshmily/hotkey/annotation/HotKeyCacheExtensionAspect.java) — Spring Cache companion aspect implementation
