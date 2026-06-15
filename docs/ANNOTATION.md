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

Marker annotation that intercepts READ operations when the cache key is classified as a local hot key.

| Condition                        | Behavior                                                     |
| -------------------------------- | ------------------------------------------------------------ |
| Key is hot + `@Fallback` present | Return the fallback value                                    |
| Key is hot + no `@Fallback`      | `peek()` — return stale entry if available, otherwise `null` |
| Key is not hot                   | Cache lookup proceeds normally                               |

```java
@Cacheable(cacheNames = "users", key = "#id")
@Intercept
public User getUser(Long id) {
  // Skipped entirely when 'users:{id}' is a local hot key
  return userService.getById(id);
}
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

| Annotation    | Role on `@Cacheable`                                            |
| ------------- | --------------------------------------------------------------- |
| `@HotKeyCacheTTL` | Override hard/soft TTL                                     |
| `@Intercept`      | Fire intercept callback on cache hit, resolve via `@Fallback` |
| `@Fallback`       | Supply fallback value when interceptor blocks                |
| `@NullCaching`    | Opt-in to caching `null` return values                       |

The aspect runs at `@Order(HIGHEST_PRECEDENCE)` — before Spring's `CacheInterceptor` — allowing it to set TTL and null-caching context parameters that `HotKeySpringCache` reads during the `get(Callable)` call.

**Limitation:** The companion aspect intercepts only `@Cacheable` (not `@CachePut` / `@CacheEvict`), and SpEL key resolution is duplicated with `CacheInterceptor` (acceptable overhead).

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
