# @HotKey Annotation Reference

## Quick Start

```java
@HotKey(key = "'user:' + #id")
public User getUser(Long id) { ... }
```

**Prerequisites:**

| Requirement                            | Detail                                                                        |
| -------------------------------------- | ----------------------------------------------------------------------------- |
| `hotkey.annotation.enabled=true`       | Enable annotation processing                                                  |
| `spring-boot-starter-aop` on classpath | Provides AspectJ weaver                                                       |
| `-parameters` compiler flag            | Enables SpEL parameter name resolution (enabled by default in the parent POM) |

```yaml
hotkey:
  annotation:
    enabled: true
```

## Annotation Attributes

| Attribute    | Type            | Default  | Description                                                                                                                   |
| ------------ | --------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `key`        | `String`        | Required | SpEL expression — method parameters via `#paramName`. Falls back to `arg0, arg1, ...` without `-parameters`.                  |
| `operation`  | `OperationType` | `READ`   | `READ` / `WRITE` / `INVALIDATE`                                                                                               |
| `condition`  | `String`        | `""`     | SpEL condition — `false` or `null` bypasses cache entirely; parameters available as `#paramName`                              |
| `unless`     | `String`        | `""`     | SpEL exclusion — evaluated after cache load succeeds. The special variable `#result` holds the loaded value.                  |
| `softExpire` | `boolean`       | `true`   | When enabled (default), stale entries are served immediately with background refresh. When `false`, behaves as plain `get()`. |

## Operation Types

| Mode             | Facade Method                   | Behavior                                                                                                                                                                                                                                                           |
| ---------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `READ` (default) | `getWithSoftExpire()` / `get()` | Method body acts as value supplier. `softExpire=true` → stale-value + async refresh; `false` → synchronous `get()`. Return type is `Optional`-aware — if method returns `Optional`, it is passed through directly; otherwise `orElse(null)` unwrapping is applied. |
| `WRITE`          | `putBeforeInvalidate()`         | Method executes as mutation: run, version bump, L1 invalidate, send INVALIDATE broadcast. Exceptions are caught and re-thrown after the facade call completes.                                                                                                     |
| `INVALIDATE`     | `invalidate()`                  | Invalidate L1 + version bump + broadcast TYPE_REFRESH (versioned) to peers, then execute the method.                                                                                                                                                               |

## SpEL Key Examples

```java
// Single parameter
@HotKey(key = "'user:' + #id")
public User getUser(Long id) { ... }

// Multiple parameters
@HotKey(key = "'shop:' + #shopId + ':item:' + #itemId")
public Item getItem(Long shopId, Long itemId) { ... }

// Nested property access
@HotKey(key = "#user.id.toString()")
public Profile getProfile(User user) { ... }

// WRITE operation
@HotKey(key = "'user:' + #id", operation = OperationType.WRITE)
public User updateUser(Long id, UserUpdate dto) { ... }

// INVALIDATE operation
@HotKey(key = "'user:' + #id", operation = OperationType.INVALIDATE)
public void clearCache(Long id) { ... }
```

## Companion Annotations

Three companion annotations refine `@HotKey` behavior. All are optional — `@HotKey` alone works with default configuration.

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
@HotKey(key = "'user:' + #id")
@Fallback(value = "'fallback-user'")
public User getUser(Long id) { ... }
```

```java
@HotKey(key = "'user:' + #id")
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
@HotKey(key = "'user:' + #id")
@Intercept
public User getUser(Long id) {
  // Skipped entirely when 'user:{id}' is a local hot key
  return userService.getById(id);
}
```

Only applies to `READ` operations. Ignored on `WRITE` and `INVALIDATE`.

### @HotKeyCacheTTL

Per-method override for hard and soft TTLs.

```java
@HotKey(key = "'user:' + #id")
@HotKeyCacheTTL(hardTtlMs = 60000, softTtlMs = 5000)
public User getUser(Long id) { ... }
```

| Attribute   | Default | Description                               |
| ----------- | ------- | ----------------------------------------- |
| `hardTtlMs` | `0`     | Hard TTL in ms. `0` = use global default. |
| `softTtlMs` | `0`     | Soft TTL in ms. `0` = use global default. |

## Priority Chain (READ)

For `@HotKey(operation = READ)`, the AOP aspect evaluates the following steps in order:

| Priority    | Step          | Description                                                                         |
| ----------- | ------------- | ----------------------------------------------------------------------------------- |
| 1 (highest) | **Blacklist** | `Rule.RuleAction#BLOCK` → fallback or `HotKeyBlockedException`                      |
| 2           | **Condition** | SpEL `condition()` evaluates to `false` → skip cache, execute method directly       |
| 3           | **Intercept** | `@Intercept` + `isLocalHotKey()` → return fallback or `peek()`                      |
| 4           | **TTL**       | `@HotKeyCacheTTL` override, or `0` → use global default per key state               |
| 5           | **Cache**     | `getWithSoftExpire()` / `get()` with loader; fallback on `RuntimeException`         |
| 6           | **Unless**    | SpEL `unless()` evaluated with `#result` variable (value remains cached per design) |

## Return Type Handling

The aspect is `Optional`-aware:

```java
// Method returns Optional → passed through as-is
@HotKey(key = "'user:' + #id")
public Optional<User> getUser(Long id) { ... }
```

```java
// Method returns concrete type → orElse(null) unwrapping
@HotKey(key = "'user:' + #id")
public User getUser(Long id) { ... }
```

## Complete Example

```java
@Service
public class UserService {

  @Autowired
  private HotKey hotKey;

  // ---- Declarative (AOP) ----

  @HotKey(key = "'user:' + #id", softExpire = true)
  @HotKeyCacheTTL(hardTtlMs = 300000, softTtlMs = 30000)
  @Intercept
  @Fallback
  public User getUser(Long id) {
    return userMapper.selectById(id);
  }

  public User getUserFallback(Long id) {
    return new User("guest");
  }

  @HotKey(key = "'user:' + #id", operation = OperationType.WRITE)
  public User updateUser(Long id, UserUpdate dto) {
    userMapper.update(id, dto);
    return userMapper.selectById(id);
  }

  @HotKey(key = "'user:' + #id", operation = OperationType.INVALIDATE)
  public void evictUser(Long id) {
    // nothing to do — cache invalidation is handled by the aspect
  }

  // ---- Equivalent imperative API ----

  public User getUserImperative(Long id) {
    String key = "user:" + id;
    return hotKey.getWithSoftExpire(key, () -> userMapper.selectById(id), 300000, 30000).orElse(null);
  }

  public void updateUserImperative(Long id, UserUpdate dto) {
    hotKey.putBeforeInvalidate("user:" + id, () -> {
      userMapper.update(id, dto);
    });
  }

  public void evictUserImperative(Long id) {
    hotKey.invalidate("user:" + id);
  }
}
```

## See Also

- [HotKey README](../README.md) — full framework documentation
- [CONFIG.md](CONFIG.md) — complete configuration reference
- [HotKeyAspect.java](../common/src/main/java/io/github/hyshmily/hotkey/annotation/HotKeyAspect.java) — aspect implementation
