# 注解参考

## 概述

HotKey 提供一组**配套注解**来细化缓存行为。它们通过 Spring Cache 集成（`hotkey.spring-cache.enabled=true`）与标准 `@Cacheable` / `@CachePut` / `@CacheEvict` 注解一起使用。

## 前置条件

| 条件                                       | 说明                                    |
| ------------------------------------------ | --------------------------------------- |
| `hotkey.spring-cache.enabled=true`         | 启用 Spring Cache 集成                  |
| classpath 中包含 `spring-boot-starter-cache` | 提供 Spring 缓存抽象                  |
| classpath 中包含 `spring-boot-starter-aop` | 提供配套切面的 AspectJ 织入器           |
| 任意 `@Configuration` 类上的 `@EnableCaching` | 激活标准 Spring 缓存基础设施          |
| `-parameters` 编译标志                     | 启用 SpEL 参数名解析（父 POM 默认启用） |

```yaml
hotkey:
  spring-cache:
    enabled: true
```

## 配套注解

四个配套注解细化 `@Cacheable` 方法级缓存行为。

### @Fallback

声明缓存路径被阻断或失败时的降级值或方法。

**触发场景：**

1. 黑名单规则阻止了缓存 key
2. key 为本地热点且同时使用了 `@Intercept`
3. 缓存加载器抛出 `RuntimeException`

**两种解析模式：**

| 模式        | 属性                 | 行为                                                        |
| ----------- | -------------------- | ----------------------------------------------------------- |
| SpEL 表达式 | `value` 非空         | 作为 SpEL 评估；方法参数通过 `#paramName` 引用              |
| 命名约定    | `value` 为空（默认） | 在同一 Bean 上查找 `{methodName}Fallback`，参数类型必须匹配 |

```java
@Cacheable(cacheNames = "users", key = "#id")
@Fallback(value = "'fallback-user'")
public User getUser(Long id) { ... }
```

```java
@Cacheable(cacheNames = "users", key = "#id")
@Fallback  // 空 value → 命名约定
public User getUser(Long id) { ... }

// 必须在同一 Bean 上存在：
public User getUserFallback(Long id) { return new User("guest"); }
```

### @Intercept

标记注解，当缓存 key 被归类为本地热点时拦截 READ 操作。

| 条件                          | 行为                                |
| ----------------------------- | ----------------------------------- |
| key 为热点 + 存在 `@Fallback` | 返回降级值                          |
| key 为热点 + 无 `@Fallback`   | `peek()`——有旧值则返回，否则 `null` |
| key 非热点                    | 正常走缓存路径                      |

```java
@Cacheable(cacheNames = "users", key = "#id")
@Intercept
public User getUser(Long id) {
  // 当 'users:{id}' 为本地热点时，此方法体完全不执行
  return userService.getById(id);
}
```

仅对 `@Cacheable` 方法（READ 操作）生效。`@CachePut` 和 `@CacheEvict` 忽略此注解。

### @HotKeyCacheTTL

硬和软 TTL 的 per-method 覆盖。

```java
@Cacheable(cacheNames = "users", key = "#id")
@HotKeyCacheTTL(hardTtlMs = 60000, softTtlMs = 5000)
public User getUser(Long id) { ... }
```

| 属性        | 默认值 | 说明                                 |
| ----------- | ------ | ------------------------------------ |
| `hardTtlMs` | `0` | 硬 TTL（毫秒）。`0` = 使用全局默认值 |
| `softTtlMs` | `0` | 软 TTL（毫秒）。`0` = 使用全局默认值 |

### @NullCaching

选择缓存 `null` 返回值。默认情况下，缓存加载器返回 `null` 被视为"无值"且不存储。当 `@NullCaching(true)` 存在时，`null` 作为内部哨兵（`NullValue`）存储，并在后续查找时以 `null` 形式返回。

| 属性    | 类型      | 默认值  | 说明                      |
| ------- | --------- | ------- | ------------------------- |
| `value` | `boolean` | `true`  | 是否缓存 `null` 返回值。设为 `false` 禁用 |

```java
@Cacheable(cacheNames = "users", key = "#id")
@NullCaching(true)
public User getUser(Long id) { ... }
```

仅对 `@Cacheable` 方法（READ 操作）生效。`@CachePut` 和 `@CacheEvict` 忽略此注解。

### Spring Cache 配套切面

当 `hotkey.spring-cache.enabled=true` 时，`HotKeyCacheExtensionAspect` 随标准 Spring Cache 注解（`@Cacheable` / `@CachePut` / `@CacheEvict`）一起激活。它将相同的配套注解应用于 `@Cacheable` 方法：

| 注解              | 在 `@Cacheable` 上的作用                          |
| ----------------- | ------------------------------------------------- |
| `@HotKeyCacheTTL` | 覆盖硬/软 TTL                                     |
| `@Intercept`      | 缓存命中时触发拦截回调，通过 `@Fallback` 解析     |
| `@Fallback`       | 拦截器阻止时提供回退值                            |
| `@NullCaching`    | 选择缓存 `null` 返回值                            |

该切面以 `@Order(HIGHEST_PRECEDENCE)` 运行——早于 Spring 的 `CacheInterceptor`——使其能够在 `HotKeySpringCache` 在 `get(Callable)` 调用期间读取之前设置 TTL 和空缓存上下文参数。

**局限性：** 配套切面仅拦截 `@Cacheable`（非 `@CachePut` / `@CacheEvict`），且 SpEL key 解析与 `CacheInterceptor` 重复（可接受的性能开销）。

```java
@Cacheable(cacheNames = "users", key = "#id")
@HotKeyCacheTTL(softTtlMs = 1000)
@Intercept
@Fallback
public User getUser(Long id) { ... }
```

## 参考

- [HotKey README](../README.zh.md) — 完整框架文档
- [CONFIG.zh.md](CONFIG.zh.md) — 完整配置参考
- [HotKeyCacheExtensionAspect.java](../common/src/main/java/io/github/hyshmily/hotkey/annotation/HotKeyCacheExtensionAspect.java) — Spring Cache 配套切面实现源码
