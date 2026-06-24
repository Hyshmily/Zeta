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

控制何时拦截 `@Cacheable` 读取操作并返回缓存/降级值。通过 `trigger()` 选择触发模式：

| 触发模式                      | 行为                                                |
| ----------------------------- | --------------------------------------------------- |
| `IS_LOCAL_HOT`（默认）        | HeavyKeeper 检测到 key 为本地热点时拦截             |
| `FORCE`                       | 始终拦截——方法体永不执行                            |
| `QPS`                         | 单 key QPS 超过阈值时拦截                           |

| 属性       | 类型              | 默认值            | 说明                                              |
| ---------- | ----------------- | ----------------- | ------------------------------------------------- |
| `trigger`  | `InterceptTrigger` | `IS_LOCAL_HOT`    | 触发条件                                          |
| `QPS`      | `int`             | `0`               | 单 key QPS 阈值（`trigger=QPS`）。`0` = 禁用      |
| `fallback` | `String`          | `""`              | SpEL 降级表达式（优先级高于 `@Fallback`）          |

拦截时降级优先级：`@Intercept.fallback()` (SpEL) → `@Fallback` → `peek()`。

```java
// IS_LOCAL_HOT 模式（默认）
@Cacheable(cacheNames = "users", key = "#id")
@Intercept
public User getUser(Long id) {
  // 当 'users:{id}' 为本地热点时，跳过方法体
  return userService.getById(id);
}

// FORCE 模式——始终跳过方法体
@Cacheable(cacheNames = "products", key = "#id")
@Intercept(trigger = InterceptTrigger.FORCE, fallback = "'from-cache'")
public Product getProduct(String id) { ... }

// QPS 模式——单 key QPS 超 100 时降级
@Cacheable(cacheNames = "products", key = "#id")
@Intercept(trigger = InterceptTrigger.QPS, QPS = 100, fallback = "'too-fast'")
public Product getProduct(String id) { ... }
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

### @HotKeyPreload

预膨胀 HeavyKeeper 检测计数，使已知热点 key 立即享受长 TTL 和 `@Intercept` 拦截，无需等待自然检测。

| 属性      | 类型       | 默认值 | 说明                                           |
| --------- | ---------- | ------ | ---------------------------------------------- |
| `keys`    | `String[]` | `{}`   | 静态预加载 key（只膨胀一次）                    |
| `keyExpr` | `String`   | `""`   | SpEL 动态 key 表达式（每次调用时评估）          |
| `count`   | `int`      | `0`    | 膨胀访问次数。`0` = `Integer.MAX_VALUE`        |

静态 key 在首次方法调用时膨胀；重复 key 通过 Caffeine 缓存（10 万条/1 小时 TTL）自动去重。

```java
@Cacheable(cacheNames = "products", key = "#id")
@HotKeyPreload(keys = {"flash-item-001", "flash-item-002"})
@Intercept
public Product getProduct(String id) { ... }
```

仅对 `@Cacheable` 方法生效。`@CachePut` 和 `@CacheEvict` 忽略此注解。

### @Broadcast

控制缓存写入/失效操作是否向对等实例广播同步消息。默认 (`@Broadcast(true)`) 所有缓存变更通过 RabbitMQ 广播。使用 `@Broadcast(false)` 走本地路径（`putLocal()` / `evictLocal()`）。

| 属性    | 类型      | 默认值  | 说明                      |
| ------- | --------- | ------- | ------------------------- |
| `value` | `boolean` | `true`  | 是否广播。设为 `false` 禁止跨实例同步 |

```java
@CachePut(cacheNames = "users", key = "#user.id")
@Broadcast(false)
public User updateUser(User user) { ... }
```

适用于 `@Cacheable`、`@CachePut` 和 `@CacheEvict` 方法。

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

| 注解              | 在 `@Cacheable` / `@CachePut` / `@CacheEvict` 上的作用          |
| ----------------- | ---------------------------------------------------------------- |
| `@HotKeyCacheTTL` | 覆盖硬/软 TTL（仅 `@Cacheable`）                                |
| `@HotKeyPreload`  | 预膨胀 HeavyKeeper 计数，使已知热点 key 立即生效（仅 `@Cacheable`） |
| `@Intercept`      | 当 key 为本地热点 / QPS 超限时跳过方法体；按 `@Intercept.fallback()`、`@Fallback`、`peek()` 优先级返回保底值（仅 `@Cacheable`） |
| `@Fallback`       | 被黑名单阻止、拦截或异常时提供回退值（仅 `@Cacheable`）          |
| `@NullCaching`    | 选择缓存 `null` 返回值（仅 `@Cacheable`）                       |
| `@Broadcast`      | 禁止跨实例同步消息                                               |

该切面以 `@Order(HIGHEST_PRECEDENCE)` 运行——早于 Spring 的 `CacheInterceptor`——使其能够在 `HotKeySpringCache` 在 `get(Callable)` 调用期间读取之前设置 TTL 和空缓存上下文参数。

**注意：** `@Cacheable` 的 SpEL key 解析与 `CacheInterceptor` 重复（可接受的性能开销）。对于 `@CachePut` 和 `@CacheEvict`，仅读取 `@Broadcast` 注解。

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
