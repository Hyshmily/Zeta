# @HotKey 注解参考

## 快速开始

```java
@HotKey(key = "'user:' + #id")
public User getUser(Long id) { ... }
```

**前置条件：**

| 条件                                       | 说明                                    |
| ------------------------------------------ | --------------------------------------- |
| `hotkey.annotation.enabled=true`           | 启用注解处理                            |
| classpath 中包含 `spring-boot-starter-aop` | 提供 AspectJ 织入器                     |
| `-parameters` 编译标志                     | 启用 SpEL 参数名解析（父 POM 默认启用） |

```yaml
hotkey:
  annotation:
    enabled: true
```

## 注解属性

| 属性         | 类型            | 默认值 | 说明                                                                                     |
| ------------ | --------------- | ------ | ---------------------------------------------------------------------------------------- |
| `key`        | `String`        | 必填   | SpEL 表达式，方法参数通过 `#paramName` 引用。无 `-parameters` 时回退到 `arg0, arg1, ...` |
| `operation`  | `OperationType` | `READ` | `READ` / `WRITE` / `INVALIDATE`                                                          |
| `condition`  | `String`        | `""`   | SpEL 条件——`false` 或 `null` 时完全绕过缓存；参数可通过 `#paramName` 引用                |
| `unless`     | `String`        | `""`   | SpEL 排除表达式——缓存加载成功后评估。特殊变量 `#result` 持有加载的值                     |
| `softExpire` | `boolean`       | `true` | 启用时（默认）过期旧值立即返回并后台刷新。`false` 时等同于 `get()`                       |

## 操作类型

| 模式           | 门面方法                        | 行为                                                                                                                                                                    |
| -------------- | ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `READ`（默认） | `getWithSoftExpire()` / `get()` | 方法体作为值 supplier。`softExpire=true` → 过期旧值 + 异步刷新；`false` → 同步 `get()`。返回类型感知 `Optional`——若方法返回 `Optional` 则透传，否则 `orElse(null)` 解包 |
| `WRITE`        | `putBeforeInvalidate()`         | 方法作为突变执行：运行、版本递增、L1 失效、发送 INVALIDATE 广播。异常被捕获，在门面调用完成后重新抛出                                                                   |
| `INVALIDATE`   | `invalidate()`                  | 失效 L1 + 版本递增 + 广播 TYPE_REFRESH（带版本号）到对端，然后执行方法                                                                                                  |

## SpEL Key 示例

```java
// 单个参数
@HotKey(key = "'user:' + #id")
public User getUser(Long id) { ... }

// 多个参数
@HotKey(key = "'shop:' + #shopId + ':item:' + #itemId")
public Item getItem(Long shopId, Long itemId) { ... }

// 嵌套属性
@HotKey(key = "#user.id.toString()")
public Profile getProfile(User user) { ... }

// WRITE 操作
@HotKey(key = "'user:' + #id", operation = OperationType.WRITE)
public User updateUser(Long id, UserUpdate dto) { ... }

// INVALIDATE 操作
@HotKey(key = "'user:' + #id", operation = OperationType.INVALIDATE)
public void clearCache(Long id) { ... }
```

## 配套注解

三个配套注解用于细化 `@HotKey` 行为。均为可选——单独 `@HotKey` 即可配合默认配置正常工作。

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
@HotKey(key = "'user:' + #id")
@Fallback(value = "'fallback-user'")
public User getUser(Long id) { ... }
```

```java
@HotKey(key = "'user:' + #id")
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
@HotKey(key = "'user:' + #id")
@Intercept
public User getUser(Long id) {
  // 当 'user:{id}' 为本地热点时，此方法体完全不执行
  return userService.getById(id);
}
```

仅对 `READ` 操作生效。`WRITE` 和 `INVALIDATE` 忽略此注解。

### @HotKeyCacheTTL

硬和软 TTL 的 per-method 覆盖。

```java
@HotKey(key = "'user:' + #id")
@HotKeyCacheTTL(hardTtlMs = 60000, softTtlMs = 5000)
public User getUser(Long id) { ... }
```

| 属性        | 默认值 | 说明                                 |
| ----------- | ------ | ------------------------------------ |
| `hardTtlMs` | `0`    | 硬 TTL（毫秒）。`0` = 使用全局默认值 |
| `softTtlMs` | `0`    | 软 TTL（毫秒）。`0` = 使用全局默认值 |

## READ 优先级链

`@HotKey(operation = READ)` 的 AOP 切面按顺序评估以下步骤：

| 优先级    | 步骤       | 说明                                                                    |
| --------- | ---------- | ----------------------------------------------------------------------- |
| 1（最高） | **黑名单** | `Rule.RuleAction#BLOCK` → 降级值或 `HotKeyBlockedException`             |
| 2         | **条件**   | SpEL `condition()` 评估为 `false` → 跳过缓存，直接执行方法              |
| 3         | **拦截**   | `@Intercept` + `isLocalHotKey()` → 返回降级值或 `peek()`                |
| 4         | **TTL**    | `@HotKeyCacheTTL` 覆盖，或 `0` → 使用 key 状态的全局默认值              |
| 5         | **缓存**   | `getWithSoftExpire()` / `get()` 带加载器；`RuntimeException` 时触发降级 |
| 6         | **排除**   | SpEL `unless()` 使用 `#result` 变量评估（按设计值仍保留在缓存中）       |

## 返回类型处理

切面感知 `Optional` 类型：

```java
// 方法返回 Optional → 直接透传
@HotKey(key = "'user:' + #id")
public Optional<User> getUser(Long id) { ... }
```

```java
// 方法返回具体类型 → orElse(null) 解包
@HotKey(key = "'user:' + #id")
public User getUser(Long id) { ... }
```

## 完整示例

```java
@Service
public class UserService {

  @Autowired
  private HotKey hotKey;

  // ---- 声明式（AOP） ----

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
    // 无需任何操作——缓存失效由切面处理
  }

  // ---- 等价的命令式 API ----

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

## 参考

- [HotKey README](../README.zh.md) — 完整框架文档
- [CONFIG.zh.md](CONFIG.zh.md) — 完整配置参考
- [HotKeyAspect.java](../common/src/main/java/io/github/hyshmily/hotkey/annotation/HotKeyAspect.java) — 切面实现源码
