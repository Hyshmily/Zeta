# Zeta

<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.hyshmily/zeta"><img src="https://img.shields.io/maven-central/v/io.github.hyshmily/zeta?color=blue" alt="Maven Central"></a>
  <a href="https://jitpack.io/#Hyshmily/Zeta"><img src="https://jitpack.io/v/Hyshmily/Zeta.svg" alt="JitPack"></a>
  <a href="https://github.com/Hyshmily/Zeta/releases"><img src="https://img.shields.io/github/v/release/Hyshmily/Zeta?color=brightgreen" alt="GitHub Release"></a>
  <a href="https://github.com/Hyshmily/Zeta/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Hyshmily/Zeta/ci.yml?branch=master&label=CI&logo=github" alt="CI"></a>
  <a href="https://coveralls.io/github/Hyshmily/Zeta?branch=master"><img src="https://coveralls.io/repos/github/Hyshmily/Zeta/badge.svg?branch=master" alt="Coveralls"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-17-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
  <a href="https://github.com/Hyshmily/zeta/commits/master"><img src="https://img.shields.io/github/last-commit/Hyshmily/zeta/master" alt="Last Commit"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.zeta"><img src="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.zeta" alt="Visitors"></a>
</p>

[**English**](README.md)

Zeta 是一款可配置、高性能、低成本的轻量级分布式缓存与预热框架, 致力于以极低的成本解决集群维度对任意突发性的、无法预先感知的热点数据分布式一致性缓存问题

通过 Redis 和 RabbitMQ 将业务代码与整个分布式协调基础设施完全解耦

默认设置下全链路端对端延迟约为:**300ms (P99)**。

基准测试：

- `peek` **约 16M ops/s**（纯 Caffeine 查询，无副作用）
- `get`（L1 命中）**约 15M ops/s**（全路径含 TopK + Reporter）

HotKey受京东[hotkey](https://gitee.com/jd-platform-opensource/hotkey)项目启发,算法支持来自[Aegis](https://github.com/go-kratos/aegis)

## 快速开始

### 1. 添加依赖

> [!CAUTION]
> 自 **v1.1.55** 起，本项目已正式更名为 **Zeta**（原 HotKey）。所有旧版本（≤ v1.1.54）仍可使用，但存在未修复的安全漏洞。

配置参考:

<details>
<summary><b>快速部署 YAML 模板</b></summary>

**本地 Local（App 侧）** — 引入 `zeta` 依赖即可运行，可按需取消注释

```yaml
zeta:
  # local 参数全部走默认值，无需显式配置

  # —— 可选功能，按需取消注释 ——

  # 跨实例缓存同步（需 spring-boot-starter-amqp + spring-boot-starter-data-redis）
  # sync:
  #   enabled: true

  # Spring Cache 集成（需 spring-boot-starter-cache）
  # spring-cache:
  #   enabled: true

  # Worker 决策监听（需 spring-boot-starter-amqp + spring-boot-starter-data-redis）
  # worker-listener:
  #   enabled: true
  # sync:
  #   enabled: true     # worker-listener 依赖 hotKeyRedisLoader Bean

  # 一致性哈希默认开启（通过心跳实现动态 Worker 路由）
  # local:
  #   consistent-hashing:
  #     enabled: true     #（默认已开启）
```

**单 Worker（独立部署节点）** — 添加 `spring-boot-starter-amqp`

```yaml
zeta:
  worker:
    enabled: true
    routing:
      app-name: myapp # 【必须】与 App 侧 zeta.local.app-name 一致
      # 一致性哈希默认开启；Worker 通过心跳自动注册

# 多 Worker 示例：3 台机器，相同 app-name
# 一致性哈希通过心跳自动将 key 路由到正确的 Worker
# 无需静态分片配置——只需增加机器即可,建议本地App优先部署再启动worker
```

**集群健康阈值** — `expected-worker-count: 0`（动态模式，默认）时，`min-alive-workers: 0` 等价于 **有 1 个存活即健康**。`expected-worker-count: N`（固定模式）时，使用多数派公式 `N/2 + 1`。设置 `min-alive-workers` 可覆盖任意模式。详见 `docs/CONFIG.md`。`

**全部参数）**
参考[CONFIG.zh.md](docs/CONFIG.zh.md)

</details>

#### 本地模式（App 侧）— Maven 依赖

**Maven Central**（无需额外仓库）：

```xml
<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>zeta</artifactId>
  <version>1.1.55</version>
</dependency>
```

**JitPack**：

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>zeta</artifactId>
    <version>1.1.55</version>
</dependency>
```

**GitHub Packages**：

```xml
<repositories>
  <repository>
    <id>github</id>
    <name>GitHub Packages</name>
    <url>https://maven.pkg.github.com/hyshmily/zeta</url>
  </repository>
</repositories>

<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>zeta</artifactId>
  <version>1.1.55</version>
</dependency>
```

#### Worker 节点（独立部署）— JAR / Docker

> [!IMPORTANT]
>
> **前置条件：**
> Redis+RabbitMQ

预构建镜像托管在 GHCR。

**拉取:** 使用具备 `read:packages` 权限的 GitHub PAT 登录：

```bash
echo $PAT | docker login ghcr.io -u hyshmily --password-stdin
```

**通过 docker compose 全栈启动**（含 Redis + RabbitMQ）：

```bash
docker compose -f worker/docker-compose.yml up -d
```

**扩缩容多个 Worker 实例：**

```bash
docker compose -f worker/docker-compose.yml up -d --scale worker=3
```

**单独运行**（外部 Redis + RabbitMQ）：

```bash
docker run -d --name zeta-worker -p 8080:8080 \
  -e SPRING_RABBITMQ_HOST=rabbitmq \
  -e SPRING_DATA_REDIS_HOST=redis \
  -e ZETA_WORKER_ENABLED=true \
  ghcr.io/hyshmily/zeta-worker:1.1.55
```

**直接运行 JAR**（无需 Docker）：

```bash
mvn clean package -pl worker
java -jar worker/target/zeta-worker-1.1.55.jar
```

### 2. 配置

默认local本地配置

**功能配置：**

| 功能               | 启用方式                                   | 说明                                                          |
| ------------------ | ------------------------------------------ | ------------------------------------------------------------- |
| Redis 二级缓存     | 添加 `RedisTemplate` Bean                  | 两级缓存，L2 兜底                                             |
| 跨实例同步         | `zeta.sync.enabled=true`                   | 基于 RabbitMQ 的缓存失效                                      |
| Worker Listener    | `zeta.worker-listener.enabled=true`        | 接收 Worker 的 HOT/COOL 决策                                  |
| Worker 模式        | `zeta.worker.enabled=true`                 | 运行专用 Worker 节点                                          |
| Worker TopK 持久化 | `zeta.worker.persistence.enabled=true`     | 重启后从 Redis 热启动                                         |
| 访问上报           | `zeta.report.enabled=true`（默认）         | 向 Worker 上报访问次数                                        |
| Reporter 自我保护  | `zeta.local.reporter.enabled=true`（默认） | Reporter 刷盘的 BBR 背压保护                                  |
| Spring Cache 集成  | `zeta.spring-cache.enabled=true`           | `@Cacheable` / `@CachePut` / `@CacheEvict` 融合 Zeta 热点检测 |

完整属性参考见 [CONFIG.zh.md](docs/CONFIG.zh.md)。

### 3. 使用

> [!NOTE]
> **序列化：** Zeta 内部使用 `StringRedisTemplate`，值的序列化完全由调用方决定。推荐使用 **Jackson**（Spring Boot 默认，JSON）或 **Kryo**（二进制，极致吞吐）。不建议使用 JDK 原生序列化。

**方法概览**

| 类别 | 方法                                                                                                                              |
| ---- | --------------------------------------------------------------------------------------------------------------------------------- |
| 读   | `get`, `getWithSoftExpire`, `computeIfAbsent`, `computeIfAbsentWithSoftExpire`, `peek`, `peekAll`                                 |
| 写   | `putThrough`, `putLocal`, `invalidateAfterPut`, `refresh`, `refreshAll`                                                           |
| 失效 | `invalidate`, `invalidateAllLocal`, `compareAndInvalidate`                                                                        |
| 原子 | `compareAndSet`, `compareAndInvalidate`                                                                                           |
| 流式 | `read(key)` → `ZetaReadQuery`, `write(key)` → `ZetaWriteCommand`                                                                  |
| 内省 | `peek`, `estimatedSize`, `stats`, `getLocalCache`, `isLocalHotKey`, `isWorkerHotKey`, `returnLocalHotKeys`, `returnWorkerHotKeys` |
| 规则 | `addBlacklist`, `removeBlacklist`, `addWhitelist`, `removeWhitelist`, `evaluateRule`, `getAllRules`, `clearAllRules`              |
| 锁   | `tryLock`, `tryLockAndRun`                                                                                                        |
| 后台 | `registerRefresh`, `updateRefresh`, `unregisterRefresh`                                                                           |
| 模式 | `isApp`, `isWorker`, `isAppOnly`, `isWorkerOnly`                                                                                  |

**读操作**

```java
@Autowired
private Zeta zeta;

// A. peek — 仅查 L1，不做热点追踪
Optional<String> r = zeta.peek("user:123"); // L1 未命中返回 Optional.empty()

// B. computeIfAbsent — 简化 get（无 Optional 包装）
String val = zeta.computeIfAbsent("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// C. get — 两级缓存（Redis 或任意后端）
Optional<String> r = zeta.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// D. getWithSoftExpire — 软失效（stale-while-revalidate）
Optional<String> r = zeta.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// E. 流式读 API + fallback 链
User user = zeta
  .read("user:42")
  .withPrimary(userRepo::findById)
  .thenExecute(backupRepo::findById)
  .withHardTtl(30_000)
  .withSoftTtl(10_000)
  .allowBroadcast()
  .executeOrNull();
```

**写操作**

```java
// F. putThrough — 写穿透 + 广播
zeta.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));

// G. invalidateAfterPut — 变异后失效（集合类型）
zeta.invalidateAfterPut(key, () -> redisTemplate.opsForSet().add(key, members));

// H. putLocal — 仅本地写，不广播、不 bump 版本
zeta.putLocal("user:123", cachedValue, hardTtlMs, softTtlMs); // 指定 TTL

// I. refresh — 本地驱逐后加载并缓存
zeta.refresh("user:123", () -> loadUser(123), hardTtlMs, softTtlMs); // 带 TTL 覆盖

// J. 流式写 API
zeta.write("user:42").withHardTtl(30_000).putThrough(newValue, dbWriter);
zeta.write("user:42").invalidateAfterPut(dbMutation);
zeta.write("user:42").invalidate();
```

**自定义 per-entry TTL**

Zeta 使用**差异化 TTL**：热点 key 和普通 key 分别有独立默认值。per-call 覆盖在此基础上生效。

| Key 状态 | 硬 TTL（Caffeine 淘汰）         | 软 TTL（stale-while-revalidate）  |
| -------- | ------------------------------- | --------------------------------- |
| 普通     | `default-hard-ttl-ms`（5min）   | `default-soft-ttl-ms`（30s）      |
| 热点     | `default-hot-hard-ttl-ms`（1h） | `default-hot-soft-ttl-ms`（5min） |

```java
// 5 分钟硬 TTL + 30 秒软 TTL
Optional<String> shopJson = zeta.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5), TimeUnit.SECONDS.toMillis(30));

// 30 秒硬 TTL，软 TTL 用默认值
zeta.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30), 0);

//  registerRefresh / updateRefresh — 定时后台刷新（softTtlMs = 间隔）
  zeta.registerRefresh("user:123", () -> loadUser(123), 300_000L, 60_000L);  // 每 60s
  zeta.updateRefresh("user:123", () -> loadUser(123), 300_000L, 30_000L);    // 改为 30s
  zeta.unregisterRefresh("user:123");                                         // 停止

```

> [!NOTE]
> **缓存雪崩防护：** `CacheExpireManager` 通过 `DelayUtil.computeTtlJitter()` 对每个过期时间戳施加均匀随机偏移（默认 ±5%）。5 分钟硬 TTL 在默认偏移下实际到期 4.75 ~ 5.25 分钟。通过 `zeta.local.ttl-jitter-ratio`（比例，默认 `0.05` = ±5%,`0`为禁用）控制。

> [!TIP]
> per-call TTL 语义：传入 `0` 表示使用该 key 状态的配置默认值。彻底逻辑过期（纯软过期，硬 TTL 永不淘汰）：向 `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)` 传入 `hardTtlMs = Long.MAX_VALUE`，entry 永久驻留 Caffeine。此用法受 Caffeine `Expiry` JavaDoc 明确支持：_"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([源码](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))

**原子操作**

基于 CAS 风格的无锁条件更新：

```java
// compareAndSet — 当前值匹配时原子替换
boolean ok = zeta.compareAndSet("user:123", oldValue, newValue);

// compareAndInvalidate — 当前值匹配时失效
boolean ok = zeta.compareAndInvalidate("user:123", staleValue);
```

两个操作均为委托模式：调用方负责在 CAS 成功后重新读取或写入。无 L2 锁——守卫条件是调用时刻 L1 缓存 entry 的当前值。条件匹配且操作应用时返回 `true`，否则返回 `false`。

**Worker 模式**

Worker 模式通过专用节点提供集群维度热点检测。App 实例定期报告访问计数，Worker 运行滑动窗口+状态机管道，将 HOT/COOL 决策广播回所有实例。状态机参数（`confirmCount`、`coolCount`、`preCoolGraceCount`）可通过 `/actuator/hotkey/worker/state` 运行时调整。

| 模式        | `worker.enabled` | 激活的 Bean                                                          |
| ----------- | ---------------- | -------------------------------------------------------------------- |
| App-only    | `false`（默认）  | `HotKeyCache`、TopK、reporter、actuator、sync                        |
| Worker-only | `true`           | 仅 Worker（无缓存——`get()`/`putThrough()` 抛出 `ZetaModeException`） |

**Worker 集群健康：** 设置 `zeta.local.expected-worker-count` 为生产环境期望的 Worker 数量。当设置 >0 时，`ClusterHealthView` 使用多数仲裁（`> expectedWorkerCount / 2`）作为健康 Worker 数量的阈值；当为 0（默认）时，集群在收到至少一个心跳之前始终被视为不健康。这实现了对部分 Worker 故障的精确检测和优雅降级决策。

**Worker TopK 持久化（热启动）：** 当 `zeta.worker.persistence.enabled=true`，Worker 定期快照 TopK 列表到 Redis。重启时 `TopKPersistService` 加载上次快照并回放到 HeavyKeeper sketch，预热从数小时缩至数秒。

**Spring Cache 集成**

启用 `zeta.spring-cache.enabled=true`。标准 `@Cacheable` / `@CachePut` / `@CacheEvict` 自动通过 Zeta 的热点检测、软过期和跨实例同步路由。

| 注解              | 在 `@Cacheable` 上的作用                                                                                             |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| `@HotKeyCacheTTL` | 覆盖硬/软 TTL                                                                                                        |
| `@HotKeyPreload`  | 预膨胀 HeavyKeeper 计数，使已知热点 key 立即生效                                                                     |
| `@Intercept`      | 通过触发模式（`IS_LOCAL_HOT`/`FORCE`/`QPS`）跳过方法体；按 `@Intercept.fallback()`、`@Fallback`、`peek()` 优先级降级 |
| `@Fallback`       | 被黑名单阻止、拦截或异常时提供回退值                                                                                 |
| `@NullCaching`    | 选择缓存 null 返回值（默认 `true`）                                                                                  |
| `@Broadcast`      | 禁止跨实例同步消息                                                                                                   |

```java
@Cacheable(cacheNames = "users", key = "#id")
@HotKeyCacheTTL(softTtlMs = 1000)
@Intercept @Fallback
public User getUser(Long id) { ... }

// qps 限流拦截
@Cacheable(cacheNames = "products", key = "#id")
@Intercept(trigger = InterceptTrigger.QPS, QPS = 500, fallback = "'throttled'")
@Fallback
public Product getProduct(String id) { ... }

// 热点预加载
@Cacheable(cacheNames = "flash", key = "#id")
@HotKeyPreload(keys = {"item-001", "item-002"})
@Intercept
public String getFlashItem(String id) { ... }
```

需 classpath 中包含 `spring-boot-starter-cache` 和 `spring-boot-starter-aop`。

## 缓存同步

启用 `zeta.sync.enabled=true`。

## 规则系统

启用 `zeta.sync.enabled=true` 以启用跨实例规则同步。规则系统提供两种操作：

| 操作              | 对匹配 key 的效果                                                                |
| ----------------- | -------------------------------------------------------------------------------- |
| `BLOCK`           | `get()` / `getWithSoftExpire()` 抛出 `ZetaBlockedException`；`putThrough()` 跳过 |
| `ALLOW_NO_REPORT` | 正常处理但跳过 Worker 上报（减少频繁访问 key 的噪音）                            |

### 模式类型

`RuleMatcher.of(pattern, action)` 自动检测模式：

| 模式                | 类型       | 匹配                       |
| ------------------- | ---------- | -------------------------- |
| `"user:123"`        | `EXACT`    | 精确 key                   |
| `"temp:*"`          | `PREFIX`   | 以 `temp:` 开头的 key      |
| `"order:*-detail"`  | `WILDCARD` | Glob 风格（`*` / `?`）匹配 |
| `"regex:user:\\d+"` | `REGEX`    | Java 正则                  |

### 持久化与广播

- **有 Redis：** 每次 `addRule()`/`removeRule()`/`clearRules()` 将规则列表序列化到 `ZetaConstants.Redis.KEY_RULES`（`"zeta:rules"`）。启动时 `RuleMatcher.initRules()` 从 Redis 加载。变更也通过 `TYPE_RULES_SYNC` 广播——对端通过 `RuleMatcher.syncRules()` 原子替换，不触发二次广播（避免风暴）。
- **无 Redis：** 相同操作通过 `CacheSyncPublisher` fanout 交换机广播到所有对端。每个对端在内存中持有完整规则集。
- **手动广播：** `zeta.broadcastAllLocalRulesManually()` 从 Redis（如可用）加载并重新广播当前规则集到所有对端。

## 监控

Zeta 提供两种互补的监控机制

完整响应格式和字段说明见 [MONITOR.zh.md](docs/MONITOR.zh.md)（[英文版](docs/MONITOR.md)）。

## 设计细节

领域术语定义见 [CONTEXT.md](CONTEXT.md)。
架构决策记录（ADR）维护在 [docs/adr/](docs/adr/0001-local-promotion-worker-fallback.md)。

## License

Apache License 2.0
