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

Zeta 是一款可配置、高性能、低成本的轻量级分布式缓存与预热框架, 致力于以极低的成本解决集群维度对任意突发性的、无法预先感知的热点数据分布式一致性缓存问题,通过 Redis 和 RabbitMQ 将业务代码与整个分布式协调基础设施完全解耦。

### 本地+分布式协作检测

Zeta 提供双级热键检测——本地进程内 HeavyKeeper 概率草图与远程 Worker 集群——并基于检测结果自动预热 L1 缓存。

- 每个应用实例运行一个本地 TopK 草图，跟踪高频访问的键。当键进入本地 TopK 集合时，其 L1 Caffeine 缓存的 TTL 会自动延长——无需等待 Worker 响应。L1 未命中时由 SingleFlight 机制合并同 key 并发请求，避免缓存击穿。同时支持软过期——在硬 TTL 到达之前，软 TTL 过期的条目可返回陈值并触发后台异步刷新，保障响应速度。

- Worker 集群聚合所有应用实例的访问报告，运行**双路径评估管线**：
  - **快车道（FastLane）**：对匹配用户配置的 glob 规则的 key（如 `product:*`），滑动窗口求和直接与规则阈值比较。达标即提升为 `CONFIRMED_HOT`——无贝叶斯置信度门控、无确认窗口。默认参数设定下,全链路端到端延迟：**~60ms（P99）**。适用于秒杀商品 ID、突发新闻 slug 等可容忍误报的场景。
  - **贝叶斯路径**：对所有其他 key，滑动窗口频率分析结合贝叶斯置信度状态机（Normal-Normal 共轭后验）产生 HOT/COOL 决策。该路径以 ~5s 延迟换取高置信度、低误报率的决策。

- 上报路径内置 BBR 拥塞控制算法，根据 CPU 负载自动降速，防止突发流量打垮通道。

- 每个缓存条目（CacheEntry）携带 dataVersion 与 decisionVersion 两个正交版本号，KeyState 标记其生命周期状态（HOT/COOL/NORMAL）。 Worker 决策通过单调递增的 decisionVersion 覆盖本地提升结果，确保集群一致性。

### 多节点缓存一致性

与主从数据库通过日志复制协议同步写入类似，Zeta 通过基于 RabbitMQ 的发布-订阅机制在各应用实例间同步缓存变更。

任一实例执行写入穿透（putThrough）或失效操作时，递增每键的 dataVersion（通过 Redis INCR，本地降级模式使用回退版本）并将事件广播到所有对等实例。每个对等实例比对传入的 dataVersion 与本地版本——过时消息静默丢弃。Worker 全部宕机时本地 TopK 接管完整仲裁权，上报静默丢弃；Worker 恢复后通过更高版本号自动收回控制权，无需人工干预。

版本比对机制确保最终一致性，无需 Paxos 或 Raft 等分布式共识协议的开销。

基准测试：

- `peek` **约 16M ops/s**（纯 Caffeine 查询，无副作用）
- `get`（L1 命中）**约 15M ops/s**（全路径含 TopK + Reporter）

HotKey受京东[hotkey](https://gitee.com/jd-platform-opensource/hotkey)项目启发,算法支持来自[Aegis](https://github.com/go-kratos/aegis)、[neural](https://github.com/yu120/neural/tree/master)

## 快速开始

### 1. 添加依赖

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

完整属性参考见 [CONFIG.zh.md](docs/CONFIG.zh.md)。

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
zeta.write("user:42").putBeforeInvalidate(dbMutation);
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
> **缓存雪崩防护：** `ExpireManager` 通过 `DelayUtil.computeTtlJitter()` 对每个过期时间戳施加均匀随机偏移（默认 ±5%）。5 分钟硬 TTL 在默认偏移下实际到期 4.75 ~ 5.25 分钟。通过 `zeta.local.ttl-jitter-ratio`（比例，默认 `0.05` = ±5%,`0`为禁用）控制。

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

## Worker 模式

Worker 模式通过专用节点提供集群维度热点检测。App 实例定期报告访问计数，Worker 运行滑动窗口+状态机管道，将 HOT/COOL 决策广播回所有实例。状态机参数（`confirmCount`、`coolCount`、`preCoolGraceCount`）可通过 `/actuator/hotkey/worker/state` 运行时调整。

| 模式        | `worker.enabled` | 激活的 Bean                                                          |
| ----------- | ---------------- | -------------------------------------------------------------------- |
| App-only    | `false`（默认）  | `HotKeyCache`、TopK、reporter、actuator、sync                        |
| Worker-only | `true`           | 仅 Worker（无缓存——`get()`/`putThrough()` 抛出 `ZetaModeException`） |

**Worker 集群健康：** 设置 `zeta.local.expected-worker-count` 为生产环境期望的 Worker 数量。当设置 >0 时，`ClusterHealthView` 使用多数仲裁（`> expectedWorkerCount / 2`）作为健康 Worker 数量的阈值；当为 0（默认）时，集群在收到至少一个心跳之前始终被视为不健康。这实现了对部分 Worker 故障的精确检测和优雅降级决策。

**Worker TopK 持久化（热启动）：** 当 `zeta.worker.persistence.enabled=true`，Worker 定期快照 TopK 列表到 Redis。重启时 `TopKPersistService` 加载上次快照并回放到 HeavyKeeper sketch，预热从数小时缩至数秒。

**快车道（FastLane，立即提升旁路）：** FastLane 是一条绕过贝叶斯置信度门控的评估路径。匹配用户配置的 glob 规则的 key（如 `product:*`），只要滑动窗口计数达到规则阈值，立即提升为 `CONFIRMED_HOT`——无需确认窗口、无需置信度评分、无需连续计数累积。全链路端到端延迟：**~60ms（P99）**。

启动时通过配置文件设置 FastLane 规则：

```yaml
zeta:
  worker:
    fast-lane:
      rules:
        - key-pattern: "product:*"      # glob 模式
          threshold: 500                 # 滑动窗口计数阈值
        - key-pattern: "flashsale:*"
          threshold: 1000
        - key-pattern: "news:breaking:*"
          threshold: 200
```

也可通过 Actuator REST API 在运行时管理规则（无需重启）：

| 方法     | 路径                                              | 操作             |
| -------- | ------------------------------------------------- | ---------------- |
| `GET`    | `/actuator/hotkey/fastlane`                       | 列出所有规则     |
| `POST`   | `/actuator/hotkey/fastlane`                       | 添加规则         |
| `PUT`    | `/actuator/hotkey/fastlane`                       | 更新规则阈值     |
| `DELETE` | `/actuator/hotkey/fastlane/{pattern}`             | 删除规则         |

示例：`curl -X POST -H 'Content-Type: application/json' -d '{"keyPattern":"promo:*","threshold":300}' http://worker:8080/actuator/hotkey/fastlane`

**FastLane 状态转换：** 匹配的 key 从任何当前状态（COLD、CANDIDATE_HOT、PRE_COOLING）直接跃迁到 `CONFIRMED_HOT`。`hotStreak` 立即设为所需的确认计数，`coolStreak` 重置为 0，并广播 HOT 决策——即使从 PRE_COOLING 状态（正常路径下静默恢复会抑制广播）。已经是 CONFIRMED_HOT 的 key 仅刷新时间戳。

**适用场景：** 秒杀商品 ID、突发新闻 slug、促销活动 key——任何可容忍一定误报、需要亚秒级检测延迟的场景。对于需要高置信度、低误报率的 key，应使用默认的贝叶斯路径。

### 说明

**`hot-threshold: -1` 在学习期内禁用热点检测**

当 `zeta.worker.threshold.hot-threshold` 设为 `-1`（比例模式）时，Worker 将滑动窗口阈值初始化为 `Long.MAX_VALUE`——实际上在 30 秒学习期（`learning-period-ms`，默认 30s）内没有任何 key 能被分类为 HOT。学习期结束后，`ThresholdLearner` 计算 `threshold = globalQps × hotThresholdRatio`，恢复正常运作。

**缓解措施：** 配合设置一个合理的绝对阈值：

```yaml
zeta:
  worker:
    threshold:
      hot-threshold: 1000 # 学习期内的回退阈值
    global-qps-dynamic-threshold:
      learning-period-ms: 5000 # 缩短学习期加速收敛
```

或在流量模式已知的情况下直接禁用学习期（`learning-period-ms: 0`）。

**Worker 广播 Exchange 声明**

Worker 模块（`zeta-worker`）已将广播 exchange（默认 `zeta.send.exchange`）声明为 Spring `FanoutExchange` bean。如果通过非 `WorkerAutoConfiguration` 的方式独立部署 Worker，需要确保该 exchange 在 RabbitMQ 中存在——否则 Worker 的 HOT/COOL 广播会因 channel 级 `not_found` 异常而失败。

**PING/PONG 验证是辅助手段**

`WorkerHeartbeatVerifier` 定期向**非存活**状态（心跳超时超过 `heartbeat.timeout-ms`，默认 10s）的 Worker 发送 PING。启动时的瞬态失败是预期的且安全的——首次 PING 可能在 Worker 的 verify 队列就绪前发出。主心跳路径（Worker → `zeta.heartbeat.exchange` → App 心跳队列）是 HealthView 更新和 RingManager 路由的权威机制。

## Spring Cache 集成

启用 `zeta.spring-cache.enabled=true`。标准 `@Cacheable` / `@CachePut` / `@CacheEvict` 自动通过 Zeta 的热点检测、软过期和跨实例同步路由。

**需要 `@EnableCaching`：** `CacheExtensionAspect` 包装在 Spring 的 `CacheInterceptor` 外层——没有 `@EnableCaching`，该拦截器不会注册，`@Cacheable` 方法会执行但结果不会被缓存。

```java
@SpringBootApplication
@EnableCaching
@EnableScheduling
public class MyApplication { ... }
```

**扩展注解**（由 `CacheExtensionAspect` 以 `HIGHEST_PRECEDENCE` 优先级处理）：

| 注解              | 目标 | 在 `@Cacheable` 上的作用                                                                                                                       |
| ----------------- | ---- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `@CacheTTL`       | M/T  | 覆盖硬/软 TTL。支持静态值和 SpEL（`hardTtlSpEl`、`softTtlSpEl`）                                                                               |
| `@Intercept`      | M    | 通过触发模式（`IS_LOCAL_HOT`/`FORCE`/`QPS`/`CONCURRENT_THREADS`）跳过方法体；fallback 优先级：`@Intercept.fallback()` → `@Fallback` → `peek()` |
| `@Fallback`       | M    | 回退值（SpEL）或命名约定方法（`{methodName}Fallback`），被拦截/异常时调用                                                                      |
| `@NullCaching`    | M    | 允许缓存 null 返回值（基于 sentinel，默认 `true`）                                                                                             |
| `@SkipBroadcast`  | M    | 禁止跨实例 AMQP 同步消息（仅本地写）                                                                                                           |
| `@SkipDetection`  | M    | 跳过 TopK 检测 + Worker 上报                                                                                                                   |
| `@Preload`        | M    | 预膨胀 HeavyKeeper 计数（静态 `keys[]` 或动态 `keyExpr` SpEL）                                                                                 |
| `@CacheCondition` | M    | SpEL `unless` — 条件满足时不缓存结果（使用 `#result` + 方法参数）                                                                              |

```java
@Cacheable(cacheNames = "users", key = "#id")
@CacheTTL(hardTtlMs = 60000, softTtlMs = 10000)
@Intercept @Fallback
public User getUser(Long id) { ... }

// 动态 TTL
@Cacheable(cacheNames = "users", key = "#id")
@CacheTTL(hardTtlSpEl = "#id.startsWith('vip') ? 600000 : 60000")
@Intercept
public User getUserVip(Long id) { ... }

// QPS 限流拦截
@Cacheable(cacheNames = "products", key = "#id")
@Intercept(type = InterceptType.QPS, qps = 500, fallback = "'throttled'")
@Fallback
public Product getProduct(String id) { ... }

// 并发线程数限流
@Cacheable(cacheNames = "orders", key = "#id")
@Intercept(type = InterceptType.CONCURRENT_THREADS, concurrentThreads = 10, fallback = "'busy'")
@Fallback
public Order getOrder(Long id) { ... }

// 热点预加载
@Cacheable(cacheNames = "flash", key = "#id")
@Preload(keys = {"item-001", "item-002"})
@Intercept
public String getFlashItem(String id) { ... }

// 条件跳过缓存
@Cacheable(cacheNames = "products", key = "#id")
@CacheCondition(unless = "#result == null || #result.disable()")
public Product getProduct(String id) { ... }

// 跳过检测（静态配置）
@Cacheable(cacheNames = "config", key = "#key")
@SkipDetection
public String getConfig(String key) { ... }

// 仅本地写，不广播
@CachePut(cacheNames = "local", key = "#id")
@SkipBroadcast
public String updateLocal(String id, String val) { ... }
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
