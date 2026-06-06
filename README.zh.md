# HotKey

<p align="center">
  <img src="img/HotKey.png" alt="HotKey" width="300">
</p>

<p align="center">
  <a href="https://jitpack.io/#Hyshmily/HotKey"><img src="https://jitpack.io/v/Hyshmily/HotKey.svg" alt="JitPack"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-25-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
</p>

[**English**](README.md)

### 写在最前面:为什么我(以下称作者)会创建HotKey？

在实际开发中，作者曾频繁面对大量缓存 Key 的处理问题。Caffeine、Redis、数据库多级缓存的手动维护，逻辑过期的设定，热点 Key 的提前统计与预热——每一个环节都极其繁琐。甚至在分布式集群环境下，如何让热点 Key 在各个节点间正确共享、避免高并发下的缓存穿透，成为了分布式系统老生常谈的问题。

我最初的想法很简单：找一个开箱即用的解决方案，或者至少封装一个顺手的多级缓存工具类。

遗憾的是，现有的方案要么过于复杂、部署运维成本极高，要么已经停止维护（如京东 [hotkey](https://gitee.com/jd-platform-opensource/hotkey) 项目，截止 2026 年 5 月早已已不再更新），难以满足轻量、易用的实际需求。尽管作者许多想法与其不谋而合，但其庞大复杂的架构让我决心另辟蹊径。

这就是 HotKey 诞生的背景:一个致力于轻量、便携、开箱即用的分布式热点 Key 缓存框架。

过去、现在、未来，它都将保持开源。

> [!TIP]
> **开始之前，有疑问？** 参见 [FAQ.zh.md](docs/FAQ.zh.md) —— 作者将关于本地 vs 中央检测、Worker 延迟、MQ 吞吐量等常见问题的解答。

### 简介

HotKey 是一个[高性能](docs/HotKey_Benchmark_Report.zh.md)、低成本、轻量级的分布式多级缓存框架。

大多数本地缓存将所有条目一律存入 Caffeine。但在海量 key 下：

- **内存浪费** — 大多数 key 只读一次
- **广播风暴** — 全量缓存 = 全量失效，广播随 key 数线性增长

而HotKey 的策略：**仅缓存真正的热点数据。**

通过 [HeavyKeeper](https://github.com/go-kratos/aegis)（Count-Min Sketch 变体）概率检测频率。**读路径**：所有加载的 key 进 L1，但**差异化 TTL**——热点 key 缓存更久（1h），普通 key 更快过期（5min）。**写路径**：`putThrough` 无论是否热点，直接写 L1 并广播——调用方拥有写入权。

**集群维度热点检测**：部署 Worker 节点聚合所有实例的访问报告——解决"被同一 pod 访问 100 次"与"被 100 个 pod 各访问 1 次"在本地无法区分的问题。

### 适用场景

| 适合                                       | 不适合                                |
| ------------------------------------------ | ------------------------------------- |
| 读密集型场景（String / List / Set / ZSet） | 写密集型 / 原子操作（秒杀、Lua）      |
| 大量 key，访问呈帕累托分布                 | key 数少（< 200），手动 Caffeine 即可 |
| 读多写少，最终一致即可                     | 要求写后立即可见                      |
| Spring Boot 3.x + Java 17+                 | 非 Spring Boot 项目                   |
| 可选 Redis + 可选 RabbitMQ（多实例）       |                                       |
| 可选 Worker 节点实现集群维度热点检测       |                                       |

## 特点

- **HeavyKeeper 算法** — Count-Min Sketch + 指数冲突衰减的概率性 Top-K 检测
- **多级缓存** — Caffeine (L1) → 可选的 reader 回调（L2，如 Redis）+ 调用方通过 `Optional.orElseGet()` 实现的 DB 回退，自动热点提升
- **差异化 TTL** — 热点 key 与普通 key 使用独立的硬/软 TTL；热点 key 缓存更久（1h/5min），普通 key 更快过期（5min/30s）
- **请求合并** — L1 未命中时同 key 并发请求共享同一 L2 读，通过专门的 `SingleFlight` bean

  > **注意：** 确保 `hotkey.local.inflight-ttl-seconds` 大于最慢的 L2 响应耗时，否则缓存条目可能在 Future 完成前过期，导致重复的 L2 读。
  > 同时确保 `hotkey.local.inflight-timeout-seconds` < `hotkey.local.inflight-ttl-seconds`。超时后 `SingleFlight.load()` 返回 `Optional.empty()`，调用方需通过 DB 回退处理。

- **软失效（逻辑过期）** — 立即返回过期旧值，同时后台异步刷新；降低 p99 延迟，代价是短暂脏读。**完全替代传统 Redis 侧逻辑过期**（`RedisData{data, expireTime}` 包装模式）——Redis 纯值存储，由 HotKey 在 L1 Caffeine 层管理过期
- **Redis 集合类型** — 通过 `putBeforeInvalidate` 支持 List/Set/ZSet 增量写入，无需 `putThrough`
- **热点同步** — 可选 RabbitMQ fanout（通过 `hotkey.sync.*`）跨实例同步缓存失效；独立的 worker-listener（通过 `hotkey.worker-listener.*`）接收 Worker 发出的 HOT/COOL 决策
- **Worker 模式** — 专用集群维度热点检测节点；基于滑动窗口 + 状态机管道实现跨实例共识；详见 [WORKER.zh.md](docs/WORKER.zh.md)
- **报告聚合** — 每次 `get()` / `getWithSoftExpire()` 调用上报到本地 `HotKeyReporter`，再由 Reporter 周期性地将访问计数批量发送到 Worker 节点（RabbitMQ），用于集群维度热点检测
- **可配置线程池** — 专用 `TaskExecutor`，有界队列
- **Spring Boot 自动配置** — 引入依赖即用，零样板代码

## 延迟与性能

<details>
<summary><b>点击展开——完整全链路延迟分解表及极限调优说明</b></summary>

详见[基准测试报告](docs/HotKey_Benchmark_Report.zh.md)。每跳延迟（Redis + RabbitMQ 容器，10 阶段，4.5 万 ops，0 错误）：

| 路径                                | 说明                                                | P50           | P95           | P99           |
| ----------------------------------- | --------------------------------------------------- | ------------- | ------------- | ------------- |
| L1 命中（Caffeine 查找）            | 纯内存查找，无网络 I/O                              | **0.001 ms**  | 0.004 ms      | 0.018 ms      |
| L1 未命中 → Redis → L1              | Redis GET RTT + SingleFlight 去重 + L1 回填         | 0.51 ms       | 0.94 ms       | 2.26 ms       |
| AMQP 发布                           | RabbitMQ 通道写入（内存到内存）                     | 0.02 ms       | 0.11 ms       | 0.27 ms       |
| AMQP 端到端投递                     | 发布 + 代理路由 + 消费者投递                        | 0.07 ms       | 0.27 ms       | 0.48 ms       |
| Redis GET 往返                      | 纯网络往返                                          | 0.62 ms       | 1.60 ms       | 4.01 ms       |
| Worker 决策（不开状态机）           | 滑动窗口检测 → 直接广播 HOT，含 jitter+AMQP+L1 轮询 | **51.64 ms**  | 97.75 ms      | 104.16 ms     |
| 状态机流水线（开状态机，20 确认窗） | 20 确认窗(2s) + AMQP 决策广播 + L1 提升             | **1,983 ms**  | 2,015 ms      | 2,015 ms      |
| **全链路（不开状态机）**            | **分步构成见下方**                                  | **155.81 ms** | **202.14 ms** | **212.69 ms** |
| ┣ L1 未命中 → Redis → L1            | 与 Phase 6 同路径                                   | 0.51 ms       | —             | —             |
| ┣ 上报批处理等待                    | `report-interval-ms=100ms` 半区间平均               | ~50 ms        | —             | —             |
| ┣ AMQP 上报投递（App→Worker）       | flush() → Report Exchange → Worker 消费             | ~1 ms         | —             | —             |
| ┣ Worker 滑动窗口 + TopK            | 内存操作                                            | <1 ms         | —             | —             |
| ┣ AMQP 决策广播（Worker→App）       | WorkerListener 收到决定                             | ~1 ms         | —             | —             |
| ┗ 余量（调度 + 轮询 + 噪音）        | 两段 AMQP 调度 + isLocalHotKey() 轮询 + 方差        | ~52 ms        | —             | —             |
| **全链路 + 状态机（20 确认窗）**    | 全链路 + 2s 确认窗口，10/10 键提升                  | **2,038 ms**  | **2,055 ms**  | **2,055 ms**  |

> [!WARNING]
> **极限参数调整**
>
> 将以下参数设为极限值可将全链路延迟压缩至 **~8ms**（P50，配合 `sliceMs` 同步缩小时），但作者**不推荐**生产环境使用。HotKey 的默认参数设计偏向保守，应该优先保证分布式集群的可靠性而非极致延迟。
>
> | 参数                                              | 极限值      | 效果                                                  | 限制                                       |
> | ------------------------------------------------- | ----------- | ----------------------------------------------------- | ------------------------------------------ |
> | `hotkey.local.report-interval-ms`                 | 0 → 最小 1  | 近似禁用上报批处理，`record()` 后几乎即时 flush       | `ScheduledExecutorService` 要求 period > 0 |
> | `hotkey.worker.warmup-jitter-ms`                  | 0           | 禁用预热抖动（羊群效应防护失效），Worker 决策即时执行 | —                                          |
> | `hotkey.worker.state-machine.confirm-duration-ms` | 0           | 禁用状态机确认窗口，首个热窗口即广播 HOT              | —                                          |
> | `hotkey.worker.sliding-window.duration-ms` / `slices` | 1000/10 → 100/100 | 缩小时间片，缩短 tick 等待（avg~50ms→~5ms）    | 窗口统计精度显著降低                       |
>
> **极端场景理论延迟：**
>
> | 场景                 | 默认 P50  | 极限 P50 | 节省      |
> | -------------------- | --------- | -------- | --------- |
> | 全链路（不开状态机） | 155.81 ms | ~8 ms    | ~148 ms   |
> | 全链路 + 状态机      | 2,038 ms  | ~8 ms    | ~2,030 ms |
>
> **代价：**
>
> - 上报 AMQP 消息量从 ~10 批/秒飙升到 ~1000 批/秒（`report-interval-ms=1`），RabbitMQ 压力剧增
> - 禁用预热抖动后，毫秒级流量毛刺即可触发 HOT 广播，误报率升高
> - 禁用确认窗口后，任何短暂突发都立即升级为全局热 Key，放大误判
> - 缩小滑动窗口时间片（`sliceMs`）会损失统计精度，短暂的突发更易触发误判

</details>

## 快速开始

### 1. 添加依赖（JitPack）

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>hotkey</artifactId>
    <version>1.1.3</version>
</dependency>
```

版本号使用最新发布版本。Redis 和 RabbitMQ 依赖均为可选——仅在使用对应功能时才需引入。

### 2. 配置

默认local本地配置（适用于大多数场景)

> [!IMPORTANT]
> 虽然hotkey本身并不严格外部依赖,但是在分布式部署情况下,除了RabbitMQ,作者强烈建议引入Redis以达到最佳效果,尽管作者在核心调用链条上做了很多降级处理。

**可选功能配置：**

| 功能            | 启用方式                                   | 说明                         |
| --------------- | ------------------------------------------ | ---------------------------- |
| Redis 二级缓存  | 添加 `RedisTemplate` Bean                  | 两级缓存，L2 兜底            |
| 跨实例同步      | `hotkey.sync.enabled=true`                 | 基于 RabbitMQ 的缓存失效     |
| Worker Listener | `hotkey.worker-listener.enabled=true`      | 接收 Worker 的 HOT/COOL 决策 |
| Worker 模式     | `hotkey.worker.enabled=true`               | 运行专用 Worker 节点         |
| `@HotKey` 注解  | `hotkey.annotation.enabled=true` + AspectJ | 声明式缓存                   |
| 访问上报        | `hotkey.report.enabled=true`（默认）       | 向 Worker 上报访问次数       |

所有选项见[配置](#配置)，完整属性参考见 [CONFIG.zh.md](docs/CONFIG.zh.md)。

<details>
<summary><b>快速部署 YAML 模板</b>（最小配置——仅需修改必要的配置项）</summary>

**本地 Local（App 侧）** — 引入 `hotkey` 依赖即可运行，可按需取消注释

```yaml
hotkey:
  # local 参数全部走默认值，无需显式配置

  # —— 可选功能，按需取消注释 ——

  # 跨实例缓存同步（需 spring-boot-starter-amqp + spring-boot-starter-data-redis）
  # sync:
  #   enabled: true

  # @HotKey 注解（需 spring-boot-starter-aop）
  # annotation:
  #   enabled: true

  # Worker 决策监听（需 spring-boot-starter-amqp + spring-boot-starter-data-redis）
  # worker-listener:
  #   enabled: true
  # sync:
  #   enabled: true     # worker-listener 依赖 hotKeyRedisLoader Bean
```

**单 Worker（独立部署节点）** — 添加 `spring-boot-starter-amqp`

```yaml
hotkey:
  worker:
    enabled: true
    routing:
      app-name: myapp # 【必须】与 App 侧 hotkey.local.app-name 一致
      shard-count: 1 # 【必须】与 App 侧 hotkey.local.shard-count 一致
      shard-index: 0 # 【必须】本实例归属分片，范围 [0, shard-count-1]


# 多 Worker 示例：3 台机器各占一个分片
# 每台 app-name / shard-count 相同，shard-index 不同
#
# 机器 A:  shard-index: 0
# 机器 B:  shard-index: 1
# 机器 C:  shard-index: 2
# shard-count: 3（三台一致）
```

**全部参数（覆盖默认值）**

```yaml
hotkey:
  local:
    # ——— 跨节点强制一致（App 侧与 Worker 侧必须对齐） ———
    app-name: "default"                     # 必须与 worker.routing.app-name 一致
    shard-count: 1                          # 必须与 worker.routing.shard-count 一致

    # ——— 实例标识 ———
    instance-id: ""                         # 显式实例 ID（空=自动生成）

    # ——— 报告上报 ———
    report-exchange: "hotkey.report.exchange"
    report-interval-ms: 100                 # 批量发送间隔 (ms)
    queue-capacity: 10000                   # 上报队列容量
    queue-offer-timeout-ms: 100             # 队列写入超时 (ms)
    consumer-count: 0                       # 消费者线程数 (0=auto)

    # ——— HeavyKeeper 算法 ———
    topK: 100                               # 保留的热 key 数
    width: 50000                            # Count-Min Sketch 宽度
    depth: 5                                # Count-Min Sketch 深度
    decay: 0.92                             # 衰减因子（每次 fading 乘以该值）
    minCount: 10                            # 热 key 最小访问次数
    expelled-queue-capacity: 50000          # 被驱逐热 key 暂存队列容量

    # ——— L1 Caffeine 缓存 ———
    local-cache-max-size: 1000              # 最大条目数
    local-cache-ttl-minutes: 5              # 缓存过期时间（分钟）
    local-cache-access-ttl-minutes: 0       # 访问后过期时间（0=不过期）

    # ——— SingleFlight 去重 ———
    inflight-max-size: 50000                # 去重记录最大 key 数
    inflight-ttl-seconds: 5                 # 去重记录 TTL（必须 > L2 响应耗时）
    inflight-timeout-seconds: 3             # 等待异步结果超时（必须 < inflight-ttl-seconds）

    # ——— 异步执行器 ———
    executor-core-pool-size: 8              # 核心线程数
    executor-max-pool-size: 32              # 最大线程数
    executor-queue-capacity: 2000           # 工作队列容量

    # ——— 刷新 & 版本控制 ———
    refresh-max-pools: 100                  # 刷新线程池上限
    version-key-ttl-minutes: 60             # Redis 版本 key TTL

    # ——— 普通 key TTL ———
    default-hard-ttl-ms: 300000             # 默认硬过期 (5min)
    hard-ttl-ms: 0                          # 覆盖值 (0=使用 default)
    default-soft-ttl-ms: 30000              # 默认软过期 (30s)
    soft-ttl-ms: 0                          # 覆盖值 (0=使用 default)

    # ——— 热 key TTL（Worker 标记 HOT 后生效） ———
    default-hot-hard-ttl-ms: 3600000        # 默认热 key 硬过期 (1h)
    hot-hard-ttl-ms: 0                      # 覆盖值 (0=使用 default)
    default-hot-soft-ttl-ms: 300000         # 默认热 key 软过期 (5min)
    hot-soft-ttl-ms: 0                      # 覆盖值 (0=使用 default)

  # 特性开关
  report:
    enabled: true                           # App→Worker 报告上报
  sync:
    enabled: false                          # 跨实例缓存同步（需 Redis + RabbitMQ）
  annotation:
    enabled: false                          # @HotKey 注解支持（需 spring-boot-starter-aop）
  scheduling:
    enabled: true                           # 定时衰减 & 驱逐清理
  decay-period: 20                          # HeavyKeeper fading 间隔（秒），scheduling 启用时生效

  # App 侧 — Worker 决策监听（接收 HOT / COOL）
  worker-listener:
    enabled: false                          # 需 Redis + RabbitMQ
    exchange-name: "hotkey.broadcast.exchange"
    queue-prefix: "hotkey.worker"
    auto-startup: true                      # 随应用启动
    warmup-jitter-ms: 100                   # 处理前随机延迟（防惊群）
    concurrent-consumers: 2                 # 并发消费者数
    scheduler-pool-size: 2                  # 延迟任务线程池大小

  # App 侧 — 跨实例缓存同步
  sync:
    enabled: false
    exchange-name: "hotkey.sync.exchange"
    queue-prefix: "hotkey.sync"
    auto-startup: true
    dedup-window-seconds: 10                # 消息去重窗口
    dedup-max-size: 10000                   # 去重缓存上限
    warmup-jitter-ms: 100                   # 处理前随机延迟（防惊群）
    concurrent-consumers: 3                 # 并发消费者数
    scheduler-pool-size: 4                  # 延迟任务线程池大小

  # Worker 侧 — 独立部署节点
  worker:
    enabled: false

    routing:
      app-name: "default"                   # 必须与 local.app-name 一致
      shard-count: 1                        # 必须与 local.shard-count 一致
      shard-index: 0                        # 本实例分片号 [0, shard-count-1]

    messaging:
      report-exchange: "hotkey.report.exchange"
      broadcast-exchange: "hotkey.broadcast.exchange"

    sliding-window:
      duration-ms: 1000                     # 窗口时长 (ms)
      slices: 10                            # 窗口切片数（必须整除 duration-ms）

    threshold:
      hot-threshold: 1000                   # 绝对 QPS 阈值（≤0 = 使用比例阈值）
      hot-threshold-ratio: 0.01             # 相对 QPS 比例阈值 (1%)

    state-machine:
      confirm-duration-ms: 2000             # 确认窗口 (ms)，持续热超过此时长判定为 HOT
      cool-duration-ms: 15000               # 冷却窗口 (ms)，持续冷超过此时长判定为 COOL
      pre-cool-grace-ms: 5000               # 预冷却宽限期 (ms)

    global-qps-dynamic-threshold:
      qps-change-tolerance: 0.5             # QPS 变化容忍倍数
      learning-period-ms: 30000             # 学习周期 (ms)
      hot-threshold-ratio: 0.01             # 动态阈值比例
      recalculate-interval-ms: 60000        # 重算间隔 (ms)

    topk-validation:
      validate-interval-ms: 60000           # 验证周期 (ms)
      pre-warm-count: 5                     # 预温次数
      pre-warm-min-appearances: 2           # 最小出现次数

    heavy-keeper:
      top-k: 100                            # 保留热 key 数
      width: 20000                          # Sketch 宽度
      depth: 10                             # Sketch 深度
      decay: 0.9                            # 衰减因子
      min-count: 10                         # 最小计数

    heartbeat:
      ping-interval-ms: 1000                # 心跳广播间隔 (ms)
```

</details>

### 3. 使用

**A. 纯本地缓存（无二级缓存）**

```java
@Autowired
private HotKey hotKey;

Optional<String> r = hotKey.peek("user:123"); // 仅查 L1，不做热点追踪
```

等价于 `peek(cacheKey)`，L1 未命中返回 `Optional.empty()`，完全跳过二级存储。

**B. 两级缓存（Redis 或任意后端）**

```java
@Autowired
private HotKey hotKey;
@Autowired
private StringRedisTemplate redisTemplate;

Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

hotKey.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));
```

**C. 数据库兜底**

```java
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123);                         // DB 回退
    redisTemplate.opsForValue().set("user:123", value);
}
// 注意:如果reader返回 null，HotKey 视为未命中（Optional.empty()），调用方需自行处理 null 值（如使用 sentinel 对象包装）。
// 注入 hotKey 实例 (io.github.hyshmily.hotkey.HotKey)
@Autowired
private HotKey hotKey;

// ... 在业务方法中

public String getUserName(String userId) {
  String cacheKey = "user:" + userId;

  // 使用 hotKey.get() 方法，把完整的回源逻辑封装在 reader 里
  Optional<Object> result = hotKey.get(cacheKey, () -> {
    // 1. 先查询 Redis
    String value = redisTemplate.opsForValue().get(cacheKey);
    if (value != null) {
      return value; // Redis 命中，直接返回
    }

    // 2. Redis 未命中，查询数据库
    User user = userService.getById(userId);
    if (user != null) {
      // 3. 数据库查到，回写 Redis 并返回
      String userName = user.getName();
      redisTemplate.opsForValue().set(cacheKey, userName, Duration.ofMinutes(10));
      return userName;
    }

    // 4. 数据库也未查到，返回空值占位符，防止缓存穿透
    redisTemplate.opsForValue().set(cacheKey, "", Duration.ofMinutes(1)); // 缓存一个短期的空值
    return NULL_SENTINEL;
  });

  // 5. 处理查询结果
  return result
    .filter(val -> val != NULL_SENTINEL) // 过滤掉占位符
    .map(Object::toString)               // 转换为字符串
    .orElse(null);                       // 未命中或数据不存在时返回 null
}
```

**D. 封装 Helper 避免重复 lambda**

```java
@Component
public class RedisHotKeyHelper {

  @Autowired
  private HotKey hotKey;

  @Autowired
  private RedisTemplate<String, Object> redisTemplate;

  public <T> Optional<T> get(String key) {
    return hotKey.get(key, () -> redisTemplate.opsForValue().get(key));
  }

  public void set(String key, Object value) {
    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
  }
}
```

**E. 自定义二级缓存（非 Redis）**

```java
// 使用 MySQL、远程 API 或任意数据源作为 L2
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));

User user = r.orElseGet(() -> createDefaultUser());
```

**F. Redis 集合类型（List、Set、ZSet）**

`putThrough` 需要传入完整新值来更新 L1，但集合的增量操作（LPUSH、SADD、ZADD）只修改单个元素——调用方无法获知全量新值。使用 `putBeforeInvalidate` 在突变后失效 L1，下次 `get()` 自动回源 Redis，保证一致性。

```java
@Component
public class CollectionHotKeyCache {

  @Autowired
  private HotKey hotKey;

  @Autowired
  private RedisTemplate<String, Object> redisTemplate;

  public Boolean sIsMember(String key, Object member) {
    return hotKey.get(key + "::member::" + member, () -> redisTemplate.opsForSet().isMember(key, member));
  }

  @SuppressWarnings("unchecked")
  public Set<Object> sMembers(String key) {
    return hotKey.get(key, () -> redisTemplate.opsForSet().members(key));
  }

  public void sAdd(String key, Object... members) {
    hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));
  }

  public List<Object> lRange(String key, long start, long end) {
    String cacheKey = key + "::range::" + start + "::" + end;
    return hotKey.get(cacheKey, () -> redisTemplate.opsForList().range(key, start, end));
  }

  public Double zScore(String key, Object member) {
    return hotKey.get(key + "::score::" + member, () -> redisTemplate.opsForZSet().score(key, member));
  }
}
```

**G. 软失效——替代传统逻辑过期**

软失效立即返回过期旧值同时后台异步刷新（stale-while-revalidate）。与传统逻辑过期（在 Redis 值里嵌入 `expireTime`）不同，HotKey 完全在 L1 Caffeine 层管理过期——**Redis 存纯值，无需任何包装**。

| 维度       | 传统逻辑过期                                   | HotKey 软失效                                             |
| ---------- | ---------------------------------------------- | --------------------------------------------------------- |
| 过期存储   | 嵌入 Redis 值（`RedisData{data, expireTime}`） | L1 Caffeine 元数据（`softExpireAt`）                      |
| 返回旧值   | 解析包装后返回旧数据                           | 直接返回 L1 旧值                                          |
| 异步重建   | Redis 分布式锁 + 自定义线程池                  | Singleflight（本地）+ `hotKeyExecutor` + `refreshLimiter` |
| Redis 格式 | 包裹 JSON                                      | 纯值（无需包装）                                          |
| DB 回退    | 手动加锁逻辑                                   | 原生 `orElseGet` / `orElseThrow`                          |

```java
// 传统方式（不再需要）：
//   redisData.setExpireTime(LocalDateTime.now().plusSeconds(30L));
//   stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));

// HotKey：Redis 存纯值，软过期由 L1 管理
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// L1 命中但已软过期 → 返回旧值 + 触发异步刷新
// L1 未命中 → singleflight 回源（同 get()）

// 自定义 per-call softTtl（覆盖全局默认值）
Optional<String> r2 = hotKey.getWithSoftExpire("user:456", () -> redisTemplate.opsForValue().get("user:456"), 3000);
```

DB 回退（无需分布式锁）：

```java
String json = hotKey
  .getWithSoftExpire("shop:" + shopId, () -> redisTemplate.opsForValue().get("shop:" + shopId))
  .orElseGet(() -> {
    User u = userMapper.selectById(shopId);
    String s = JSONUtil.toJsonStr(u);
    if (u != null) {
      redisTemplate.opsForValue().set("shop:" + shopId, s);
    }
    return s;
  });

User user = JSONUtil.toBean(json, User.class);
```

> **注意：** 彻底逻辑过期（纯软过期，硬 TTL 永不淘汰）：向 `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)` 传入 `hardTtlMs = Long.MAX_VALUE`。entry 永久驻留 Caffeine——硬 TTL 永不会将其移除。`softExpireAt` 过期后，读取立即返回旧值并触发异步刷新（软过期**不会**淘汰 entry）。不传 `Long.MAX_VALUE` 时，默认硬 TTL 可能先将 entry 淘汰出 Caffeine（L1 未命中 → 更高延迟），而非走旧值 + 异步刷新路径。

见[配置](#配置)中的 TTL 属性选项。

**H. 自定义 per-entry 硬 TTL**

默认情况下，热点 key 和普通 key 使用不同 TTL。通过 `get(key, reader, hardTtlMs, softTtlMs)` 或 `putThrough(key, value, writer, hardTtlMs, softTtlMs)` 可为单个 entry 设置独立的硬和软 TTL。

```java
// 5 分钟硬 TTL + 30 秒软 TTL
Optional<Shop> shop = hotKey.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5),   // hardTtlMs
    TimeUnit.SECONDS.toMillis(30)); // softTtlMs

// 30 秒硬 TTL，不设软 TTL
hotKey.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30),  // hardTtlMs
    0);                              // softTtlMs（使用默认值）
```

> **per-call TTL 语义说明：**
>
> - per-call 的 `hardTtlMs`/`softTtlMs` 仅对本次调用生效。下次调用不传参数时，回退到 key 当前状态（热点或普通）对应的默认 TTL。
> - 传入 `0` 表示使用该 key 状态的配置默认值。
> - 传入 `Long.MAX_VALUE` 作为 `hardTtlMs` 可实现永久缓存——该条目永不会被 TTL 淘汰（仅受 Caffeine `maximumSize` 淘汰约束）。Caffeine 的 `Expiry` JavaDoc 官方明确支持此用法：_"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([源码](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))
> - 与 `getWithSoftExpire` 配合使用时，per-entry 硬 TTL 在后台刷新中会被保留。配合 `hardTtlMs = Long.MAX_VALUE`，即为彻底逻辑过期：仅 Caffeine `maximumSize` 可淘汰 entry——软过期永不移除 entry，仅返回旧值并异步刷新。

**I. Worker 模式**

如需跨多实例的集群维度热点检测，详见 [WORKER.zh.md](docs/WORKER.zh.md)。

**J. @HotKey 注解**

`@HotKey` 注解为方法返回值提供声明式缓存——基于 AOP 的替代方案，无需显式调用 `hotKey.get()` / `putBeforeInvalidate()` / `invalidate()`。

```java
@HotKey(key = "'user:' + #id", hardTtlMs = 5000)
public User getUser(Long id) { ... }
```

**注解属性：**

| 属性              | 类型            | 默认值  | 说明                                                                                                             |
| ----------------- | --------------- | ------- | ---------------------------------------------------------------------------------------------------------------- |
| `key`             | `String`        | 必填    | SpEL 表达式，方法参数通过 `#paramName` 引用。需要 `-parameters` 编译标志；无调试信息时回退到 `arg0, arg1, ...`。 |
| `operation`       | `OperationType` | `READ`  | `READ` / `WRITE` / `INVALIDATE`                                                                                  |
| `hardTtlMs`       | `long`          | `0`     | 硬 TTL 覆盖（毫秒）。`0` = 使用配置默认值。                                                                      |
| `softTtlMs`       | `long`          | `0`     | 软 TTL 覆盖（毫秒）。`0` = 使用配置默认值。                                                                      |
| `softExpire`      | `boolean`       | `true`  | 是否在 READ 时启用 stale-while-revalidate。`false` 时等同于 `get()`。                                            |
| `fallbackEnabled` | `boolean`       | `false` | 是否启用降级。触发条件：HeavyKeeper 判为热点 key 或 `RuntimeException`。                                         |
| `fallback`        | `String`        | `""`    | SpEL 降级表达式。为空 → `{methodName}Fallback` 命名约定（同一 Bean 上查找方法）。                                |

> **热点降级说明：** 当 `fallbackEnabled=true` 且 HeavyKeeper 检测到 key 为热点时，Aspect 在缓存路径前调用降级——跳过 supplier 直接返回。`RuntimeException` 同样触发降级。两种解析模式：SpEL（`fallback` 属性）优先，其次为命名约定 `{methodName}Fallback`。

**三种操作模式：**

| 模式           | 门面方法                         | 行为                                                                                                                                                               |
| -------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `READ`（默认） | `getWithSoftExpire()` 或 `get()` | 方法体作为值 supplier。`softExpire=true` → `getWithSoftExpire()`（过期旧值 + 异步刷新）；`false` → `get()`。方法返回 `Optional` 则透传，否则 `orElse(null)` 解包。 |
| `WRITE`        | `putBeforeInvalidate()`          | 方法作为突变执行：运行、版本递增、L1 失效、发送 INVALIDATE 广播。异常被捕获，在门面调用完成后重新抛出。                                                            |
| `INVALIDATE`   | `invalidate()`                   | 失效 L1 + 版本递增 + 广播 TYPE_REFRESH（带版本号）到对端，然后执行方法。                                                                                           |

**SpEL key 示例：**

```java
@HotKey(key = "'user:' + #id")
public User getUser(Long id) { ... }

@HotKey(key = "'shop:' + #shopId + ':item:' + #itemId")
public Item getItem(Long shopId, Long itemId) { ... }

@HotKey(key = "#user.id.toString()")
public Profile getProfile(User user) { ... }
```

**WRITE / INVALIDATE 示例：**

```java
@HotKey(key = "'user:' + #id", operation = OperationType.WRITE)
public User updateUser(Long id, UserUpdate dto) { ... }

@HotKey(key = "'user:' + #id", operation = OperationType.INVALIDATE)
public void clearCache(Long id) { ... }
```

**配置：**

```yaml
hotkey:
  annotation:
    enabled: true
```

需要在 classpath 中包含 `spring-boot-starter-aop`（提供 AspectJ）。SpEL 参数名解析需要 `-parameters` 编译标志（父 POM 默认启用）。

> **注意：** `@HotKey(operation=READ)` 将原始方法调用包装为 L2 supplier——方法体同时充当缓存读取和数据库回退。这对读密集型端点很方便，但会绕过 `peek()` 语义。`softExpire=true`（默认）启用 stale-while-revalidate：过期值立即返回，后台线程异步刷新。`softExpire=false` 时，缓存未命中总是同步调用方法。

### 4. 降级

HotKey 通过 `supplier` 回调形成三级降级链路：

```
hotKey.get(key, supplier)
  ├─ L1(Caffeine) 命中 → 直接返回
  ├─ L1 未命中 → supplier()
  │    ├─ 返回数据 → 热 key？ → 写 L1（使用相应 TTL）+ 返回
  │    ├─ 返回 null → Optional.empty() → 调用方 orElseGet/orElseThrow
  │    │                     (null = miss。HotKey 遵循 Caffeine 的约定；
  │    │                      如果你的后端存储了可空值，
  │    │                      请在调用方用 sentinel 对象包装。)
  │    │
  │    │                      示例：Redis 中存放了可空值
  │    │                      private static final Object NULL_SENTINEL = new Object();
  │    │                      Optional<Object> r = hotKey.get("k", () -> {
  │    │                          Object val = redisTemplate.opsForValue().get("k");
  │    │                          return val != null ? val : NULL_SENTINEL;
  │    │                      });
  │    │                      Object actual = r.orElse(null); // sentinel 转回 null
  │    └─ 抛出异常 → SingleFlight.load() 捕获 → Optional.empty() → 调用方兜底
  └─ HotKey 自身异常 → 异常 → 降级（@HotKey fallbackEnabled=true 时）→ 调用方
```

各组件故障表现：

| 故障组件               | 影响                                | 恢复方式              |
| ---------------------- | ----------------------------------- | --------------------- |
| HotKey 自身            | L1 不可用；异常或热点降级（若启用） | 应用重启              |
| L2 后端 (Redis/DB/API) | 每次请求穿透到调用方兜底            | 后端恢复后自动恢复    |
| L1 Caffeine OOM / 驱逐 | 单 key 被驱逐，下次读取重新回源     | 自动（Caffeine 内部） |

> 调用方始终需要处理 `Optional.empty()` — HotKey 不会隐藏后端故障。

写路径故障表现：

| 写方法                                              | 故障场景                    | 表现                                                                          |
| --------------------------------------------------- | --------------------------- | ----------------------------------------------------------------------------- |
| `putThrough`                                        | 线程池队列满（非事务）      | `RejectedExecutionException` 传播到调用方                                     |
| `putThrough`                                        | `writer.run()` / Redis 失败 | 错误记录到日志，L1 版本号未更新，不发送广播                                   |
| `putBeforeInvalidate`                               | `mutation.run()` 抛出异常   | 捕获突变异常并记录日志；跳过本地失效和广播                                    |
| `invalidate` / `putBeforeInvalidate` / `putThrough` | `nextVersion()` Redis 失败  | 回退到节点本地计数器（`Long.MIN_VALUE + counter`，非持久化，`degraded=true`） |

Worker 模式故障表现：

| 故障组件        | 影响                                   | 恢复方式                  |
| --------------- | -------------------------------------- | ------------------------- |
| Worker 崩溃     | App 实例继续使用本地 TopK；无集群共识  | 重启 Worker；实例自动重连 |
| 报告通道故障    | 报告排队/缓冲（RabbitMQ）              | RabbitMQ 恢复后自动恢复   |
| Worker 广播故障 | 无跨实例 HOT/COOL 同步；本地 TopK 正常 | 重启 Worker broadcaster   |

## HotKey 门面 API 参考

推荐入口是 `HotKey` 门面（已自动配置为 Spring Bean）。除了上面展示的 `get`/`peek`/`putThrough`/`putBeforeInvalidate` 之外，还提供：

| 方法                                                   | 说明                                                                                                                             |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `peek(key)`                                            | 仅查 L1，不做频率追踪，不读 L2，不上报                                                                                           |
| `get(key, reader)`                                     | 从 L1 或 L2 reader 读取；每次访问触发本地 TopK 追踪 + App→Worker 上报；热点 key 提升到 L1（使用热点 TTL），普通 key 使用普通 TTL |
| `get(key, reader, hardTtlMs, softTtlMs)`               | 同上，带 per-entry 硬和软 TTL 覆盖（传入 0 使用配置默认值）                                                                      |
| `getWithSoftExpire(key, reader)`                       | 软失效——返回过期旧值+触发异步刷新；每次访问触发本地 TopK 追踪 + App→Worker 上报；根据 key 状态使用全局默认 TTL                   |
| `getWithSoftExpire(key, reader, softTtlMs)`            | 同上，带 per-call 软 TTL 覆盖（毫秒）                                                                                            |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | 同上，同时带 per-entry 硬 TTL 和 per-call 软 TTL 覆盖（毫秒）                                                                    |
| `putThrough(key, value, writer)`                       | 写穿透：writer.run()、nextVersion()、L1 更新（根据 key 状态使用有效 TTL）、可选同步                                              |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | 同上，带 per-entry 硬和软 TTL 覆盖（传入 0 使用配置默认值）                                                                      |
| `putBeforeInvalidate(key, mutation)`                   | 先写后失效，用于集合增量操作（LPUSH、SADD、ZADD）                                                                                |
| `isLocalHotKey(cacheKey)`                              | 检查 key 是否在 L1 中为 HOT 状态（O(1)）                                                                                         |
| `isWorkerHotKey(cacheKey)`                             | 检查 key 是否在 Worker TopK 中为集群热点（O(n)）                                                                                 |
| `invalidate(cacheKey)`                                 | 使单个 key 在所有缓存层失效                                                                                                      |
| `invalidateAll(cacheKeys...)`                          | 可变参数重载 — 批量失效多个 key                                                                                                  |
| `invalidateAll(Collection)`                            | Collection 重载                                                                                                                  |
| `returnLocalHotKeys()`                                 | 应用端 Top-K 快照（key + 计数）                                                                                                  |
| `returnLocalExpelledHotKeys()`                         | 获取应用端被挤出的热点 key 队列；由内部定时器周期性清空                                                                          |
| `returnLocalTotalDataStreams()`                        | 经过应用端 HeavyKeeper 的累计读取数                                                                                              |
| `returnWorkerHotKeys()`                                | Worker 端（集群维度）Top-K 快照                                                                                                  |
| `returnWorkerExpelledHotKeys()`                        | Worker 端被挤出的热点 key 队列                                                                                                   |
| `returnWorkerTotalDataStreams()`                       | Worker 端 HeavyKeeper 累计读取数                                                                                                 |

> **注意：** `invalidate()` 通过 Redis `INCR` 生成单调版本号，以 `TYPE_REFRESH` 广播——对端通过 `CacheSyncListener` 从 Redis 重新加载，跳过旧版本。`invalidateAll()` **不调用** `INCR`——它以 `TYPE_INVALIDATE_ALL` 广播（无版本头），所有对端无条件从 L1 移除所有列出的 key。

## 配置

快速入门配置见[配置](#2-配置)。完整属性列表见 [CONFIG.zh.md](docs/CONFIG.zh.md)。Worker 模式见 [WORKER.zh.md](docs/WORKER.zh.md)。

### TTL 覆盖属性

通过 `hard-ttl-ms`、`hot-hard-ttl-ms`、`soft-ttl-ms`、`hot-soft-ttl-ms` 覆盖 TTL（0 = 使用默认值）。TTL 默认值和各方法行为见 [TTL 参考](#ttl-参考)。

### 同步与 Worker Listener

```yaml
hotkey:
  sync:
    enabled: true # 跨实例缓存同步
  worker-listener:
    enabled: true # 接收 Worker HOT/COOL 决策
```

### Worker 模式

启用 `hotkey.worker.enabled=true`。两种部署模式——见 [Worker 模式](#worker-模式)和 [WORKER.zh.md](docs/WORKER.zh.md)。

### 监控

通过 `management.endpoints.web.exposure.include=health,info,hotkey` 启用。

## TTL 参考

HotKey 使用**差异化 TTL**：热点 key 和普通 key 分别有独立的默认硬和软 TTL。per-call 覆盖在此基础上生效。

各 key 状态默认 TTL：

| Key 状态 | 硬 TTL（Caffeine 淘汰）         | 软 TTL（stale-while-revalidate）  |
| -------- | ------------------------------- | --------------------------------- |
| 普通     | `default-hard-ttl-ms`（5min）   | `default-soft-ttl-ms`（30s）      |
| 热点     | `default-hot-hard-ttl-ms`（1h） | `default-hot-soft-ttl-ms`（5min） |

通过 `hard-ttl-ms`、`hot-hard-ttl-ms`、`soft-ttl-ms`、`hot-soft-ttl-ms` 覆盖（0 = 使用默认值）。

方法级 TTL 行为：

| 方法                                                   | TTL 含义                                                        |
| ------------------------------------------------------ | --------------------------------------------------------------- |
| `get(key, reader)`                                     | 根据 key 当前状态使用有效 TTL（热点用热点 TTL，否则用普通 TTL） |
| `get(key, reader, hardTtlMs, softTtlMs)`               | 覆盖硬和软 TTL；传入 0 表示使用配置默认值                       |
| `getWithSoftExpire(key, reader)`                       | 立即返回过期旧值 + 异步刷新；TTL 根据 key 状态确定              |
| `getWithSoftExpire(key, reader, softTtlMs)`            | 同上，带 per-call 软 TTL 覆盖（硬 TTL 来自默认值）              |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | 同上，同时覆盖两者                                              |
| `putThrough(key, value, writer)`                       | L1 entry 始终使用普通 key 的 TTL（以 KeyState.NORMAL 存储）     |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | L1 entry 使用覆盖 TTL（0 = 使用配置默认值）                     |
| Legacy `local-cache-ttl-minutes`                       | 写入 TTL（位于 `hotkey.local.*`下；被差异化 TTL 补充）          |
| Legacy `local-cache-access-ttl-minutes`                | 基于访问的硬 TTL（每次读取重置，位于 `hotkey.local.*`下）       |

> **每次调用语义：** 所有 per-call 的 TTL 覆盖（硬和软）均为一次性——下次不传参数则回退到 key 状态对应的默认值。唯一的例外是软过期后台刷新会保留原始 per-entry 硬 TTL。

## 缓存同步

启用 `hotkey.sync.enabled=true`。

每个实例声明独立队列（`hotkey.sync:<实例ID>`）绑定到 fanout 交换机。四种消息类型：

- **`TYPE_REFRESH`** — 带版本号的刷新。对端通过 `CacheSyncListener.handleRefresh()` 从 Redis 重新加载，根据 `dataVersion` 跳过旧更新。4 种情况的比较（正常-正常、正常-降级、降级-正常、降级-降级）保证正常的（Redis INCR）dataVersion 始终优先于降级的（节点本地）版本。由 `invalidate()` 和 `putThrough()` 发送。
- **`TYPE_INVALIDATE`** — 单 key 失效（带版本守卫）。对端仅在传入 `dataVersion` 不陈旧时移除 L1 条目。由 `putBeforeInvalidate()` 发送。
- **`TYPE_INVALIDATE_ALL`** — 批量失效（无版本守卫）。对端立即从 L1 移除所有列出的 key，不重新加载。由 `invalidateAll()` 发送。
- **`TYPE_RULES_SYNC`** — 规则集替换。body 为 JSON 序列化的 `List<Rule>`，接收端调用 `RuleMatcher.syncRules()` 原子替换本地规则列表。不触发二次广播。

> [!SECURITY]
> 所有三个 RabbitMQ 交换机（`hotkey.sync.exchange`、`hotkey.report.exchange`、`hotkey.broadcast.exchange`）默认使用明文 AMQP 连接。生产环境中应通过 Spring Boot 的 `spring.rabbitmq.ssl.*` 属性配置 TLS：
>
> ```yaml
> spring:
>   rabbitmq:
>     ssl:
>       enabled: true
>       key-store: classpath:client.p12
>       key-store-password: changeit
>       trust-store: classpath:truststore.jks
>       trust-store-password: changeit
> ```
>
> 详见 [Spring Boot RabbitMQ SSL 文档](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title)。

## 规则系统

启用 `hotkey.sync.enabled=true` 以启用跨实例规则同步。规则系统提供两种操作：

| 操作              | 对匹配 key 的效果                                                                  |
| ----------------- | ---------------------------------------------------------------------------------- |
| `BLOCK`           | `get()` / `getWithSoftExpire()` 抛出 `HotKeyBlockedException`；`putThrough()` 跳过 |
| `ALLOW_NO_REPORT` | 正常处理但跳过 Worker 上报（减少频繁访问 key 的噪音）                              |

### 模式类型

`RuleMatcher.of(pattern, action)` 自动检测模式：

| 模式                | 类型       | 匹配                       |
| ------------------- | ---------- | -------------------------- |
| `"user:123"`        | `EXACT`    | 精确 key                   |
| `"temp:*"`          | `PREFIX`   | 以 `temp:` 开头的 key      |
| `"order:*-detail"`  | `WILDCARD` | Glob 风格（`*` / `?`）匹配 |
| `"regex:user:\\d+"` | `REGEX`    | Java 正则                  |

### 持久化与广播

- **有 Redis：** 每次 `addRule()`/`removeRule()`/`clearRules()` 将规则列表序列化到 `HotKeyConstants.REDIS_KEY_RULES`（`"hotkey:rules"`）。启动时 `RuleMatcher.initRules()` 从 Redis 加载。变更也通过 `TYPE_RULES_SYNC` 广播——对端通过 `RuleMatcher.syncRules()` 原子替换，不触发二次广播（避免风暴）。
- **无 Redis：** 相同操作通过 `CacheSyncPublisher` fanout 交换机广播到所有对端。每个对端在内存中持有完整规则集。
- **手动广播：** `hotKey.broadcastAllLocalRulesManually()` 从 Redis（如可用）加载并重新广播当前规则集到所有对端。

### 编程使用

```java
// 封锁敏感 key 的访问
hotKey.addBlacklist("secret:*");
hotKey.addBlacklist("regex:token-\\d+");

// 白名单 key 跳过 Worker 上报
hotKey.addWhitelist("health:*");
hotKey.addWhitelist("metrics:*");

// 检查规则
List<Rule> rules = hotKey.getAllRules();
RuleAction action = hotKey.evaluateRule("user:123"); // BLOCK / ALLOW_NO_REPORT / ALLOW

// 移除规则
hotKey.removeBlacklist("secret:*");
hotKey.clearAllRules();
```

### Worker Listener

如需接收专用 Worker 节点发出的 HOT/COOL 决策，启用 `hotkey.worker-listener.enabled=true`。详见 [WORKER.zh.md](docs/WORKER.zh.md)。

## Worker 模式

Worker 模式通过专用节点提供集群维度热点检测。App 实例定期报告访问计数，Worker 运行滑动窗口+状态机管道，将 HOT/COOL 决策广播回所有实例。

两种部署模式：

| 模式        | `worker.enabled` | 激活的 Bean                                                                      |
| ----------- | ---------------- | -------------------------------------------------------------------------------- |
| App-only    | `false`（默认）  | `HotKeyCache`、TopK、reporter、actuator、sync                                    |
| Worker-only | `true`           | 仅 Worker（无缓存——`get()`/`putThrough()` 抛出 `UnsupportedOperationException`） |

**Worker-only** 模式下，缓存操作抛出 `UnsupportedOperationException`。

完整文档（Worker 搭建、状态机、滑动窗口、配置）见 [WORKER.zh.md](docs/WORKER.zh.md)。

## 监控

当 classpath 包含 `spring-boot-starter-actuator` 时，HotKey Endpoint 自动注册在 `/actuator/hotkey`。通过 `management.endpoints.web.exposure.include=health,info,hotkey` 启用。

响应分为三个板块 — **local**（应用端检测、缓存、上报、规则、TTL、版本）、**worker**（集群 TopK、健康状态、状态机）和 **sync**（广播去重）。完整响应格式和字段说明见 [MONITOR.zh.md](docs/MONITOR.zh.md)（[英文版](docs/MONITOR.md)）。

## 架构和设计细节

详见 [ARCH.md](docs/ARCH.md)（英文）和 [ARCH.zh.md](docs/ARCH.zh.md)（中文）。

## 方法调用路径

详见 [METHODS.md](docs/METHODS.md)（英文）和 [METHODS.zh.md](docs/METHODS.zh.md)（中文）。

## License

Apache License 2.0
