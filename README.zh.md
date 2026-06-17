# HotKey

<p align="center">
  <a href="https://jitpack.io/#Hyshmily/HotKey"><img src="https://jitpack.io/v/Hyshmily/HotKey.svg" alt="JitPack"></a>
  <a href="https://coveralls.io/github/hyshmily/hotkey"><img src="https://coveralls.io/repos/github/hyshmily/hotkey/badge.svg" alt="Coveralls"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-17-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
</p>

[**English**](README.md)

HotKey 是一款高度自定义化的[高性能](docs/HotKey_Benchmark_Report.zh.md)、低成本、轻量级分布式缓存框架，集**缓存读写（get/put）、热点自动检测、多级缓存预热、跨实例广播同步、AOP 注解拦截、黑白名单过滤**于一体。

大多数的本地缓存将所有条目一律存入 Caffeine。但在海量 key 下：

- **内存浪费** — 大多数 key 只读一次
- **广播风暴** — 全量缓存 = 全量失效，广播随 key 数线性增长
- **缓存雪崩** — 海量 key 使用相同 TTL，同时过期时全部穿透至 DB，瞬时打穿后端

而HotKey 的策略却是：**仅缓存真正的热点数据。**

通过 [HeavyKeeper](https://github.com/go-kratos/aegis)（Count-Min Sketch 变体）实现**集群维度热点检测**：部署 Worker 节点聚合所有实例的访问报告——解决"被同一 pod 访问 100 次"与"被 100 个 pod 各访问 1 次"在本地无法区分的问题。

HotKey受京东[hotkey](https://gitee.com/jd-platform-opensource/hotkey)项目启发,算法支持来自[Aegis](https://github.com/go-kratos/aegis)

## 特点

<details>
<summary><b>点击展开详细的hotkey特点</b></summary>

- **HeavyKeeper 算法** — Count-Min Sketch + 指数冲突衰减的概率性 Top-K 检测
- **多级缓存** — Caffeine (L1) → 可选的 reader 回调（L2，如 Redis）+ 调用方通过 `Optional.orElseGet()` 实现的 DB 回退，自动热点提升
- **差异化 TTL** — 热点 key 与普通 key 使用独立的硬/软 TTL；热点 key 缓存更久（1h/5min），普通 key 更快过期（5min/30s）
- **请求合并** — L1 未命中时同 key 并发请求共享同一 L2 读，通过专门的 `SingleFlight` bean
- **软失效（逻辑过期）** — 立即返回过期旧值，同时后台异步刷新；降低 p99 延迟，代价是短暂脏读。**完全替代传统 Redis 侧逻辑过期**（`RedisData{data, expireTime}` 包装模式）——Redis 纯值存储，由 HotKey 在 L1 Caffeine 层管理过期
- **Redis 集合类型** — 通过 `putBeforeInvalidate` 支持 List/Set/ZSet 增量写入，无需 `putThrough`
- **热点同步** — 可选 RabbitMQ fanout（通过 `hotkey.sync.*`）跨实例同步缓存失效；独立的 worker-listener（通过 `hotkey.worker-listener.*`）接收 Worker 发出的 HOT/COOL 决策
- **Worker 模式** — 专用集群维度热点检测节点；基于滑动窗口 + 状态机管道实现跨实例共识；通过 `/actuator/hotkey/worker/state` REST 端点运行时调整状态机配置，基于心跳的对等广播传播变更；详见 [Worker 模式](#worker-模式)
- **报告聚合** — 每次 `get()` / `getWithSoftExpire()` 调用上报到本地 `HotKeyReporter`，再由 Reporter 周期性地将访问计数批量发送到 Worker 节点（RabbitMQ），用于集群维度热点检测
- **TTL 随机偏移（缓存雪崩防护）** — `CacheExpireManager` 通过 `ThreadLocalRandom` 对每个硬/软 TTL 施加可配置的随机偏移（默认 ±10%），打散过期时间戳，防止缓存雪崩；由 `hotkey.local.ttl-jitter-enabled` / `ttl-jitter-ratio` 控制
- **一致性哈希（默认开启）** — 基于 murmur3_32 的一致性哈希环，通过心跳实现动态 Worker 路由；弹性扩缩容无需静态分片配置
- **BBR 自适应速率限制** — 通过 BBR 拥塞控制融合 CPU EMA 监控实现自我保护；在 Reporter 刷盘路径施加背压，防止 RabbitMQ/Worker 过载
- **SRE 自适应限流** — WorkerListener HOT 路径的 Google SRE 公式背压（`K = 1 / successThreshold`，成功率低于阈值时概率性丢弃）；独立于 BBR，保护 HOT 决策消费路径
- **CPU 监控与 EMA 平滑** — 专用守护线程每 500ms 轮询进程 CPU 负载，可配置 EMA 衰减因子实现稳定过载检测
- **Spring Boot 自动配置** — 引入依赖即用，零样板代码
- **Worker TopK 持久化** — 将 Worker 的 HeavyKeeper 定期快照到 Redis；重启恢复只需数秒而非数小时重新积累
- **Spring Cache 集成** — 标准 `@Cacheable` / `@CachePut` / `@CacheEvict` 注解与 HotKey 热点检测、软过期、跨实例同步和配套注解 (`@HotKeyCacheTTL`、`@Intercept`、`@Fallback`、`@NullCaching`) 协同工作，提供 TTL、拦截、降级和空值缓存能力；通过 `hotkey.spring-cache.enabled=true` 开启
- **事务支持** — `TransactionSupport` 将写入延迟到 Spring `@Transactional` 事务提交后执行，保证写入路径的缓存-数据一致性

</details>

## 适用场景与限制

HotKey 的定位是**读热点治理**框架，而非通用分布式缓存或分布式锁。

### 适合的场景

| 场景                       | 为什么适合                                                                      |
| -------------------------- | ------------------------------------------------------------------------------- |
| **商品详情/价格页**        | 大促时某几个 SKU 流量暴涨，HeavyKeeper 自动探测 → 延长本地 TTL → 减少穿透 Redis |
| **爆款文章/视频元数据**    | 突然上热搜，HotKey 自动识别 + 跨实例广播主动预热每个 pod 的 L1                  |
| **配置/规则/字典缓存**     | 读极频繁、写极少；通过 AMQP 广播跨实例同步，保持一致性                          |
| **用户会话/权限快照**      | 热用户（VIP 操作）的权限缓存自动获得更长 L1 TTL                                 |
| **预热类只读缓存（旁路）** | 券面额、商品图、描述等只读字段的缓存——**搭配**独立的原子扣减系统使用            |

### 不适合的场景

| 场景                             | 原因                                                                                                | 替代方案                             |
| -------------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------ |
| **原子库存扣减**（秒杀扣 stock） | HotKey 无跨 JVM 原子性保证；`VersionGuard` 是乐观的，瞬态不一致在下一个心跳周期才能自愈（ADR-0006） | Redis Lua 脚本或分布式锁（Redisson） |
| **分布式锁**                     | HotKey 不提供分布式锁抽象                                                                           | Redisson、ZooKeeper                  |
| **强一致性写入**                 | 跨实例同步是至多一次 + 自愈广播（ADR-0004, ADR-0007）                                               | 数据库事务、Paxos/Raft               |
| **均匀流量（无热点）**           | 无法检测到热点 → HotKey 相对裸 Caffeine 没有任何收益                                                | 裸 Caffeine 缓存                     |

> [!WARNING]
> **极限性能**
>
> 将部分参数设为极限值可将全链路延迟压缩至 **~9ms**（P50）
>
> HotKey 的本身默认参数设计偏向保守，支持高度自定义以适配不同场景的性能需求。

<details>
<summary><b>点击展开——完整全链路延迟分解表及极限调优说明</b></summary>

详见[基准测试报告](docs/HotKey_Benchmark_Report.zh.md)。

**极限参数调整**

| 参数                                                  | 极限值            | 效果                                                         | 限制                                       |
| ----------------------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------ |
| `hotkey.local.report-interval-ms`                     | 0 → 最小 1        | 近似禁用上报批处理，`record()` 后几乎即时 flush              | `ScheduledExecutorService` 要求 period > 0 |
| `hotkey.worker-listener.warmup-jitter-ms`             | 0                 | 禁用预热抖动（羊群效应防护失效），Worker 决策即时执行        | —                                          |
| `hotkey.sync.warmup-jitter-ms`                        | 0                 | 同上，跨实例同步监听器                                       | —                                          |
| `hotkey.worker.state-machine.confirm-duration-ms`     | 0                 | 禁用状态机确认窗口，首个热窗口即广播 HOT                     | —                                          |
| `hotkey.worker.sliding-window.duration-ms` / `slices` | 1000/10 → 100/100 | 时间片从 100ms 缩短至 1ms，减少 tick 等待（avg~50ms→~0.5ms） | 窗口统计精度显著降低                       |

**实测极端场景延迟**（[极端参数调优对比](docs/img/extreme_tuning_comparison.png)）：

| 场景              | 默认(3确认窗) P50 | 极限(0确认窗) P50 | 极限 P95 | 极限 P99 |
| ----------------- | ----------------- | ----------------- | -------- | -------- |
| Worker 决策流水线 | 56.38 ms          | **2.41 ms**       | 11.89 ms | 12.40 ms |
| SM 确认流水线     | 246.46 ms         | **7.71 ms**       | 8.53 ms  | 8.53 ms  |
| 全链路            | 298.19 ms         | **9.23 ms**       | 10.93 ms | 10.93 ms |

**状态机通过”每个 Key 每个生命周期只广播一次”**，从根本上消除了冗余广播导致的客户端 CPU 过载和自造拥塞。极限测试中，SM 0 确认路径仅产生 10 条广播，而无状态机路径产生了大量冗余广播，延迟因此大幅放大。

**默认的 300ms 确认窗口**（3 个确认窗口 × 100ms 切片）不是反应迟钝，而是用这 300ms 的连续观察，换取几乎零误报的全局决策。在这 300ms 内，本地 HeavyKeeper 已在纳秒级完成保护，用户请求没有经历任何阻塞。

**预热抖动和批处理间隔**是分布式系统中经典的“防惊群”和“平滑负载”手段，牺牲几十毫秒的延迟，换来的是全集群的稳定。

</details>

## 快速开始

### 1. 添加依赖

**Maven Central**（无需额外仓库）：

```xml
<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>hotkey</artifactId>
    <version>1.1.4</version>
</dependency>
```

**JitPack**（始终最新快照）：

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
    <version>1.1.5.Beta</version>
</dependency>
```

### 2. 配置

默认local本地配置（适用于大多数场景)

> [!IMPORTANT]
> 在分布式部署情况下,需引入RabbitMQ和Redis

**可选功能配置：**

| 功能               | 启用方式                                     | 说明                                                            |
| ------------------ | -------------------------------------------- | --------------------------------------------------------------- |
| Redis 二级缓存     | 添加 `RedisTemplate` Bean                    | 两级缓存，L2 兜底                                               |
| 跨实例同步         | `hotkey.sync.enabled=true`                   | 基于 RabbitMQ 的缓存失效                                        |
| Worker Listener    | `hotkey.worker-listener.enabled=true`        | 接收 Worker 的 HOT/COOL 决策                                    |
| Worker 模式        | `hotkey.worker.enabled=true`                 | 运行专用 Worker 节点                                            |
| Worker TopK 持久化 | `hotkey.worker.persistence.enabled=true`     | 重启后从 Redis 热启动                                           |
| 访问上报           | `hotkey.report.enabled=true`（默认）         | 向 Worker 上报访问次数                                          |
| Reporter 自我保护  | `hotkey.local.reporter.enabled=true`（默认） | Reporter 刷盘的 BBR 背压保护                                    |
| Spring Cache 集成  | `hotkey.spring-cache.enabled=true`           | `@Cacheable` / `@CachePut` / `@CacheEvict` 融合 HotKey 热点检测 |

所有选项见[配置](#配置)，完整属性参考见 [CONFIG.zh.md](docs/CONFIG.zh.md)。

<details>
<summary><b>快速部署 YAML 模板</b></summary>

**本地 Local（App 侧）** — 引入 `hotkey` 依赖即可运行，可按需取消注释

```yaml
hotkey:
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
hotkey:
  worker:
    enabled: true
    routing:
      app-name: myapp # 【必须】与 App 侧 hotkey.local.app-name 一致
      # 一致性哈希默认开启；Worker 通过心跳自动注册

# 多 Worker 示例：3 台机器，相同 app-name
# 一致性哈希通过心跳自动将 key 路由到正确的 Worker
# 无需静态分片配置——只需增加机器即可
```

**全部参数（覆盖默认值）**

```yaml
hotkey:
  local:
    # ——— 跨节点强制一致（App 侧与 Worker 侧必须对齐） ———
    app-name: "default" # 必须与 worker.routing.app-name 一致

    # ——— 实例标识 ———
    instance-id: "" # 显式实例 ID（空=自动生成）

    # ——— 报告上报 ———
    report-exchange: "hotkey.report.exchange"
    report-interval-ms: 100 # 批量发送间隔 (ms)
    queue-capacity: 10000 # 上报队列容量
    queue-offer-timeout-ms: 100 # 队列写入超时 (ms)
    consumer-count: 0 # 消费者线程数 (0=auto)

    # ——— HeavyKeeper 算法 ———
    topK: 100 # 保留的热 key 数
    width: 50000 # Count-Min Sketch 宽度
    depth: 5 # Count-Min Sketch 深度
    decay: 0.92 # 衰减因子（每次 fading 乘以该值）
    minCount: 10 # 热 key 最小访问次数
    expelled-queue-capacity: 50000 # 被驱逐热 key 暂存队列容量

    # ——— L1 Caffeine 缓存 ———
    local-cache-max-size: 1000 # 最大条目数
    local-cache-ttl-minutes: 5 # 缓存过期时间（分钟）

    # ——— SingleFlight 去重 ———
    inflight-max-size: 50000 # 去重记录最大 key 数
    inflight-ttl-seconds: 5 # 去重记录 TTL（必须 > L2 响应耗时）
    inflight-timeout-seconds: 3 # 等待异步结果超时（必须 < inflight-ttl-seconds）

    # ——— 异步执行器 ———
    executor-core-pool-size: 8 # 核心线程数
    executor-max-pool-size: 32 # 最大线程数
    executor-queue-capacity: 2000 # 工作队列容量
    scheduler-pool-size: 8 # 调度线程池大小

    # ——— TTL 抖动（缓存雪崩防护） ———
    ttl-jitter-enabled: true # 启用 TTL 随机偏移
    ttl-jitter-ratio: 0.1 # 偏移比例（0.0~1.0），0.1 = ±10%

    # ——— 刷新 & 版本控制 ———
    refresh-max-pools: 100 # 刷新线程池上限
    version-key-ttl-minutes: 60 # Redis 版本 key TTL

    # ——— 普通 key TTL ———
    default-hard-ttl-ms: 300000 # 默认硬过期 (5min)
    hard-ttl-ms: 0 # 覆盖值 (0=使用 default)
    default-soft-ttl-ms: 30000 # 默认软过期 (30s)
    soft-ttl-ms: 0 # 覆盖值 (0=使用 default)

    # ——— 热 key TTL（Worker 标记 HOT 后生效） ———
    default-hot-hard-ttl-ms: 3600000 # 默认热 key 硬过期 (1h)
    hot-hard-ttl-ms: 0 # 覆盖值 (0=使用 default)
    default-hot-soft-ttl-ms: 300000 # 默认热 key 软过期 (5min)
    hot-soft-ttl-ms: 0 # 覆盖值 (0=使用 default)

    # ——— 一致性哈希（默认开启；动态 Worker 路由） ———
    consistent-hashing:
      enabled: true # 通过心跳实现动态 Worker 路由
      virtual-nodes: 500 # 每个物理 Worker 的虚拟节点数

    # ——— 心跳（App 侧；Worker 健康监控） ———
    heartbeat:
      exchange-name: "hotkey.heartbeat.exchange"
      timeout-ms: 3000 # 此窗口内无心跳即判定 Worker 死亡
      verify-interval-ms: 1500 # 可疑 Worker 验证间隔
      ping-timeout-ms: 2000 # Direct reply-to PING 超时
      degrade-after-failures: 2 # 连续 PING 失败 N 次后降级

    # ——— Reporter 速率限制器（BBR + CPU 融合） ———
    reporter:
      enabled: true # BBR 自适应速率限制
      cpu-threshold: 800 # CPU 阈值 (0-1000, 800=80%)
      cpu-poll-interval-ms: 500 # CPU 轮询间隔 (ms)
      cpu-decay: 0.95 # EMA 平滑衰减因子
      bbr-window-ms: 10000 # BBR 滑动窗口 (ms)
      bbr-window-buckets: 100 # BBR 滑动窗口桶数
      bbr-cooldown-ms: 1000 # 丢弃后的冷却时间 (ms)

  # 特性开关
  report:
    enabled: true # App→Worker 报告上报
  scheduling:
    enabled: true # 定时衰减 & 驱逐清理
  decay-period: 20 # HeavyKeeper fading 间隔（秒），scheduling 启用时生效

  # App 侧 — Worker 决策监听（接收 HOT / COOL）
  worker-listener:
    enabled: false # 需 Redis + RabbitMQ
    exchange-name: "hotkey.broadcast.exchange"
    queue-prefix: "hotkey.worker"
    auto-startup: true # 随应用启动
    warmup-jitter-ms: 100 # 处理前随机延迟（防惊群）
    concurrent-consumers: 2 # 并发消费者数
    scheduler-pool-size: 2 # 延迟任务线程池大小
    prefetch-count: 5 # AMQP 每个消费者预取数量

    # SRE 自适应速率限制器（HOT 决策处理路径）
    sre:
      enabled: true # 启用 SRE 速率限制器
      window-ms: 3000 # 滑动窗口时长 (ms)
      buckets: 10 # 滑动窗口桶数
      min-samples: 20 # 开始限流前的最小样本数
      success-threshold: 0.6 # 成功率阈值 (0.0-1.0)

  # App 侧 — 跨实例缓存同步
  sync:
    enabled: false
    exchange-name: "hotkey.sync.exchange"
    queue-prefix: "hotkey.sync"
    auto-startup: true
    dedup-window-seconds: 10 # 消息去重窗口
    dedup-max-size: 10000 # 去重缓存上限
    warmup-jitter-ms: 100 # 处理前随机延迟（防惊群）
    concurrent-consumers: 3 # 并发消费者数
    scheduler-pool-size: 4 # 延迟任务线程池大小
    prefetch-count: 5 # AMQP 每个消费者预取数量

  # Worker 侧 — 独立部署节点
  worker:
    enabled: false

    routing:
      app-name: "default" # 必须与 local.app-name 一致
      # 路由通过一致性哈希和心跳自动管理

    messaging:
      report-exchange: "hotkey.report.exchange"
      broadcast-exchange: "hotkey.broadcast.exchange"
      heartbeat-exchange: "hotkey.heartbeat.exchange"

    sliding-window:
      duration-ms: 1000 # 窗口时长 (ms)
      slices: 10 # 窗口切片数（必须整除 duration-ms）

    threshold:
      hot-threshold: 1000 # 绝对 QPS 阈值（≤0 = 使用比例阈值）
      hot-threshold-ratio: 0.01 # 相对 QPS 比例阈值 (1%)

    state-machine:
      confirm-duration-ms: 300 # 确认窗口 (ms)，持续热超过此时长判定为 HOT
      cool-duration-ms: 15000 # 冷却窗口 (ms)，持续冷超过此时长判定为 COOL
      pre-cool-grace-ms: 5000 # 预冷却宽限期 (ms)
      evict-interval-ms: 30000 # 过期状态擦除间隔 (ms)，必须 >= coolDurationMs * 2

    global-qps-dynamic-threshold:
      qps-change-tolerance: 0.5 # QPS 变化容忍倍数
      learning-period-ms: 30000 # 学习周期 (ms)
      hot-threshold-ratio: 0.01 # 动态阈值比例
      recalculate-interval-ms: 60000 # 重算间隔 (ms)

    topk-validation:
      validate-interval-ms: 60000 # 验证周期 (ms)
      pre-warm-count: 5 # 预温次数
      pre-warm-min-appearances: 2 # 最小出现次数

    heavy-keeper:
      top-k: 100 # 保留热 key 数
      width: 20000 # Sketch 宽度
      depth: 10 # Sketch 深度
      decay: 0.9 # 衰减因子
      min-count: 10 # 最小计数

    heartbeat:
      ping-interval-ms: 1000 # 心跳广播间隔 (ms)

    persistence:
      enabled: false # 周期性将 TopK 快照到 Redis（需手动开启）
      persist-interval-ms: 30000 # 快照间隔 (ms)
      topk-count: 100 # 每次快照保存的 key 数
      redis-key-prefix: "hotkey:topk:worker:" # Redis key 前缀
      ttl-days: 3 # Redis 数据过期时间（天）
```

</details>

### 3. 使用

**读操作**

```java
@Autowired
private HotKey hotKey;

// A. peek — 仅查 L1，不做热点追踪
Optional<String> r = hotKey.peek("user:123"); // L1 未命中返回 Optional.empty()

// B. get — 两级缓存（Redis 或任意后端）
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// C. getWithSoftExpire — 软失效（stale-while-revalidate）
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// D. 流式读 API + fallback 链
Optional<User> user = hotKey
  .read("user:42")
  .withPrimary(userRepo::findById)
  .thenExecute(backupRepo::findById)
  .withHardTtl(30_000)
  .withSoftTtl(10_000)
  .allowBroadcast()
  .execute();
```

**软失效** 立即返回过期旧值同时后台异步刷新。Redis 存纯值无需包装——HotKey 完全在 L1 Caffeine 层管理过期。

| 维度       | 传统逻辑过期                                   | HotKey 软失效                                             |
| ---------- | ---------------------------------------------- | --------------------------------------------------------- |
| 过期存储   | 嵌入 Redis 值（`RedisData{data, expireTime}`） | L1 Caffeine 元数据（`softExpireAt`）                      |
| 返回旧值   | 解析包装后返回旧数据                           | 直接返回 L1 旧值                                          |
| 异步重建   | Redis 分布式锁 + 自定义线程池                  | Singleflight（本地）+ `hotKeyExecutor` + `refreshLimiter` |
| Redis 格式 | 包裹 JSON                                      | 纯值（无需包装）                                          |
| DB 回退    | 手动加锁逻辑                                   | 原生 `orElseGet` / `orElseThrow`                          |

```java
// 自定义 per-call softTtl（覆盖全局默认值）
Optional<String> r2 = hotKey.getWithSoftExpire("user:456", () -> redisTemplate.opsForValue().get("user:456"), 3000);

// DB 回退（无需分布式锁）：
String json = hotKey
  .getWithSoftExpire("shop:" + shopId, () -> redisTemplate.opsForValue().get("shop:" + shopId))
  .orElseGet(() -> {
    User u = userMapper.selectById(shopId);
    String s = JSONUtil.toJsonStr(u);
    if (u != null) redisTemplate.opsForValue().set("shop:" + shopId, s);
    return s;
  });
```

**写操作**

```java
// E. putThrough — 写穿透 + 广播
hotKey.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));

// F. putBeforeInvalidate — 变异后失效（集合类型）
hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));

// G. putLocal — 仅本地写，不广播、不 bump 版本
hotKey.putLocal("user:123", cachedValue);
hotKey.putLocal("user:123", cachedValue, hardTtlMs, softTtlMs); // 指定 TTL

// H. 流式写 API
hotKey.write("user:42").withHardTtl(30_000).putThrough(newValue, dbWriter);
hotKey.write("user:42").putBeforeInvalidate(dbMutation);
hotKey.write("user:42").invalidate();
```

`putBeforeInvalidate` 专为 Redis 集合类型（List、Set、ZSet）设计。`putThrough` 需要完整新值更新 L1，但 LPUSH/SADD/ZADD 只改单个元素——调用方无法获知全量新值。突变后失效 L1，下次 `get()` 自动回源 Redis。

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

  public Optional<Set<Object>> sMembers(String key) {
    return hotKey.get(key, () -> redisTemplate.opsForSet().members(key));
  }

  public void sAdd(String key, Object... members) {
    hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));
  }

  public Optional<List<Object>> lRange(String key, long start, long end) {
    return hotKey.get(key + "::range::" + start + "::" + end, () -> redisTemplate.opsForList().range(key, start, end));
  }

  public Optional<Double> zScore(String key, Object member) {
    return hotKey.get(key + "::score::" + member, () -> redisTemplate.opsForZSet().score(key, member));
  }
}
```

**数据库兜底与缓存穿透防护**

```java
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123); // DB 回退
    redisTemplate.opsForValue().set("user:123", value);
}
```

```java
// 封装 Helper 避免重复 lambda
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

// 自定义二级缓存（非 Redis）：MySQL、远程 API 或任意数据源
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));

User user = r.orElseGet(() -> createDefaultUser());
```

**自定义 per-entry TTL**

HotKey 使用**差异化 TTL**：热点 key 和普通 key 分别有独立默认值。per-call 覆盖在此基础上生效。

| Key 状态 | 硬 TTL（Caffeine 淘汰）         | 软 TTL（stale-while-revalidate）  |
| -------- | ------------------------------- | --------------------------------- |
| 普通     | `default-hard-ttl-ms`（5min）   | `default-soft-ttl-ms`（30s）      |
| 热点     | `default-hot-hard-ttl-ms`（1h） | `default-hot-soft-ttl-ms`（5min） |

```java
// 5 分钟硬 TTL + 30 秒软 TTL
Optional<String> shopJson = hotKey.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5), TimeUnit.SECONDS.toMillis(30));

// 30 秒硬 TTL，软 TTL 用默认值
hotKey.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30), 0);
```

> [!NOTE]
> **缓存雪崩防护：** `CacheExpireManager` 计算每个过期时间戳时使用 `ThreadLocalRandom` 施加可配置的均匀随机偏移（默认 ±10%）。5 分钟硬 TTL 在默认偏移下实际到期 4.5 ~ 5.5 分钟。通过 `hotkey.local.ttl-jitter-enabled`（开关）和 `hotkey.local.ttl-jitter-ratio`（比例，默认 `0.1` = ±10%）控制。

> [!TIP]
> per-call TTL 语义：传入 `0` 表示使用该 key 状态的配置默认值。彻底逻辑过期（纯软过期，硬 TTL 永不淘汰）：向 `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)` 传入 `hardTtlMs = Long.MAX_VALUE`，entry 永久驻留 Caffeine。此用法受 Caffeine `Expiry` JavaDoc 明确支持：_"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([源码](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))

**Worker 模式**

Worker 模式通过专用节点提供集群维度热点检测。App 实例定期报告访问计数，Worker 运行滑动窗口+状态机管道，将 HOT/COOL 决策广播回所有实例。状态机参数（`confirmCount`、`coolCount`、`preCoolGraceCount`）可通过 `/actuator/hotkey/worker/state` 运行时调整。

| 模式        | `worker.enabled` | 激活的 Bean                                                                      |
| ----------- | ---------------- | -------------------------------------------------------------------------------- |
| App-only    | `false`（默认）  | `HotKeyCache`、TopK、reporter、actuator、sync                                    |
| Worker-only | `true`           | 仅 Worker（无缓存——`get()`/`putThrough()` 抛出 `UnsupportedOperationException`） |

**Worker-only** 模式下缓存操作抛出 `UnsupportedOperationException`。

**Worker TopK 持久化（热启动）：** 当 `hotkey.worker.persistence.enabled=true`，Worker 定期快照 TopK 列表到 Redis。重启时 `TopKPersistService` 加载上次快照并回放到 HeavyKeeper sketch，预热从数小时缩至数秒。

**Spring Cache 集成**

启用 `hotkey.spring-cache.enabled=true`。标准 `@Cacheable` / `@CachePut` / `@CacheEvict` 自动通过 HotKey 的热点检测、软过期和跨实例同步路由。

| 注解              | 在 `@Cacheable` 上的作用                            |
| ----------------- | --------------------------------------------------- |
| `@HotKeyCacheTTL` | 覆盖硬/软 TTL                                       |
| `@Intercept`      | 缓存命中时触发拦截回调，通过 `@Fallback` 解析回退值 |
| `@Fallback`       | 拦截器阻止时提供回退值                              |
| `@NullCaching`    | 选择缓存 null 返回值（默认 `true`）                 |

```java
@Cacheable(cacheNames = "users", key = "#id")
@HotKeyCacheTTL(softTtlMs = 1000)
@Intercept @Fallback
public User getUser(Long id) { ... }
```

需 classpath 中包含 `spring-boot-starter-cache` 和 `spring-boot-starter-aop`。

---

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
> ````yaml
> spring:
>  rabbitmq:
>    ssl:
>      enabled: true
>      key-store: classpath:client.p12
>      key-store-password: changeit
>      trust-store: classpath:truststore.jks
>      trust-store-password: changeit
> . ```
>
> 详见 [Spring Boot RabbitMQ SSL 文档](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title)。
> ````

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
boolean blocked = hotKey.isBlacklisted("user:123");         // true 表示被封锁
boolean skipReport = hotKey.isWhitelisted("health:ping");   // true 表示跳过上报

// 移除规则
hotKey.removeBlacklist("secret:*");
hotKey.clearAllRules();

// 缓存监控
long size = hotKey.estimatedSize();    // L1 条目数量
HotKeyCacheStats s = hotKey.stats();   // 命中率、驱逐数等

// 紧急清空（不广播）
hotKey.invalidateAll();
```

### 降级

HotKey 通过 `supplier` 回调形成三级降级链路：

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
| `putLocal`                                          | 任何场景                    | 无操作（无 DB/网络依赖）                                                      |
| `putThrough`                                        | 线程池队列满（非事务）      | `RejectedExecutionException` 传播到调用方                                     |
| `putThrough`                                        | `writer.run()` / Redis 失败 | 错误记录到日志，L1 版本号未更新，不发送广播                                   |
| `putBeforeInvalidate`                               | `mutation.run()` 抛出异常   | 捕获突变异常并记录日志；跳过本地失效和广播                                    |
| `invalidate` / `putBeforeInvalidate` / `putThrough` | `nextVersion()` Redis 失败  | 回退到节点本地计数器（`Long.MIN_VALUE + counter`，非持久化，`degraded=true`） |

Worker 模式故障表现：

| 故障组件           | 影响                                                                      | 恢复方式                                  |
| ------------------ | ------------------------------------------------------------------------- | ----------------------------------------- |
| Worker 全部崩溃    | 本地 TopK 驱动 L1 TTL；COOL 条目可升级为 HOT；Worker 决策优雅降级         | 重启 Worker 集群；Worker 广播覆盖本地升级 |
| Worker 部分崩溃    | 未受影响的分片继续正常工作                                                | 重启崩溃的 Worker；自动重连               |
| 报告通道故障       | 报告排队/缓冲（RabbitMQ）                                                 | RabbitMQ 恢复后自动恢复                   |
| Worker 广播故障    | 无跨实例 HOT/COOL 同步；本地 TopK 正常                                    | 重启 Worker broadcaster                   |
| Reporter BBR 背压  | 并发超出预算时 BBR 丢弃批次（CPU≥阈值）；低于阈值时宽松处理               | 负载下降后自动恢复                        |
| Worker TopK 持久化 | Redis 不可用时静默跳过持久化，`error` 日志记录；Worker 冷启动（无热启动） | Redis 恢复后下一次定时持久化自动成功      |

## 监控

HotKey 提供两种互补的监控机制

完整响应格式和字段说明见 [MONITOR.zh.md](docs/MONITOR.zh.md)（[英文版](docs/MONITOR.md)）。

## 设计细节

领域术语定义见 [CONTEXT.md](CONTEXT.md)。
架构决策记录（ADR）维护在 [docs/adr/](docs/adr/0001-local-promotion-worker-fallback.md)。

## License

Apache License 2.0
