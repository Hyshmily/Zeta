# Zeta

<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.hyshmily/zeta"><img src="https://img.shields.io/maven-central/v/io.github.hyshmily/zeta?color=blue" alt="Maven Central"></a>
  <a href="https://jitpack.io/#Hyshmily/Zeta"><img src="https://jitpack.io/v/Hyshmily/Zeta.svg" alt="JitPack"></a>
  <a href="https://github.com/Hyshmily/Zeta/releases"><img src="https://img.shields.io/github/v/release/Hyshmily/Zeta?color=brightgreen" alt="GitHub Release"></a>
  <a href="https://github.com/Hyshmily/Zeta/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Hyshmily/Zeta/ci.yml?branch=master&label=CI&logo=github" alt="CI"></a>
  <a href="https://coveralls.io/github/Hyshmily/Zeta?branch=master"><img src="https://coveralls.io/repos/github/Hyshmily/Zeta/badge.svg" alt="Coveralls"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-17-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
  <a href="https://github.com/Hyshmily/zeta/commits/master"><img src="https://img.shields.io/github/last-commit/Hyshmily/zeta/master" alt="Last Commit"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.zeta"><img src="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.zeta" alt="Visitors"></a>
</p>

[**English**](README.md)

Zeta 是面向 Spring Boot 的本地优先（Local-First）多实例缓存与实时热点治理框架。无需引入 Redis / ZooKeeper 等外部协调存储，仅借助消息广播即可在单个 Key 零预注册的前提下，以极低开销实现全链路热点发现、预热保活、多实例版本同步与故障降级。

> [!NOTE]
>
> Zeta 1.2.0 即将发布。

## 工作原理

### 本地 / 集群检测

```java
private final Zeta zeta;

// 读穿透缓存：L1 命中直接返回；未命中经 SingleFlight 合并回源，自动参与热点计数
User user = zeta.get("user:123", () -> userRepository.findById(123L));

// 仅做热点计数与上报（不读写 L1，适合外部缓存或只做热点探测的场景）
zeta.tag("product:123");
zeta.tagAll(Map.of("product:123", 3L, "product:456", 1L));

// 纯内存探测：不触发回源，也不产生热点计数与上报副作用
Product cached = zeta.peek("product:123");
```

- **本地层（默认）**：每个实例运行 HeavyKeeper Top-K 草图；进入 Top-K 即成为**本地热**（Local-Hot）：它是本地续鲜（keep-warm）的独立依据，也用于可观测性，但从不改写你声明的 TTL。L1 未命中合并并发请求；软 TTL 默认关闭。
- **集群层（可选）**：实例计数经一致性哈希环上报至 RabbitMQ，由 Worker 集群评估。Worker 为每个 Key 维护滑动窗口，并将窗口和与当前规则的 `hotCount` / `coolCount` 比较（`EXACT` / `PREFIX` / `REGEX` 按优先级排序，命中的精确规则按自身阈值判定，其余受提升下限约束）。HOT / COOL 决策通过直连交换机广播回该应用的全部实例。

## 多节点缓存一致性

与主从数据库通过日志复制协议同步写入类似，Zeta 通过基于 RabbitMQ 的发布-订阅机制在各应用实例间同步缓存变更：

- 任一实例执行集群同步写（`put(key, value, PutPolicy.synced())`）或失效操作时，由本地无锁 `StampClock` 分配单调递增的 `dataVersion`；
- `BroadcastBuffer` 在短延迟窗口内聚合写扩散，发出 `INVALIDATE` 前自动取消缓冲区中旧版本的待发写记录（因果消去）；
- 同步写直接携带序列化后的值（超限或不可序列化自动降级为 `REFRESH`），对端经 `KeyDispatcher` 按 Key 顺序归约、比对版本后直接应用，无需回源，一次广播即可收敛整个集群；
- 依赖版本比对、失效墓碑与硬 TTL 兜底实现最终一致性，无需承担 Paxos / Raft 等共识协议开销。

基准测试（`HotHitBenchmark`，Intel Ultra 9 285H，JDK 26）：

- `peek` **约 123M ops/s**（8.1 ns/op：Caffeine 查询 + 解包，无副作用）
- `get`（L1 命中）单线程**约 40M ops/s**（25.3 ns/op），8 线程聚合**约 99M ops/s**（80.8 ns/op；全路径含 TopK + Reporter）
- `get`（L1 命中，1KB String 值）**约 39M ops/s**（25.9 ns/op）

HotKey 受京东 [hotkey](https://gitee.com/jd-platform-opensource/hotkey) 项目启发，算法支持来自 [Aegis](https://github.com/go-kratos/aegis) 与 [neural](https://github.com/yu120/neural/tree/master)。

## 配置

### 单机App

需要 Java 17 与 Spring Boot 3.5.x。

```xml
<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>zeta</artifactId>
  <version>1.2.0</version>
</dependency>
```

`zeta.report.enabled` 默认是 `true`，但在这一步是空转的：没有 `spring-boot-starter-amqp` 就没有 `RabbitTemplate`，上报器不会被启用。

```yaml
zeta:
  local:
    top-k: 100 # 本地排行的席位数
    min-count: 10 # 进入 TopK 所需的最小访问次数
    cache:
      max-size: 100000 # 进程级 L1 上限
    hard-ttl-ms: 300000 # Caffeine 淘汰（Long.MAX_VALUE = 永久）
    soft-ttl-ms: 0 # 默认关闭后台刷新（-1 同样表示关闭）
    key-format: TYPED # TYPED（防碰撞）| PLAIN（cache:value，单标量类型）
    warm:
      enabled: true # 预热与续鲜总开关
      prewarm:
        trigger: OFF # OFF | LOCAL | CLUSTER | ANY；命名空间必须显式开启
      keep-warm:
        trigger: OFF # 与 soft-ttl 正交；命名空间必须显式开启
```

加上 `spring-boot-starter-actuator` 暴露端点：

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,hotkey,hotkeyring
```

```bash
curl -s localhost:8080/actuator/hotkey | jq '{topK: .local.topK, cacheSize: .local.cacheSize, total: .local.totalRequests}'
```

完整的属性、默认值与调优说明见 [CONFIG.zh.md](docs/CONFIG.zh.md)（§2–§9）；端点与指标的字段说明见 [MONITOR.zh.md](docs/MONITOR.zh.md)。

### 集群App

在单机基础上加上 AMQP starter：

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-amqp</artifactId>
</dependency>
```

然后把 App 指向 broker，并按需打开各条通路：

```yaml
spring:
  rabbitmq:
    host: ${RABBITMQ_HOST:localhost}
    port: 5672
    username: ${RABBITMQ_USER:zeta}
    password: ${RABBITMQ_PASSWORD:zeta}

zeta:
  app-name: order # 租户键 + 决策路由键，必须与 Worker 一致；[A-Za-z0-9_-]+
  report:
    enabled: true # 默认值
  worker-listener:
    enabled: true # 心跳 + HOT/COOL
  sync:
    enabled: true # 可选：跨实例 PUT/REFRESH/INVALIDATE
  local:
    instance-id: ${HOSTNAME}-1 # 每个实例唯一（同宿主多实例请加副本号/端口）
    node-id: 1 # 每个实例唯一；不设则取 instance-id 的 CRC32
```

完整属性见 [CONFIG.zh.md](docs/CONFIG.zh.md)（§6 上报队列、§7 Worker 监听与集群健康、§8 缓存同步）；端点字段见 [MONITOR.zh.md](docs/MONITOR.zh.md)。

### Worker

**方式 A —— compose，自带 broker**

```bash
mvn clean package -pl worker -am # 镜像会复制打包好的 JAR
docker compose -f worker/docker-compose.yml up -d --build
```

compose 会自动读取 `worker/.env`。`ZETA_APP_NAME` 必须与 App 的 `zeta.app-name` 一致——它决定这个 Worker 接收哪个应用的上报，同时Worker 向直连交换机 `zeta.send.exchange` 以 `send.<app-name>` 发布，每个 App 实例用同一个键绑定自己的队列。

```dotenv
ZETA_APP_NAME=myapp
ZETA_WORKER_TAG=1.2.0
RABBITMQ_USER=zeta
RABBITMQ_PASSWORD=change-me
```

**方式 B —— 独立容器，对接自有 broker**

```bash
docker run -d --name zeta-worker \
  -p 127.0.0.1:8080:8080 \
  -e SPRING_RABBITMQ_HOST=<rabbitmq-host> \
  -e SPRING_RABBITMQ_USERNAME=<user> \
  -e SPRING_RABBITMQ_PASSWORD=<password> \
  -e ZETA_APP_NAME=myapp \
  -e ZETA_WORKER_EPOCH_FILE=/var/lib/zeta/epoch \
  -v zeta_worker_epoch:/var/lib/zeta \
  ghcr.io/hyshmily/zeta-worker:1.2.0
```

**方式 C —— 自建 Worker 进程**

```java
@SpringBootApplication(scanBasePackages = { "com.example.worker", "io.github.hyshmily.zeta.worker" })
public class MyWorkerApplication {

  public static void main(String[] args) {
    SpringApplication.run(MyWorkerApplication.class, args);
  }
}
```

以下配置适用于自建 Worker 或直接运行的 jar：

```yaml
zeta:
  app-name: order # 必须与 App 的 zeta.app-name 一致
  report:
    enabled: false # Worker-only 节点不上报
  worker:
    enabled: true
    node-id: worker-1 # 稳定 id；队列与 epoch 文件名必须跨重启保持
    epoch-file: /var/lib/zeta/epoch # 必须能跨重启保留
    detection:
      defaults:
        hot-qps: 1000
        cool:
          strategy: COOL_FOR_LOW_QPS
          cool-qps: 100
          low-qps-for: 30s

management:
  endpoints:
    web:
      exposure:
        include: health
  endpoint:
    health:
      probes:
        enabled: true
```

发行包默认只暴露 `health`。只读检测视图需要显式开启：在
`management.endpoints.web.exposure.include` 加入 `hotkeyworker`，并设置
`management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker`，公开路径即为
`/actuator/hotkey-worker`（Boot 3.5 要求端点 id 为字母数字）。自建 Worker 同样按此方式开启。

Worker 侧全部属性见 [CONFIG.zh.md](docs/CONFIG.zh.md)（§10 消息入口、§11 检测、§12 调度）；运行时端点见 [MONITOR.zh.md](docs/MONITOR.zh.md)（§4 Worker 状态）。

### 验证

**1. Broker 拓扑。** Worker 会为每个分片声明一个持久上报队列
`zeta.reportToWorker.<appName>.<workerNodeId>`：

```bash
docker compose -f worker/docker-compose.yml exec rabbitmq rabbitmqctl list_queues name consumers messages
docker compose -f worker/docker-compose.yml exec rabbitmq \
  rabbitmqctl list_bindings source_name destination_name routing_key
```

队列的消费者数应等于 `zeta.worker.report-consumer.concurrent-consumers`（默认 8），消息数随流量增长。
绑定的源 exchange 必须是 `zeta.reportToWorker.exchange`；否则检查
[CONFIG.zh.md §10](docs/CONFIG.zh.md#10-worker-基础与消息入口) 中的两个 `report-exchange` 配置。

**2. App 能看到 Worker：**

```bash
curl -s localhost:8080/actuator/hotkey | jq '.worker'
```

`health` 应为 `healthy`，`msSinceLastAnyHeartbeat` 稳定在 1000 附近。若是 `-1`，说明从未收到心跳：broker 不对、`app-name` 不对，或 `worker-listener.enabled=false`。

**3. Worker 能看到上报**（需先开启 `hotkeyworker` 的 opt-in 暴露）：

```bash
docker compose -f worker/docker-compose.yml exec worker \
  curl -fsS 'localhost:8080/actuator/hotkey-worker?key=user:42'
```

`windowSum` 大于 0，或持续流量后出现 `hot: true`，说明上报已经到达。字段说明见
[MONITOR.zh.md](docs/MONITOR.zh.md) §4。

## 数据加载器

Zeta 不自带数据访问客户端，每个值都经提供的 loader 进入 L1。

### SPI

`io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader<K, V>`：

| 方法                       | 回答                                | 默认     |
| -------------------------- | ----------------------------------- | -------- |
| `V load(K key)`            | 值从哪来——前台未命中时调用          | 抽象方法 |
| `V reload(K key)`          | 刷新从哪取值——不在原调用栈上执行    | `load`   |
| `boolean isCacheable(V v)` | 引擎代 loader 取得的值是否可写入 L1 | `true`   |

```java
CacheLoader<String, User> userLoader = new CacheLoader<>() {
  @Override
  public User load(String key) {
    // 前台未命中：callerRunsLoader = true 时在原调用栈执行，可访问调用方事务/安全上下文
    return userRepository.findByKey(key);
  }

  @Override
  public User reload(String key) {
    // 后台刷新 / 预热 / 对端 REFRESH：不在原调用栈执行，必须无状态且线程安全
    // 返回 null 表示本次不替换，旧条目继续服务到 hardTtl 到期，不会把可服务值刷成未命中
    return userReadOnlyRepository.findSnapshotByKey(key);
  }

  @Override
  public boolean isCacheable(User value) {
    // 写入 L1 前的准入判定：false 仍返回给调用方但不落 L1；对端 PUT 携带的值同样经过这里
    // null 永不缓存，也不会回调本方法；不要在此处做阻塞 I/O
    return !value.isPlaceholder() && value.isActive();
  }
};

User user = zeta.get("user:1", userLoader);
```

| 消费者                                                    | 用它来                            | 没有它时                               |
| --------------------------------------------------------- | --------------------------------- | -------------------------------------- |
| `HotKeyHandler` 预热（`prewarmIfAbsent` / `prewarmWith`） | 在有效 HOT 租约上加载缺失条目     | 不预热；HOT 只触发业务 `onHot`         |
| `CacheSyncListener` → `DefaultSyncContext`                | 对端 `REFRESH` 后重新加载         | 退化为本地失效，下一次读取回源         |
| 同一个 context 的带值 `PUT`                               | 用 `isCacheable` 校验线上携带的值 | 无 loader 即无校验：携带的值按原样应用 |

### 编程式注册

注解之外，`ZetaWarmConfigurer` 在启动期登记命名空间的 loader 与预热/续鲜触发器；所有 configurer 执行完后注册表冻结，之后不再接受注册。

```java
@Configuration
public class WarmConfiguration {

  @Bean
  ZetaWarmConfigurer warmConfig(UserRepository userRepo, QuoteFeed quoteFeed, GenericSource fallbackSource) {
    return (registry) -> {
      // 类型化命名空间：框架按命名空间的 key 类型反解物理 Key，loader 直接收到 Long
      registry
        .namespace("users", Long.class)
        .format(KeyFormat.PLAIN) // 省略时继承注解声明或全局默认；与注解冲突在启动期失败
        .cacheLoader(
          new CacheLoader<Long, User>() {
            @Override
            public User load(Long id) {
              return userRepo.findById(id);
            }

            @Override
            public boolean isCacheable(User user) {
              return user.isActive();
            }
          }
        )
        .prewarm(HeatTrigger.CLUSTER) // OFF | LOCAL | CLUSTER | ANY
        .keepWarm(HeatTrigger.ANY) // 与 softTtl 独立，需显式开启
        .register();

      // 原始 Key 命名空间：loader 直接处理完整物理 Key，适合 PLAIN 多形态 Key 或自定义解析
      registry
        .namespaceRaw("quotes")
        .format(KeyFormat.PLAIN)
        .loader((physicalKey) -> quoteFeed.snapshot(physicalKey.substring("quotes:".length())))
        .prewarm(HeatTrigger.ANY)
        .keepWarm(HeatTrigger.CLUSTER)
        .register();

      // 兜底 loader：只对未归属任何已注册命名空间的 Key 生效（例如动态多租户前缀）
      registry.fallbackLoader((physicalKey) -> fallbackSource.load(physicalKey));
    };
  }
}
```

| 声明                        | loader 收到            | 适用场景                             |
| --------------------------- | ---------------------- | ------------------------------------ |
| `.namespace(name, K.class)` | 反解后的逻辑 key       | key 类型单一、可逆（常见标量、UUID） |
| `.namespaceRaw(name)`       | 完整物理 key（含前缀） | PLAIN 多形态 key、自定义解析         |
| `.fallbackLoader(loader)`   | 完整物理 key           | 动态多租户前缀等未注册命名空间       |

`.loader(Function)` 与 `.cacheLoader(CacheLoader)` 二选一：前者只提供值，后者同时拥有 `reload` 与 `isCacheable`。一个命名空间至多一个 loader；注册第二个、或 `format` 与注解声明冲突，都在启动期失败而不是覆盖。

## 广播监听与扩展

### 观察热度决策与同步事件

`HotKeyListener` 与 `SyncHook` 都以 `@Bean` 注册，可同时存在多个；单个回调抛异常只被隔离并记录，不会中断按 Key 保序的分发线程。

```java
@Configuration
public class ZetaObserverConfiguration {

  // 热度租约：Worker 授予/收回的完整生命周期
  @Bean
  public HotKeyListener heatLifecycleMonitor(MeterRegistry metrics) {
    return new HotKeyListener() {
      @Override
      public void onHot(WorkerDecision decision, boolean newlyHot) {
        // newlyHot == false 表示同一租约的重复广播（续租）
        if (newlyHot) {
          metrics.counter("zeta.hot.granted", "worker", decision.stamp().decisionNodeId()).increment();
        }
      }

      @Override
      public void onCool(HeatLease lease, HeatLease.EndReason reason) {
        // 覆盖全部终止原因：COOL 决策、Worker 实例死亡/重启、租约表容量淘汰、租约到期
        metrics.counter("zeta.hot.ended", "reason", reason.name()).increment();
      }

      @Override
      public void onHotSkipped(WorkerDecision decision, DecisionSkipReason reason) {
        log.debug("HOT skipped key={} reason={}", decision.cacheKey(), reason);
      }

      @Override
      public void onCoolSkipped(WorkerDecision decision, DecisionSkipReason reason) {
        log.debug("COOL skipped key={} reason={}", decision.cacheKey(), reason);
      }
    };
  }

  // 同步终态：每条经分发器处理的事件恰好命中五个回调之一
  @Bean
  public SyncHook syncAuditHook(MeterRegistry metrics) {
    return new SyncHook() {
      @Override
      public void afterValueApplied(SyncEvent event, EntryView entry) {
        metrics.counter("zeta.sync.applied", "type", event.type().name()).increment();
      }

      @Override
      public void afterValueDegraded(SyncEvent event, SyncSkipReason reason) {
        metrics.counter("zeta.sync.degraded", "reason", reason.name()).increment();
      }

      @Override
      public void afterInvalidate(SyncEvent event) {
        metrics.counter("zeta.sync.invalidated").increment();
      }

      @Override
      public void afterClear(List<SyncEvent> clears, int affectedEntries) {
        // 一批 clear 合并为一次表扫描后只回调一次
        log.info("Sync clear applied: batches={}, removed={}", clears.size(), affectedEntries);
      }

      @Override
      public void onSkipped(SyncEvent event, SyncSkipReason reason) {
        metrics.counter("zeta.sync.skipped", "reason", reason.name()).increment();
      }
    };
  }
}
```

| 监听器           | 方法                              | 触发时机                                                                                                                                                                                                       |
| ---------------- | --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HotKeyListener` | `onHot(decision, newlyHot)`       | 有效 HOT 租约被授予（重复广播时 `newlyHot` 为 false）                                                                                                                                                          |
| `HotKeyListener` | `onCool(lease, reason)`           | 活跃 HOT 租约结束：`COOL`、`INCARNATION_DEAD`、`EVICTED`、`EXPIRED`                                                                                                                                            |
| `HotKeyListener` | `onHotSkipped(decision, reason)`  | HOT 被拒绝（`VERSION_STALE`、`COVERED_BY_NEWER_DECISION`）                                                                                                                                                     |
| `HotKeyListener` | `onCoolSkipped(decision, reason)` | COOL 被拒绝（`VERSION_STALE`、`GHOST_COOL`、`COVERED_BY_NEWER_DECISION`）                                                                                                                                      |
| `SyncHook`       | `afterValueApplied`               | 已写入对端 REFRESH 回源值或带值 PUT                                                                                                                                                                            |
| `SyncHook`       | `afterValueDegraded`              | 值事件被降级为本地失效（`VALUE_NOT_FOUND`、`PAYLOAD_UNREADABLE`、`VALUE_DECLINED_BY_LOADER`、`DEGRADED_TO_INVALIDATE`、`ABSENT_NOT_PREWARMABLE`、`TTL_NOT_PREWARMABLE`、`REFRESH_REJECTED`、`REFRESH_FAILED`） |
| `SyncHook`       | `afterInvalidate`                 | 已应用对端 INVALIDATE                                                                                                                                                                                          |
| `SyncHook`       | `afterClear`                      | 一批 clear 完成一次合并扫描                                                                                                                                                                                    |
| `SyncHook`       | `onSkipped`                       | 事件未生效（`VERSION_STALE`、`INVALIDATION_TOMBSTONE`、`COVERED_BY_NEWER_EVENT`、`POLICY_REJECTED`、`POLICY_FAILED`）                                                                                          |

### 替换默认热度动作与同步策略

暴露 `HotKeyHandler` 改变决策对数据面的动作，暴露 `SyncPolicy` 定制对端同步准入；自动配置以 `@ConditionalOnMissingBean` 兜底，各取第一个 Bean。

```java
@Configuration
public class ZetaStrategyConfiguration {

  private static final String FLASH_NAMESPACE = "flash";
  private static final String USERS_TYPED = "users\u0000"; // TYPED 命名空间物理前缀
  private static final String ORDERS_TYPED = "orders\u0000";

  // 运行在按 Key 保序的分发线程，禁止同步阻塞 I/O
  @Bean
  public HotKeyHandler customHotKeyHandler(FlashSaleLoader flashSaleLoader) {
    return new HotKeyHandler() {
      @Override
      public void onHot(HotKeyContext context) {
        if (!context.isNewlyHot()) {
          return; // 同一租约的重复 HOT 不再动作
        }
        // namespace() 对 TYPED 与 PLAIN 两种编码都返回裸名字
        if (FLASH_NAMESPACE.equals(context.namespace())) {
          if (context.hasLiveEntry()) {
            context.armKeepWarm(); // 已驻留：为它开启本地续期
          } else {
            // 用指定无状态 loader 异步预热：跳过命名空间 prewarm 触发器，仍受静态 TTL 约束
            context.prewarmWith(flashSaleLoader);
          }
          return;
        }
        // 默认行为：未驻留则按命名空间策略异步预热；已驻留且满足 keepWarm 则纳入本地续鲜
        HotKeyHandler.super.onHot(context);
      }
      // onCool 无需覆写：默认实现会撤掉 keepWarm 触发器不再接受的任务，且 COOL 从不驱逐条目、不改 TTL
    };
  }

  @Bean
  public SyncPolicy customSyncPolicy() {
    return new SyncPolicy() {
      @Override
      public SyncAdmission admit(SyncEvent event, @Nullable EntryView current) {
        // clear 事件：current 恒为 null，prefix() == null 表示整表清理
        if (event.isClear()) {
          String prefix = event.prefix();
          return prefix == null || prefix.startsWith(USERS_TYPED) || USERS_TYPED.startsWith(prefix)
            ? SyncAdmission.PROCEED
            : SyncAdmission.SKIP;
        }

        String key = event.key();
        // 丢弃值路径转本地失效：orders 由本机回源，拒绝对端值但必须清掉旧副本。
        // 对 INVALIDATE 而言同一答案就是 PROCEED，所以这条规则可以安全地覆盖整个命名空间。
        if (event.isValue() && key != null && key.startsWith(ORDERS_TYPED)) {
          return SyncAdmission.INVALIDATE_LOCAL;
        }
        return key != null && key.startsWith(USERS_TYPED) ? SyncAdmission.PROCEED : SyncAdmission.SKIP;
      }

      @Override
      public SyncValueAction settle(SyncEvent event, Object resolvedValue, @Nullable EntryView current) {
        // 只在 PUT 解码成功或 REFRESH 回源成功后调用；写入前框架复检版本、墓碑与 isCacheable
        if (resolvedValue instanceof User user) {
          if (user.deletedAt() != null) {
            return SyncValueAction.INVALIDATE; // 软删除降级为按版本本地失效
          }
          if (!user.isComplete() && EntryView.isLive(current)) {
            return SyncValueAction.SKIP; // 保留未过期的完整副本
          }
          return SyncValueAction.apply(user.masked());
        }
        return new SyncValueAction.Apply(resolvedValue);
      }
    };
  }
}
```

| 接口            | 方法                                                                  | 框架负责                                                               |
| --------------- | --------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `HotKeyHandler` | `onHot(HotKeyContext)`, `onCool(HotKeyContext)`                       | 租约排序、预热守卫、每个已应用决策恰好一次监听器通知                   |
| `SyncPolicy`    | `admit(SyncEvent, EntryView)`, `settle(SyncEvent, Object, EntryView)` | 批次归约、版本/墓碑/clear 守卫、值解码/回源、每个事件恰好一次终态 Hook |

`admit` 对 key 批次只调用一次生效事件（被覆盖的消息直接以 `COVERED_BY_NEWER_EVENT` 报告），对 clear 逐条调用；过期/墓碑预检先于策略执行。

| `SyncAdmission`    | PUT / REFRESH                                       | INVALIDATE                     | INVALIDATE_ALL / INVALIDATE_PREFIX       |
| ------------------ | --------------------------------------------------- | ------------------------------ | ---------------------------------------- |
| `PROCEED`          | 解码/回源，随后 `settle`                            | 按事件版本逐出                 | 记录水位 + 一次合并扫描                  |
| `INVALIDATE_LOCAL` | 跳过解码/回源；本地失效（`DEGRADED_TO_INVALIDATE`） | 视为 `PROCEED`（本身已是删除） | 视为 `PROCEED`（本身已在本地扫描）       |
| `SKIP`             | 条目不变（`POLICY_REJECTED`）                       | 条目不变（`POLICY_REJECTED`）  | 该 clear 不参与扫描（`POLICY_REJECTED`） |

clear 基于前缀（`prefix() == null` 表示表级），`current` 恒为 `null`；被拒绝或失败的 clear 只跳过自身。策略抛异常或返回 `null` 按 `POLICY_FAILED` 结算，每条路径恰好触发一次 `SyncHook` 回调。

| `SyncValueAction` | 效果                                                               |
| ----------------- | ------------------------------------------------------------------ |
| `Apply(value)`    | 按事件版本写入，写入前复检守卫与 `isCacheable`；被拒绝时降级为失效 |
| `Invalidate()`    | 丢弃值，按版本本地失效（`DEGRADED_TO_INVALIDATE`）                 |
| `Skip()`          | 常驻条目不变（`POLICY_REJECTED`）                                  |

## 门面 API

`Zeta` Bean 提供 `get` / `put` / `invalidate` / `refresh` 及批量形式，以及 `stats()`、`detector()`、`unsafe()` 诊断视图。完整说明按主题索引：

- **读写、失效、刷新、TTL、同步范围与部署模式**：[CONFIG.zh.md](docs/CONFIG.zh.md)
- **Actuator 端点、统计与指标**：[MONITOR.zh.md](docs/MONITOR.zh.md)
- **声明式 API**：本文 [注解](#注解)
- **扩展点与回调**：本文 [广播监听与扩展](#广播监听与扩展)

## 注解

| 注解              | 目标         | 作用                                                                                                                                                          |
| ----------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@ZetaCached`     | M, A         | 读穿透：命中不执行方法体；未命中执行一次并缓存非 `null` 结果。`condition` 在 key 解析前绕过缓存；`Optional<T>` 底层存领域值。                                 |
| `@ZetaPut`        | M, A         | 写穿透：先执行方法体，再缓存非 `null` 结果。`condition` 在 key 解析前跳过写入；`Optional<T>` 底层存领域值；读取 `#result` 的 key 在方法体后解析。             |
| `@ZetaInvalidate` | M, A, 可重复 | 默认在方法体之后删除条目（condition 与 key 可用 `#result`）；`beforeInvocation = true` 改为先删除。`eachKey` 展开集合/数组，`allEntries` 清空整个 namespace。 |

`M` = 方法，`A` = 注解类型。

### `@ZetaConfig`

类级 `cache`/TTL/触发器默认值，按属性从最近的声明逐级合并：子类可以只覆盖 `hardTtl` 并继承父类的命名空间，方法上的非空值优先。

```java
// 类级默认值
@ZetaConfig(
  cache = "products",
  hardTtl = "300000",
  softTtl = "15000",
  prewarm = HeatTrigger.Option.CLUSTER,
  keepWarm = HeatTrigger.Option.ANY,
  keyFormat = KeyFormat.Option.PLAIN // 只对本类 cache 的继承方法生效
)
public abstract class BaseProductService {}

// 子类只覆盖 hardTtl，cache / softTtl / prewarm / keepWarm / keyFormat 继承父类
@ZetaConfig(hardTtl = "600000")
@Service
public class DetailedProductService extends BaseProductService {

  // 非空即覆盖："0" 回退到全局默认，"-1" 显式关闭软刷新；两者都不再继承类级值
  @ZetaCached(key = "#id", hardTtl = "0", softTtl = "-1")
  public Optional<Product> findRaw(Long id) {
    return productRepository.findById(id); // Optional.empty() 永不缓存
  }
}
```

方法上写了别的 `cache`，就不再继承本类的 `keyFormat`/TTL——那份默认值属于另一个命名空间。

### 组合注解

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ZetaCached(cache = "dict", hardTtl = "3600000", softTtl = "-1")
public @interface DictCached {
  @AliasFor(annotation = ZetaCached.class, attribute = "key")
  String key() default "";
}
```

`@AliasFor` 覆盖与直接写 `@ZetaCached` 等价，组合注解之间也可以叠加。

### `@ZetaCached`

```java
// 1. 默认组合 Key 与配置占位符
@Service
public class CatalogService {

  // cache / hardTtl / softTtl 都支持 ${...} 占位符（启动期一次性解析，可带默认值）
  // 省略 key：按 DefaultKey 规范编码全部参数（区分类型、Set 按元素排序、数组按内容编码）
  @ZetaCached(cache = "${app.cache.catalog:catalog}", hardTtl = "${app.cache.catalog-ttl:120000}")
  public PageResult<Product> listProducts(Long categoryId, Set<String> tags, int page) {
    return productRepository.findByCategory(categoryId, tags, page);
  }

  // 支持 #root.methodName / @beanName.method(...) / 位置变量 #p0(#a0)
  @ZetaCached(cache = "catalog_views", key = "#root.methodName + ':' + @tenantContext.current() + ':' + #p0")
  public ProductView view(Long productId) {
    return productRepository.findView(productId);
  }
}

// 2. 软过期 + 动态 TTL（stale-while-revalidate）
@Service
public class OrderReadService {

  // softTtl = 10s：软过期后立即返回旧值，同时只跑一个后台重算
  // hardTtl 是动态表达式：只在未命中或刷新时求值，命中路径不求值
  // 后台刷新通过反射调用目标方法，不经过缓存 advice 链；依赖请求态或调用方事务的方法不要配 softTtl
  @ZetaCached(cache = "orders", key = "#orderId", softTtl = "10000", hardTtl = "#isVip ? 120000 : 60000")
  public Order getOrder(String orderId, boolean isVip) {
    return orderRepository.findById(orderId);
  }
}

// 3. 读取前绕过 + 读取后否决
@Service
public class OrderGuardService {

  // condition 在 key 之前求值：orderId 为 null 时直接执行方法体（不查 L1、不触发空 key 报错）
  // unless 在方法返回非 null 后求值：#result 为拆包后的领域值；true 时正常返回但不写 L1
  // null 返回值永不缓存，无需在 unless 里重复判断 #result == null
  @ZetaCached(
    cache = "orders",
    key = "#orderId",
    condition = "#orderId != null && !#bypass",
    unless = "#result.status == 'PENDING'"
  )
  public Order getOrder(String orderId, boolean bypass) {
    return orderRepository.findById(orderId);
  }
}

// 4. PLAIN 格式 + 自注册后台 loader（命名空间内至多一个）
@ZetaConfig(cache = "products", keyFormat = KeyFormat.Option.PLAIN)
@Service
public class ProductWarmReadService {

  // selfWarmLoader = true：启动期把本方法绑定为 "products" 的后台 loader
  // 约束：key 必须可逆（无参常量 / 单标量参数 / 默认标量参数）、禁止 condition、unless 只能读 #result、预热需静态 TTL
  // 后台调用不经过调用方的切面链：方法上的事务/方法安全切面会在启动期以 WARN 点名，
  // 依赖它们的 body 应改用 ZetaWarmConfigurer 注册 loader
  @ZetaCached(
    key = "#id",
    prewarm = HeatTrigger.Option.CLUSTER,
    keepWarm = HeatTrigger.Option.ANY,
    selfWarmLoader = true
  )
  public Product getProduct(Long id) {
    return productRepository.findById(id).orElse(null);
  }
}
```

同一命名空间也可以改用 [编程式注册](#编程式注册) 绑定 loader，二者只能有其一。

### `@ZetaPut`

```java
// 5. 插入后回写自增主键，并按结果决定 TTL 与否决
@Service
public class AccountWriteService {

  // 读取 #result 的 key 在方法体返回后求值，因此可用 #result.id 读取数据库生成的主键
  // unless 为 true 时跳过写入并正常返回；#result.id 为 null 同样跳过
  // sync = true（默认）：写值广播给对等节点直接应用；此模式下 hardTtl 不允许解析为 Long.MAX_VALUE
  @ZetaPut(
    cache = "accounts",
    key = "#result.id",
    unless = "#result.status != 'ACTIVE'",
    hardTtl = "#result.isVip() ? 3600000 : 600000",
    softTtl = "30000"
  )
  public Account createAccount(AccountDraft draft) {
    Account account = new Account(draft.getUsername(), draft.getEmail());
    accountRepository.insert(account); // 返回时 account.id 已回填
    return account;
  }

  // 6. 单机本地写：sync = false 不发广播，允许永久条目
  // condition 在方法体前求值；Optional<UserSession> 底层只写 UserSession，空 Optional 跳过写入
  @ZetaPut(
    cache = "user_sessions",
    key = "#token",
    condition = "#token != null",
    sync = false,
    hardTtl = "1800000",
    softTtl = "-1"
  )
  public Optional<UserSession> refreshLocalSession(String token) {
    return sessionClient.fetchRemoteSession(token);
  }
}
```

### `@ZetaInvalidate`

```java
// 7. 前置失效、批量展开、整命名空间清空
@Service
public class InventoryWriteService {

  // beforeInvocation = true：方法体执行前删除，且不受事务 afterCommit 延迟影响；此时 key/condition 不可用 #result
  @ZetaInvalidate(cache = "inventory", key = "#skuId", beforeInvocation = true, scope = InvalidatePolicy.Scope.BOTH)
  public void deductStock(Long skuId, int quantity) {
    inventoryRepository.decrease(skuId, quantity);
  }

  // eachKey = true：把数组或 Iterable 展开为逐个物理 Key（单次上限 1024，需显式声明 key）
  @ZetaInvalidate(cache = "inventory", key = "#skuIds", eachKey = true, condition = "#skuIds != null")
  public void deleteAll(long[] skuIds) {
    inventoryRepository.deleteAll(skuIds);
  }

  // allEntries = true：清空整个命名空间，与 key / eachKey 互斥
  @ZetaInvalidate(cache = "inventory", allEntries = true)
  public void rebuildInventory() {
    inventoryRepository.rebuild();
  }
}

// 8. 可重复注解 + 后置 #result 安全导航 + 仅清远端
@Service
public class ConfigWriteService {

  // 每个 @ZetaInvalidate 独立拥有 cache / key / condition / scope
  // 后置失效（默认）：方法抛异常则不删；用 #result?.key 容忍 null 返回（解析为 null 时静默跳过）
  // scope = REMOTE：本机已更新，只通知远端节点丢弃副本
  @ZetaInvalidate(
    cache = "config",
    key = "#result?.configKey",
    condition = "#result != null && #result.published",
    scope = InvalidatePolicy.Scope.REMOTE
  )
  @ZetaInvalidate(
    cache = "config_by_group",
    key = "#result != null ? #result.groupCode : #form.groupCode",
    scope = InvalidatePolicy.Scope.BOTH
  )
  public ConfigItem updateConfig(ConfigUpdateForm form) {
    return configRepository.update(form);
  }
}
```

## 监控

Zeta 提供两种监控机制：Actuator 端点（`/actuator/hotkey`、`/actuator/hotkeyring`、`/actuator/hotkey-worker`）与 Micrometer 指标。

端点受 `management.endpoints.web.exposure.include` 控制，默认只暴露 `health`：App 侧需显式加入 `hotkey,hotkeyring`；Worker 侧默认只暴露 `health`，如需只读检测视图，再显式加入 `hotkeyworker` 并配置 `management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker`（Boot 3.5 的端点 id 必须为字母数字，公开 URL 仍保留连字符）。

完整响应格式和字段说明见 [MONITOR.zh.md](docs/MONITOR.zh.md)。

## License

Apache License 2.0
