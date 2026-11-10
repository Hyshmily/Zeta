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

Zeta 是面向 Spring Boot 的轻量级分布式缓存与预热框架。key 无需预注册即可实现热点发现、决策、预热与降级,旨在以最低成本解决任意突发热点在集群内的一致性缓存问题。

> [!NOTE]
>
> Zeta 1.2.0 即将发布。

## 本地/集群检测

```java
private final Zeta zeta;

// 在同一命名空间中标记 key
zeta.tag("product:123");

// 获取 key
zeta.peek("product:123");
```

- **本地层（默认）：** 每个实例运行 HeavyKeeper TopK 草图。key 进入 TopK 集合即自动延长其 L1 Caffeine TTL,无需 Worker 反馈。L1 未命中则合并并发请求；软过期条目回陈旧值，同时后台刷新。
- **集群层（可选）：** 实例计数上报至 RabbitMQ，由 Worker 集群评估。Worker 为每个 key 维护滑动窗口，并把窗口和与当前规则的 `hotCount`/`coolCount` 比较（regex、前缀或精确规则，按优先级排序）；命中的精确规则按自己的阈值判定，其余 key 受提升下限约束。HOT/COOL 决策会广播回所有 App 实例。

## 多节点缓存一致性

与主从数据库通过日志复制协议同步写入类似，Zeta 通过基于 RabbitMQ 的发布-订阅机制在各应用实例间同步缓存变更。

任一实例执行集群同步写（`put(key, value, PutPolicy.synced())`）或失效操作时，由各节点本地无锁的 `StampClock` 为每个 key 分配 `dataVersion`，并将结果广播给所有对等实例,同步写直接携带序列化后的值，对端比对版本后直接应用、无需回源，一次发送即可收敛整个集群。

版本比对机制确保最终一致性，无需承担 Paxos 或 Raft 等共识协议的开销。

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
    hard-ttl-ms: 300000 # 普通 key：Caffeine 淘汰
    soft-ttl-ms: 30000 # 普通 key：stale-while-revalidate
    hot-hard-ttl-ms: 3600000 # Local-Hot key
    hot-soft-ttl-ms: 300000 # Local-Hot key
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
    instance-id: ${HOSTNAME} # 队列名与 node-id 由它派生
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

| 方法                       | 回答                                 | 默认     |
| -------------------------- | ------------------------------------ | -------- |
| `V load(K key)`            | 值从哪来——前台未命中时调用           | 抽象方法 |
| `V reload(K key)`          | 后台刷新从哪取值——不在原调用栈上执行 | `load`   |
| `boolean isCacheable(V v)` | 引擎代 loader 取得的值是否可写入 L1  | `true`   |

```java
// reader —— 便捷形态，非 null 即缓存
User a = zeta.get("user:1", () -> repo.findUser(1), GetPolicy.defaults());

// loader —— 同样的调用，外加一个准入决定
CacheLoader<String, User> loader = new CacheLoader<>() {
  @Override
  public User load(String key) {
    return repo.findUser(key);
  }

  @Override
  public boolean isCacheable(User v) {
    return !v.isPlaceholder();
  }
};

User b = zeta.get("user:1", loader, GetPolicy.defaults());
```

| 消费者                                             | 用它来                            | 没有它时                                                           |
| -------------------------------------------------- | --------------------------------- | ------------------------------------------------------------------ |
| `WorkerListener` → `DefaultWorkerContext`  | 提升 key 为 HOT 前加载权威值      | 提升 L1 中已有的值；无可用条目时该 HOT 被跳过（`VALUE_NOT_FOUND`） |
| `CacheSyncListener` → `DefaultSyncContext` | 对端 `REFRESH` 后重新加载         | 退化为本地失效，下一次读取回源                                     |
| 同一个 context 的带值 `PUT`                        | 用 `isCacheable` 校验线上携带的值 | 无 loader 即无校验：携带的值按原样应用                             |

**自定义 per-entry TTL**

Zeta 使用**差异化 TTL**：热点 key 和普通 key 分别有独立默认值。

| Key 状态 | 硬 TTL（Caffeine 淘汰） | 软 TTL（stale-while-revalidate） |
| -------- | ----------------------- | -------------------------------- |
| 普通     | `hard-ttl-ms`（5min）   | `soft-ttl-ms`（30s）             |
| 热点     | `hot-hard-ttl-ms`（1h） | `hot-soft-ttl-ms`（5min）        |

> [!NOTE]
> **缓存雪崩防护：** `TtlPolicy` 通过其内部 `computeTtlJitter()` 对每个过期时间戳施加均匀随机偏移（默认 ±5%）。5 分钟硬 TTL 在默认偏移下实际到期 4.75 ~ 5.25 分钟。通过 `zeta.local.ttl-jitter-ratio`（比例，默认 `0.05` = ±5%，`0` 为禁用）控制。

## Worker 监听

**通过 Hook 观察广播决策**

实现 `WorkerDecisionHook` 或 `SyncHook` 接口即可观察广播处理结果：

```java
@Bean
public WorkerDecisionHook myMonitorHook() {
  return new WorkerDecisionHook() {
    @Override
    public void afterHotPromotion(WorkerDecision decision, CacheEntry entry) {
      metrics.counter("hot.promotion").increment();
    }

    @Override
    public void onHotSkipped(WorkerDecision decision, HotSkipReason reason) {
      log.warn("HOT 跳过 {}：{}", decision.cacheKey(), reason);
    }
  };
}
```

| Hook                 | 方法                 | 触发时机                                                                                           |
| -------------------- | -------------------- | -------------------------------------------------------------------------------------------------- |
| `WorkerDecisionHook` | `afterHotPromotion`  | Key 被提升为 HOT（扩展 TTL）                                                                       |
| `WorkerDecisionHook` | `afterCoolDowngrade` | Key 恢复为普通 TTL                                                                                 |
| `WorkerDecisionHook` | `onHotSkipped`       | HOT 被跳过（`VERSION_STALE` 版本过旧 / `VALUE_NOT_FOUND` 无可用值 / `POLICY_REJECTED` 自定义拒绝） |
| `WorkerDecisionHook` | `onCoolSkipped`      | COOL 被跳过（L1 中无对应 entry、版本过旧，或被同一批中更高版本的 COOL 覆盖）                       |
| `SyncHook`           | `afterRefresh`       | 已应用对端 REFRESH 回源或带值 PUT                                                                  |
| `SyncHook`           | `afterInvalidate`    | 因对端广播而从 L1 移除 key                                                                         |
| `SyncHook`           | `onRefreshSkipped`   | REFRESH 或 PUT 未生效（版本过旧、值缺失、被失效水位拒绝，或被同批更高版本覆盖）                    |

**替换默认的广播处理逻辑**

```java
@Bean
@ConditionalOnMissingBean(WorkerPolicy.class)
public WorkerPolicy myPolicy() {
  return new WorkerPolicy() {
    @Override
    public HotAction hot(WorkerDecision decision, HotValue value) {
      return myPolicy(value.value())
        ? new HotAction.Promote(value)
        : new HotAction.Skip(HotSkipReason.POLICY_REJECTED);
    }

    // cool 继承默认：返回 true
  };
}
```

| 接口           | 方法                                                                                                        | 框架负责                                                                                       |
| -------------- | ----------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `WorkerPolicy` | `hot(WorkerDecision, HotValue)`, `cool(WorkerDecision)`，默认都走标准流水线                                | per-key 调度、`resolveHot`/`settleHot`/`cool`/`skip`、版本/墓碑守卫、Hook                   |
| `SyncPolicy`   | `value(Batch.Value, SyncContext)`, `invalidate(Batch.Value, SyncContext)`, `clear(Batch.Clear, SyncContext)` | 批次归约、`value`/`invalidate`/`clear`/`skip`、版本/墓碑守卫、clear watermark、Hook          |

## 注解

| 注解              | 目标 | 作用                                                                                                                                                                                                                                                                                                                                                                                                      |
| ----------------- | ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@ZetaCached`     | M    | 读穿透：命中直接返回、不执行方法体；未命中执行一次并存储非 `null` 结果。方法体即 loader，因此软过期（stale-while-revalidate）与 SingleFlight 合并同样生效。key 与 TTL 在方法体之前解析，因此不可使用 `#result`；未知 SpEL 变量、保留参数名（`root`、`this`、`result`、`pN`、`aN`；方法只要编译了任一表达式就会校验）与超出 `long` 范围的纯数字 TTL 会在启动期失败。                                       |
| `@ZetaPut`        | M    | 写穿透：先执行方法体，再写入非 `null` 结果。不读取 `#result` 的 key 在方法体之前解析（空参数、空 key 在事务前失败）；同步写入的动态 hard TTL 会在方法体前做 permanent+sync 预检。其余 TTL 与 `unless` 在方法体返回、结果非 `null` 且未被否决后求值。`sync = false` 表示只写本实例。                                                                                                                       |
| `@ZetaInvalidate` | M    | 删除条目：默认在方法体之后执行，因此 key 可使用 `#result`；不读取 `#result` 的 key 在方法体之前解析。读取 `#result` 的 key 交给 SpEL 求值：方法体可能返回 `null` 时请用 `#result?.id`（结果为 `null` 即本次不删除），直接写 `#result.id` 遇到 `null` 会失败；带自身空值兜底的表达式仍会执行。`beforeInvocation = true` 改为方法体之前（立即）执行，此时不可使用 `#result`。`scope` 选择本地、远端或两者。 |

### `@ZetaCached`

```java
// 1. 默认 Key 与配置占位符（基础读取）
@Service
public class ProductService {

  // cache 支持 ${...} 占位符（可带默认值），在方法解析时一次性替换
  // 省略 key：按 (categoryId, page, size) 的规范编码自动生成组合键
  @ZetaCached(cache = "${app.cache.catalog-namespace:catalog}")
  public PageResult<Product> listProducts(Long categoryId, int page, int size) {
    return productRepository.findByCategory(categoryId, page, size);
  }
}

// 2. 软过期 + 动态 TTL（stale-while-revalidate）
@Service
public class RankingService {

  // VIP 与普通用户的榜单分开缓存：VIP 120s，普通用户 60s（单位毫秒）
  // softTtl=10s：软过期后立即返回旧榜单，同时只跑一个后台重算任务
  // 后台刷新直接反射调用方法体，不经过事务/安全拦截器；依赖请求作用域状态的方法不要配置 softTtl
  @ZetaCached(
    cache = "rankings",
    key = "#region + ':' + #isVip",
    softTtl = "10000",
    hardTtl = "#isVip ? 120000 : 60000"
  )
  public RankList getTopRankings(String region, boolean isVip) {
    return rankingCalculator.calculateTop100(region, isVip);
  }
}

// 3. 读取后条件否决（unless）
@Service
public class OrderService {

  // unless 在方法体返回后求值：#result 为返回值；为 true 时照常返回给调用方，但本次结果不写缓存
  // 返回 null 本来就不会写缓存；PENDING 订单由 unless 额外否决，下次读取继续穿透到数据库
  @ZetaCached(cache = "orders", key = "#orderId", unless = "#result == null || #result.status == 'PENDING'")
  public Order getOrder(String orderId) {
    return orderRepository.findById(orderId);
  }
}
```

### `@ZetaPut`

```java
// 4. 插入后回写自增主键（@ZetaPut + #result）
@Service
public class AccountService {

  // 读取 #result 的 key（此处）在方法体返回后求值，因此可用 #result.id 读取数据库生成的主键；
  // 不读取 #result 的 key 在方法体之前求值，不会等事务提交后才暴露配置错误。
  // TTL 在确认要写入时求值；同步写入的动态 hard TTL 会在方法体前做 permanent+sync 预检。
  // sync = true（默认）：写值通过消息通道广播给对等节点，对等节点直接应用、不重新回源
  @ZetaPut(cache = "accounts", key = "#result.id", hardTtl = "3600000")
  public Account createAccount(AccountDraft draft) {
    Account account = new Account(draft.getUsername(), draft.getEmail());
    accountRepository.insert(account); // 返回时 account.id 已回填
    return account;
  }
}

// 5. 单机本地写（关闭集群广播）
@Service
public class SessionService {

  // sync = false：只更新当前节点的 L1，不发送同步消息；对等节点保留各自副本直到 hardTtl 到期
  @ZetaPut(cache = "user_sessions", key = "#token", sync = false, hardTtl = "1800000")
  public UserSession refreshLocalSession(String token) {
    return sessionClient.fetchRemoteSession(token);
  }
}
```

### `@ZetaInvalidate`

```java
// 6. 方法执行前强制失效（@ZetaInvalidate 前置）
@Service
public class InventoryService {

  // 默认后置失效：方法抛异常时缓存保持不变；beforeInvocation = true 改为方法体前删除，此时 key 不能用 #result
  // scope = BOTH（默认）：删除本地副本并通知远端节点一起删除
  @ZetaInvalidate(cache = "inventory", key = "#skuId", beforeInvocation = true, scope = InvalidatePolicy.Scope.BOTH)
  public void deductStock(Long skuId, int quantity) {
    inventoryRepository.decrease(skuId, quantity);
  }
}

// 7. 只清理远端副本（@ZetaInvalidate 指定 scope）
@Service
public class BroadcastService {

  // scope = REMOTE：本节点已通过其他路径（如 binlog 回放）刷新，只需通知远端节点丢弃副本
  // 默认后置失效，key 可用 #result；方法体可能返回 null 时请写 "#result?.id"
  // （key 为 null 即本次不删除），"#result != null ? #result.id : #fallbackId" 这类兜底表达式仍会执行
  @ZetaInvalidate(cache = "config", key = "#result.configKey", scope = InvalidatePolicy.Scope.REMOTE)
  public ConfigItem updateRemoteCluster(ConfigUpdateForm form) {
    return configRepository.update(form);
  }
}
```

## 监控

Zeta 提供两监控机制：Actuator 端点（`/actuator/hotkey`、`/actuator/hotkeyring`、`/actuator/hotkey-worker`）与 Micrometer 指标。

端点受 `management.endpoints.web.exposure.include` 控制，默认只暴露 `health`：App 侧需显式加入 `hotkey,hotkeyring`；Worker 侧默认只暴露 `health`，如需只读检测视图，再显式加入 `hotkeyworker` 并配置 `management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker`（Boot 3.5 的端点 id 必须为字母数字，公开 URL 仍保留连字符）。

完整响应格式和字段说明见 [MONITOR.zh.md](docs/MONITOR.zh.md)。

## License

Apache License 2.0
