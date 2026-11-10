[← 返回首页](../README.zh.md)

# 监控

Zeta 提供两种互补的监控面：

- **Actuator 端点**
- **Micrometer 指标**

## 1. 暴露方式

**前置条件。** 端点需要 `spring-boot-starter-actuator`；指标需要 `micrometer-core` 加具体注册表实现。没有 `MeterRegistry` 时 Worker 以无指标方式运行，而不是启动失败。

这些端点是 Actuator `@Endpoint` Bean，不是 MVC Controller：它们受 `management.endpoints.web.exposure.include` 控制，并运行在管理平面（端口、暴露范围与角色都走 `management.*`）。端点 id 不能嵌套，因此所有路径都是扁平的。

```yaml
# 应用进程
management:
  endpoints:
    web:
      exposure:
        include: health,hotkey,hotkeyring
  endpoint:
    hotkey:
      roles: ADMIN # 仅在存在 Spring Security 时生效
    hotkeyring:
      roles: ADMIN

# Worker 进程（默认只暴露 health）
management:
  endpoints:
    web:
      exposure:
        include: health
  endpoint:
    health:
      probes:
        enabled: true
      group:
        liveness:
          include: livenessState,ping # 不含 RabbitMQ
        readiness:
          include: readinessState,rabbit

# 仅按需开启：把上面的 include 改为 `health,hotkeyworker`，再添加：
# management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker
# management.endpoint.hotkeyworker.roles: ADMIN
```

> `zeta-worker` 发行包（jar/镜像）默认只暴露 `health`。检测视图需要显式开启：将 `hotkeyworker` 加入 include、配置路径映射（使用 Spring Security 时再配置端点角色）。Boot 3.5 要求端点 id 为字母数字，因此 id 是 `hotkeyworker`，公开 URL 仍为 `/actuator/hotkey-worker`。应用侧始终需要自己配置 include。

**Worker 的健康检查分为两个探针，按依赖切分。** liveness（`/actuator/health/liveness`）只包含
`livenessState,ping`，刻意排除 RabbitMQ，因此 broker 断连不会重启 Worker——重启会丢掉它的内存窗口并抬高
epoch。readiness（`/actuator/health/readiness`）额外包含 `rabbit` 指标：它为 `DOWN` 表示 Worker 无法消费
上报、应被摘出服务，此时 liveness 仍为 `UP`。`zeta-worker` 镜像的 `HEALTHCHECK` 调用
`/actuator/health/liveness`；编排器或负载均衡器应探测 `/actuator/health/readiness`。聚合的
`/actuator/health` 不是容器探针。

| 端点                      | 方法  | 服务方 | 创建条件                                                       | 回答的问题                             | CONFIG                                         |
| ------------------------- | ----- | ------ | -------------------------------------------------------------- | -------------------------------------- | ---------------------------------------------- |
| `/actuator/hotkey`        | `GET` | 应用   | classpath 中有 Actuator；每个区块均可选                        | 检测、缓存、上报、健康、同步的实时计数 | [§5](CONFIG.zh.md#5-上报节拍)                  |
| `/actuator/hotkeyring`    | `GET` | 应用   | `zeta.local.consistent-hashing.enabled=true`（默认）+ Actuator | 哈希环拓扑与 key 路由                  | [§7](CONFIG.zh.md#7-worker-监听心跳与集群健康) |
| `/actuator/hotkey-worker` | `GET` | Worker | Worker + Actuator + 显式开启 `hotkeyworker` 暴露               | 检测摘要、单 key 窗口状态与规则计数    | [§11](CONFIG.zh.md#11-检测窗口规则容量与冷却)  |

## 2. 应用运行诊断：`/actuator/hotkey`

当某个 key 意外变热/变冷、上报疑似丢失，或读取比预期更陈旧时，先调用这个端点。它把应用的检测、缓存、上报、刷新、版本与同步状态聚合在一个响应里。

### 2.1 请求模板

```bash
APP=http://localhost:8080

# 完整快照
curl -s "$APP/actuator/hotkey" | jq

# 限制 TopK 与 staleDebt 列表长度
curl -s "$APP/actuator/hotkey?limit=20" | jq

# 只投影告警或看板使用的信号
curl -s "$APP/actuator/hotkey" \
  | jq '{health: .worker.health,
         heartbeatAgeMs: .worker.msSinceLastAnyHeartbeat,
         hotKeys: [.local.topK[].key],
         reportDepth: .local.reportQueueDepth,
         reportDropped: .local.reportQueueFullCount}'
```

`limit` 限制 `topK` 与 `staleDebt`。默认 `100`；小于 `1` 的值按 `1` 处理，`staleDebt` 最多返回 100 条。

### 2.2 响应结构

示例展示所有平面都已接线的应用。组件缺失时，整个区块或单个字段会消失（见 §2.4）。

```jsonc
{
  "instanceId": "a1b2c3d4", // zeta.local.instance-id（或其派生回退值）
  "jvmId": 7263819201938475, // JVM 进程级随机标识
  "local": {
    "topK": [{ "key": "cache:shop:17", "add": 1523 }], // 本地 HeavyKeeper 排名
    "totalRequests": 158392,
    "cacheSize": 87, // L1 预估条目数
    "inFlightSize": 3, // 正在执行的加载器数
    "reportQueueDepth": 0,
    "reportQueueFullCount": 0,
    "reportPendingKeys": 0, // 访问计数器中缓冲的 key 数
    "refreshPoolAvailable": 100,
    "staleDebt": { "k1": 95000 }, // key -> 当前租约开始至今的毫秒数
    "versionClockSkewMillis": 12,
  },
  "worker": {
    "health": "healthy",
    "msSinceLastAnyHeartbeat": 1234, // -1 = 从未收到心跳
    "foreignAppDrops": 0,
    "lastForeignApp": "appB",
    "coolDispatchRejections": 0,
  },
  "sync": {
    "foreignAppDrops": 0,
    "lastForeignApp": "appB",
    "nodeIdCollisions": 0,
  },
}
```

### 2.3 主题

**身份**

| 字段         | 含义                                                                       | CONFIG                    |
| ------------ | -------------------------------------------------------------------------- | ------------------------- |
| `instanceId` | 用于队列命名与版本戳的实例标识；改变 `zeta.local.instance-id` 时随之改变。 | [§1](CONFIG.zh.md#1-部署) |
| `jvmId`      | 进程级随机标识；每次重启都会变化。                                         | —                         |

**本地：检测、缓存与去重**

| 字段            | 含义                                                                              | CONFIG                                |
| --------------- | --------------------------------------------------------------------------------- | ------------------------------------- |
| `topK[]`        | 当前本地排名，按频率降序。每项为 `{ "key": ..., "add": ... }`，受 `?limit` 限制。 | [§2](CONFIG.zh.md#2-本地热点检测topk) |
| `totalRequests` | 衰减调整后的跟踪访问量；流量回落时会下降，不是累计访问次数。                      | [§2](CONFIG.zh.md#2-本地热点检测topk) |
| `cacheSize`     | 单张 L1 Caffeine 表的预估条目数。                                                 | [§3](CONFIG.zh.md#3-l1-缓存与-ttl)    |
| `inFlightSize`  | 当前正在执行的加载器数——不是 SingleFlight 去重表的大小。                          | [§4](CONFIG.zh.md#4-加载并发与刷新)   |

**本地：上报**

| 字段                   | 含义                                                                                         | CONFIG                        |
| ---------------------- | -------------------------------------------------------------------------------------------- | ----------------------------- |
| `reportQueueDepth`     | Reporter 分发队列中等待的批次数。持续上升说明 Reporter 正在落后。                            | [§6](CONFIG.zh.md#6-上报队列) |
| `reportQueueFullCount` | 分发队列持续满超过 `queue-offer-timeout-ms` 后被丢弃，或 dispatcher 停机时撤回的累计批次数。 | [§6](CONFIG.zh.md#6-上报队列) |
| `reportPendingKeys`    | 共享访问计数器中缓冲、等待下一轮 tide 的 key 数。                                            | [§5](CONFIG.zh.md#5-上报节拍) |

**本地：刷新与版本**

| 字段                     | 含义                                                                                                                          | CONFIG                              |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `refreshPoolAvailable`   | 后台刷新限流器的可用许可数。`0` 表示软过期刷新正在线程池中排队。                                                              | [§4](CONFIG.zh.md#4-加载并发与刷新) |
| `staleDebt`              | 当前被租约持有的 key，按最旧优先（最多 100 条）：key → 当前租约开始至今的毫秒数。它是"有界陈旧"的审计面。                     | [§4](CONFIG.zh.md#4-加载并发与刷新) |
| `versionClockSkewMillis` | 最新已知时间戳（本地 stamp clock 或观测到的对端时间戳，含被拒绝的离群值）领先本地物理时间的毫秒数。持续增长说明存在时钟漂移。 | [§1](CONFIG.zh.md#1-部署)           |

**Worker：集群健康与决策隔离**

| 字段                      | 含义                                                                                                                                                | CONFIG                                         |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| `health`                  | 心跳视图通过集群闸门时为 `healthy`，否则为 `unhealthy`。                                                                                            | [§7](CONFIG.zh.md#7-worker-监听心跳与集群健康) |
| `msSinceLastAnyHeartbeat` | 距最近一次任意 Worker 心跳的毫秒数；`-1` 表示从未收到。                                                                                             | [§7](CONFIG.zh.md#7-worker-监听心跳与集群健康) |
| `foreignAppDrops`         | 因 `appName` 头不匹配而累计丢弃的 HOT/COOL 决策数。                                                                                                 | [§7](CONFIG.zh.md#7-worker-监听心跳与集群健康) |
| `lastForeignApp`          | 最近一次外来 `appName`；仅在发生丢弃后出现。                                                                                                        | [§7](CONFIG.zh.md#7-worker-监听心跳与集群健康) |
| `coolDispatchRejections`  | 有序 dispatcher 在提交时拒收的 COOL 决策数（每 key 队列已满或已关闭）。每条都在消费线程上就地补执行；持续增长是 dispatcher 饱和信号，不是数据丢失。 | [§7](CONFIG.zh.md#7-worker-监听心跳与集群健康) |

**同步**

| 字段               | 含义                                                                                                     | CONFIG                        |
| ------------------ | -------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `foreignAppDrops`  | 因 `appName` 不匹配而累计丢弃的同步消息数。                                                              | [§8](CONFIG.zh.md#8-缓存同步) |
| `lastForeignApp`   | 最近一次外来同步发送方；仅在发生丢弃后出现。                                                             | [§8](CONFIG.zh.md#8-缓存同步) |
| `nodeIdCollisions` | 与本节点共享 `dataVersion` node id 的对端发来的同步消息数。持续增长说明应显式配置 `zeta.local.node-id`。 | [§8](CONFIG.zh.md#8-缓存同步) |

### 2.4 哪些部分会出现，哪些被刻意省略

| 部分                                                                 | 出现条件                                                                            |
| -------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `local`                                                              | 至少存在一个应用端组件（纯 Worker 进程没有）                                        |
| `local.topK`、`local.totalRequests`                                  | 存在应用端 TopK（`zeta.worker.enabled=false`）                                      |
| `local.cacheSize` / `local.inFlightSize`                             | 存在 L1 缓存 / SingleFlight                                                         |
| `local.reportQueueDepth`、`reportQueueFullCount`                     | Reporter 已接线（`zeta.report.enabled=true`、classpath 中有 RabbitMQ、非纯 Worker） |
| `local.reportPendingKeys`                                            | 存在共享访问计数器（否则回退到 Reporter）                                           |
| `local.refreshPoolAvailable`、`staleDebt`                            | 存在后台刷新器                                                                      |
| `worker.health`、`worker.msSinceLastAnyHeartbeat`                    | 存在 `HealthView`（始终存在；在应用侧才有意义）                                     |
| `worker.foreignAppDrops`、`lastForeignApp`、`coolDispatchRejections` | `zeta.worker-listener.enabled=true`                                                 |
| `sync.*`                                                             | `zeta.sync.enabled=true`，且同步监听器或默认决策处理器已接线                        |

## 3. 哈希环巡检：`/actuator/hotkeyring`

哈希环决定一条上报由哪个 Worker 接收。当上报落到错误的 Worker、Worker 重启后路由不变，或怀疑 `min-alive-workers` 降级时，使用这个端点。

端点在返回前会用健康视图重建哈希环，因此在上一次上报 flush 之后死亡的 Worker 已经不在列表中；检查故障转移不需要制造额外流量。

**开关。** `zeta.local.consistent-hashing.enabled=true`（默认）会创建端点；它只控制巡检能力——即使关闭端点，上报路由仍始终使用哈希环（[CONFIG §7](CONFIG.zh.md#7-worker-监听心跳与集群健康)）。上报平面必须已接线，否则每次调用都返回 **503**，并在消息中说明原因。

### 3.1 请求模板

```bash
APP=http://localhost:8080

# 1. 拓扑：当前有哪些 Worker 存活
curl -s "$APP/actuator/hotkeyring" | jq

# 2. 某个 key 由哪个 Worker 负责
curl -s "$APP/actuator/hotkeyring/cache:shop:17" | jq

# 3. 含 '/' 的 key 作为剩余路径段直接传入
curl -s "$APP/actuator/hotkeyring/api/v1/product:42" | jq
```

响应示例：

```json
{
  "nodes": ["worker-1", "worker-2", "worker-3"]
}
```

```json
{
  "key": "cache:shop:17",
  "nodeId": "worker-2"
}
```

## 4. Worker 检测运行时：`/actuator/hotkey-worker`

**前提。** Worker 模式（`zeta.worker.enabled=true`，需 classpath 上有 RabbitMQ）、引入 Actuator，
并显式开启：发行包默认只暴露 `health`。将 `hotkeyworker` 加入 web exposure include 并设置
`management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker` 后才会发布该端点。只读：
检测配置在启动时一次性校验，没有运行时写入口。

### 4.1 汇总

```bash
WORKER=http://worker:8080
curl -s "$WORKER/actuator/hotkey-worker" | jq
```

```json
{
  "detection": {
    "window": "992ms/16",
    "minHotCount": 100,
    "rulesHash": "1f2a3b4c5d6e7f80"
  },
  "hot": { "active": 1234, "rejected": 12 },
  "cooling": { "scheduled": 900 },
  "rules": [
    {
      "id": "user-prefix",
      "effectiveHotQps": 200,
      "matches": 12034,
      "promotions": 10,
      "aborts": 2,
      "cools": 8
    }
  ]
}
```

| 字段                      | 回答的问题                                                                          |
| ------------------------- | ----------------------------------------------------------------------------------- |
| `detection.window`        | 实际生效的窗口，不是配置值                                                          |
| `detection.rulesHash`     | 实际生效规则文本的指纹；两个 Worker 的 hash 不同即判定规则不同                      |
| `detection.minHotCount`   | 默认规则与非 `exact` 规则的晋升下界；生效的 `exact` 规则按键索引并在此之前检查      |
| `rules[].effectiveHotQps` | 实际生效的阈值，由窗口计数阈值与有效窗口反算；取整后可能与配置值相差 1              |
| `rules[].matches`         | 窗口和达到该规则 `hotCount` 的上报次数；exact、prefix、regex 与默认规则统计同一个量 |
| `hot.active`              | 当前持有 HOT 名额的 key 数；`active` 贴着名额上限且 `rejected` 上涨说明容量是瓶颈   |
| `cooling.scheduled`       | 冷却轮中已挂载的检查数；应贴近热 Key 数，明显偏高说明冷却跟不上                     |

### 4.2 单 key

```bash
curl -s "$WORKER/actuator/hotkey-worker?key=user:42" | jq
```

```json
{
  "key": "user:42",
  "hot": true,
  "rule": "user-prefix",
  "windowSum": 230,
  "promotedForMs": 720000,
  "belowForMs": null,
  "nextCheckInMs": 800,
  "promotedVersion": 9182
}
```

`promotedForMs` 在两种策略下都表示"已 HOT 多久"；`belowForMs` 只有 `COOL_FOR_LOW_QPS` 且当前低于冷却带时非空，
`COOL_FOR_EXPIRE` 恒为 `null`。两个时间字段分开是刻意的——单一 `sinceMs` 会随策略悄悄变义。
`nextCheckInMs` 是距已登记冷却检查的剩余时间；检查过期时为负数（已到 deadline 但尚未被 wheel 取走，或正在等待修复重挂），
这是有用的"已逾期多久"读数，而不是错误。

端点返回真实 key 名和流量，生产环境按 App 侧 `hotkey` 端点的同等口径做安全防护。

## 5. Micrometer 指标

指标按模块分两处注册：common 模块注册应用侧指标、L1 缓存 Binder 与决策监听计数；Worker 模块注册检测面指标。每个指标都以组件存在为条件，因此抓取结果里缺少某个指标，意味着对应平面不在该进程中。

### 5.1 请求模板

```bash
APP=http://localhost:8080

# 列出该进程上所有 zeta 指标
curl -s "$APP/actuator/metrics" | jq -r '.names[] | select(startswith("zeta."))'

# 读取单个指标
curl -s "$APP/actuator/metrics/zeta.reporter.queue.depth" | jq '{name, measurements}'

# 读取带标签的序列（Caffeine 指标使用 cache.* 命名空间）
curl -s "$APP/actuator/metrics/cache.gets?tag=cache:zeta.l1&tag=result:hit" | jq '.measurements'

# Prometheus 抓取（需要 micrometer-registry-prometheus）
curl -s "$APP/actuator/prometheus" | grep -E '^(zeta_|cache_)'
```

Prometheus 会把点转为下划线，并为 Counter 追加 `_total`：`zeta.reporter.queue.depth` 变成 `zeta_reporter_queue_depth`，`zeta.worker.decisions.hot` 变成 `zeta_worker_decisions_hot_total`。

### 5.2 L1 缓存（`cache.*`，标签 `cache=zeta.l1`）

Caffeine 指标使用 Micrometer 标准的 `cache.*` 命名空间，并带标签 `cache=zeta.l1`；**不是** `zeta.l1.*` 前缀。L1 是普通 `Cache`，通过 SingleFlight 加载，因此仅 `LoadingCache` 才有的 `cache.load` 与 `cache.load.duration` 不会注册。

| 指标                    | 类型            | 标签                                 | 含义                                                       |
| ----------------------- | --------------- | ------------------------------------ | ---------------------------------------------------------- |
| `cache.gets`            | FunctionCounter | `cache=zeta.l1`，`result=hit`/`miss` | 按结果统计的缓存读取                                       |
| `cache.puts`            | FunctionCounter | `cache=zeta.l1`                      | 写入条目数                                                 |
| `cache.evictions`       | FunctionCounter | `cache=zeta.l1`                      | 因容量、过期或引用回收而驱逐的条目数（显式失效不计入驱逐） |
| `cache.eviction.weight` | FunctionCounter | `cache=zeta.l1`                      | 驱逐条目权重                                               |
| `cache.size`            | Gauge           | `cache=zeta.l1`                      | 当前预估大小                                               |

Zeta 的 L1 始终启用 `recordStats()`；自定义缓存 Bean 若未开启，则只会报告 `cache.size`。

### 5.3 本地检测与去重

| 指标                                        | 类型            | 标签         | 含义                                                   |
| ------------------------------------------- | --------------- | ------------ | ------------------------------------------------------ |
| `zeta.topk.size`                            | Gauge           | `type=local` | 当前本地 TopK 大小                                     |
| `zeta.topk.total`                           | Gauge           | `type=local` | 本地 sketch 衰减后的追踪访问量（流量下降时会回落）     |
| `zeta.expelled.queue.size`                  | Gauge           | —            | 被逐出 TopK 的 key 暂存队列积压                        |
| `zeta.singleflight.inflight`                | Gauge           | —            | 正在执行的加载器数（不是去重表大小）                   |
| `zeta.stall.source_degraded.stopped`        | Gauge           | —            | 熔断器打开时为 `1`（加载快速失败；读取可返回软过期值） |
| `zeta.stall.source_degraded.timeouts.total` | FunctionCounter | —            | 触发单次加载读取超时的 leader 加载累计次数             |

### 5.4 上报与 feed loop

| 指标                                       | 类型            | 含义                                                                                                             |
| ------------------------------------------ | --------------- | ---------------------------------------------------------------------------------------------------------------- |
| `zeta.reporter.queue.depth`                | Gauge           | Reporter 分发队列中等待的批次数                                                                                  |
| `zeta.reporter.queue.dropped.total`        | FunctionCounter | 因队列满而丢弃，或 dispatcher 停机时撤回的累计批次数                                                             |
| `zeta.reporter.queue.expired.dead.total`   | FunctionCounter | 目标 Worker 已死亡的过期批次数                                                                                   |
| `zeta.reporter.queue.expired.stale.total`  | FunctionCounter | 在队列中等待超过 5 秒而过期的批次数                                                                              |
| `zeta.reporter.pending.keys`               | Gauge           | 共享访问计数器中缓冲的 key 数                                                                                    |
| `zeta.reporter.feedloop.interval`          | Gauge           | feed-loop 基准上报间隔（毫秒）；`shadow` 模式下这是"将要应用"的轨迹（仅 `report-interval-tuning != off` 时注册） |
| `zeta.reporter.feedloop.score`             | Gauge           | feed-loop 得分（bp）——平滑到达率投影到测量窗口所用的间隔后相对目标（`10000` 表示正中目标）                       |
| `zeta.reporter.feedloop.batch`             | Gauge           | 按产生该得分的间隔投影的每批 key 数                                                                              |
| `zeta.stall.broadcast_storm.stopped.total` | FunctionCounter | 因 Broker 错误或发送线程池饱和而丢失的 PUT/刷新广播数                                                            |
| `zeta.sync.invalidate.dropped.total`       | FunctionCounter | 因发送线程池拒绝而丢弃的 INVALIDATE 族消息数；按消息计数，不按携带的 key 数                                      |

### 5.5 刷新、版本与同步

| 指标                                       | 类型            | 含义                                                                                         |
| ------------------------------------------ | --------------- | -------------------------------------------------------------------------------------------- |
| `zeta.expire.refresh.available`            | Gauge           | 后台刷新限流器可用许可数                                                                     |
| `zeta.expire.refresh.failure.leased.total` | FunctionCounter | 获准续租的刷新失败累计数（陈旧值仍可服务）                                                   |
| `zeta.expire.refresh.lease.debt.keys`      | Gauge           | 当前被租约持有的 key 数（`staleDebt` 大小）                                                  |
| `zeta.version.clock.skew`                  | Gauge           | 最新已知版本戳（本地戳时钟或被观测到的对端戳，含被拒收的越界值）领先本地物理时间的毫秒数     |
| `zeta.sync.send.failed.total`              | FunctionCounter | 同步面发布失败累计数；与 `zeta.sync.invalidate.dropped.total` 配合覆盖发布前的线程池拒绝     |
| `zeta.sync.nodeid.collision.total`         | FunctionCounter | 与本节点共享 `dataVersion` node id 的对端同步消息数；持续增长应显式配置 `zeta.local.node-id` |

### 5.6 Worker 集群与决策面

| 指标                                                  | 类型                  | 含义                                                                                                                                  |
| ----------------------------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.worker.alive`                                   | Gauge                 | 心跳视图通过集群闸门时为 `1`；应在应用侧读取                                                                                          |
| `zeta.worker.detector.keys`                           | Gauge                 | 持有活跃滑动窗口条目的 key 数                                                                                                         |
| `zeta.worker.detector.capacity.evictions`             | FunctionCounter       | 本次采样真正移除、用于在 `max-tracked-keys` 上接纳新 key 的空闲窗口数（Prometheus `_total`）                                          |
| `zeta.worker.detector.capacity.rejections`            | FunctionCounter       | 在 `max-tracked-keys` 上被拒绝的新 key 数：硬余量已满，或完成采样未找到空闲窗口后开启的 50 ms 饱和窗口仍在生效（Prometheus `_total`） |
| `zeta.worker.hot.active`                              | Gauge                 | 当前持有 HOT 名额的 key 数                                                                                                            |
| `zeta.worker.hot.capacity`                            | Gauge                 | 名额容量（`detection.limits.max-active-hot-keys`）                                                                                    |
| `zeta.worker.hot.rejected`                            | FunctionCounter       | 满员时被拒绝的晋升（读取注册表的 LongAdder）                                                                                          |
| `zeta.worker.hot.aborted`                             | Counter               | HOT 未到达 broker 而被撤销的晋升（名额已归还）                                                                                        |
| `zeta.worker.decisions.hot` / `.cool`                 | Counter               | 已交给 AMQP 客户端且未抛异常的决策；未启用 publisher confirm                                                                          |
| `zeta.worker.decisions.dropped`                       | Counter，tag `type`   | 未到达 broker 即丢失的决策（发送失败、队列饱和、drain 异常、停机）                                                                    |
| `zeta.worker.decisions.elided`                        | Counter，tag `type`   | 无状态变化、在内存中省略的决策：100ms 同类型去抖、已接受版本的精确重放，或集群已处于 COOL 状态时再次下发的 COOL                       |
| `zeta.worker.decisions.superseded`                    | Counter，tag `type`   | 已有严格更新的版本排队或已被接受，或 registry 已不再持有该 HOT 的晋升，旧决策被淘汰                                                   |
| `zeta.worker.decisions.expired`                       | Counter，tag `type`   | 本应下发但超过 `max-decision-age-ms` 的决策；被漏斗省略或取代的决策计入对应指标，不计入此处                                           |
| `zeta.worker.broadcast.hot.depth` / `.cool.depth`     | Gauge                 | 两条待发队列深度                                                                                                                      |
| `zeta.worker.cooling.scheduled`                       | Gauge                 | 冷却轮中当前挂载的检查数                                                                                                              |
| `zeta.worker.cooling.lateness`                        | Timer                 | 定时轮触发的到期检查被处理的迟到程度                                                                                                  |
| `zeta.worker.cooling.recheck`                         | Counter               | LOW_QPS 复核发现窗口达到或高于冷却带的次数；EXPIRE 不会重新挂载，到期即冷却，不会计入这里                                             |
| `zeta.worker.cooling.reason`                          | Counter，tag `reason` | 冷却触发原因：`low_qps` / `expire`                                                                                                    |
| `zeta.worker.rule.matches` / `.promotions` / `.cools` | Counter，tag `rule`   | 按规则的达标上报、晋升与冷却计数，读取规则的 Micrometer Counter                                                                       |
| `zeta.worker.reports.epoch.dropped`                   | Counter               | 任期围栏丢弃的报告批次                                                                                                                |
| `zeta.worker.reports.stale.dropped`                   | Counter               | 可选陈旧过滤丢弃的报告批次                                                                                                            |
| `zeta.worker.detection.disabled.dropped`              | Counter               | detection 关闭时跳过的报告                                                                                                            |
| `zeta.worker.report.eval`                             | Timer                 | 每批上报的评估耗时（单调时钟，纳秒分辨率）；全部条目畸形、未进入处理管线的批次不记录采样                                              |

规则计数器和三个 `cooling.reason` 序列按当前生效配置注册，而非懒加载：每条当前规则从第一次抓取起就以 0 出现；发布移除某条规则时，它的三个序列也一并移除。被移除规则的迟到冷却只丢弃，不会重建序列。

决策监听器自身的计数器由 **common** 模块的 `ZetaMicrometerAutoConfiguration` 在存在
`WorkerListener` Bean 时注册：

| 指标                                            | 类型            | 含义                                       |
| ----------------------------------------------- | --------------- | ------------------------------------------ |
| `zeta.worker.decision.cool.rejected.total`      | FunctionCounter | App 有序分发器拒绝并就地重放的 COOL 决策数 |
| `zeta.worker.decision.epoch.regress.skip.total` | FunctionCounter | 因同 nodeId 任期回退而跳过的决策数         |

Worker 检测面指标由 **worker 模块的 `WorkerAutoConfiguration`** 注册，而不是
`ZetaMicrometerAutoConfiguration`：其余每一行读取的都是 worker 模块类型（`SlidingWindowDetector`、
`HotKeyRegistry`、`RuleResolver`、队列本身），common 自动配置看不到。`zeta.worker.alive` 是例外：
它由 common 的 `ZetaMicrometerAutoConfiguration` 从 common 的 `HealthView` 注册，所以没有 worker
模块的 App 也会出现。计数器随 meters bean 创建，gauge 在其依赖组件就绪后绑定一次。没有
`MeterRegistry` Bean 时全部为空操作。

指标恒等式（按决策类型分别成立）：`promotions == decisions.hot + dropped{type=hot} +
elided{type=hot} + superseded{type=hot} + expired{type=hot}`，COOL 侧同理
（`cools`/`decisions.cool`/`type=cool`）。每个进入投递的决策恰好落在一个桶里；因终态投递失败而
未到达发送调用的 HOT 还会计入 `hot.aborted`，因为晋升被撤销；被更新 COOL 淘汰的 HOT 已由该次冷却
转换移除并归还名额，因此只体现为 `cools`。在无并发变更的抓取点上，名额恒等式因此是
`promotions == cools + hot.aborted + hot.active`。
