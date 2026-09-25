# 监控

Zeta 提供两种互补的监控机制。

---

## 1. Actuator 端点

**前置条件：** classpath 中包含 `spring-boot-starter-actuator`。

Zeta 端点是普通的 Spring `@RestController`，**不是** Actuator `@Endpoint`——不受 `management.endpoints.web.exposure.include` 控制，无需在 include 列表中登记。只要 classpath 中存在 `spring-boot-starter-actuator`（注册条件）且 Spring MVC 可用，即自动注册：

| 端点                                      | 路径                            |
| ----------------------------------------- | ------------------------------- |
| 应用诊断（`ZetaEndpoint`）                | `/actuator/hotkey`              |
| 哈希环查询（`RingEndpoint`，见第 3 节）   | `/actuator/hotkeyring`          |
| 状态机运行时配置（Worker，见第 4 节）     | `/actuator/hotkey/worker/state` |
| FastLane 规则管理（Worker）               | `/actuator/hotkey/fastlane`     |

支持可选的 `?limit=N` 查询参数限制返回的应用端 TopK 条目数（默认 100）。

```javascript
{
  "instanceId": "a1b2c3d4",        // 实例唯一标识
  "nodeId": "node-1",               // 集群内节点标识
  "local": {
    // ── 应用端 TopK 检测 ──
    "topK": [{ "key": "cache:shop:17", "count": 1523 }],  // 热 key 列表（按频率降序）
    "topKCount": 1,                                        // 返回的 TopK 条目数（受 ?limit 限制）
    "totalRequests": 158392,                               // 追踪的总请求数
    "recentlyExpelled": ["cache:shop:5", "cache:shop:99"], // 最近被驱逐的 key

    // ── HeavyKeeper 算法配置 ──
    "topKCapacity": 100,            // 最大热 key 数（HeavyKeeper K）
    "sketchWidth": 65536,           // Count-Min Sketch 宽度（配置 50000，自动向上对齐到 2 的幂）
    "sketchDepth": 5,               // Count-Min Sketch 深度
    "minCountThreshold": 10,        // 晋升为热 key 的最小计数
    "expelledQueueSize": 2,         // 驱逐队列积压量
    "expelledQueueRemaining": 9998, // 驱逐队列剩余容量

    // ── L1 Caffeine 缓存 ──
    "cacheSize": 87,                // L1 预估大小
    "cacheMaxSize": 1000,           // L1 最大条目数（max-weight 为 0 时使用）
    "cacheMaxWeight": 0,            // L1 内存权重上限（字节，0 = 条目数模式）

    // ── SingleFlight 去重 ──
    "inflightSize": 3,              // 进行中的去重请求数
    "inflightMaxSize": 50000,       // 最大去重 key 数
    "inflightTtlSec": 5,            // 去重条目 TTL（秒）
    "inflightTimeoutSec": 3,        // 异步等待超时（秒）

    // ── Reporter（应用→Worker） ──
    "reportQueueDepth": 0,          // Reporter 分发器队列深度
    "reportQueueCapacity": 10000,   // Reporter 分发器队列容量
    "reportExpiredCount": 0,        // 累计过期批次
    "reportQueueFullCount": 0,      // 累计丢弃批次（队列满）
    "reportPendingKeys": 0,         // 计数缓存中缓冲的 key 数

    // ── 规则 ──
    "rules": [                      // 当前生效的黑/白名单规则
      { "id": "...", "type": "BLOCK", "pattern": "secret:*", "createdAt": 1700000000000 }
    ],

    // ── TTL 配置 ──
    "hardTtlMs": 300000,            // 有效硬 TTL——普通 key（毫秒）
    "softTtlMs": 30000,             // 有效软 TTL——普通 key（毫秒）
    "hotHardTtlMs": 3600000,        // 有效硬 TTL——热 key（毫秒）
    "hotSoftTtlMs": 300000,         // 有效软 TTL——热 key（毫秒）
    "nullValueTtlSec": 10,          // null 缓存条目 TTL（秒）
    "refreshPoolAvailable": 100,    // 刷新信号量可用许可数

    // ── 版本追踪 ──
    "versionRedisEnabled": true,    // Redis 版本追踪是否启用
    "versionDegradedCount": 0       // 使用降级节点本地版本的 key 数
  },
  "worker": {
    // ── Worker 健康状态 ──
    "health": "healthy",                  // 集群健康状态："healthy" 或 "unhealthy"
    "msSinceLastAnyHeartbeat": 1234,      // 距最近一次任意 Worker 心跳的毫秒数（-1 = 尚未收到）
    "trackedKeys": 42                     // 状态机追踪的 key 数
  },
  "sync": {
    "dedupCacheSize": 20            // 广播去重缓存条目数
  }
}
```

## 2. Micrometer 指标

当 classpath 中存在 `io.micrometer:micrometer-core` 时，`ZetaMicrometerAutoConfiguration` 自动注册 MeterBinder Bean，暴露以下指标。

### Caffeine L1 缓存指标（`zeta.l1.*`）

通过 `CaffeineCacheMetrics.monitor()` 提供的标准 Caffeine 缓存指标：

| 指标                               | 类型    | 说明                                              |
| ---------------------------------- | ------- | ------------------------------------------------- |
| `zeta.l1.cache.gets`             | Counter | 缓存读取次数（标签 `result=hit` / `result=miss`） |
| `zeta.l1.cache.puts`             | Counter | 缓存写入次数                                      |
| `zeta.l1.cache.evictions`        | Counter | 缓存驱逐次数（标签 `cause=...`）                  |
| `zeta.l1.cache.evictions.weight` | Counter | 驱逐条目权重                                      |
| `zeta.l1.cache.hit.ratio`        | Gauge   | 当前命中率                                        |
| `zeta.l1.cache.miss.ratio`       | Gauge   | 当前未命中率                                      |
| `zeta.l1.cache.size`             | Gauge   | 缓存预估大小                                      |
| `zeta.l1.cache.max`              | Gauge   | 缓存最大大小                                      |

### 自定义 Zeta 业务指标

| 指标                                  | 类型  | 标签                 | 说明                             |
| ------------------------------------- | ----- | -------------------- | -------------------------------- |
| `zeta.topk.size`                    | Gauge | `type=local`         | TopK 当前排名数                  |
| `zeta.topk.total`                   | Gauge | `type=local`         | TopK 追踪的总请求数              |
| `zeta.expelled.queue.size`          | Gauge | —                    | 驱逐队列积压量                   |
| `zeta.expelled.queue.remaining`     | Gauge | —                    | 驱逐队列剩余容量                 |
| `zeta.singleflight.inflight`        | Gauge | —                    | SingleFlight 进行中的去重数      |
| `zeta.reporter.queue.depth`         | Gauge | —                    | Reporter 队列积压量              |
| `zeta.reporter.queue.dropped.total` | Gauge | —                    | 累计丢弃批次（队列满）           |
| `zeta.reporter.queue.expired.total` | Gauge | —                    | 累计过期批次（下列两种原因之和） |
| `zeta.reporter.queue.expired.dead.total` | Gauge | —               | 目标 Worker 已死亡的过期批次     |
| `zeta.reporter.queue.expired.stale.total` | Gauge | —              | 在队列中等待超过 5 秒而过期的批次 |
| `zeta.reporter.pending.keys`        | Gauge | —                    | Reporter 计数缓存中缓冲的 key 数 |
| `zeta.reporter.bbr.passed`          | Gauge | —                    | Reporter BBR 通过次数             |
| `zeta.reporter.bbr.dropped`         | Gauge | —                    | Reporter BBR 丢弃次数             |
| `zeta.reporter.bbr.inflight`        | Gauge | —                    | Reporter BBR 进行中请求数         |
| `zeta.reporter.bbr.maxinflight`     | Gauge | —                    | Reporter BBR 最大进行中请求数     |
| `zeta.reporter.feedloop.interval`   | Gauge | —                    | FeedLoop 基准上报间隔（毫秒）；shadow 模式下显示"将要应用"的轨迹（ADR-0078；仅 `report-interval-tuning` ≠ off 时注册） |
| `zeta.reporter.feedloop.score`      | Gauge | —                    | FeedLoop 得分（bp）——两窗口平均批大小相对目标（10000 == 正中目标） |
| `zeta.reporter.feedloop.batch`      | Gauge | —                    | FeedLoop 平均批大小（每次完成 flush 的键数） |
| `zeta.stall.report_backpressure.delayed` | Gauge | —               | Reporter 队列深度（拥塞前兆，ADR-0076） |
| `zeta.stall.report_backpressure.stopped.total` | Gauge | —      | 因队列满或过期而丢失的批次数      |
| `zeta.stall.broadcast_storm.stopped.total` | Gauge | —           | 因 Broker 错误或发送线程池饱和而丢失的刷新广播数 |
| `zeta.stall.redis_degraded.stopped` | Gauge | —                    | 熔断器打开时为 1（加载快速失败）  |
| `zeta.stall.redis_degraded.timeouts.total` | Gauge | —             | 因读取超时而解析为空的去重加载数  |
| `zeta.stall.worker_partition.stopped` | Gauge | —                  | 无存活 Worker 分片时为 1（报告路由无目标） |
| `zeta.expire.refresh.available`     | Gauge | —                    | 刷新信号量可用许可数             |
| `zeta.version.degraded.total`       | Gauge | —                    | 累计版本回退次数                 |
| `zeta.sync.dedup.size`              | Gauge | —                    | 广播去重缓存大小                 |
| `zeta.worker.alive`                 | Gauge | —                    | 任意 Worker 分片是否存活（0/1）  |
| `zeta.worker.tracked.keys`          | Gauge | —                    | 状态机追踪的 key 数              |
| `zeta.cpu.load`                     | Gauge | —                    | 当前 CPU 负载（0-1000 范围）     |
| `zeta.dispatch.pending.units`       | Gauge | `plane`              | 按 key 分发器闸门已计入的积压    |
| `zeta.dispatch.remaining.units`     | Gauge | `plane`              | 距开始丢弃还剩的闸门容量         |
| `zeta.dispatch.active.keys`         | Gauge | `plane`              | 当前持有任务的分发器 key 数      |
| `zeta.dispatch.backlogged`          | Gauge | `plane`              | 存在未执行任务时为 1             |
| `zeta.dispatch.dropped.total`       | Gauge | `plane`              | 被预算闸门累计丢弃数             |
| `zeta.dispatch.rejected.total`      | Gauge | `plane`              | 被单 key 队列上限拒绝数          |

`zeta.dispatch.*` 指标带 `plane=sync`（同步面）或 `plane=worker`（决策面）标签；当前部署模式下不存在的面不会注册任何指标。把 `pending.units` 与 `remaining.units` 一起看，才能区分"没有流量"与"闸门已饱和"——在这组指标出现之前，这两种情况在观测上无法区分，且只在提交已被丢弃之后才通过一条限流 WARN 体现。

## 3. 一致性哈希环管理

当启用一致性哈希（`zeta.local.consistent-hashing.enabled=true`）且 classpath 中包含 `spring-boot-starter-actuator` 与 `spring-boot-starter-web` 时，会在 `/actuator/hotkeyring` 注册一个 REST 控制器（`RingEndpoint.java`），用于环查询。

| 方法  | 路径                         | 说明                        |
| ----- | ---------------------------- | --------------------------- |
| `GET` | `/actuator/hotkeyring`       | 环拓扑和节点数量            |
| `GET` | `/actuator/hotkeyring/{key}` | 查询指定 key 由哪个节点处理 |

## 4. Worker 状态机运行时配置

当启用 Worker 模式（`zeta.worker.enabled=true`）且 classpath 中包含 `spring-boot-starter-actuator` 与 `spring-boot-starter-web` 时，会在 `/actuator/hotkey/worker/state` 注册一个 REST 控制器（`StateMachineEndpoint.java`），用于运行时读取和更新状态机配置。

| 方法   | 路径                            | 说明                                                                     |
| ------ | ------------------------------- | ------------------------------------------------------------------------ |
| `GET`  | `/actuator/hotkey/worker/state` | 返回当前 `confirmCount`、`coolCount`、`preCoolGraceCount`、`trackedKeys` |
| `POST` | `/actuator/hotkey/worker/state` | 更新一个或多个参数（请求体：`{"confirmCount":"5"}`）                     |

**读取当前状态：**

```bash
curl http://localhost:8080/actuator/hotkey/worker/state
```

**响应示例：**

```json
{
  "confirmCount": 1,
  "coolCount": 10,
  "preCoolGraceCount": 3,
  "trackedKeys": 42
}
```

**更新参数：**

变更通过心跳广播传播到对等 Worker。每次 POST 会递增内部的 `configTimestampCounter`——接收方 Worker 仅当时间戳严格更新于自身时才应用新值。

```bash
curl -X POST http://localhost:8080/actuator/hotkey/worker/state \
  -H "Content-Type: application/json" \
  -d '{"confirmCount":"5","coolCount":"15"}'
```

**响应示例：**

```json
{
  "status": "ok"
}
```

**校验规则：** POST 应用后的参数组合必须满足与配置协商层对心跳 gossip 相同的不变量——`confirmCount >= 1`、`preCoolGraceCount >= 1` 且 `coolCount > preCoolGraceCount`（提供的字段覆盖，其余字段保持当前值）。违反不变量的 POST 会被拒绝并返回 `"status": "error"`，不做任何修改：本地接受但被 gossip 拒绝的配置会让该 Worker 永久偏离集群且无法自动收敛。不含任何可识别字段的 POST 是纯 no-op（不重写、不递增时间戳）。
