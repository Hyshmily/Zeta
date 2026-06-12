# 监控

HotKey 提供两种互补的监控机制。

---

## 1. Actuator 端点

**前置条件：** classpath 中包含 `spring-boot-starter-actuator`。

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,hotkey
```

当 classpath 中存在 `spring-boot-starter-actuator` 时，HotKey 端点将自动注册到 `/actuator/hotkey`。

通过 `management.endpoints.web.exposure.include=health,info,hotkey` 启用。

```javascript
{
  "instanceId": "a1b2c3d4",        // 实例唯一标识
  "nodeId": "node-1",               // 集群内节点标识
  "local": {
    // ── 应用端 TopK 检测 ──
    "topK": [{ "key": "cache:shop:17", "count": 1523 }],  // 热 key 列表（按频率降序）
    "topKCount": 1,                                        // 当前热 key 数
    "totalRequests": 158392,                               // 追踪的总请求数
    "recentlyExpelled": ["cache:shop:5", "cache:shop:99"], // 最近被驱逐的 key

    // ── HeavyKeeper 算法配置 ──
    "topKCapacity": 100,            // 最大热 key 数（HeavyKeeper K）
    "sketchWidth": 50000,           // Count-Min Sketch 宽度
    "sketchDepth": 5,               // Count-Min Sketch 深度
    "minCountThreshold": 10,        // 晋升为热 key 的最小计数
    "expelledQueueSize": 2,         // 驱逐队列积压量
    "expelledQueueRemaining": 49998,// 驱逐队列剩余容量

    // ── L1 Caffeine 缓存 ──
    "cacheSize": 87,                // L1 预估大小
    "cacheMaxSize": 1000,           // L1 最大容量

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
    "softExpireEnabled": true,      // 是否全局启用软过期
    "hardTtlMs": 300000,            // 有效硬 TTL——普通 key（毫秒）
    "softTtlMs": 30000,             // 有效软 TTL——普通 key（毫秒）
    "hotHardTtlMs": 3600000,        // 有效硬 TTL——热 key（毫秒）
    "hotSoftTtlMs": 300000,         // 有效软 TTL——热 key（毫秒）
    "refreshPoolAvailable": 100,    // 刷新信号量可用许可数

    // ── 版本追踪 ──
    "versionRedisEnabled": true,    // Redis 版本追踪是否启用
    "versionDegradedCount": 0       // 使用降级节点本地版本的 key 数
  },
  "worker": {
    // ── Worker 端 TopK（集群级） ──
    "topK": [{ "key": "cache:shop:17", "count": 8921 }],  // 集群级热 key 列表
    "topKCount": 1,                                        // Worker 热 key 数
    "totalRequests": 784512,                               // Worker 总请求数
    "recentlyExpelled": ["cache:shop:3"],                  // Worker 最近驱逐的 key
    "topKCapacity": 100,            // Worker HeavyKeeper K
    "sketchWidth": 20000,           // Worker sketch 宽度
    "sketchDepth": 10,              // Worker sketch 深度
    "minCountThreshold": 10,        // Worker 最小晋升计数

    // ── Worker 健康状态 ──
    "health": "healthy",            // 集群健康状态："healthy"、"unhealthy"、"unknown"
    "trackedKeys": 7                // 状态机追踪的 key 数
  },
  "sync": {
    "dedupCacheSize": 20            // 广播去重缓存条目数
  }
}
```

## 2. Micrometer 指标

当 classpath 中存在 `io.micrometer:micrometer-core` 时，`HotKeyMicrometerAutoConfiguration` 自动注册 MeterBinder Bean，暴露以下指标。

### Caffeine L1 缓存指标（`hotkey.l1.*`）

通过 `CaffeineCacheMetrics.monitor()` 提供的标准 Caffeine 缓存指标：

| 指标                               | 类型    | 说明                                              |
| ---------------------------------- | ------- | ------------------------------------------------- |
| `hotkey.l1.cache.gets`             | Counter | 缓存读取次数（标签 `result=hit` / `result=miss`） |
| `hotkey.l1.cache.puts`             | Counter | 缓存写入次数                                      |
| `hotkey.l1.cache.evictions`        | Counter | 缓存驱逐次数（标签 `cause=...`）                  |
| `hotkey.l1.cache.evictions.weight` | Counter | 驱逐条目权重                                      |
| `hotkey.l1.cache.hit.ratio`        | Gauge   | 当前命中率                                        |
| `hotkey.l1.cache.miss.ratio`       | Gauge   | 当前未命中率                                      |
| `hotkey.l1.cache.size`             | Gauge   | 缓存预估大小                                      |
| `hotkey.l1.cache.max`              | Gauge   | 缓存最大大小                                      |

### 自定义 HotKey 业务指标

| 指标                                  | 类型  | 标签                 | 说明                             |
| ------------------------------------- | ----- | -------------------- | -------------------------------- |
| `hotkey.topk.size`                    | Gauge | `type=local\|worker` | TopK 当前排名数                  |
| `hotkey.topk.total`                   | Gauge | `type=local\|worker` | TopK 追踪的总请求数              |
| `hotkey.expelled.queue.size`          | Gauge | —                    | 驱逐队列积压量                   |
| `hotkey.expelled.queue.remaining`     | Gauge | —                    | 驱逐队列剩余容量                 |
| `hotkey.singleflight.inflight`        | Gauge | —                    | SingleFlight 进行中的去重数      |
| `hotkey.reporter.queue.depth`         | Gauge | —                    | Reporter 队列积压量              |
| `hotkey.reporter.queue.dropped.total` | Gauge | —                    | 累计丢弃批次（队列满）           |
| `hotkey.reporter.queue.expired.total` | Gauge | —                    | 累计过期批次                     |
| `hotkey.reporter.pending.keys`        | Gauge | —                    | Reporter 计数缓存中缓冲的 key 数 |
| `hotkey.expire.refresh.available`     | Gauge | —                    | 刷新信号量可用许可数             |
| `hotkey.version.degraded.total`       | Gauge | —                    | 累计版本回退次数                 |
| `hotkey.sync.dedup.size`              | Gauge | —                    | 广播去重缓存大小                 |
| `hotkey.worker.alive`                 | Gauge | —                    | 任意 Worker 分片是否存活（0/1）  |
| `hotkey.worker.tracked.keys`          | Gauge | —                    | 状态机追踪的 key 数              |

## 3. 一致性哈希环管理

当启用一致性哈希（`hotkey.local.consistent-hashing.enabled=true`）且 classpath 中包含 `spring-boot-starter-web` 时，会在 `/actuator/hotkeyring` 注册一个 REST 控制器（`RingEndpoint.java`），用于运行时环管理。

| 方法     | 路径                                | 说明                         |
| -------- | ----------------------------------- | ---------------------------- |
| `GET`    | `/actuator/hotkeyring`              | 环拓扑和当前模式（`auto`/`manual`） |
| `GET`    | `/actuator/hotkeyring/{key}`        | 查询指定 key 由哪个节点处理      |
| `POST`   | `/actuator/hotkeyring`              | 添加节点（请求体：`{"nodeId":"..."}`）|
| `DELETE` | `/actuator/hotkeyring/{nodeId}`     | 移除节点                       |
| `POST`   | `/actuator/hotkeyring/rebuild`      | 切换回自动模式                  |

**示例 — 添加节点：**

```bash
curl -X POST http://localhost:8080/actuator/hotkeyring \
  -H "Content-Type: application/json" \
  -d '{"nodeId":"worker-3"}'
```

## 4. Worker 状态机运行时配置

当启用 Worker 模式（`hotkey.worker.enabled=true`）且 classpath 中包含 `spring-boot-starter-web` 时，会在 `/actuator/hotkey/worker/state` 注册一个 REST 控制器（`StateMachineEndpoint.java`），用于运行时读取和更新状态机配置。

| 方法   | 路径                                  | 说明                                                         |
| ------ | ------------------------------------- | ------------------------------------------------------------ |
| `GET`  | `/actuator/hotkey/worker/state`       | 返回当前 `confirmCount`、`coolCount`、`preCoolGraceCount`、`trackedKeys` |
| `POST` | `/actuator/hotkey/worker/state`       | 更新一个或多个参数（请求体：`{"confirmCount":"5"}`）           |

**读取当前状态：**

```bash
curl http://localhost:8080/actuator/hotkey/worker/state
```

**响应示例：**

```json
{
  "confirmCount": 3,
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
