# 监控

当 classpath 中存在 `spring-boot-starter-actuator` 时，HotKey 端点将自动注册到 `/actuator/hotkey`。

通过 `management.endpoints.web.exposure.include=health,info,hotkey` 启用。

```json
{
  "instanceId": "a1b2c3d4",
  "nodeId": "node-1",
  "local": {
    "topK": [{ "key": "cache:shop:17", "count": 1523 }],
    "topKCount": 1,
    "totalRequests": 158392,
    "recentlyExpelled": ["cache:shop:5", "cache:shop:99"],
    "topKCapacity": 100,
    "sketchWidth": 50000,
    "sketchDepth": 5,
    "minCountThreshold": 10,
    "expelledQueueSize": 2,
    "expelledQueueRemaining": 49998,
    "cacheSize": 87,
    "cacheMaxSize": 1000,
    "inflightSize": 3,
    "inflightMaxSize": 50000,
    "inflightTtlSec": 5,
    "inflightTimeoutSec": 3,
    "reportQueueDepth": 0,
    "reportQueueCapacity": 10000,
    "reportExpiredCount": 0,
    "reportQueueFullCount": 0,
    "reportPendingKeys": 0,
    "rules": [
      { "id": "...", "type": "BLOCK", "pattern": "secret:*", "createdAt": 1700000000000 }
    ],
    "softExpireEnabled": true,
    "hardTtlMs": 300000,
    "softTtlMs": 30000,
    "hotHardTtlMs": 3600000,
    "hotSoftTtlMs": 300000,
    "refreshPoolAvailable": 100,
    "versionRedisEnabled": true,
    "versionDegradedCount": 0
  },
  "worker": {
    "topK": [{ "key": "cache:shop:17", "count": 8921 }],
    "topKCount": 1,
    "totalRequests": 784512,
    "recentlyExpelled": ["cache:shop:3"],
    "topKCapacity": 100,
    "sketchWidth": 20000,
    "sketchDepth": 10,
    "minCountThreshold": 10,
    "shards": [0, 1, 2],
    "health": { "0": { "alive": true, ... } },
    "trackedKeys": 7
  },
  "sync": {
    "dedupCacheSize": 20
  }
}
```

## 板块说明

### `local`

应用端检测、缓存、上报、规则、TTL 及版本状态。

| Key | 说明 |
| --- | ---- |
| `topK` | 应用端 Top-K 热 key 列表（按次数降序） |
| `topKCount` | 应用端 Top-K 集中的热 key 数量 |
| `totalRequests` | 应用端检测的总请求数 |
| `recentlyExpelled` | 应用端 Top-K 最近被驱逐的 key |
| `topKCapacity` | 最大热 key 数量（HeavyKeeper K） |
| `sketchWidth` | Count-Min Sketch 宽度 |
| `sketchDepth` | Count-Min Sketch 深度 |
| `minCountThreshold` | 晋升为热 key 的最小计数 |
| `expelledQueueSize` | 当前驱逐队列大小 |
| `expelledQueueRemaining` | 驱逐队列剩余容量 |
| `cacheSize` | L1 Caffeine 当前大小 |
| `cacheMaxSize` | L1 Caffeine 最大限制 |
| `inflightSize` | 当前正在去重的请求数 |
| `inflightMaxSize` | 最大去重 key 数 |
| `inflightTtlSec` | 去重条目 TTL（秒） |
| `inflightTimeoutSec` | 异步等待超时（秒） |
| `reportQueueDepth` | 上报分发器队列深度 |
| `reportQueueCapacity` | 上报分发器队列容量 |
| `reportExpiredCount` | 上报分发器过期任务数 |
| `reportQueueFullCount` | 上报分发器丢弃数（队列满） |
| `reportPendingKeys` | 上报缓冲中待处理的 key 数 |
| `rules` | 当前生效的黑/白名单规则 |
| `softExpireEnabled` | 是否全局启用软过期 |
| `hardTtlMs` | 有效硬 TTL（普通 key，毫秒） |
| `softTtlMs` | 有效软 TTL（普通 key，毫秒） |
| `hotHardTtlMs` | 有效硬 TTL（热 key，毫秒） |
| `hotSoftTtlMs` | 有效软 TTL（热 key，毫秒） |
| `refreshPoolAvailable` | 刷新信号量可用许可数 |
| `versionRedisEnabled` | Redis 版本追踪是否启用 |
| `versionDegradedCount` | 使用降级版本（节点本地）的 key 数 |

### `worker`

Worker 端 TopK、健康状态和状态机。仅在 Worker 组件可用时出现。

| Key | 说明 |
| --- | ---- |
| `topK` | Worker 端（集群级）Top-K 热 key |
| `topKCount` | Worker 端 Top-K 中的热 key 数量 |
| `totalRequests` | Worker 端检测的总请求数 |
| `recentlyExpelled` | Worker 端 Top-K 最近被驱逐的 key |
| `topKCapacity` | Worker HeavyKeeper 最大热 key 数 |
| `sketchWidth` | Worker Count-Min Sketch 宽度 |
| `sketchDepth` | Worker Count-Min Sketch 深度 |
| `minCountThreshold` | Worker 最小晋升计数 |
| `shards` | 已知的分片索引列表 |
| `health` | 按分片索引分组的健康元数据（存活、心跳时间、计数） |
| `trackedKeys` | Worker 状态机中追踪的 key 数 |

### `sync`

跨实例广播去重。仅在 `CacheSyncPublisher` 激活时出现。

| Key | 说明 |
| --- | ---- |
| `dedupCacheSize` | 当前去重缓存条目数 |
