# Monitoring

When `spring-boot-starter-actuator` is on the classpath, the HotKey endpoint is automatically registered at `/actuator/hotkey`.

Enable via `management.endpoints.web.exposure.include=health,info,hotkey`.

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

## Sections

### `local`

App-side detection, cache, reporting, rules, TTL, and version state.

| Key | Description |
| --- | ----------- |
| `topK` | App-side Top-K hot keys (descending by count) |
| `topKCount` | Number of hot keys in app-side Top-K set |
| `totalRequests` | Total requests through app-side detection |
| `recentlyExpelled` | Recently evicted keys from app-side Top-K |
| `topKCapacity` | Max hot key count (HeavyKeeper K) |
| `sketchWidth` | Count-Min Sketch width |
| `sketchDepth` | Count-Min Sketch depth |
| `minCountThreshold` | Minimum count for promotion to hot |
| `expelledQueueSize` | Current expelled queue size |
| `expelledQueueRemaining` | Remaining expelled queue capacity |
| `cacheSize` | L1 Caffeine current size |
| `cacheMaxSize` | L1 Caffeine max limit |
| `inflightSize` | Current in-flight dedup requests |
| `inflightMaxSize` | Max in-flight dedup keys |
| `inflightTtlSec` | In-flight entry TTL (seconds) |
| `inflightTimeoutSec` | Async wait timeout (seconds) |
| `reportQueueDepth` | Report dispatcher queue depth |
| `reportQueueCapacity` | Report dispatcher queue capacity |
| `reportExpiredCount` | Report dispatcher expired task count |
| `reportQueueFullCount` | Report dispatcher dropped (queue full) count |
| `reportPendingKeys` | Report buffer pending key count |
| `rules` | Active blacklist/whitelist rules |
| `softExpireEnabled` | Whether soft expire is globally enabled |
| `hardTtlMs` | Effective hard TTL (normal keys, ms) |
| `softTtlMs` | Effective soft TTL (normal keys, ms) |
| `hotHardTtlMs` | Effective hard TTL (hot keys, ms) |
| `hotSoftTtlMs` | Effective soft TTL (hot keys, ms) |
| `refreshPoolAvailable` | Available permits in the refresh semaphore |
| `versionRedisEnabled` | Whether Redis-based version tracking is active |
| `versionDegradedCount` | Number of keys using degraded (node-local) version |

### `worker`

Worker-side TopK, health, and state machine. Only present when Worker components are available.

| Key | Description |
| --- | ----------- |
| `topK` | Worker-side (cluster-wide) Top-K hot keys |
| `topKCount` | Number of hot keys in Worker-side Top-K set |
| `totalRequests` | Total requests through Worker-side detection |
| `recentlyExpelled` | Recently evicted keys from Worker-side Top-K |
| `topKCapacity` | Worker HeavyKeeper max hot key count |
| `sketchWidth` | Worker Count-Min Sketch width |
| `sketchDepth` | Worker Count-Min Sketch depth |
| `minCountThreshold` | Worker minimum count for promotion |
| `shards` | List of known shard indices tracked by this Worker |
| `health` | Per-shard health metadata (alive, heartbeat age, counts) |
| `trackedKeys` | Number of keys in the Worker state machine |

### `sync`

Cross-instance broadcast dedup. Only present when `CacheSyncPublisher` is active.

| Key | Description |
| --- | ----------- |
| `dedupCacheSize` | Current dedup cache entry count |
