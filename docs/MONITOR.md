# Monitoring

HotKey provides two complementary monitoring mechanisms.

---

## 1. Actuator Endpoint

**Prerequisite:** `spring-boot-starter-actuator` on classpath.

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,hotkey
```

When `spring-boot-starter-actuator` is on the classpath, the HotKey endpoint is automatically registered at `/actuator/hotkey`.

Enable via `management.endpoints.web.exposure.include=health,info,hotkey`.

```javascript
{
  "instanceId": "a1b2c3d4",        // Instance unique identifier
  "nodeId": "node-1",               // Node identifier within cluster
  "local": {
    // ── App-side TopK detection ──
    "topK": [{ "key": "cache:shop:17", "count": 1523 }],  // Hot keys list (descending by freq)
    "topKCount": 1,                                        // Current hot key count
    "totalRequests": 158392,                               // Total requests tracked
    "recentlyExpelled": ["cache:shop:5", "cache:shop:99"], // Recently evicted keys

    // ── HeavyKeeper algorithm config ──
    "topKCapacity": 100,            // Max hot keys (HeavyKeeper K)
    "sketchWidth": 50000,           // Count-Min Sketch width
    "sketchDepth": 5,               // Count-Min Sketch depth
    "minCountThreshold": 10,        // Minimum count for hot promotion
    "expelledQueueSize": 2,         // Expelled queue backlog
    "expelledQueueRemaining": 49998,// Expelled queue remaining capacity

    // ── L1 Caffeine cache ──
    "cacheSize": 87,                // Estimated current L1 size
    "cacheMaxSize": 1000,           // L1 maximum capacity

    // ── SingleFlight dedup ──
    "inflightSize": 3,              // In-flight dedup requests
    "inflightMaxSize": 50000,       // Max in-flight keys
    "inflightTtlSec": 5,            // In-flight entry TTL (seconds)
    "inflightTimeoutSec": 3,        // Async wait timeout (seconds)

    // ── Reporter (app→Worker) ──
    "reportQueueDepth": 0,          // Reporter dispatcher queue depth
    "reportQueueCapacity": 10000,   // Reporter dispatcher queue capacity
    "reportExpiredCount": 0,        // Cumulative expired batches
    "reportQueueFullCount": 0,      // Cumulative dropped batches (queue full)
    "reportPendingKeys": 0,         // Keys buffered in counter cache

    // ── Rules ──
    "rules": [                      // Active blacklist/whitelist rule definitions
      { "id": "...", "type": "BLOCK", "pattern": "secret:*", "createdAt": 1700000000000 }
    ],

    // ── TTL configuration ──
    "softExpireEnabled": true,      // Soft-expire (stale-while-revalidate) enabled
    "hardTtlMs": 300000,            // Effective hard TTL — normal keys (ms)
    "softTtlMs": 30000,             // Effective soft TTL — normal keys (ms)
    "hotHardTtlMs": 3600000,        // Effective hard TTL — hot keys (ms)
    "hotSoftTtlMs": 300000,         // Effective soft TTL — hot keys (ms)
    "refreshPoolAvailable": 100,    // Available refresh limiter permits

    // ── Version tracking ──
    "versionRedisEnabled": true,    // Redis-based version tracking active
    "versionDegradedCount": 0       // Keys using degraded node-local version
  },
  "worker": {
    // ── Worker-side TopK (cluster-wide) ──
    "topK": [{ "key": "cache:shop:17", "count": 8921 }],  // Cluster-wide hot keys
    "topKCount": 1,                                        // Worker hot key count
    "totalRequests": 784512,                               // Worker total requests
    "recentlyExpelled": ["cache:shop:3"],                  // Worker recently evicted keys
    "topKCapacity": 100,            // Worker HeavyKeeper K
    "sketchWidth": 20000,           // Worker sketch width
    "sketchDepth": 10,              // Worker sketch depth
    "minCountThreshold": 10,        // Worker minimum hot count

    // ── Worker health & state ──
    "shards": [0, 1, 2],            // Known shard indices
    "health": { "0": { "alive": true, ... } },  // Per-shard health metadata
    "trackedKeys": 7                // Keys tracked by state machine
  },
  "sync": {
    "dedupCacheSize": 20            // Broadcast dedup cache entry count
  }
}
```

## 2. Micrometer Metrics

When `io.micrometer:micrometer-core` is on the classpath, `HotKeyMicrometerAutoConfiguration` automatically registers MeterBinder beans exposing the following metrics.

### Caffeine L1 Cache Metrics (`hotkey.l1.*`)

Standard Caffeine cache metrics via `CaffeineCacheMetrics.monitor()`:

| Metric                             | Type    | Description                                                |
| ---------------------------------- | ------- | ---------------------------------------------------------- |
| `hotkey.l1.cache.gets`             | Counter | Cache get operations (tagged `result=hit` / `result=miss`) |
| `hotkey.l1.cache.puts`             | Counter | Cache put operations                                       |
| `hotkey.l1.cache.evictions`        | Counter | Cache evictions (tagged `cause=...`)                       |
| `hotkey.l1.cache.evictions.weight` | Counter | Evicted entry weight                                       |
| `hotkey.l1.cache.hit.ratio`        | Gauge   | Current hit ratio                                          |
| `hotkey.l1.cache.miss.ratio`       | Gauge   | Current miss ratio                                         |
| `hotkey.l1.cache.size`             | Gauge   | Estimated current cache size                               |
| `hotkey.l1.cache.max`              | Gauge   | Maximum cache size                                         |

### Custom HotKey Business Metrics

| Metric                                | Type  | Tags                 | Description                             |
| ------------------------------------- | ----- | -------------------- | --------------------------------------- |
| `hotkey.topk.size`                    | Gauge | `type=local\|worker` | TopK current ranking count              |
| `hotkey.topk.total`                   | Gauge | `type=local\|worker` | TopK total requests tracked             |
| `hotkey.expelled.queue.size`          | Gauge | —                    | Expelled queue backlog                  |
| `hotkey.expelled.queue.remaining`     | Gauge | —                    | Expelled queue remaining capacity       |
| `hotkey.singleflight.inflight`        | Gauge | —                    | SingleFlight in-flight dedup count      |
| `hotkey.reporter.queue.depth`         | Gauge | —                    | Reporter queue backlog                  |
| `hotkey.reporter.queue.dropped.total` | Gauge | —                    | Cumulative dropped batches (queue full) |
| `hotkey.reporter.queue.expired.total` | Gauge | —                    | Cumulative expired batches              |
| `hotkey.reporter.pending.keys`        | Gauge | —                    | Keys buffered in reporter counter cache |
| `hotkey.expire.refresh.available`     | Gauge | —                    | Available refresh limiter permits       |
| `hotkey.version.degraded.total`       | Gauge | —                    | Cumulative version fallback count       |
| `hotkey.sync.dedup.size`              | Gauge | —                    | Broadcast dedup cache size              |
| `hotkey.worker.alive`                 | Gauge | —                    | Whether any worker shard is alive (0/1) |
| `hotkey.worker.tracked.keys`          | Gauge | —                    | Keys tracked by state machine           |

## 3. Consistent Hash Ring Management

When consistent hashing is enabled (`hotkey.local.consistent-hashing.enabled=true`) and `spring-boot-starter-web` is on the classpath, a REST controller (`RingEndpoint.java`) is registered at `/actuator/hotkeyring` for runtime ring management.

| Method   | Path                                | Description                                           |
| -------- | ----------------------------------- | ----------------------------------------------------- |
| `GET`    | `/actuator/hotkeyring`              | Ring topology and current mode (`auto`/`manual`)      |
| `GET`    | `/actuator/hotkeyring/{key}`        | Query which node handles a given key                  |
| `POST`   | `/actuator/hotkeyring`              | Add a node (body: `{"nodeId":"..."}`)                 |
| `DELETE` | `/actuator/hotkeyring/{nodeId}`     | Remove a node                                         |
| `POST`   | `/actuator/hotkeyring/rebuild`      | Switch back to automatic mode from manual             |

**Example — add a node:**

```bash
curl -X POST http://localhost:8080/actuator/hotkeyring \
  -H "Content-Type: application/json" \
  -d '{"nodeId":"worker-3"}'
```
