# Monitoring

Zeta provides two complementary monitoring mechanisms.

---

## 1. Actuator Endpoint

**Prerequisite:** `spring-boot-starter-actuator` on classpath.

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,zeta
```

When `spring-boot-starter-actuator` is on the classpath, the Zeta endpoint is automatically registered at `/actuator/hotkey`.

Enable via `management.endpoints.web.exposure.include=health,info,hotkey`.

Supports an optional `?limit=N` query parameter to cap the number of TopK entries returned (default 100).

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
    "nullValueTtlSec": 10,          // TTL (seconds) for null/cache-miss entries
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
    "health": "healthy",            // Cluster health: "healthy", "unhealthy", or "unknown"
    "trackedKeys": 7                // Keys tracked by state machine
  },
  "sync": {
    "dedupCacheSize": 20            // Broadcast dedup cache entry count
  }
}
```

## 2. Micrometer Metrics

When `io.micrometer:micrometer-core` is on the classpath, `ZetaMicrometerAutoConfiguration` automatically registers MeterBinder beans exposing the following metrics.

### Caffeine L1 Cache Metrics (`zeta.l1.*`)

Standard Caffeine cache metrics via `CaffeineCacheMetrics.monitor()`:

| Metric                             | Type    | Description                                                |
| ---------------------------------- | ------- | ---------------------------------------------------------- |
| `zeta.l1.cache.gets`             | Counter | Cache get operations (tagged `result=hit` / `result=miss`) |
| `zeta.l1.cache.puts`             | Counter | Cache put operations                                       |
| `zeta.l1.cache.evictions`        | Counter | Cache evictions (tagged `cause=...`)                       |
| `zeta.l1.cache.evictions.weight` | Counter | Evicted entry weight                                       |
| `zeta.l1.cache.hit.ratio`        | Gauge   | Current hit ratio                                          |
| `zeta.l1.cache.miss.ratio`       | Gauge   | Current miss ratio                                         |
| `zeta.l1.cache.size`             | Gauge   | Estimated current cache size                               |
| `zeta.l1.cache.max`              | Gauge   | Maximum cache size                                         |

### Custom Zeta Business Metrics

| Metric                                | Type  | Tags                 | Description                             |
| ------------------------------------- | ----- | -------------------- | --------------------------------------- |
| `zeta.topk.size`                    | Gauge | `type=local\|worker` | TopK current ranking count              |
| `zeta.topk.total`                   | Gauge | `type=local\|worker` | TopK total requests tracked             |
| `zeta.expelled.queue.size`          | Gauge | —                    | Expelled queue backlog                  |
| `zeta.expelled.queue.remaining`     | Gauge | —                    | Expelled queue remaining capacity       |
| `zeta.singleflight.inflight`        | Gauge | —                    | SingleFlight in-flight dedup count      |
| `zeta.reporter.queue.depth`         | Gauge | —                    | Reporter queue backlog                  |
| `zeta.reporter.queue.dropped.total` | Gauge | —                    | Cumulative dropped batches (queue full) |
| `zeta.reporter.queue.expired.total` | Gauge | —                    | Cumulative expired batches              |
| `zeta.reporter.pending.keys`        | Gauge | —                    | Keys buffered in reporter counter cache |
| `zeta.expire.refresh.available`     | Gauge | —                    | Available refresh limiter permits       |
| `zeta.version.degraded.total`       | Gauge | —                    | Cumulative version fallback count       |
| `zeta.sync.dedup.size`              | Gauge | —                    | Broadcast dedup cache size              |
| `zeta.worker.alive`                 | Gauge | —                    | Whether any worker shard is alive (0/1) |
| `zeta.worker.tracked.keys`          | Gauge | —                    | Keys tracked by state machine           |

## 3. Consistent Hash Ring Management

When consistent hashing is enabled (`zeta.local.consistent-hashing.enabled=true`) and `spring-boot-starter-web` is on the classpath, a REST controller (`RingEndpoint.java`) is registered at `/actuator/hotkeyring` for ring inspection.

| Method | Path                         | Description                          |
| ------ | ---------------------------- | ------------------------------------ |
| `GET`  | `/actuator/hotkeyring`       | Ring topology and node count         |
| `GET`  | `/actuator/hotkeyring/{key}` | Query which node handles a given key |

## 4. Worker State Machine Runtime Configuration

When Worker mode is active (`zeta.worker.enabled=true`) and `spring-boot-starter-web` is on the classpath, a REST controller (`StateMachineEndpoint.java`) is registered at `/actuator/hotkey/worker/state` for reading and updating the state-machine configuration at runtime.

| Method | Path                            | Description                                                                    |
| ------ | ------------------------------- | ------------------------------------------------------------------------------ |
| `GET`  | `/actuator/hotkey/worker/state` | Return current `confirmCount`, `coolCount`, `preCoolGraceCount`, `trackedKeys` |
| `POST` | `/actuator/hotkey/worker/state` | Update one or more parameters (body: `{"confirmCount":"5"}`)                   |

**Read current state:**

```bash
curl http://localhost:8080/actuator/hotkey/worker/state
```

**Example response:**

```json
{
  "confirmCount": 1,
  "coolCount": 10,
  "preCoolGraceCount": 3,
  "trackedKeys": 42
}
```

**Update parameters:**

Changes are propagated to peer Workers via the heartbeat broadcast. Each POST increments an internal `configTimestampCounter` — receiving Workers apply the new values only if the timestamp is strictly newer than their own.

```bash
curl -X POST http://localhost:8080/actuator/hotkey/worker/state \
  -H "Content-Type: application/json" \
  -d '{"confirmCount":"5","coolCount":"15"}'
```

**Example response:**

```json
{
  "status": "ok"
}
```
