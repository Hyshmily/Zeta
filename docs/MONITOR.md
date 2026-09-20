# Monitoring

Zeta provides two complementary monitoring mechanisms.

---

## 1. Actuator Endpoint

**Prerequisite:** `spring-boot-starter-actuator` on classpath.

The Zeta endpoints are plain Spring `@RestController`s, **not** Actuator `@Endpoint`s — they do not participate in `management.endpoints.web.exposure.include` and need no include-list entry. They are auto-registered when `spring-boot-starter-actuator` is on the classpath (registration condition) and Spring MVC is available:

| Endpoint                                  | Path                            |
| ----------------------------------------- | ------------------------------- |
| App diagnostics (`ZetaEndpoint`)          | `/actuator/hotkey`              |
| Hash-ring inspection (`RingEndpoint`, §3) | `/actuator/hotkeyring`          |
| State-machine runtime config (Worker, §4) | `/actuator/hotkey/worker/state` |
| FastLane rule management (Worker)         | `/actuator/hotkey/fastlane`     |

Supports an optional `?limit=N` query parameter to cap the number of app-side TopK entries returned (default 100).

```javascript
{
  "instanceId": "a1b2c3d4",        // Instance unique identifier
  "nodeId": "node-1",               // Node identifier within cluster
  "local": {
    // ── App-side TopK detection ──
    "topK": [{ "key": "cache:shop:17", "count": 1523 }],  // Hot keys list (descending by freq)
    "topKCount": 1,                                        // Returned top-K entry count (capped by ?limit)
    "totalRequests": 158392,                               // Total requests tracked
    "recentlyExpelled": ["cache:shop:5", "cache:shop:99"], // Recently evicted keys

    // ── HeavyKeeper algorithm config ──
    "topKCapacity": 100,            // Max hot keys (HeavyKeeper K)
    "sketchWidth": 65536,           // Count-Min Sketch width (configured 50000, auto-aligned up to a power of two)
    "sketchDepth": 5,               // Count-Min Sketch depth
    "minCountThreshold": 10,        // Minimum count for hot promotion
    "expelledQueueSize": 2,         // Expelled queue backlog
    "expelledQueueRemaining": 9998, // Expelled queue remaining capacity

    // ── L1 Caffeine cache ──
    "cacheSize": 87,                // Estimated current L1 size
    "cacheMaxSize": 1000,           // L1 maximum entry count (used when max-weight is 0)
    "cacheMaxWeight": 0,            // L1 memory weight limit in bytes (0 = entry-count mode)

    // ── SingleFlight dedup ──
    "inflightSize": 3,              // In-flight dedup requests
    "inflightMaxSize": 50000,       // Max in-flight keys
    "inflightTtlSec": 5,            // In-flight entry TTL (seconds)
    "inflightTimeoutSec": 3,        // Async wait timeout (seconds)

    // ── Reporter (app→Worker) ──
    "reportQueueDepth": 0,          // Reporter dispatcher queue depth
    "reportQueueCapacity": 10000,   // Reporter dispatcher queue capacity
    "reportExpiredCount": 0,        // Cumulative expired batches (dead routing target OR stale >5s in queue)
    "reportQueueFullCount": 0,      // Cumulative dropped batches (queue full)
    "reportPendingKeys": 0,         // Keys buffered in counter cache

    // ── Rules ──
    "rules": [                      // Active blacklist/whitelist rule definitions
      { "id": "...", "type": "BLOCK", "pattern": "secret:*", "createdAt": 1700000000000 }
    ],

    // ── TTL configuration ──
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
    // ── Worker health & state ──
    "health": "healthy",                  // Cluster health: "healthy" or "unhealthy"
    "msSinceLastAnyHeartbeat": 1234,      // ms since the last heartbeat from any Worker (-1 = none received yet)
    "trackedKeys": 42,                    // Keys tracked by the state machine

    // ── Decision-plane dispatcher gate (capacity: zeta.worker-listener.max-pending-units) ──
    "dispatchPendingUnits": 0,            // Weighted backlog currently charged to the gate
    "dispatchRemainingUnits": 200000,     // Gate capacity left before submissions are dropped
    "dispatchMaxPendingUnits": 200000,    // Configured gate capacity
    "dispatchActiveKeys": 0,              // Keys whose worker holds work (running or queued)
    "dispatchBacklogged": false,          // Whether any submitted decision is still pending
    "dispatchDropped": 0,                 // Cumulative submissions dropped by the global gate
    "dispatchRejected": 0                 // Cumulative submissions rejected by the per-key queue bound
  },
  "sync": {
    "dedupCacheSize": 20,                 // Broadcast dedup cache entry count

    // ── Sync-plane dispatcher gate (capacity: zeta.sync.max-pending-units) ──
    "dispatchPendingUnits": 0,            // Weighted backlog currently charged to the gate
    "dispatchRemainingUnits": 50000,      // Gate capacity left before submissions are dropped
    "dispatchMaxPendingUnits": 50000,     // Configured gate capacity
    "dispatchActiveKeys": 0,              // Keys whose worker holds work (running or queued)
    "dispatchBacklogged": false,          // Whether any submitted sync task is still pending
    "dispatchDropped": 0,                 // Cumulative submissions dropped by the global gate
    "dispatchRejected": 0                 // Cumulative submissions rejected by the per-key queue bound
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
| `zeta.topk.size`                    | Gauge | `type=local`         | TopK current ranking count              |
| `zeta.topk.total`                   | Gauge | `type=local`         | TopK total requests tracked             |
| `zeta.expelled.queue.size`          | Gauge | —                    | Expelled queue backlog                  |
| `zeta.expelled.queue.remaining`     | Gauge | —                    | Expelled queue remaining capacity       |
| `zeta.singleflight.inflight`        | Gauge | —                    | SingleFlight in-flight dedup count      |
| `zeta.reporter.queue.depth`         | Gauge | —                    | Reporter queue backlog                  |
| `zeta.reporter.queue.dropped.total` | Gauge | —                    | Cumulative dropped batches (queue full) |
| `zeta.reporter.queue.expired.total` | Gauge | —                    | Cumulative expired batches (sum of the two causes below) |
| `zeta.reporter.queue.expired.dead.total` | Gauge | —               | Expired batches whose target Worker was no longer alive |
| `zeta.reporter.queue.expired.stale.total` | Gauge | —              | Expired batches that waited >5s in the queue (staleness) |
| `zeta.reporter.pending.keys`        | Gauge | —                    | Keys buffered in reporter counter cache |
| `zeta.reporter.bbr.passed`          | Gauge | —                    | Reporter BBR passed count              |
| `zeta.reporter.bbr.dropped`         | Gauge | —                    | Reporter BBR dropped count             |
| `zeta.reporter.bbr.inflight`        | Gauge | —                    | Reporter BBR in-flight count           |
| `zeta.reporter.bbr.maxinflight`     | Gauge | —                    | Reporter BBR max in-flight count       |
| `zeta.stall.report_backpressure.delayed` | Gauge | —               | Reporter dispatcher queue depth (congestion building; ADR-0076) |
| `zeta.stall.report_backpressure.stopped.total` | Gauge | —      | Batches lost to a full queue or staleness expiry |
| `zeta.stall.broadcast_storm.stopped.total` | Gauge | —           | Refresh broadcasts lost to broker errors or a saturated send executor |
| `zeta.stall.redis_degraded.stopped` | Gauge | —                    | 1 while the circuit breaker is open (loads fast-fail) |
| `zeta.stall.redis_degraded.timeouts.total` | Gauge | —             | Cumulative dedup loads resolved empty by a reader timeout |
| `zeta.stall.worker_partition.stopped` | Gauge | —                  | 1 while no Worker shard is alive (report routing has no target) |
| `zeta.expire.refresh.available`     | Gauge | —                    | Available refresh limiter permits       |
| `zeta.version.degraded.total`       | Gauge | —                    | Cumulative version fallback count       |
| `zeta.sync.dedup.size`              | Gauge | —                    | Broadcast dedup cache size              |
| `zeta.worker.alive`                 | Gauge | —                    | Whether any worker shard is alive (0/1) |
| `zeta.worker.tracked.keys`          | Gauge | —                    | Keys tracked by state machine           |
| `zeta.cpu.load`                     | Gauge | —                    | Current CPU load (0-1000 scale)         |
| `zeta.dispatch.pending.units`       | Gauge | `plane`              | Weighted backlog on the per-key gate    |
| `zeta.dispatch.remaining.units`     | Gauge | `plane`              | Gate capacity left before drops         |
| `zeta.dispatch.active.keys`         | Gauge | `plane`              | Keys whose worker holds work            |
| `zeta.dispatch.backlogged`          | Gauge | `plane`              | 1 while a submitted task is pending     |
| `zeta.dispatch.dropped.total`       | Gauge | `plane`              | Cumulative drops by the budget gate     |
| `zeta.dispatch.rejected.total`      | Gauge | `plane`              | Cumulative rejections by key bound      |

The `zeta.dispatch.*` gauges carry `plane=sync` for the sync plane and `plane=worker` for the decision plane; a plane absent from the current deployment mode registers no gauges. Reading `pending.units` together with `remaining.units` is what distinguishes "no traffic" from "gate saturated" — before these gauges existed, both looked identical and only surfaced as a rate-limited WARN after submissions had already been dropped.

## 3. Consistent Hash Ring Management

When consistent hashing is enabled (`zeta.local.consistent-hashing.enabled=true`) and `spring-boot-starter-actuator` + `spring-boot-starter-web` are on the classpath, a REST controller (`RingEndpoint.java`) is registered at `/actuator/hotkeyring` for ring inspection.

| Method | Path                         | Description                          |
| ------ | ---------------------------- | ------------------------------------ |
| `GET`  | `/actuator/hotkeyring`       | Ring topology and node count         |
| `GET`  | `/actuator/hotkeyring/{key}` | Query which node handles a given key |

## 4. Worker State Machine Runtime Configuration

When Worker mode is active (`zeta.worker.enabled=true`) and `spring-boot-starter-actuator` + `spring-boot-starter-web` are on the classpath, a REST controller (`StateMachineEndpoint.java`) is registered at `/actuator/hotkey/worker/state` for reading and updating the state-machine configuration at runtime.

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

**Validation:** the POST-applied combination must satisfy the same invariant the config-negotiation layer enforces on heartbeat gossip — `confirmCount >= 1`, `preCoolGraceCount >= 1` and `coolCount > preCoolGraceCount` (provided fields override, the others keep their current values). A violating POST is rejected with `"status": "error"` and nothing is applied: a locally-accepted but gossip-rejected config would leave this Worker permanently divergent with no reconciliation path. A POST with no recognized fields is a pure no-op (no rewrite, no timestamp bump).
