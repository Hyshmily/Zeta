# Monitoring

Zeta provides two complementary monitoring mechanisms.

---

## 1. Actuator Endpoint

**Prerequisite:** `spring-boot-starter-actuator` on classpath.

The Zeta endpoints are Actuator `@Endpoint` beans (not plain `@RestController`s) — they participate in `management.endpoints.web.exposure.include` and must be listed there (e.g. `management.endpoints.web.exposure.include=health,info,hotkey,hotkeyring`). They are auto-registered when `spring-boot-starter-actuator` is on the classpath (registration condition):

| Endpoint                                  | Path                            |
| ----------------------------------------- | ------------------------------- |
| App diagnostics (`ZetaEndpoint`)          | `/actuator/hotkey`              |
| Hash-ring inspection (`RingEndpoint`, §3) | `/actuator/hotkeyring`          |
| State-machine runtime config (Worker, §4) | `/actuator/hotkey-worker-state` |
| FastLane rule management (Worker)         | `/actuator/hotkey-fastlane`     |

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
    "refreshFailureLeased": 3,      // Cumulative background-refresh failures granted a lease (ADR-0036)
    "leaseStaleCapped": 1,          // Cumulative lease refusals by the 30-minute staleness cap
    "leaseSuppressedByClassifier": 0, // Cumulative lease refusals by the failure classifier
    "staleDebt": {"k1": 95000},     // Currently-leased keys (top 100) to staleness ms — the audit surface for "bounded staleness"

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
    "dispatchRejected": 0,                // Cumulative submissions rejected by the per-key queue bound

    // ── Shared-broker appName isolation (ADR-0068) ──
    "foreignAppDrops": 0,               // Cumulative HOT/COOL decisions dropped for appName mismatch (misconfigured zeta.local.app-name discards 100% here)
    "lastForeignApp": "appB"            // Most recent foreign sender appName (present only after a drop)
  },
  "sync": {
    "dedupCacheSize": 20,                 // Broadcast dedup cache entry count
    "foreignAppDrops": 0,                 // Cumulative sync messages dropped for appName mismatch
    "lastForeignApp": "appB",             // Most recent foreign sender appName (present only after a drop)

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

When `io.micrometer:micrometer-core` is on the classpath, `ZetaMicrometerAutoConfiguration` automatically registers MeterBinder beans exposing the following metrics (the `zeta.worker.*` detection-plane meters marked ADR-0080 below are the exception — the Worker module registers those itself, see the note after the custom-metrics table).

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
| `zeta.reporter.bbr.balanced`        | Gauge | —                    | Reporter BBR damped baseline (kernel `dirty_ratelimit` analog) |
| `zeta.reporter.bbr.maxpass`         | Gauge | —                    | Sliding-window max pass per bucket (Little-Law estimate input; decay-on-read) |
| `zeta.reporter.bbr.minrt`           | Gauge | —                    | Sliding-window min avg RT in ms (Little-Law estimate input; decay-on-read) — the curve to watch for publisher-path queue pollution |
| `zeta.reporter.bbr.yielded`         | Gauge | —                    | Cumulative downstream-yield steps (two consecutive baseline intervals with publish-failure/staleness drops lower the baseline ×7/8 per step) |
| `zeta.reporter.feedloop.interval`   | Gauge | —                    | Feed-loop base flush interval (ms); shadow mode shows the trajectory that *would* be applied (ADR-0078; registered only when `report-interval-tuning` != off) |
| `zeta.reporter.feedloop.score`      | Gauge | —                    | Feed-loop score in bp — two-window-averaged batch size vs target (10000 == on target) |
| `zeta.reporter.feedloop.batch`      | Gauge | —                    | Feed-loop averaged batch size (keys per completed flush) |
| `zeta.stall.report_backpressure.delayed` | Gauge | —               | Reporter dispatcher queue depth (congestion building; ADR-0076) |
| `zeta.stall.report_backpressure.stopped.total` | Gauge | —      | Batches lost to a full queue or staleness expiry |
| `zeta.stall.broadcast_storm.stopped.total` | Gauge | —           | Refresh broadcasts lost to broker errors or a saturated send executor |
| `zeta.stall.redis_degraded.stopped` | Gauge | —                    | 1 while the circuit breaker is open (loads fast-fail) |
| `zeta.stall.redis_degraded.timeouts.total` | Gauge | —             | Cumulative dedup loads resolved empty by a reader timeout |
| `zeta.stall.worker_partition.stopped` | Gauge | —                  | 1 while no Worker shard is alive (report routing has no target) |
| `zeta.l1.refault.admit.total` | Gauge | — | Cumulative admit verdicts (ADR-0079; registered only when `refault-admission` != off) |
| `zeta.l1.refault.reject.total` | Gauge | — | Cumulative reject verdicts; shadow-mode rejects are the would-reject rate |
| `zeta.l1.refault.distance` | Gauge | — | Distance of the latest evidence-backed decision, -1 = no evidence |
| `zeta.l1.refault.capacity` | Gauge | — | Static capacity estimate the distances compare against |
| `zeta.l1.refault.clock.rate` | Gauge | — | Capacity-eviction rate (evictions/sec, scan-pressure alarm) |
| `zeta.rules.persist.failed` | Gauge | — | Rule mutations that reached memory but never Redis/broadcast |
| `zeta.expire.refresh.available`     | Gauge | —                    | Available refresh limiter permits       |
| `zeta.expire.refresh.failure.leased.total` | Gauge | —           | Cumulative refresh failures granted a lease (ADR-0036) |
| `zeta.expire.refresh.lease.stale.capped.total` | Gauge | —       | Cumulative lease refusals by the staleness cap |
| `zeta.expire.refresh.lease.suppressed.total` | Gauge | —        | Cumulative lease refusals by the failure classifier |
| `zeta.expire.refresh.lease.debt.keys` | Gauge | —                | Currently-leased keys (staleDebt size) |
| `zeta.version.degraded.total`       | Gauge | —                    | Cumulative version fallback count       |
| `zeta.sync.dedup.size`              | Gauge | —                    | Broadcast dedup cache size              |
| `zeta.worker.alive`                 | Gauge | —                    | Whether any worker shard is alive (0/1) |
| `zeta.worker.tracked.keys`          | Gauge | —                    | Keys tracked by state machine (only keys that have ever been hot) |
| `zeta.worker.detector.keys`         | Gauge | —                    | Keys holding a live sliding-window entry in the Worker detector (the per-key memory driver) |
| `zeta.worker.decisions.hot`         | Counter | —                  | HOT decisions successfully emitted to the cluster |
| `zeta.worker.decisions.cool`        | Counter | —                  | COOL decisions successfully emitted to the cluster |
| `zeta.worker.report.eval`           | Timer | —                    | Per-report-batch evaluation latency of the Worker's per-key loop (monotonic clock, ns resolution) |
| `zeta.cpu.load`                     | Gauge | —                    | Current CPU load (0-1000 scale)         |
| `zeta.dispatch.pending.units`       | Gauge | `plane`              | Weighted backlog on the per-key gate    |
| `zeta.dispatch.remaining.units`     | Gauge | `plane`              | Gate capacity left before drops         |
| `zeta.dispatch.active.keys`         | Gauge | `plane`              | Keys whose worker holds work            |
| `zeta.dispatch.backlogged`          | Gauge | `plane`              | 1 while a submitted task is pending     |
| `zeta.dispatch.dropped.total`       | Gauge | `plane`              | Cumulative drops by the budget gate     |
| `zeta.dispatch.rejected.total`      | Gauge | `plane`              | Cumulative rejections by key bound      |

The `zeta.dispatch.*` gauges carry `plane=sync` for the sync plane and `plane=worker` for the decision plane; a plane absent from the current deployment mode registers no gauges. Reading `pending.units` together with `remaining.units` is what distinguishes "no traffic" from "gate saturated" — before these gauges existed, both looked identical and only surfaced as a rate-limited WARN after submissions had already been dropped.

### Worker Detection-Plane Metrics (`zeta.worker.detector.keys`, `zeta.worker.decisions.*`, `zeta.worker.report.eval`)

The ADR-0080 detection-plane meters — one gauge, two emission counters and one timer — are the measurement baseline a Worker detection-architecture change is judged against. They are registered by the **Worker module's `WorkerAutoConfiguration`** (not by `ZetaMicrometerAutoConfiguration`: every one of them reads a worker-module class — `SlidingWindowDetector`, `WorkerBroadcaster`, `ReportConsumer` — while the common auto-configuration can only see common-module types), and only when a `MeterRegistry` bean is present; without one the Worker runs meter-less instead of failing startup.

- **`zeta.worker.detector.keys`** gauges the sliding-window detector's per-key window map — the structure that actually holds the Worker's memory (≈418.5 B of the ≈678 B retained per reported key, ADR-0080 §3). The pre-existing `zeta.worker.tracked.keys` gauge cannot see it: it reads the state machine, which returns early and materializes no state for a key that has never been hot, so it counts only ever-hot keys. The gap between the two gauges is the never-hot key mass the detector still pays for.
- **`zeta.worker.decisions.hot` / `zeta.worker.decisions.cool`** count **successful emissions only**, incremented at the single send funnel (`WorkerBroadcaster.broadcastHot`/`broadcastCool` — the state-machine path and the idle-eviction COOL path both publish through it). A failed AMQP send returns `false`, the caller rolls the key's state machine back, and the decision is re-emitted by the next evaluation or by the periodic HOT rebroadcast (ADR-0024); counting attempts would therefore count one decision twice. The 100 ms per-key dedup elision is not counted either — it emits nothing, because the decision it suppresses was already counted when it was sent.
- **`zeta.worker.report.eval`** is a `Timer` whose samples are **monotonic nanosecond deltas** (`Timer.record(elapsed, NANOSECONDS)`), taken around the per-key evaluation loop of `ReportConsumer.doOnReport` once per processed report batch (one sample per batch, not per key). Micrometer then stores and exports the value in the registry's base time unit — seconds by default for the standard registries, hence `_seconds` in a Prometheus export — but the measured quantity is a clock delta, not a timestamp. It uses the **monotonic clock only** and is deliberately *not* the report's cross-host wall-clock age (`now - message.timestamp()`), which is unusable under App clock skew — the same reason the optional staleness filter defaults to off. **AMQP queue wait is therefore excluded by construction**, as are the staleness filter and the global-ratio sampling that precede the loop; in the default buffered broadcast mode (ADR-0061) the decision sends run on the buffer's drain thread and are excluded too, while the legacy synchronous mode folds its in-line sends into the sample. Batches dropped as stale or empty never reach the loop and record no sample.

## 3. Consistent Hash Ring Management

When consistent hashing is enabled (`zeta.local.consistent-hashing.enabled=true`) and `spring-boot-starter-actuator` is on the classpath, a standard Actuator endpoint (`RingEndpoint.java`, id `hotkeyring`, `@ReadOperation`s) is registered at `/actuator/hotkeyring` for ring inspection. It runs on the management plane (port, exposure, and roles via `management.*`), not the application port.

| Method | Path                         | Description                          |
| ------ | ---------------------------- | ------------------------------------ |
| `GET`  | `/actuator/hotkeyring`       | Ring topology and node count         |
| `GET`  | `/actuator/hotkeyring/{key}` | Query which node handles a given key |

## 4. Worker State Machine Runtime Configuration

When Worker mode is active (`zeta.worker.enabled=true`) and `spring-boot-starter-actuator` is on the classpath, a standard Actuator endpoint (`StateMachineEndpoint.java`, id `hotkey-worker-state`) is registered at `/actuator/hotkey-worker-state` for reading and updating the state-machine configuration at runtime. It runs on the management plane (port, exposure, and roles via `management.*`): the runtime config mutation no longer sits on the application port. (An endpoint id cannot nest like the former `/actuator/hotkey/worker/state` MVC path; the read shape is unchanged.)

| Method | Path                             | Description                                                                    |
| ------ | -------------------------------- | ------------------------------------------------------------------------------ |
| `GET`  | `/actuator/hotkey-worker-state`  | Return current `confirmCount`, `coolCount`, `preCoolGraceCount`, `trackedKeys` |
| `POST` | `/actuator/hotkey-worker-state`  | Update one or more parameters (body: `{"confirmCount":5,"coolCount":15}` — typed fields, absent means keep) |

**Read current state:**

```bash
curl http://localhost:8080/actuator/hotkey-worker-state
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
curl -X POST http://localhost:8080/actuator/hotkey-worker-state \
  -H "Content-Type: application/json" \
  -d '{"confirmCount":5,"coolCount":15}'
```

**Example response:**

```json
{
  "status": "ok"
}
```

**Validation:** the write-applied combination must satisfy the same invariant the config-negotiation layer enforces on heartbeat gossip — `confirmCount >= 1`, `preCoolGraceCount >= 1` and `coolCount > preCoolGraceCount` (provided fields override, the others keep their current values). A violating write is rejected with `"status": "error"` and nothing is applied: a locally-accepted but gossip-rejected config would leave this Worker permanently divergent with no reconciliation path. A write with no recognized fields is a pure no-op (no rewrite, no timestamp bump). Malformed (non-numeric) input never reaches the method — the actuator framework rejects it with a 400 before dispatch.
