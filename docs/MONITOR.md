[← Back to Home](../README.md)

# Monitoring

Zeta exposes two complementary surfaces:

- **Actuator endpoints**
- **Micrometer meters**

## 1. Exposure and Role Matrix

**Prerequisites.** `spring-boot-starter-actuator` for the endpoints; `micrometer-core` plus a
registry implementation for the meters. Without a `MeterRegistry` the Worker runs meter-less rather
than failing startup.

The endpoints are Actuator `@Endpoint` beans, not MVC controllers: they participate in
`management.endpoints.web.exposure.include` and run on the management plane (port, exposure and
roles all follow `management.*`). Endpoint ids cannot nest, so every path is flat.

```yaml
# App process
management:
  endpoints:
    web:
      exposure:
        include: health,hotkey,hotkeyring
  endpoint:
    hotkey:
      roles: ADMIN # enforced only when Spring Security is present
    hotkeyring:
      roles: ADMIN

# Worker process (default exposure: health only)
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
          include: livenessState,ping # no RabbitMQ
        readiness:
          include: readinessState,rabbit

# Opt-in only: change the include above to `health,hotkeyworker`, then add:
# management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker
# management.endpoint.hotkeyworker.roles: ADMIN
```

> The official `zeta-worker` distribution (jar/image) exposes only `health`. The detection view is opt-in: add `hotkeyworker` to the include and set the path mapping (and, with Spring Security, the endpoint role). Boot 3.5 requires an alphanumeric endpoint id, so the id is `hotkeyworker` while the public URL stays `/actuator/hotkey-worker`. Apps always configure their own include.

**Worker health is two probes, split by dependency.** Liveness (`/actuator/health/liveness`)
covers `livenessState,ping` and deliberately excludes RabbitMQ, so a broker outage cannot restart
a Worker — a restart drops its in-memory windows and bumps its epoch. Readiness
(`/actuator/health/readiness`) adds the `rabbit` indicator: `DOWN` there means the Worker cannot
consume reports and should be taken out of service, while liveness stays `UP`. The shipped
`zeta-worker` image's `HEALTHCHECK` calls `/actuator/health/liveness`; an orchestrator or load
balancer should probe `/actuator/health/readiness`. The aggregate `/actuator/health` is not the
container probe.

| Endpoint                  | Methods | Served by | Created when                                                      | Answers                                                     | CONFIG                                                         |
| ------------------------- | ------- | --------- | ----------------------------------------------------------------- | ----------------------------------------------------------- | -------------------------------------------------------------- |
| `/actuator/hotkey`        | `GET`   | App       | Actuator on the classpath; every section is optional              | Live counters for detection, cache, reporting, health, sync | [§5](CONFIG.md#5-reporting-cadence)                            |
| `/actuator/hotkeyring`    | `GET`   | App       | `zeta.local.consistent-hashing.enabled=true` (default) + Actuator | Ring topology and per-key routing                           | [§7](CONFIG.md#7-worker-listener-heartbeat-and-cluster-health) |
| `/actuator/hotkey-worker` | `GET`   | Worker    | Worker + Actuator + explicit `hotkeyworker` exposure opt-in       | Detection summary, per-key window state and rule counters   | [§11](CONFIG.md#11-detection-window-rules-limits-cooling)      |

## 2. App Runtime Diagnostics: `/actuator/hotkey`

This is the first endpoint to call when a key is unexpectedly HOT/COOL, when reports seem lost, or
when reads are staler than expected. It aggregates the App's detection, cache, reporter, refresh,
version and sync state in one response.

### 2.1 Request templates

```bash
APP=http://localhost:8080

# Full snapshot
curl -s "$APP/actuator/hotkey" | jq

# Cap the TopK and staleDebt lists
curl -s "$APP/actuator/hotkey?limit=20" | jq

# Project only the signals used by an alert or dashboard
curl -s "$APP/actuator/hotkey" \
  | jq '{health: .worker.health,
         heartbeatAgeMs: .worker.msSinceLastAnyHeartbeat,
         hotKeys: [.local.topK[].key],
         reportDepth: .local.reportQueueDepth,
         reportDropped: .local.reportQueueFullCount}'
```

`limit` caps `topK` and `staleDebt`. The default is `100`; values below `1` are clamped to `1`, and
`staleDebt` never returns more than 100 entries.

### 2.2 Response shape

The example shows an App with every plane wired. Sections and individual fields disappear when
their component is absent (see §2.4).

```jsonc
{
  "instanceId": "a1b2c3d4", // zeta.local.instance-id (or its derived fallback)
  "jvmId": 7263819201938475, // random per-JVM process id
  "local": {
    "topK": [{ "key": "cache:shop:17", "add": 1523 }], // local HeavyKeeper ranking
    "totalRequests": 158392,
    "cacheSize": 87, // estimated L1 entries
    "inFlightSize": 3, // loaders currently executing
    "reportQueueDepth": 0,
    "reportQueueFullCount": 0,
    "reportPendingKeys": 0, // keys buffered in the access counter
    "refreshPoolAvailable": 100,
    "staleDebt": { "k1": 95000 }, // key -> ms since its lease episode began
    "versionClockSkewMillis": 12,
  },
  "worker": {
    "health": "healthy",
    "msSinceLastAnyHeartbeat": 1234, // -1 = no heartbeat ever received
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

### 2.3 Topics

**Identity**

| Field        | Meaning                                                                                                  | CONFIG                                |
| ------------ | -------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| `instanceId` | Instance identity used in queue names and version stamps. Changes when `zeta.local.instance-id` changes. | [§1](CONFIG.md#1-deployment-identity) |
| `jvmId`      | Random per-process identifier; changes on every restart.                                                 | —                                     |

**Local: detection, cache and dedup**

| Field           | Meaning                                                                                                | CONFIG                                            |
| --------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------- |
| `topK[]`        | Current local ranking, descending. Each entry is `{ "key": ..., "add": ... }`; capped by `?limit`.     | [§2](CONFIG.md#2-local-hot-key-detection-topk)    |
| `totalRequests` | Decay-adjusted tracked access volume; it moves down as traffic subsides and is not a cumulative count. | [§2](CONFIG.md#2-local-hot-key-detection-topk)    |
| `cacheSize`     | Estimated entries in the single L1 Caffeine table.                                                     | [§3](CONFIG.md#3-l1-cache-and-ttl)                |
| `inFlightSize`  | Loader suppliers currently executing — not the size of the SingleFlight dedup table.                   | [§4](CONFIG.md#4-loading-concurrency-and-refresh) |

**Local: reporting**

| Field                  | Meaning                                                                                                                                      | CONFIG                              |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `reportQueueDepth`     | Batches waiting in the reporter dispatch queue. A rising depth means the reporter is falling behind.                                         | [§6](CONFIG.md#6-report-queue)      |
| `reportQueueFullCount` | Cumulative batches dropped when the dispatch queue stayed full past `queue-offer-timeout-ms`, or withdrawn because the dispatcher shut down. | [§6](CONFIG.md#6-report-queue)      |
| `reportPendingKeys`    | Keys currently buffered in the shared access counter, waiting for the next tide.                                                             | [§5](CONFIG.md#5-reporting-cadence) |

**Local: refresh and version**

| Field                    | Meaning                                                                                                                                                                     | CONFIG                                            |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `refreshPoolAvailable`   | Available permits of the background-refresh limiter. `0` means soft-expiry refreshes are queued behind the pool.                                                            | [§4](CONFIG.md#4-loading-concurrency-and-refresh) |
| `staleDebt`              | Currently leased keys, oldest first (max 100): key → milliseconds since the current lease episode began. This is the audit surface for bounded staleness.                   | [§4](CONFIG.md#4-loading-concurrency-and-refresh) |
| `versionClockSkewMillis` | How far the newest known stamp (local stamp clock or an observed peer stamp, refused outliers included) leads local physical time. Persistent growth points at clock drift. | [§1](CONFIG.md#1-deployment-identity)             |

**Worker: cluster health and decision isolation**

| Field                     | Meaning                                                                                                                                                                                                    | CONFIG                                                         |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `health`                  | `healthy` when the heartbeat view passes the cluster gate; otherwise `unhealthy`.                                                                                                                          | [§7](CONFIG.md#7-worker-listener-heartbeat-and-cluster-health) |
| `msSinceLastAnyHeartbeat` | Milliseconds since any Worker heartbeat; `-1` means none has ever arrived.                                                                                                                                 | [§7](CONFIG.md#7-worker-listener-heartbeat-and-cluster-health) |
| `foreignAppDrops`         | Cumulative HOT/COOL decisions dropped because their `appName` header did not match.                                                                                                                        | [§7](CONFIG.md#7-worker-listener-heartbeat-and-cluster-health) |
| `lastForeignApp`          | Most recent foreign `appName`; present only after a drop.                                                                                                                                                  | [§7](CONFIG.md#7-worker-listener-heartbeat-and-cluster-health) |
| `coolDispatchRejections`  | COOL decisions the ordered dispatcher refused at submit time (per-key queue full, or closed). Each is applied inline on the consumer thread; a growing value signals dispatcher saturation, not data loss. | [§7](CONFIG.md#7-worker-listener-heartbeat-and-cluster-health) |

**Sync**

| Field              | Meaning                                                                                                                                               | CONFIG                       |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| `foreignAppDrops`  | Cumulative sync messages dropped for an `appName` mismatch.                                                                                           | [§8](CONFIG.md#8-cache-sync) |
| `lastForeignApp`   | Most recent foreign sync sender; present only after a drop.                                                                                           | [§8](CONFIG.md#8-cache-sync) |
| `nodeIdCollisions` | Inbound sync messages from a peer sharing this node's `dataVersion` node id. A growing value means explicit `zeta.local.node-id` values are required. | [§8](CONFIG.md#8-cache-sync) |

### 2.4 Which parts appear, and what is deliberately omitted

| Part                                                                 | Appears when                                                                               |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `local`                                                              | At least one App-side component exists (absent on a Worker-only process)                   |
| `local.topK`, `local.totalRequests`                                  | App-side TopK exists (`zeta.worker.enabled=false`)                                         |
| `local.cacheSize` / `local.inFlightSize`                             | L1 cache / SingleFlight exists                                                             |
| `local.reportQueueDepth`, `reportQueueFullCount`                     | Reporter is wired (`zeta.report.enabled=true`, RabbitMQ on the classpath, not Worker-only) |
| `local.reportPendingKeys`                                            | Shared access counter exists (reporter fallback otherwise)                                 |
| `local.refreshPoolAvailable`, `staleDebt`                            | Background refresher exists                                                                |
| `worker.health`, `worker.msSinceLastAnyHeartbeat`                    | `HealthView` exists (always; meaningful on an App)                                         |
| `worker.foreignAppDrops`, `lastForeignApp`, `coolDispatchRejections` | `zeta.worker-listener.enabled=true`                                                        |
| `sync.*`                                                             | `zeta.sync.enabled=true` and the sync listener or default decision handler is wired        |

## 3. Ring Inspection: `/actuator/hotkeyring`

The ring decides which Worker receives a report. Use this endpoint when reports reach the wrong
Worker, when a Worker restart does not change routing, or when `min-alive-workers` degradation is
suspected.

The endpoint reconciles the ring against the health view before returning, so a Worker that died
since the last report flush is already absent; no synthetic traffic is needed to inspect failover.

**Gate.** `zeta.local.consistent-hashing.enabled=true` (default) creates the endpoint; it gates
inspection only — report routing always uses the ring even when the endpoint is disabled
([CONFIG §7](CONFIG.md#7-worker-listener-heartbeat-and-cluster-health)). The report plane must be
wired; otherwise every call returns **503** with a message naming the cause.

### 3.1 Request templates

```bash
APP=http://localhost:8080

# 1. Topology: which Workers are currently live
curl -s "$APP/actuator/hotkeyring" | jq

# 2. Which Worker owns a key
curl -s "$APP/actuator/hotkeyring/cache:shop:17" | jq

# 3. A key containing '/' is passed as remaining path segments
curl -s "$APP/actuator/hotkeyring/api/v1/product:42" | jq
```

Example responses:

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

## 4. Worker Detection Runtime: `/actuator/hotkey-worker`

**Gate.** Worker mode (`zeta.worker.enabled=true`, which requires RabbitMQ on the classpath),
Actuator on the classpath, and an explicit opt-in: the shipped Worker exposes only `health`.
Add `hotkeyworker` to the web exposure include and set
`management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker` to publish the endpoint.
Read-only: detection is configured once at startup and validated as a whole; there is no runtime
write.

### 4.1 Summary

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

| Field                     | Answers                                                                                                                                                    |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `detection.window`        | The effective window actually in force, not the configured duration                                                                                        |
| `detection.rulesHash`     | Fingerprint of the effective rule text; two Workers reporting different hashes judge the same traffic by different rules                                   |
| `detection.minHotCount`   | The promotion floor over the default and non-`exact` rules; an effective `exact` rule is indexed and checked before it                                     |
| `rules[].effectiveHotQps` | The threshold actually in force, reverse-converted from the window-count threshold and the effective window; rounding can differ from the configured value |
| `rules[].matches`         | Reports whose window sum cleared this rule's `hotCount`; the counter is the same quantity for exact, prefix, regex and default rules                       |
| `hot.active`              | Keys currently holding a HOT seat; `active` pinned at the seat cap with a rising `rejected` means the cap is binding                                       |
| `cooling.scheduled`       | Checks currently linked in the cool-down wheels; it tracks the hot-key count, and a value far above it means entries are not being cooled                  |

### 4.2 Single key

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

`promotedForMs` is "hot for this long" under either policy. `belowForMs` exists only for
`COOL_FOR_LOW_QPS` while the key is actually below the band; `EXPIRE` reports `null`. The two
fields are separate on purpose — a single `sinceMs` would silently change meaning with the policy.
`nextCheckInMs` is the remaining time to the registered cool-down check; it is negative while the
check is overdue (between its deadline and the wheel hand-off, or while the repair sweep is
pending), which is the useful "overdue by" reading rather than an error.

Returns real key names and their traffic; protect the endpoint like the App's `hotkey` endpoint
in production.

## 5. Micrometer Metrics

The meters are split across two registries by module: the common module registers the App-side
meters, the L1 cache meter binder and the decision-listener counters; the Worker module registers
the detection-plane meters. Every meter is conditional on its component, so a meter missing from
the scrape means the corresponding plane is not present in that process.

### 5.1 Request templates

```bash
APP=http://localhost:8080

# List every zeta meter on this process
curl -s "$APP/actuator/metrics" | jq -r '.names[] | select(startswith("zeta."))'

# Read one meter
curl -s "$APP/actuator/metrics/zeta.reporter.queue.depth" | jq '{name, measurements}'

# Read one tagged series (Caffeine metrics use the cache.* namespace)
curl -s "$APP/actuator/metrics/cache.gets?tag=cache:zeta.l1&tag=result:hit" | jq '.measurements'

# Prometheus scrape (requires micrometer-registry-prometheus)
curl -s "$APP/actuator/prometheus" | grep -E '^(zeta_|cache_)'
```

Prometheus converts dots to underscores and appends `_total` to counters: `zeta.reporter.queue.depth`
becomes `zeta_reporter_queue_depth`, `zeta.worker.decisions.hot` becomes
`zeta_worker_decisions_hot_total`.

### 5.2 L1 cache (`cache.*`, tag `cache=zeta.l1`)

Caffeine metrics use Micrometer's standard `cache.*` namespace, tagged `cache=zeta.l1`; they are
**not** prefixed `zeta.l1.*`. The L1 is a plain `Cache` loaded through SingleFlight, so the
`LoadingCache`-only `cache.load` and `cache.load.duration` meters are not registered.

| Meter                   | Type            | Tags                                 | Meaning                                                                                            |
| ----------------------- | --------------- | ------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `cache.gets`            | FunctionCounter | `cache=zeta.l1`, `result=hit`/`miss` | Cache lookups by result                                                                            |
| `cache.puts`            | FunctionCounter | `cache=zeta.l1`                      | Entries written                                                                                    |
| `cache.evictions`       | FunctionCounter | `cache=zeta.l1`                      | Entries evicted by size, expiry or reference collection (explicit invalidation is not an eviction) |
| `cache.eviction.weight` | FunctionCounter | `cache=zeta.l1`                      | Evicted entry weight                                                                               |
| `cache.size`            | Gauge           | `cache=zeta.l1`                      | Estimated current size                                                                             |

`recordStats()` is always enabled on Zeta's L1; a custom cache bean that omits it will report only
`cache.size`.

### 5.3 Local detection and dedup

| Meter                                       | Type            | Tags         | Meaning                                                                                      |
| ------------------------------------------- | --------------- | ------------ | -------------------------------------------------------------------------------------------- |
| `zeta.topk.size`                            | Gauge           | `type=local` | Current local TopK size                                                                      |
| `zeta.topk.total`                           | Gauge           | `type=local` | Decay-adjusted tracked access volume (moves down as traffic subsides)                        |
| `zeta.expelled.queue.size`                  | Gauge           | —            | Keys waiting in the expelled-key staging queue                                               |
| `zeta.singleflight.inflight`                | Gauge           | —            | Loader suppliers currently executing (not the dedup table size)                              |
| `zeta.stall.source_degraded.stopped`        | Gauge           | —            | `1` while the circuit breaker is open (loads fast-fail; reads may serve soft-expired values) |
| `zeta.stall.source_degraded.timeouts.total` | FunctionCounter | —            | Cumulative leader loads that hit the per-load reader timeout                                 |

### 5.4 Reporting and the feed loop

| Meter                                      | Type            | Meaning                                                                                                                                                                  |
| ------------------------------------------ | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `zeta.reporter.queue.depth`                | Gauge           | Batches waiting in the reporter dispatch queue                                                                                                                           |
| `zeta.reporter.queue.dropped.total`        | FunctionCounter | Cumulative batches dropped because the queue was full or withdrawn during dispatcher shutdown                                                                            |
| `zeta.reporter.queue.expired.dead.total`   | FunctionCounter | Expired batches whose target Worker was no longer alive                                                                                                                  |
| `zeta.reporter.queue.expired.stale.total`  | FunctionCounter | Expired batches that waited more than 5 s in the queue                                                                                                                   |
| `zeta.reporter.pending.keys`               | Gauge           | Keys buffered in the shared access counter                                                                                                                               |
| `zeta.reporter.feedloop.interval`          | Gauge           | Feed-loop base flush interval in ms; in `shadow` mode this is the trajectory that _would_ be applied (registered only when `report-interval-tuning != off`)              |
| `zeta.reporter.feedloop.score`             | Gauge           | Feed-loop score in basis points — smoothed arrival rate projected onto the interval in force for the measured window, compared with the target (`10000` means on target) |
| `zeta.reporter.feedloop.batch`             | Gauge           | Projected keys per flush at the interval that produced the score                                                                                                         |
| `zeta.stall.broadcast_storm.stopped.total` | FunctionCounter | Put/refresh broadcasts lost to broker errors or a saturated send executor                                                                                                |

### 5.5 Refresh, version and sync

| Meter                                      | Type            | Meaning                                                                                                                                       |
| ------------------------------------------ | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.expire.refresh.available`            | Gauge           | Available background-refresh limiter permits                                                                                                  |
| `zeta.expire.refresh.failure.leased.total` | FunctionCounter | Cumulative refresh failures granted a lease (the stale value stayed servable)                                                                 |
| `zeta.expire.refresh.lease.debt.keys`      | Gauge           | Keys currently leased (the `staleDebt` size)                                                                                                  |
| `zeta.version.clock.skew`                  | Gauge           | How far the newest known stamp — local stamp clock or observed peer stamp, refused outliers included — leads local physical time, in ms       |
| `zeta.sync.send.failed.total`              | FunctionCounter | Cumulative sync-plane publish failures; the only signal for an INVALIDATE that never left the node                                            |
| `zeta.sync.nodeid.collision.total`         | FunctionCounter | Inbound sync messages from a peer sharing this node's `dataVersion` node id; persistent growth calls for explicit `zeta.local.node-id` values |

### 5.6 Worker detection plane

| Meter                                                 | Type                        | Meaning                                                                                                                                                                                                           |
| ----------------------------------------------------- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.worker.alive`                                   | Gauge                       | `1` while the heartbeat view passes the cluster gate; read it on an App                                                                                                                                           |
| `zeta.worker.detector.keys`                           | Gauge                       | Keys holding a live sliding-window entry                                                                                                                                                                          |
| `zeta.worker.detector.capacity.evictions`             | FunctionCounter             | Idle windows this sampling pass actually removed to admit a new key at `max-tracked-keys` (Prometheus `_total`)                                                                                                   |
| `zeta.worker.detector.capacity.rejections`            | FunctionCounter             | New keys rejected at `max-tracked-keys`: the hard headroom is full, or the 50 ms saturation window opened by a completed reclaim sample that found no idle window is still active (Prometheus `_total`)           |
| `zeta.worker.hot.active`                              | Gauge                       | Keys currently holding a HOT seat                                                                                                                                                                                 |
| `zeta.worker.hot.capacity`                            | Gauge                       | Seat capacity (`detection.limits.max-active-hot-keys`)                                                                                                                                                            |
| `zeta.worker.hot.rejected`                            | FunctionCounter             | Promotions refused at the seat cap (read from the registry's LongAdder)                                                                                                                                           |
| `zeta.worker.hot.aborted`                             | Counter                     | Promotions whose HOT never reached the broker; the seat was returned                                                                                                                                              |
| `zeta.worker.decisions.hot` / `.cool`                 | Counter                     | Decisions handed to the AMQP client without an exception; no publisher confirm is requested                                                                                                                       |
| `zeta.worker.decisions.dropped`                       | Counter, tag `type`         | Decisions lost without reaching the broker (publish failure, queue saturation, unexpected drain failure, shutdown)                                                                                                |
| `zeta.worker.decisions.elided`                        | Counter, tag `type`         | Decisions elided as no-ops: same type inside the 100 ms debounce, a replay of the exact accepted version, a COOL for a key the cluster already holds cold, or a COOL whose displaced HOT never reached the funnel |
| `zeta.worker.decisions.superseded`                    | Counter, tag `type`         | Decisions dropped because a strictly newer version for the key was already queued or accepted, or because the registry no longer holds the HOT's promotion                                                        |
| `zeta.worker.decisions.expired`                       | Counter, tag `type`         | Decisions that would have been sent but exceeded `max-decision-age-ms`; a decision the funnel elides or supersedes is counted there, not here                                                                     |
| `zeta.worker.broadcast.hot.depth` / `.cool.depth`     | Gauge                       | Pending decisions in each queue                                                                                                                                                                                   |
| `zeta.worker.cooling.scheduled`                       | Gauge                       | Cool-down checks currently linked in the wheels                                                                                                                                                                   |
| `zeta.worker.cooling.lateness`                        | Timer                       | How late a wheel-fired due check was processed (the repair sweep's grace is excluded)                                                                                                                             |
| `zeta.worker.cooling.recheck`                         | Counter                     | LOW_QPS re-checks that found the window at or above its cool band; an EXPIRE entry never re-arms, so it cools on its deadline and never counts here                                                               |
| `zeta.worker.cooling.reason`                          | Counter, tag `reason`       | Cool-downs by trigger: `low_qps`, `expire`, `sweep`                                                                                                                                                               |
| `zeta.worker.rule.matches` / `.promotions` / `.cools` | FunctionCounter, tag `rule` | Per-rule eligible-report, promotion and cool-out counts, read from the rule's counter field                                                                                                                       |
| `zeta.worker.reports.epoch.dropped`                   | Counter                     | Report batches dropped by the incarnation fence                                                                                                                                                                   |
| `zeta.worker.reports.stale.dropped`                   | Counter                     | Report batches dropped by the optional staleness filter                                                                                                                                                           |
| `zeta.worker.detection.disabled.dropped`              | Counter                     | Reports skipped because detection is disabled                                                                                                                                                                     |
| `zeta.worker.report.eval`                             | Timer                       | Per-report-batch evaluation latency (monotonic clock, ns resolution)                                                                                                                                              |

The rule counters and the three `cooling.reason` series are registered with the active
configuration, not lazily: every current rule appears at zero from the first scrape, and a rule that
a publication removes has its three series removed with it. A retired rule's late cool-down is
dropped rather than recreating the series.

The decision listener's own counters are registered by the **common** module's
`ZetaMicrometerAutoConfiguration` when a `WorkerListener` bean exists:

| Meter                                           | Type            | Meaning                                                                   |
| ----------------------------------------------- | --------------- | ------------------------------------------------------------------------- |
| `zeta.worker.decision.cool.rejected.total`      | FunctionCounter | COOL decisions the App's ordered dispatcher refused and re-applied inline |
| `zeta.worker.decision.epoch.regress.skip.total` | FunctionCounter | Decisions skipped for a same-nodeId epoch rollback                        |

The Worker detection-plane meters are registered by the **Worker module's `WorkerAutoConfiguration`**,
not by `ZetaMicrometerAutoConfiguration`: every other row reads a worker-module class
(`SlidingWindowDetector`, `HotKeyRegistry`, `RuleResolver`, the queues), while the common
auto-configuration can only see common-module types. `zeta.worker.alive` is the exception: the
common `ZetaMicrometerAutoConfiguration` registers it from the common `HealthView`, so it also
appears on an App that has no Worker module. Counters are created with the meters bean;
gauges bind once the components they read exist. They register only when a `MeterRegistry` bean
exists; without one, every recording call is a no-op.

Metric identities, per decision type: `promotions == decisions.hot + dropped{type=hot} +
elided{type=hot} + superseded{type=hot} + expired{type=hot}` and the same shape for
`cools`/`decisions.cool`/`type=cool`. Every decision that enters delivery ends in exactly one
bucket. A HOT lost to a terminal delivery failure is also counted in `hot.aborted` because its
promotion was undone; a HOT superseded by a newer COOL was already removed and its seat released by
that transition, so it appears as `cools` instead. At a quiescent scrape the seat ledger is
therefore `promotions == cools + hot.aborted + hot.active`.
