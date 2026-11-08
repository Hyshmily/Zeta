[← Back to Home](../README.md)

# Configuration Reference

Every Zeta setting lives under `zeta.*`. An App mainly uses `zeta.local.*`, `zeta.report.*`, `zeta.sync.*` and
`zeta.worker-listener.*`; a Worker uses `zeta.worker.*`; `zeta.app-name` is shared by both ends.

## 1. Deployment Identity

`zeta.app-name` is the only tenant key. Report routing, the decision **routing key** (`send.<app-name>`, which
carries HOT/COOL — the message header is only a receiver-side check), cache-sync messages and Worker heartbeats all
discriminate applications by it, so the App and the Worker must configure the same value. `zeta.worker.enabled` decides whether
a process is an App or Worker-only; `zeta.worker-listener.enabled` decides whether an App consumes the Worker's
heartbeats and HOT/COOL decisions. `instance-id` and `node-id` keep queue names and version stamps from colliding
across instances.

App side:

```yaml
zeta:
  app-name: order
  local:
    instance-id: ${HOSTNAME} # empty: ${server.port:instance}-${HOSTNAME}
    node-id: 1 # unique per instance; unset: CRC32 of instance-id
  report:
    enabled: true
  worker-listener:
    enabled: true # must be true when a Worker cluster is deployed
```

Worker side:

```yaml
zeta:
  app-name: order
  worker:
    enabled: true
    epoch-file: /data/zeta/epoch # must survive restarts
```

| Property                                | Default   | Notes                                                                                                                                                                                                                                    |
| --------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.app-name`                         | `default` | Logical application name, non-blank, must match `[A-Za-z0-9_-]+`. Deployments sharing a broker must each set a unique value; the default logs a WARN at startup.                                                                         |
| `zeta.local.instance-id`                | `""`      | Instance identity used for queue naming and node-id derivation; when empty it resolves to `${server.port:instance}-${HOSTNAME}`, reading the `server.port` **system property** and substituting a per-JVM UUID when `HOSTNAME` is unset. |
| `zeta.local.node-id`                    | unset     | 10-bit version-stamp node id (`0..1023`); when unset it is the stable CRC32 of `instance-id`. Set it explicitly per instance so two nodes cannot mint equal stamps in the same millisecond.                                              |
| `zeta.local.clock-max-forward-drift-ms` | `60000`   | How far an observed peer stamp may lead local physical time. A stamp past the bound does not raise the local logical clock, but its message is still applied; `0` accepts only stamps not later than the current local time.             |
| `zeta.worker.enabled`                   | `false`   | `true` selects Worker-only mode: no L1 cache is created, and `get()`/`put()` throw `ZetaModeException`.                                                                                                                                  |
| `zeta.worker-listener.enabled`          | `false`   | `true` makes the App consume Worker heartbeats and HOT/COOL decisions. **With a Worker cluster deployed and this off, every report is silently discarded.**                                                                              |

## 2. Local Hot-Key Detection (TopK)

The App maintains a local TopK with HeavyKeeper (a Count-Min Sketch variant). It is a fast heuristic only: a key in
the TopK set is promoted to HOT locally and gets the longer TTL, but that is not a cluster-wide verdict — authority
stays with the Worker.

```yaml
zeta:
  local:
    top-k: 100
    width: 50000 # aligned up to a power of two when the bean is created
    depth: 5
    decay: 0.92
    min-count: 10
    sketch-window-count: 4
    expelled-queue-capacity: 10000
```

| Property                             | Default | Notes                                                                                                                                                                             |
| ------------------------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.top-k`                   | `100`   | Size of the TopK set.                                                                                                                                                             |
| `zeta.local.width`                   | `50000` | Sketch width; aligned up to the nearest power of two when the bean is created (65,536 in practice by default) so bucket indexing takes the bit-mask fast path. Capped at 200,000. |
| `zeta.local.depth`                   | `5`     | Sketch depth (rows). The App only needs a fast heuristic, so a shallow sketch suffices; the authoritative decision is the Worker's.                                               |
| `zeta.local.decay`                   | `0.92`  | Counter decay factor.                                                                                                                                                             |
| `zeta.local.min-count`               | `10`    | Minimum count for a key to enter the TopK.                                                                                                                                        |
| `zeta.local.sketch-window-count`     | `4`     | Sliding windows per sketch slot (ring buffer). Range 2–10, rounded up to a power of two when the bean is created; the default covers four decay periods and damps hotspot drift.  |
| `zeta.local.expelled-queue-capacity` | `10000` | Staging queue for keys expelled from the TopK, bounding growth while the sketch scales up.                                                                                        |

## 3. L1 Cache and TTL

All cache names share one Caffeine table, and `cache.max-size` is the process-wide bound on that table. Entries
carry two TTL regimes — normal and hot: the hard TTL is Caffeine eviction, while the soft TTL serves the stale value
and refreshes in the background (stale-while-revalidate). Every TTL gets a ±5% random jitter by default, so entries
do not expire in lockstep.

```yaml
zeta:
  local:
    cache:
      max-size: 100000
      tombstone-maximum-size: 10000
      tombstone-ttl-seconds: 600
    hard-ttl-ms: 300000 # normal key: 5 minutes
    soft-ttl-ms: 30000 # normal key: 30 seconds
    hot-hard-ttl-ms: 3600000 # hot key: 1 hour
    hot-soft-ttl-ms: 300000 # hot key: 5 minutes
    ttl-jitter-ratio: 0.05
```

| Property                                  | Default          | Notes                                                                                                                                        |
| ----------------------------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.cache.max-size`               | `100000`         | Maximum entries in the single L1 table, shared by every cache name.                                                                          |
| `zeta.local.cache.tombstone-maximum-size` | `10000`          | Cap on per-key invalidation tombstones, bounding the anti-resurrection state during a mass-invalidation burst.                               |
| `zeta.local.cache.tombstone-ttl-seconds`  | `600`            | Tombstone retention. It only has to outlive one in-flight read/load; past it, a fill that started before the invalidation is admitted again. |
| `zeta.local.hard-ttl-ms`                  | `300000` (5 min) | Hard TTL for normal keys; `0` means permanent (no hard expiry).                                                                              |
| `zeta.local.hot-hard-ttl-ms`              | `3600000` (1 h)  | Hard TTL for hot keys; `0` means permanent (no hard expiry).                                                                                 |
| `zeta.local.soft-ttl-ms`                  | `30000` (30 s)   | Soft TTL for normal keys; once expired, the stale value is served and refreshed asynchronously. `0` disables soft expiry.                    |
| `zeta.local.hot-soft-ttl-ms`              | `300000` (5 min) | Soft TTL for hot keys; `0` disables soft expiry.                                                                                             |
| `zeta.local.ttl-jitter-ratio`             | `0.05`           | Random offset ratio applied to every TTL; `0.05` = ±5%, `0` disables it.                                                                     |

## 4. Loading, Concurrency and Refresh

On a read miss, `SingleFlight` collapses concurrent loads of the same key into one execution; `in-flight-timeout-seconds`
bounds a single load's execution time (queue wait is not counted). Loads and background refreshes share one bounded
pool, and `refresh-max-pools` further limits concurrent refreshes with a semaphore.

```yaml
zeta:
  local:
    in-flight-max-size: 50000
    in-flight-ttl-seconds: 5
    in-flight-timeout-seconds: 3
    executor-core-pool-size: 8
    executor-max-pool-size: 32
    executor-queue-capacity: 2000
    executor-rejection: abort
    refresh-max-pools: 0 # 0 = executor-max-pool-size / 2
```

| Property                               | Default    | Notes                                                                                                                                                                                                                                                                                                                                                                       |
| -------------------------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.in-flight-max-size`        | `50000`    | Maximum number of SingleFlight deduplication entries.                                                                                                                                                                                                                                                                                                                       |
| `zeta.local.in-flight-ttl-seconds`     | `5`        | Deduplication entry TTL; must exceed the slowest remote data source response.                                                                                                                                                                                                                                                                                               |
| `zeta.local.in-flight-timeout-seconds` | `3`        | Per-load execution timeout. A timeout is suppressed as `LoadStatus.SUPPRESSED_TIMEOUT` and the read throws `ZetaSuppressedException` (design decisions: read path). Deduplication retention is `max(in-flight-ttl-seconds, timeout + 1)`.                                                                                                                                   |
| `zeta.local.executor-core-pool-size`   | `8`        | Load pool core size.                                                                                                                                                                                                                                                                                                                                                        |
| `zeta.local.executor-max-pool-size`    | `32`       | Load pool maximum size; must be greater than or equal to `executor-core-pool-size`.                                                                                                                                                                                                                                                                                         |
| `zeta.local.executor-queue-capacity`   | `2000`     | Load pool queue capacity.                                                                                                                                                                                                                                                                                                                                                   |
| `zeta.local.executor-rejection`        | `abort`    | Policy when both the queue and the max pool are saturated: `abort` throws a rejection, `caller-runs` runs on the submitting thread. **Refresh fan-out pool only** — the load and send pools always abort. A load-pool rejection surfaces as `LoadStatus.SUPPRESSED_EXECUTOR`, and the read throws `ZetaSuppressedException` unless the breaker is open; it is never a miss. |
| `zeta.local.refresh-max-pools`         | `0` (auto) | Maximum concurrent soft-expiry background refreshes. `0` takes half of `executor-max-pool-size` (at least 1); refreshes share the pool with read misses, so a value above the pool size is clamped to `executor-max-pool-size`.                                                                                                                                             |

## 5. Reporting Cadence

The App accumulates access counts in a shared counter and merges them per "tide" before reporting to the Worker.
`zeta.local.report-interval-ms` is the base interval (50 ms by default; anything below 50 ms is clamped by the
WaveCounter schedule to 50 ms). `zeta.local.report-interval-tuning` decides whether that interval is fixed or adapts
per instance:

| Mode               | Behaviour                                                                                                                          |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------- |
| `off`              | No tuner; the interval stays `report-interval-ms`, and `min-ms`, `max-ms` and `target-batch` have no effect.                       |
| `shadow` (default) | The tuner computes the interval and exposes what it would use through the `zeta.reporter.feedloop.*` meters, but never applies it. |
| `on`               | Every flush (including an empty window) applies the computed interval, clamped into `[min-ms, max-ms]`.                            |

Example — a 100–800 ms dynamic reporting period that still fits inside the Worker's 1 s window:

```yaml
zeta:
  report:
    enabled: true
  local:
    report-interval-ms: 200 # seed; clamped when outside [min, max]
    report-interval-min-ms: 100
    report-interval-max-ms: 800 # idle worst case: 2 x 800 = 1600 ms, still < 2 x 1 s Worker windows
    report-interval-target-batch: 512
    report-interval-tuning: on
```

Under `on`, `report-interval-ms` is only a seed. `report-interval-max-ms` is also the first-report bound for a new
hot key: at most `max-ms` under traffic and at most `2 x max-ms` once idle. With `shadow` or `on` only
`max-ms >= min-ms` is validated, and a violation fails fast at startup; an out-of-range seed is clamped instead of
failing. `target-batch` is the tuner's goal key count per flush: sparse batches lengthen the interval, dense batches
shorten it.

With no reporter wired (`zeta.report.enabled=false`, no AMQP on the classpath, or a Worker-only deployment), the
shared counter feeds the local sketch directly on the `detect-interval-ms` cadence, bypassing reporting.

| Property                                  | Default                        | Notes                                                                                                                                                                                                                                |
| ----------------------------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `zeta.report.enabled`                     | `true`                         | Enables App→Worker report aggregation; requires a `RabbitTemplate` bean.                                                                                                                                                             |
| `zeta.local.report-exchange`              | `zeta.reportToWorker.exchange` | DirectExchange the App publishes reports to, with a JSON body. Must match the Worker's `zeta.worker.messaging.report-exchange`.                                                                                                      |
| `zeta.local.report-interval-ms`           | `50`                           | Tide base / tuner seed; values below 50 ms are still scheduled at 50 ms.                                                                                                                                                             |
| `zeta.local.detect-interval-ms`           | `500`                          | Maximum gap between two sketch ingests. A due (or over-cap) tide is handed to the sketch together with the accumulator without a merge; only an earlier tide merges into the accumulator. With no reporter it is also the tide base. |
| `zeta.local.report-interval-tuning`       | `shadow`                       | `off` / `shadow` / `on`, see the table above.                                                                                                                                                                                        |
| `zeta.local.report-interval-target-batch` | `512`                          | The tuner's goal keys per batch; only meaningful when `tuning != off`.                                                                                                                                                               |
| `zeta.local.report-interval-min-ms`       | `50`                           | Tuner interval floor; below the WaveCounter's 50 ms cadence it is effectively 50 ms.                                                                                                                                                 |
| `zeta.local.report-interval-max-ms`       | `1000`                         | Tuner interval ceiling, and the first-report delay bound for a new hotspot; must be >= `min-ms`. The idle worst case is twice this value.                                                                                            |

See [design decisions: reporting](design-decisions.md#reporting) and
[design decisions: access counting](design-decisions.md#access-counting).

## 6. Report Queue

Report batches enter a bounded dispatch queue and consumer threads send them synchronously. The queue is the only
backpressure: when it is full, the submitting thread blocks for at most `queue-offer-timeout-ms` for the whole flush
and then drops the remaining batches, and a batch that waits more than 5 s in the queue is discarded on dequeue.
Nothing retries — the next tide reports fresh counts.

```yaml
zeta:
  local:
    queue-capacity: 1024
    queue-offer-timeout-ms: 100
    consumer-count: 0 # 0 = max(4, availableProcessors / 2)
```

| Property                            | Default | Notes                                                                                                                                                                                                                                                                               |
| ----------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.queue-capacity`         | `1024`  | Report dispatch queue capacity, in batches. Consumers blocked on a publish cannot drain, so this is the bound on a stalled broker's backlog; 1024 batches covers one 5 s staleness window at up to ~200 batches/s. Raise it for a deployment that fans a flush out to many Workers. |
| `zeta.local.queue-offer-timeout-ms` | `100`   | Queue write timeout, shared by the whole flush; after blocking this long the remaining batches are dropped.                                                                                                                                                                         |
| `zeta.local.consumer-count`         | `0`     | Report consumer threads; `0` resolves to `max(4, availableProcessors / 2)`.                                                                                                                                                                                                         |

## 7. Worker Listener, Heartbeat and Cluster Health

The App consumes Worker HOT/COOL decisions through `worker-listener`, and consumes heartbeats to maintain a
`HealthView`. That view decides which Worker a report is routed to, and whether the Worker cluster still holds
TTL authority or the local TopK has to take it over. Heartbeats carry the sender's `zeta.app-name` in the AMQP
`appName` header; an App drops a heartbeat whose `appName` header differs from its own, so a shared broker cannot put
another application's Workers into this tenant's ring. Heartbeats from pre-isolation Workers carry no `appName` header
and are accepted for rolling-upgrade compatibility.

```yaml
zeta:
  local:
    consistent-hashing:
      enabled: true
      virtual-nodes: 500
    heartbeat:
      exchange-name: zeta.heartbeat.exchange
      timeout-ms: 10000
      adaptive-timeout: true
      adaptive-floor-ms: 3000
      min-alive-workers: 0
  worker-listener:
    enabled: true
    exchange-name: zeta.send.exchange
    queue-prefix: zeta.worker
    auto-startup: true
    broadcast-jitter-ms: 0
    concurrent-consumers: 2
    scheduler-pool-size: 4
    prefetch-count: 5
    max-pending-units: 200000
```

### Decision listener

| Property                                    | Default              | Notes                                                                                                                                                                                                                    |
| ------------------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `zeta.worker-listener.enabled`              | `false`              | Turns on heartbeat consumption, the HOT/COOL decision listener and `HealthView`. Must be `true` when a Worker cluster is deployed.                                                                                       |
| `zeta.worker-listener.exchange-name`        | `zeta.send.exchange` | DirectExchange carrying HOT/COOL; each instance queue binds under `send.<app-name>`. Must match the Worker's `zeta.worker.messaging.broadcast-exchange`. Heartbeats use a separate `zeta.local.heartbeat.exchange-name`. |
| `zeta.worker-listener.queue-prefix`         | `zeta.worker`        | Prefix of the per-instance listener queue; the final name is `{prefix}:{instanceId}`.                                                                                                                                    |
| `zeta.worker-listener.auto-startup`         | `true`               | Whether the listener container starts with the application.                                                                                                                                                              |
| `zeta.worker-listener.broadcast-jitter-ms`  | `0`                  | Random jitter applied before a broadcast is acted on; `0` applies it on receipt.                                                                                                                                         |
| `zeta.worker-listener.concurrent-consumers` | `2`                  | Concurrent consumers of the decision queue.                                                                                                                                                                              |
| `zeta.worker-listener.scheduler-pool-size`  | `4`                  | Minimum pool size for the scheduler that runs jitter-delayed tasks; the effective value is `max(this, concurrent-consumers × 2)`.                                                                                        |
| `zeta.worker-listener.prefetch-count`       | `5`                  | AMQP prefetch count per consumer.                                                                                                                                                                                        |
| `zeta.worker-listener.max-pending-units`    | `200000`             | Global pending cap (weighted units) of the decision dispatcher; work beyond it is dropped (fire-and-forget; the entry converges through its hard TTL or a later transition).                                             |

### Heartbeat and liveness

| Property                                 | Default                   | Notes                                                                                                                                                                                                                              |
| ---------------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.heartbeat.exchange-name`     | `zeta.heartbeat.exchange` | TopicExchange carrying structured Worker heartbeats; must match `zeta.worker.messaging.heartbeat-exchange`.                                                                                                                        |
| `zeta.local.heartbeat.timeout-ms`        | `10000`                   | Heartbeat timeout. With the adaptive estimate on, it is the warm-up window and the ceiling rather than a flat threshold.                                                                                                           |
| `zeta.local.heartbeat.adaptive-timeout`  | `true`                    | Per-Worker Jacobson/Karels estimate over heartbeat inter-arrival (`SRTT + 4×RTTVAR`), clamped into `[adaptive-floor-ms, timeout-ms]`. `false` restores the fixed `timeout-ms`.                                                     |
| `zeta.local.heartbeat.adaptive-floor-ms` | `3000`                    | Lower bound of the adaptive window; must be `<= timeout-ms`. Keep it above a few heartbeat intervals (the Worker defaults to one per second) so a single lost beat or a short GC pause is not read as death.                       |
| `zeta.local.heartbeat.min-alive-workers` | `0`                       | Minimum alive Workers for cluster health. `0` derives it: one third of the observed Worker count, rounded up, floor 1 (3 observed → 1 alive is healthy, 5 → 2, 9 → 3). A positive value overrides it with an absolute alive count. |

### Hash ring

| Property                                      | Default | Notes                                                                                                                                                                  |
| --------------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.consistent-hashing.enabled`       | `true`  | Only controls whether the `/actuator/hotkeyring` inspection endpoint is created. Report routing always uses the consistent-hash ring; `false` does not change routing. |
| `zeta.local.consistent-hashing.virtual-nodes` | `500`   | Virtual nodes per physical Worker on the hash ring.                                                                                                                    |

## 8. Cache Sync

`zeta.sync.*` controls write propagation between instances. Once enabled, `put(..., PutPolicy.synced())`, `refresh`,
`invalidate` and `clearAll` broadcast PUT / REFRESH / INVALIDATE messages to the other instances of the same
`app-name`. Sync is eventually consistent: when a message is lost, the peer keeps serving the old value until the
entry's hard TTL expires.

A synced write whose effective hard TTL is permanent is rejected before the cache call: peers could only drop that
entry through the hard deadline, so it would pin a stale value forever. This includes `zeta.local.hard-ttl-ms: 0`
with no per-call override, and the default `@ZetaPut(sync = true)`. `refreshAll` is always synced and applies the
same check to its default TTL.

```yaml
zeta:
  sync:
    enabled: true
    flush-delay-ms: 50
    max-defer-ms: 500
    put-max-bytes: 32768
    put-allowed-packages:
      - com.example.order.dto
    exchange-name: zeta.sync.exchange
    queue-prefix: zeta.sync
    dedup-window-seconds: 10
    dedup-max-size: 10000
    warmup-jitter-ms: 50
    concurrent-consumers: 3
    scheduler-pool-size: 4
    prefetch-count: 5
    max-pending-units: 50000
    auto-startup: true
```

| Property                         | Default              | Notes                                                                                                                                                                                                                                                                                                                                                                        |
| -------------------------------- | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.sync.enabled`              | `false`              | Enables cross-instance cache sync (requires RabbitMQ).                                                                                                                                                                                                                                                                                                                       |
| `zeta.sync.flush-delay-ms`       | `50`                 | Merge window for pending sync records; writes to the same key inside the window are combined into one batch.                                                                                                                                                                                                                                                                 |
| `zeta.sync.max-defer-ms`         | `500`                | Maximum deferral; a flush is forced even under continuous writes, so debouncing cannot starve.                                                                                                                                                                                                                                                                               |
| `zeta.sync.put-max-bytes`        | `32768`              | Payload ceiling for one value-carrying PUT. A larger payload, or one that cannot round-trip through Jackson, degrades to a value-less REFRESH; `0` sends every synced write as a REFRESH.                                                                                                                                                                                    |
| `zeta.sync.put-allowed-packages` | `[]`                 | Extra package prefixes admitted by the deserialization whitelist of value-carrying PUTs. The built-in whitelist holds only JDK value types and `io.github.hyshmily.zeta.*`; a business DTO outside it fails closed and degrades to a versioned INVALIDATE. **Any sender able to write to the broker can instantiate a class under a listed prefix — keep the list minimal.** |
| `zeta.sync.exchange-name`        | `zeta.sync.exchange` | FanoutExchange for sync messages.                                                                                                                                                                                                                                                                                                                                            |
| `zeta.sync.queue-prefix`         | `zeta.sync`          | Per-instance queue prefix; the full name is `{prefix}:{instanceId}`.                                                                                                                                                                                                                                                                                                         |
| `zeta.sync.dedup-window-seconds` | `10`                 | Receiver-side deduplication window.                                                                                                                                                                                                                                                                                                                                          |
| `zeta.sync.dedup-max-size`       | `10000`              | Maximum entries in the deduplication cache.                                                                                                                                                                                                                                                                                                                                  |
| `zeta.sync.warmup-jitter-ms`     | `50`                 | Upper bound of the random park before a key's first sync dispatch after a broadcast, spreading peer read-backs across instances to avoid a thundering herd.                                                                                                                                                                                                                  |
| `zeta.sync.concurrent-consumers` | `3`                  | Concurrent consumers of the sync queue.                                                                                                                                                                                                                                                                                                                                      |
| `zeta.sync.scheduler-pool-size`  | `4`                  | Pool size for jitter-delayed sync scheduling.                                                                                                                                                                                                                                                                                                                                |
| `zeta.sync.prefetch-count`       | `5`                  | AMQP prefetch count per sync consumer.                                                                                                                                                                                                                                                                                                                                       |
| `zeta.sync.max-pending-units`    | `50000`              | Global pending cap of the sync dispatcher (weighted units, roughly 1 KB of payload each); work beyond it is dropped outright — the message is already acked, and the next write re-sends it.                                                                                                                                                                                 |
| `zeta.sync.auto-startup`         | `true`               | Whether the sync listener container starts with the application.                                                                                                                                                                                                                                                                                                             |

## 9. Circuit Breaker

The breaker wraps `SingleFlight.load()`. Once the shard owning a key prefix is open, loads are suppressed
immediately and `HotKeyCache.get()` tries to serve a stale entry that is still hard-valid (soft-expired, hard-alive).
A hard-expired entry is never served as stale. The breaker is off by default because enabling it changes the read
path for every user.

```yaml
zeta:
  local:
    circuit-breaker:
      enabled: true
      window-time-ms: 10000
      window-buckets: 10
      fail-threshold: 0.5
      request-volume-threshold: 20
      single-test-interval-ms: 5000
      half-open-max-probes: 3
      consecutive-success-threshold: 3
      max-prefixes: 64
      exclude-exceptions:
        - java.lang.IllegalArgumentException
```

| Property                                                   | Default | Notes                                                                                                                                                                                                                                                           |
| ---------------------------------------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.circuit-breaker.enabled`                       | `false` | Enables the sliding-window breaker.                                                                                                                                                                                                                             |
| `zeta.local.circuit-breaker.window-time-ms`                | `10000` | Sliding-window length; the window advances with traffic, there is no background rotation.                                                                                                                                                                       |
| `zeta.local.circuit-breaker.window-buckets`                | `10`    | Number of buckets in the sliding window.                                                                                                                                                                                                                        |
| `zeta.local.circuit-breaker.fail-threshold`                | `0.5`   | Failure-rate threshold (0.0–1.0); the breaker opens above it.                                                                                                                                                                                                   |
| `zeta.local.circuit-breaker.request-volume-threshold`      | `20`    | Minimum requests before the failure rate is evaluated.                                                                                                                                                                                                          |
| `zeta.local.circuit-breaker.single-test-interval-ms`       | `5000`  | Half-open probe interval; a half-open episode with no probe settlement for this long is force-retired, so a hung probe cannot pin it open.                                                                                                                      |
| `zeta.local.circuit-breaker.half-open-max-probes`          | `3`     | Maximum concurrent probes in HALF_OPEN; further calls are rejected.                                                                                                                                                                                             |
| `zeta.local.circuit-breaker.consecutive-success-threshold` | `3`     | Consecutive successes required to close from HALF_OPEN, so one lucky probe cannot cause flapping.                                                                                                                                                               |
| `zeta.local.circuit-breaker.log-enabled`                   | `true`  | Whether OPEN/CLOSE/HALF-OPEN transitions are logged.                                                                                                                                                                                                            |
| `zeta.local.circuit-breaker.exclude-exceptions`            | `[]`    | Fully qualified exception names that must not trip the breaker; a match (on the exception or its `cause`) is counted as a success.                                                                                                                              |
| `zeta.local.circuit-breaker.include-exceptions`            | `[]`    | When non-empty, only the listed exceptions trip the breaker; empty means all of them do. Excludes are checked before includes.                                                                                                                                  |
| `zeta.local.circuit-breaker.max-prefixes`                  | `64`    | Maximum live breaker shards. An annotated key shards on the cache namespace before the U+0000 namespace delimiter; a programmatic key on the segment before the first `:`; a key with neither shares one bucket. One sick data source trips only its own shard. |

## 10. Worker Basics and Messaging Ingress

A Worker-only process consumes App reports, maintains sliding windows and resolves rules, and broadcasts HOT/COOL
decisions back to the Apps. `epoch-file` drives restart detection and must live on storage that survives restarts;
`max-tracked-keys` is the OOM guard. Each tracked key costs roughly 350–400 B at the default window (a doubled
`long[32]` ring plus map, window and key-string overhead), so the default 1,000,000 cap projects ~350–400 MB of
heap for the window table alone, before hot keys, Caffeine, queues and the rest of the process.

`zeta.worker.enabled` picks one of two deployment shapes:

| Shape       | `worker.enabled`  | Beans activated                                                    |
| ----------- | ----------------- | ------------------------------------------------------------------ |
| App-only    | `false` (default) | `HotKeyCache`, TopK, reporter, actuator, sync                      |
| Worker-only | `true`            | Worker only (no cache — `get()`/`put()` throw `ZetaModeException`) |

The detection plane is configured once at startup and validated as a whole: thresholds are converted to window
counts against the effective window, and any violation fails the context. The runtime view, when enabled, is
read-only: `/actuator/hotkey-worker` (see [MONITOR.md](MONITOR.md) §4).

The public path is a mapping from the actuator endpoint id `hotkeyworker`. The shipped Worker
jar/image exposes only `health`; the read-only detection view is opt-in. Add `hotkeyworker` to
`management.endpoints.web.exposure.include` and set
`management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker` when an operator needs it. A
self-built Worker uses the same opt-in (Boot 3.5 requires alphanumeric endpoint ids).

```yaml
zeta:
  app-name: order
  worker:
    enabled: true
    max-tracked-keys: 1000000
    epoch-file: /data/zeta/epoch
    messaging:
      report-exchange: zeta.reportToWorker.exchange
      broadcast-exchange: zeta.send.exchange
      heartbeat-exchange: zeta.heartbeat.exchange
    report-consumer:
      concurrent-consumers: 8
      prefetch-count: 50
      staleness-threshold-ms: 0
    report-queue:
      max-length: 10000
      message-ttl-ms: 60000
    broadcast:
      hot-queue-capacity: 10000
      cool-queue-capacity: 10000
      cool-burst: 32
      hot-burst: 8
      max-decision-age-ms: 30000
    heartbeat:
      ping-interval-ms: 1000
      ready-grace-ms: 3000
```

`report-exchange` accepts App reports; the Worker's shard queue must bind to it, not to
`broadcast-exchange` (Worker→App decisions). Both are `DirectExchange` beans, so configure them by
name and verify the live binding with `rabbitmqctl list_bindings` (README "Verification" §1).

| Property                                             | Default                                                 | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ---------------------------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.worker.enabled`                                | `false`                                                 | Enables Worker mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `zeta.worker.max-tracked-keys`                       | `1000000`                                               | Soft admission cap on the distinct keys a Worker tracks at once; the hard headroom makes the OOM bound finite. At the cap, every sampled window untouched for more than a full window is reclaimed first, so one walk can release several seats for concurrent admissions; the pass's own admission still uses the hard headroom, so only a full hard ledger rejects it (counted and logged at DEBUG). An admission that arrives while another thread holds the sample is admitted instead of rejected: the holder may be releasing the very seats it needs, and the cap is approximate. A completed sample that finds none opens a 50 ms saturation window during which other new keys at the cap are rejected without another sample, so the admissions that miss the sample lock cannot outrun it; a sample that still finds the table below the cap (a concurrent sweep released seats) admits instead. A key already holding a HOT seat bypasses the sampling gate and the hard headroom when its window is re-created, so the cap can be exceeded by at most `max-active-hot-keys`. A hard headroom of `max(8, 1%)` above the cap refuses every new key without sampling; a sample that still leaves the table at the cap keeps the relief scan armed. `0` disables the cap. Memory: each tracked key holds a doubled ring of `2 × alignedSlices` longs plus about 80 B of object/map/string overhead — roughly 350–400 B/key at the default 16 slices, so `1000000` projects ~350–400 MB of heap for the window table alone. Startup logs a WARN when that projection exceeds half the max heap. |
| `zeta.worker.epoch-file`                             | `""` (auto → `${java.io.tmpdir}/zeta-epoch-<workerId>`) | Epoch file for restart detection. It is incremented under a file lock on every start and must survive restarts. One binary `long` (8 bytes); a file with any other size is treated as absent and repaired on the next start. Wiping the temp directory resets the epoch, and Apps reject the new incarnation's decisions until the cached entries expire. Filesystem-significant characters in the id are replaced by `_` in the default name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `zeta.worker.messaging.report-exchange`              | `zeta.reportToWorker.exchange`                          | DirectExchange for App reports; must match the App's `zeta.local.report-exchange`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `zeta.worker.messaging.broadcast-exchange`           | `zeta.send.exchange`                                    | DirectExchange for HOT/COOL broadcasts, published under `send.<app-name>`; must match the App's `zeta.worker-listener.exchange-name`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `zeta.worker.messaging.heartbeat-exchange`           | `zeta.heartbeat.exchange`                               | TopicExchange for heartbeats; must match the App's `zeta.local.heartbeat.exchange-name`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `zeta.worker.report-consumer.concurrent-consumers`   | `8`                                                     | Concurrent consumers of the report queue, minimum 1.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `zeta.worker.report-consumer.prefetch-count`         | `50`                                                    | Prefetch count per consumer, balancing throughput against memory.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `zeta.worker.report-consumer.staleness-threshold-ms` | `0`                                                     | Consumer-side staleness filter; `0` disables it and leaves staleness to the queue's `x-message-ttl`. When enabled, only reports with a positive age beyond the threshold are dropped; a negative age (reporter clock ahead) always passes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `zeta.worker.report-queue.max-length`                | `10000`                                                 | Maximum messages per shard's report queue; beyond it the broker drops the oldest messages (`drop-head`) and keeps the freshest statistical signal.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `zeta.worker.report-queue.message-ttl-ms`            | `60000`                                                 | TTL of a message in the queue; the broker discards it on expiry.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `zeta.worker.broadcast.hot-queue-capacity`           | `10000`                                                 | Pending HOT decisions. A decision arriving when it is full is dropped immediately, counted in `zeta.worker.decisions.dropped{type="hot"}` and never retried; the promotion that produced it is undone so the seat returns. The same hand-back applies to a HOT that expires, fails to publish, or is discarded at shutdown.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `zeta.worker.broadcast.cool-queue-capacity`          | `10000`                                                 | Pending COOL decisions. A burst buffer, not a per-seat guarantee: a seat is released before its COOL is queued and can be reused for another lifecycle, so a smaller queue is valid. A dropped COOL converges through the App's hard TTL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `zeta.worker.broadcast.cool-burst`                   | `32`                                                    | Most COOL decisions <b>sent</b> in one drain round before yielding to HOT. Terminal items (elided/superseded/expired) are bounded separately by the per-lane work budget, so they cannot monopolize the drain thread. The token bucket's ceiling is the larger of `max-cool-broadcasts-per-second` and this burst, so a burst above the per-second rate is still reachable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `zeta.worker.broadcast.hot-burst`                    | `8`                                                     | Most HOT decisions <b>sent</b> in one drain round; the floor that keeps a COOL wave from starving HOT. Terminal items use the separate per-lane work budget. The token bucket's ceiling is the larger of `max-hot-broadcasts-per-second` and this burst, so a burst above the per-second rate is still reachable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `zeta.worker.broadcast.max-decision-age-ms`          | `30000`                                                 | Maximum queue residency (1ms–1h). The drain drops an older decision as expired (`zeta.worker.decisions.expired`, tagged `type=hot or type=cool`) instead of sending it; an expired HOT also returns its seat. The broadcaster keeps a cold accepted record for twice this bound with no size cap, so no still-travel-eligible decision loses its ordering predecessor; a record whose last accepted decision is HOT is kept until a COOL is accepted, because it is the only proof the cluster still holds the key hot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `zeta.worker.heartbeat.ping-interval-ms`             | `1000`                                                  | Interval between structured heartbeats.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `zeta.worker.heartbeat.ready-grace-ms`               | `3000`                                                  | How long a Worker with no tracked keys still reports `ready`. It gates ring admission only, breaking the cold-start cycle of no ring slot → no reports → no keys.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

## 11. Detection (Window, Rules, Limits, Cooling)

One block owns the detection plane: one shared sliding window, one required default rule, an ordered rule list, the
cool-down scheduler's cadences, and the per-Worker seat and broadcast budget. Thresholds are written in **QPS** and
converted to window counts once at startup — against the _effective_ window (`alignedSlices * sliceMs`), never the
configured duration. `1s / 16` runs 16 slices of 62 ms (992 ms), and `hot-qps: 100` converts to `ceil(100 * 992 / 1000)`
= 100 accesses.

Promotion is single-observation: `windowSum >= hotCount` promotes immediately. Cooling has two strategies:

- `COOL_FOR_LOW_QPS` — cool once the sum has stayed strictly below `cool-qps` for `low-qps-for`. Hysteresis band keys
  keep their seat indefinitely.
- `COOL_FOR_EXPIRE` — cool exactly `cool-after` after promotion, regardless of traffic; a still-hot key re-earns its
  seat on the next report. Prefer this when a lost HOT must self-heal.

Rules are matched in precedence order: `priority` descending, then type specificity (`exact` > `prefix` > `regex`),
then pattern length descending, then configuration order. A key is bound to the rule that promoted it; replacing the
rule set cannot change the bar an already-hot key cools against. The default rule is what nothing else matched.

```yaml
zeta:
  worker:
    detection:
      enabled: true
      window:
        duration: 1s
        slices: 16
        retention: 2m
      defaults:
        hot-qps: 1000
        cool:
          strategy: COOL_FOR_LOW_QPS
          cool-qps: 100
          low-qps-for: 30s
      rules:
        - id: user-prefix
          match:
            type: prefix
            key: "user:"
          hot-qps: 200
          cool:
            strategy: COOL_FOR_EXPIRE
            cool-after: 10m
      cooling:
        low-qps-check-interval: 1s
        sweep-interval: 30s
        threads: 2
      limits:
        max-active-hot-keys: 10000
        max-hot-broadcasts-per-second: 500
        max-cool-broadcasts-per-second: 500
```

| Property                                                      | Default             | Notes                                                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.worker.detection.enabled`                               | `true`              | `false` keeps the report listener but skips every evaluation and counts the drops (`zeta.worker.detection.disabled.dropped`). `detection.defaults` is not required while disabled.                                                                                                                                  |
| `zeta.worker.detection.window.duration`                       | `1s`                | Configured window length; a warning is logged when the aligned slices do not divide it evenly.                                                                                                                                                                                                                      |
| `zeta.worker.detection.window.slices`                         | `16`                | Slice count; aligned up to a power of two.                                                                                                                                                                                                                                                                          |
| `zeta.worker.detection.window.retention`                      | `2m`                | Idle-window reclamation threshold (memory bound). Must be at least the effective window (`alignedSlices × sliceMs`); a window at the exact boundary may still carry its guard slice, and the cap-relief scan waits one extra slice before reclaiming.                                                               |
| `zeta.worker.detection.defaults.hot-qps`                      | — (required)        | HOT threshold for unmatched keys. Boxed on purpose: a missing value fails startup when detection is enabled, instead of silently using a default.                                                                                                                                                                   |
| `zeta.worker.detection.defaults.cool.strategy`                | — (required)        | `COOL_FOR_LOW_QPS` or `COOL_FOR_EXPIRE`.                                                                                                                                                                                                                                                                            |
| `zeta.worker.detection.defaults.cool.cool-qps`                | LOW_QPS: required   | Cool-band ceiling; must convert to a `coolCount` strictly below `hotCount` (an empty hysteresis band fails startup).                                                                                                                                                                                                |
| `zeta.worker.detection.defaults.cool.low-qps-for`             | LOW_QPS: required   | Dwell below the band before cooling; at least the effective window (`alignedSlices × sliceMs`; 992 ms for the default 1 s / 16 slices).                                                                                                                                                                             |
| `zeta.worker.detection.defaults.cool.cool-after`              | EXPIRE: required    | HOT lifetime; at least 1s. Setting it on a LOW_QPS rule — or `cool-qps` on an EXPIRE rule — is a startup failure.                                                                                                                                                                                                   |
| `zeta.worker.detection.rules[].id`                            | auto `rule-<index>` | Stable id for metrics tags; must be unique and must not be `defaults`, which is reserved for the default rule.                                                                                                                                                                                                      |
| `zeta.worker.detection.rules[].match.type`                    | —                   | `exact`, `prefix` or `regex` (whole-key match). Effective `exact` rules are indexed by key and checked before the global promotion floor; the exact rule's own threshold decides, so a below-threshold report never enters the match cache, and a low exact threshold does not lower the floor for every other key. |
| `zeta.worker.detection.rules[].match.key`                     | —                   | Pattern; blank is a startup failure, an uncompilable regex fails startup.                                                                                                                                                                                                                                           |
| `zeta.worker.detection.rules[].priority`                      | `0`                 | Higher wins.                                                                                                                                                                                                                                                                                                        |
| `zeta.worker.detection.rules[].hot-qps` / `.cool.*`           | —                   | Required in full; nothing is inherited from `defaults`.                                                                                                                                                                                                                                                             |
| `zeta.worker.detection.cooling.low-qps-check-interval`        | `1s`                | LOW_QPS re-check period. Bounded between one slice and the configured window: it sets the check rate `max-active-hot-keys × 1000 / interval` checks/s.                                                                                                                                                              |
| `zeta.worker.detection.cooling.sweep-interval`                | `30s`               | Reclaims idle windows and re-evaluates overdue cool-down timers; minimum 1s. A lost check is late by at most one interval plus a one-second grace, so a check that only just fell due is left to its wheel thread.                                                                                                  |
| `zeta.worker.detection.cooling.threads`                       | `2`                 | Striped cool-down wheels, one advancing thread each; keys are partitioned across them by hash.                                                                                                                                                                                                                      |
| `zeta.worker.detection.limits.max-active-hot-keys`            | `10000`             | HOT seats. At the cap new promotions are refused (`zeta.worker.hot.rejected`) and existing keys are never evicted.                                                                                                                                                                                                  |
| `zeta.worker.detection.limits.max-hot-broadcasts-per-second`  | `500`               | HOT send budget, enforced on the drain thread with a token bucket.                                                                                                                                                                                                                                                  |
| `zeta.worker.detection.limits.max-cool-broadcasts-per-second` | `500`               | COOL send budget. COOL is drained first with a burst floor, so a cool wave cannot starve HOT.                                                                                                                                                                                                                       |

Startup validation collects every violation into one failure. Hard errors: QPS/geometry bounds, `coolCount < hotCount`,
`low-qps-for < effective window`, one-slice lower bound on the re-check interval, no-effect fields under the wrong
strategy, incomplete `defaults` or rules (only while detection is enabled), duplicate or reserved rule ids,
a sweep interval below 1s. Warnings: seats above
`max-tracked-keys`, a re-check rate above 200,000/s per wheel thread (the default rule is included), thresholds under
10 accesses (Poisson noise), a cool band less than 10% below `hot-qps`, and EXPIRE refresh rates above the
broadcast budget.

## 12. Scheduling and Built-in Cycles

`zeta.scheduling.enabled` is the master switch for the App's internal periodic tasks. With it off, HeavyKeeper
decay, expelled-queue draining, TopK admission promotion draining and L1 expiry sweeping all stop, and so does the
event-driven NORMAL→HOT promotion; a TopK hit during a load or write still marks the key HOT directly.

```yaml
zeta:
  scheduling:
    enabled: true
  local:
    scheduler-pool-size: 8 # shared scheduler pool
```

| Property                         | Default | Notes                                                                                                                                                                                                                                   |
| -------------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.scheduling.enabled`        | `true`  | Master switch for internal periodic tasks.                                                                                                                                                                                              |
| `zeta.local.scheduler-pool-size` | `8`     | Size of Zeta's shared scheduler pool. Tasks never overlap; a failure is reported through `ZetaExceptionHandler` and the next run is still scheduled. The Worker control plane uses its own single-threaded scheduler and is unaffected. |
