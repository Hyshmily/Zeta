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

| Property                                | Default   | Notes                                                                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.app-name`                         | `default` | Logical application name, non-blank, must match `[A-Za-z0-9_-]+`. Deployments sharing a broker must each set a unique value; the default logs a WARN at startup.                                                                                                                                                                                                                                              |
| `zeta.local.instance-id`                | `""`      | Instance identity used for queue naming and node-id derivation, resolved once per application context; when empty it resolves to `<server.port / SERVER_PORT / instance>-<HOSTNAME / local>-<random 8 hex>`. The random suffix is per resolution: a hostname, a port or a process-wide UUID is shared by at least two instances (same host, two contexts, or `server.port` living only in `application.yml`). |
| `zeta.local.node-id`                    | unset     | 10-bit version-stamp node id (`0..1023`); when unset it is the stable CRC32 of `instance-id`. Set it explicitly per instance so two nodes cannot mint equal stamps in the same millisecond.                                                                                                                                                                                                                   |
| `zeta.local.clock-max-forward-drift-ms` | `60000`   | How far an observed peer stamp may lead local physical time. A stamp past the bound does not raise the local logical clock, but its message is still applied; `0` accepts only stamps not later than the current local time.                                                                                                                                                                                  |
| `zeta.worker.enabled`                   | `false`   | `true` selects Worker-only mode: no L1 cache is created, and `get()`/`put()` throw `ZetaModeException`.                                                                                                                                                                                                                                                                                                       |
| `zeta.worker-listener.enabled`          | `false`   | `true` makes the App consume Worker heartbeats and HOT/COOL decisions. **With a Worker cluster deployed and this off, every report is silently discarded.**                                                                                                                                                                                                                                                   |

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

| Property                             | Default | Notes                                                                                                                                                                                                                         |
| ------------------------------------ | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.top-k`                   | `100`   | Size of the TopK set.                                                                                                                                                                                                         |
| `zeta.local.width`                   | `50000` | Sketch width; aligned up to the nearest power of two when the bean is created (65,536 in practice by default) so bucket indexing takes the bit-mask fast path. Capped at 200,000.                                             |
| `zeta.local.depth`                   | `5`     | Sketch depth (rows). The App only needs a fast heuristic, so a shallow sketch suffices; the authoritative decision is the Worker's.                                                                                           |
| `zeta.local.decay`                   | `0.92`  | Base of the exponential collision-decay probability: a colliding unit removes one occupant unit with probability `decay^count`. Range `(0, 1]`; larger values decay faster, smaller values protect established counts longer. |
| `zeta.local.min-count`               | `10`    | Minimum count for a key to enter the TopK.                                                                                                                                                                                    |
| `zeta.local.sketch-window-count`     | `4`     | Sliding windows per sketch slot (ring buffer). Range 2–10, rounded up to a power of two when the bean is created; the default covers four decay periods and damps hotspot drift.                                              |
| `zeta.local.expelled-queue-capacity` | `10000` | Staging queue for keys expelled from the TopK, bounding growth while the sketch scales up.                                                                                                                                    |

## 3. L1 Cache and TTL

All cache names share one Caffeine table, and `cache.max-size` is the process-wide bound on that table. An entry has
exactly one TTL declaration: the hard TTL is Caffeine eviction, while the soft TTL serves the stale value and refreshes
in the background (stale-while-revalidate). Every TTL gets a ±5% random jitter by
default, so entries do not expire in lockstep.

```yaml
zeta:
  local:
    cache:
      max-size: 100000
      tombstone-maximum-size: 10000
      tombstone-ttl-seconds: 600
    hard-ttl-ms: 300000 # 5 minutes
    soft-ttl-ms: 0 # background refresh off by default
    ttl-jitter-ratio: 0.05
    key-format: TYPED
    warm:
      enabled: true
      max-cluster-leases: 50000
      cool-tombstone-ms: 60000
      prewarm:
        trigger: OFF
        jitter-ms: 50
      keep-warm:
        trigger: OFF
        max-tasks: 100000
        min-window-ms: 2000
        lead-max-ms: 30000
```

| Property                                  | Default          | Notes                                                                                                                                                                         |
| ----------------------------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.cache.max-size`               | `100000`         | Maximum entries in the single L1 table, shared by every cache name; at least 1.                                                                                               |
| `zeta.local.cache.tombstone-maximum-size` | `10000`          | Cap on per-key invalidation tombstones, bounding the anti-resurrection state during a mass-invalidation burst; at least 1.                                                    |
| `zeta.local.cache.tombstone-ttl-seconds`  | `600`            | Tombstone retention. It only has to outlive one in-flight read/load; past it, a fill that started before the invalidation is admitted again. At least 1 second.               |
| `zeta.local.hard-ttl-ms`                  | `300000` (5 min) | Hard TTL. `0` and `-1` mean the built-in default (`300000`), never "permanent": use `Long.MAX_VALUE` for a permanent entry.                                                   |
| `zeta.local.soft-ttl-ms`                  | `0` (off)        | Soft TTL (stale-while-revalidate). Off by default: a background refresh runs the loader outside the caller's context. `0`/`-1` disable it, `Long.MAX_VALUE` never goes stale. |
| `zeta.local.ttl-jitter-ratio`             | `0.05`           | One-sided random offset in `[0, 1]`: up to 5% earlier, never later. `0` disables it.                                                                                          |
| `zeta.local.key-format`                   | `TYPED`          | `TYPED` (type-tagged, collision-safe) or `PLAIN` (`cache:value`, one scalar type per namespace). A method that names its own cache does not inherit the class format.         |
| `zeta.local.warm.enabled`                 | `true`           | Global kill switch for prewarm and keep-warm.                                                                                                                                 |
| `zeta.local.warm.max-cluster-leases`      | `50000`          | Upper bound on cluster lease/tombstone records.                                                                                                                               |
| `zeta.local.warm.cool-tombstone-ms`       | `60000`          | How long a COOL tombstone stays to reject a late lower-version HOT.                                                                                                           |
| `zeta.local.warm.prewarm.trigger`         | `OFF`            | `OFF` / `LOCAL` / `CLUSTER` / `ANY`: which heat signal may load a missing entry. A namespace must opt in; registering a loader does not enable prewarm.                       |
| `zeta.local.warm.prewarm.jitter-ms`       | `50`             | Random delay that staggers nodes receiving the same HOT broadcast.                                                                                                            |
| `zeta.local.warm.keep-warm.trigger`       | `OFF`            | Renewal trigger. Independent of `soft-ttl-ms`: a hard-only namespace can renew, and a soft-windowed one stays `OFF` unless it opts in.                                        |
| `zeta.local.warm.keep-warm.max-tasks`     | `100000`         | Maximum scheduled renewals; excess admissions are dropped.                                                                                                                    |
| `zeta.local.warm.keep-warm.min-window-ms` | `2000`           | TTL windows shorter than this are not worth a background renewal.                                                                                                             |
| `zeta.local.warm.keep-warm.lead-max-ms`   | `30000`          | Maximum lead time before the deadline; the effective lead is also capped at a quarter of the remaining window.                                                                |
| `zeta.local.warm.overrides.caches`        | `{}`             | Operator trigger overrides keyed by annotation cache name; each value may set `prewarm` and/or `keepWarm` to `OFF`/`LOCAL`/`CLUSTER`/`ANY`. An unknown name fails startup.    |
| `zeta.local.warm.overrides.prefixes`      | `{}`             | Operator trigger overrides keyed by programmatic key prefix; the prefix is exact-match and an unknown prefix fails startup.                                                    |

## 4. Loading, Concurrency and Refresh

On a read miss, `SingleFlight` collapses concurrent loads of the same key into one execution; `in-flight-timeout-seconds`
bounds a single load's execution time (queue wait is not counted). Loads and background refreshes share one bounded
pool, and `refresh-max-pools` further limits concurrent refreshes with a semaphore, while
`refresh-timeout-ms` bounds one refresh's execution.

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
    load-executor-core-pool-size: 0 # 0 = inherit executor-core-pool-size
    load-executor-max-pool-size: 0 # 0 = inherit executor-max-pool-size
    load-executor-queue-capacity: 0 # 0 = inherit executor-queue-capacity
    send-executor-core-pool-size: 0 # 0 = inherit executor-core-pool-size
    send-executor-max-pool-size: 0 # 0 = inherit executor-max-pool-size
    send-executor-queue-capacity: 0 # 0 = inherit executor-queue-capacity
    refresh-max-pools: 0 # 0 = effective load-pool max / 2
    refresh-timeout-ms: 30000
```

| Property                                  | Default       | Notes                                                                                                                                                                                                                                                                                                                                                                                                  |
| ----------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `zeta.local.in-flight-max-size`           | `50000`       | Maximum number of SingleFlight deduplication entries.                                                                                                                                                                                                                                                                                                                                                  |
| `zeta.local.in-flight-ttl-seconds`        | `5`           | Deduplication entry TTL; must exceed the slowest remote data source response.                                                                                                                                                                                                                                                                                                                          |
| `zeta.local.in-flight-timeout-seconds`    | `3`           | Per-load execution timeout. A timeout is suppressed as `LoadStatus.SUPPRESSED_TIMEOUT`; the read serves a hard-valid stale value when available, otherwise throws `ZetaSuppressedException` (design decisions: read path). Deduplication retention is `max(in-flight-ttl-seconds, timeout + 1)`.                                                                                                       |
| `zeta.local.executor-core-pool-size`      | `8`           | Shared default core size for the refresh fan-out and both dedicated pools.                                                                                                                                                                                                                                                                                                                             |
| `zeta.local.executor-max-pool-size`       | `32`          | Shared default maximum size; must be greater than or equal to `executor-core-pool-size`.                                                                                                                                                                                                                                                                                                               |
| `zeta.local.executor-queue-capacity`      | `2000`        | Shared default queue capacity.                                                                                                                                                                                                                                                                                                                                                                         |
| `zeta.local.executor-rejection`           | `abort`       | Policy when both the queue and the max pool are saturated: `abort` throws a rejection, `caller-runs` runs on the submitting thread. **Refresh fan-out pool only** — the load and send pools always abort. A load-pool rejection surfaces as `LoadStatus.SUPPRESSED_EXECUTOR`; the read serves a hard-valid stale value when available, otherwise throws `ZetaSuppressedException`; it is never a miss. |
| `zeta.local.load-executor-core-pool-size` | `0` (inherit) | Interruptible load pool core size. `0` inherits `executor-core-pool-size`; set it to widen a slow data source's pool without touching the send pool.                                                                                                                                                                                                                                                   |
| `zeta.local.load-executor-max-pool-size`  | `0` (inherit) | Interruptible load pool maximum size. `0` inherits `executor-max-pool-size`; the effective value must be greater than or equal to the effective core size.                                                                                                                                                                                                                                             |
| `zeta.local.load-executor-queue-capacity` | `0` (inherit) | Interruptible load pool queue capacity. `0` inherits `executor-queue-capacity`.                                                                                                                                                                                                                                                                                                                        |
| `zeta.local.send-executor-core-pool-size` | `0` (inherit) | Broadcast-send pool core size. `0` inherits `executor-core-pool-size`.                                                                                                                                                                                                                                                                                                                                 |
| `zeta.local.send-executor-max-pool-size`  | `0` (inherit) | Broadcast-send pool maximum size. `0` inherits `executor-max-pool-size`; the effective value must be greater than or equal to the effective core size.                                                                                                                                                                                                                                                 |
| `zeta.local.send-executor-queue-capacity` | `0` (inherit) | Broadcast-send pool queue capacity. `0` inherits `executor-queue-capacity`.                                                                                                                                                                                                                                                                                                                            |
| `zeta.local.refresh-max-pools`            | `0` (auto)    | Maximum concurrent soft-expiry background refreshes. `0` takes half of the effective load-pool maximum (at least 1); refreshes share the load pool, so a value above its size is clamped to the effective load-pool maximum.                                                                                                                                                                           |
| `zeta.local.refresh-timeout-ms`           | `30000`       | Execution budget of one background soft refresh or hard renewal. The refresh permit and the key's dedup slot stay reserved for the whole budget, so keep it below the shortest hard TTL the application declares; a stuck source otherwise blocks that key's refreshes for a full TTL.                                                                                                                 |

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

| Property                                  | Default                        | Notes                                                                                                                                                                                                                                           |
| ----------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.report.enabled`                     | `true`                         | Enables App→Worker report aggregation over the data-plane connection factory; Zeta owns its report template.                                                                                                                                    |
| `zeta.local.report-exchange`              | `zeta.reportToWorker.exchange` | DirectExchange the App publishes reports to, with a JSON body. Must match the Worker's `zeta.worker.messaging.report-exchange`.                                                                                                                 |
| `zeta.local.report-interval-ms`           | `50`                           | Tide base / tuner seed; minimum 1, and values below 50 ms are still scheduled at 50 ms.                                                                                                                                                         |
| `zeta.local.detect-interval-ms`           | `500`                          | Maximum gap between two sketch ingests, minimum 1. A due (or over-cap) tide is handed to the sketch together with the accumulator without a merge; only an earlier tide merges into the accumulator. With no reporter it is also the tide base. |
| `zeta.local.report-interval-tuning`       | `shadow`                       | `off` / `shadow` / `on`, see the table above.                                                                                                                                                                                                   |
| `zeta.local.report-interval-target-batch` | `512`                          | The tuner's goal keys per batch; only meaningful when `tuning != off`.                                                                                                                                                                          |
| `zeta.local.report-interval-min-ms`       | `50`                           | Tuner interval floor; below the WaveCounter's 50 ms cadence it is effectively 50 ms.                                                                                                                                                            |
| `zeta.local.report-interval-max-ms`       | `1000`                         | Tuner interval ceiling, and the first-report delay bound for a new hotspot; must be >= `min-ms`. The idle worst case is twice this value.                                                                                                       |

See [design decisions: reporting](design-decisions.md#reporting) and
[design decisions: access counting](design-decisions.md#access-counting).

## 6. Report Queue

Report batches enter a bounded dispatch queue and consumer threads send them synchronously. The queue is the only
backpressure: when it is full, the submitting thread blocks for at most `queue-offer-timeout-ms` for the whole flush
and then drops the remaining batches (`0` means non-blocking offers), and a batch that waits more than 5 s in the queue is discarded on dequeue.
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
| `zeta.local.queue-offer-timeout-ms` | `100`   | Queue write timeout, shared by the whole flush; after blocking this long the remaining batches are dropped. `0` means non-blocking: every shard is offered without waiting and the deadline check never drops one.                                                                  |
| `zeta.local.consumer-count`         | `0`     | Report consumer threads; `0` resolves to `max(4, availableProcessors / 2)`.                                                                                                                                                                                                         |

## 7. Worker Listener, Heartbeat and Cluster Health

The App consumes Worker HOT/COOL decisions through `worker-listener`, and consumes heartbeats to maintain a
`HealthView`. That view decides which Worker a report is routed to, and whether the Worker cluster still holds
TTL authority or the local TopK has to take it over. Heartbeats carry the sender's `zeta.app-name` in the AMQP
`appName` header; an App drops a heartbeat whose `appName` header differs from its own, so a shared broker cannot put
another application's Workers into this tenant's ring. Heartbeats from pre-isolation Workers carry no `appName` header
and are accepted for rolling-upgrade compatibility.

The per-instance heartbeat queue is non-durable and auto-delete, so it is declared again on every control-plane
connection; a control-plane-only reconnect cannot rely on the data-plane `RabbitAdmin`. The dedicated heartbeat
connection inherits `spring.rabbitmq.addresses` and `address-shuffle-mode` (through Boot's
`CachingConnectionFactoryConfigurer`), so it fails over across brokers exactly like the data plane.

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
    jitter-ms: 50
    concurrent-consumers: 2
    scheduler-pool-size: 4
    prefetch-count: 100
    max-pending-units: 200000
```

### Decision listener

| Property                                    | Default              | Notes                                                                                                                                                                                                                    |
| ------------------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `zeta.worker-listener.enabled`              | `false`              | Turns on heartbeat consumption, the HOT/COOL decision listener and `HealthView`. Must be `true` when a Worker cluster is deployed.                                                                                       |
| `zeta.worker-listener.exchange-name`        | `zeta.send.exchange` | DirectExchange carrying HOT/COOL; each instance queue binds under `send.<app-name>`. Must match the Worker's `zeta.worker.messaging.broadcast-exchange`. Heartbeats use a separate `zeta.local.heartbeat.exchange-name`. |
| `zeta.worker-listener.queue-prefix`         | `zeta.worker`        | Prefix of the per-instance listener queue; the final name is `{prefix}:{instanceId}`.                                                                                                                                    |
| `zeta.worker-listener.auto-startup`         | `true`               | Whether the listener container starts with the application.                                                                                                                                                              |
| `zeta.worker-listener.jitter-ms`            | `50`                 | Random jitter applied before a HOT broadcast is acted on, spreading cold prewarm across the cluster; `0` applies it on receipt. COOL decisions are control messages and never jitter.                                    |
| `zeta.worker-listener.concurrent-consumers` | `2`                  | Concurrent consumers of the decision queue.                                                                                                                                                                              |
| `zeta.worker-listener.scheduler-pool-size`  | `4`                  | Minimum pool size for the scheduler that runs jitter-delayed tasks; the effective value is `max(this, concurrent-consumers × 2)`.                                                                                        |
| `zeta.worker-listener.prefetch-count`       | `100`                | AMQP prefetch count per consumer. Decisions are acknowledged after an O(1) enqueue, so the window bounds in-flight work rather than processing time.                                                                     |
| `zeta.worker-listener.max-pending-units`    | `200000`             | Global pending cap (weighted units, must be `>= 1`) of the decision dispatcher; work beyond it is dropped (fire-and-forget; the entry converges through its hard TTL or a later transition).                             |

### Heartbeat and liveness

| Property                                 | Default                   | Notes                                                                                                                                                                                                                              |
| ---------------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.heartbeat.exchange-name`     | `zeta.heartbeat.exchange` | TopicExchange carrying structured Worker heartbeats; must match `zeta.worker.messaging.heartbeat-exchange`.                                                                                                                        |
| `zeta.local.heartbeat.timeout-ms`        | `10000`                   | Heartbeat timeout; must be `> 0`. With the adaptive estimate on, it is the warm-up window and the ceiling rather than a flat threshold.                                                                                            |
| `zeta.local.heartbeat.adaptive-timeout`  | `true`                    | Per-Worker Jacobson/Karels estimate over heartbeat inter-arrival (`SRTT + 4×RTTVAR`), clamped into `[adaptive-floor-ms, timeout-ms]`. `false` restores the fixed `timeout-ms`.                                                     |
| `zeta.local.heartbeat.adaptive-floor-ms` | `3000`                    | Lower bound of the adaptive window; must be `<= timeout-ms`. Keep it above a few heartbeat intervals (the Worker defaults to one per second) so a single lost beat or a short GC pause is not read as death.                       |
| `zeta.local.heartbeat.min-alive-workers` | `0`                       | Minimum alive Workers for cluster health. `0` derives it: one third of the observed Worker count, rounded up, floor 1 (3 observed → 1 alive is healthy, 5 → 2, 9 → 3). A positive value overrides it with an absolute alive count. |

### Hash ring

| Property                                      | Default | Notes                                                                                                                                                                  |
| --------------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.consistent-hashing.enabled`       | `true`  | Only controls whether the `/actuator/hotkeyring` inspection endpoint is created. Report routing always uses the consistent-hash ring; `false` does not change routing. |
| `zeta.local.consistent-hashing.virtual-nodes` | `500`   | Virtual nodes per physical Worker on the hash ring.                                                                                                                    |

## 8. Cache Sync

`zeta.sync.*` controls write propagation between instances. Once enabled, `put(..., PutPolicy.synced())`,
`refresh(..., sync = true)`, `invalidate` and `clearAll` broadcast PUT / REFRESH / INVALIDATE messages to the other
instances of the same `app-name`. Sync is eventually consistent: when a message is lost, the peer keeps serving the
old value until the entry's hard TTL expires.

A synced write whose effective hard TTL is permanent is rejected before the cache call: peers could only drop that
entry through the hard deadline, so it would pin a stale value forever. This includes `zeta.local.hard-ttl-ms: 9223372036854775807`
with no per-call override, and the default `@ZetaPut(sync = true)`. `refresh(key, loader)` and
`refreshAll(loaders)` default to local-only; pass `sync = true` to broadcast. A refresh inherits the resident
entry's TTL policy, so the permanent-TTL check applies to that resolved baseline (or to the configured default for
an absent key) — even when the batch is empty.

A peer that holds **no entry** cannot always materialize one from a message. A value-carrying PUT needs a static
namespace TTL baseline: when the namespace's declaration is dynamic or two methods disagree, the peer refuses to
guess a lifetime and applies the PUT as a versioned INVALIDATE (the sender's copy is unaffected). A REFRESH for an
absent key is cold-loaded only when the namespace's `prewarm` trigger accepts the key; otherwise it degrades to the
same versioned invalidation. A live resident never needs either check — it keeps its own stored durations.

```yaml
zeta:
  sync:
    enabled: true
    flush-delay-ms: 50
    put-max-bytes: 32768
    put-allowed-packages:
      - com.example.order.dto
    exchange-name: zeta.sync.exchange
    queue-prefix: zeta.sync
    dedup-window-seconds: 10
    dedup-max-size: 10000
    jitter-ms: 50
    concurrent-consumers: 3
    scheduler-pool-size: 4
    prefetch-count: 32
    max-pending-units: 50000
    auto-startup: true
```

| Property                         | Default              | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| -------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `zeta.sync.enabled`              | `false`              | Enables cross-instance cache sync (requires RabbitMQ).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `zeta.sync.flush-delay-ms`       | `50`                 | Merge window for pending sync records; writes to the same key inside the window are combined into one batch. The window starts at the first record and never slides, so a record waits at most this long.                                                                                                                                                                                                                                                                                                                                            |
| `zeta.sync.put-max-bytes`        | `32768`              | Payload ceiling for one value-carrying PUT. A larger payload, or one that cannot round-trip through Jackson, degrades to a value-less REFRESH; `0` sends every synced write as a REFRESH.                                                                                                                                                                                                                                                                                                                                                              |
| `zeta.sync.put-allowed-packages` | `[]`                 | Extra package prefixes admitted by the deserialization whitelist of value-carrying PUTs. A trailing `.` is added when absent, so `com.example.dto` does not admit the sibling `com.example.dtoOther`. The built-in whitelist holds only JDK value types (Collection/Map implementations only from `java.util`) and `io.github.hyshmily.zeta.model.*`; a business DTO outside it fails closed and degrades to a versioned INVALIDATE. **Any sender able to write to the broker can instantiate a class under a listed prefix — keep the list minimal.** |
| `zeta.sync.exchange-name`        | `zeta.sync.exchange` | FanoutExchange for sync messages.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `zeta.sync.queue-prefix`         | `zeta.sync`          | Per-instance queue prefix; the full name is `{prefix}:{instanceId}`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `zeta.sync.dedup-window-seconds` | `10`                 | Publisher-side deduplication window: a REFRESH/INVALIDATE/PUT that does not outrank the last claimed version for its key is not sent.                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `zeta.sync.dedup-max-size`       | `10000`              | Maximum entries in the deduplication cache.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `zeta.sync.jitter-ms`            | `50`                 | Upper bound of the random park before a key's first sync dispatch after a broadcast, spreading peer read-backs across instances to avoid a thundering herd. Only a REFRESH needs the park; the load itself runs on the dedicated peer-refresh pool.                                                                                                                                                                                                                                                                                                    |
| `zeta.sync.concurrent-consumers` | `3`                  | Concurrent consumers of the sync queue.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `zeta.sync.scheduler-pool-size`  | `4`                  | Pool size for the ordered sync dispatchers. It runs O(1) INVALIDATE/PUT/CLEAR work and the start-jitter park; peer REFRESH loading runs on the dedicated `peerRefreshExecutor`, not here.                                                                                                                                                                                                                                                                                                                                                              |
| `zeta.sync.prefetch-count`       | `32`                 | AMQP prefetch count per sync consumer. Deliveries are acknowledged after an O(1) enqueue, so the window only bounds in-flight work; it is narrower than the Worker's because one delivery can carry a `put-max-bytes` payload.                                                                                                                                                                                                                                                                                                                         |
| `zeta.sync.max-pending-units`    | `50000`              | Global pending cap of the sync dispatcher (weighted units, roughly 1 KB of payload each; must be `>= 1`); work beyond it is dropped outright — the message is already acked, and the next write re-sends it. Single-key and clear INVALIDATE are control messages and are exempt from this budget.                                                                                                                                                                                                                                                     |
| `zeta.sync.auto-startup`         | `true`               | Whether the sync listener container starts with the application.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

## 9. Circuit Breaker

The breaker wraps `SingleFlight.load()`. Once the shard owning a key prefix is open, loads are suppressed
immediately and `HotKeyCache.get()` tries to serve a stale entry that is still hard-valid (soft-expired, hard-alive).
A hard-expired entry is never served as stale. A **failed** load under an open breaker follows the same
stale-first step; when nothing hard-valid is servable, the original failure propagates instead of being
reported as a miss. The breaker is off by default because enabling it changes the read
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
| `zeta.local.circuit-breaker.window-buckets`                | `10`    | Number of buckets in the sliding window; must be <= `window-time-ms` (each bucket spans at least 1 ms).                                                                                                                                                         |
| `zeta.local.circuit-breaker.fail-threshold`                | `0.5`   | Failure-rate threshold (0.0–1.0); the breaker opens above it.                                                                                                                                                                                                   |
| `zeta.local.circuit-breaker.request-volume-threshold`      | `20`    | Minimum requests before the failure rate is evaluated.                                                                                                                                                                                                          |
| `zeta.local.circuit-breaker.single-test-interval-ms`       | `5000`  | Half-open probe interval; a half-open episode with no probe settlement for this long is force-retired, so a hung probe cannot pin it open.                                                                                                                      |
| `zeta.local.circuit-breaker.half-open-max-probes`          | `3`     | Maximum concurrent probes in HALF_OPEN; further calls are rejected.                                                                                                                                                                                             |
| `zeta.local.circuit-breaker.consecutive-success-threshold` | `3`     | Consecutive successes required to close from HALF_OPEN, so one lucky probe cannot cause flapping.                                                                                                                                                               |
| `zeta.local.circuit-breaker.log-enabled`                   | `true`  | Whether OPEN/CLOSE/HALF-OPEN transitions are logged.                                                                                                                                                                                                            |
| `zeta.local.circuit-breaker.exclude-exceptions`            | `[]`    | Fully qualified exception names that must not trip the breaker; a match (on the exception or its `cause`) is counted as a success.                                                                                                                              |
| `zeta.local.circuit-breaker.include-exceptions`            | `[]`    | When non-empty, only the listed exceptions trip the breaker; empty means all of them do. A configured name that cannot be loaded matches nothing (a list where every name fails trips on no exception). Excludes are checked before includes.                   |
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
    node-id: worker-1
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

| Property                                             | Default                                                 | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ---------------------------------------------------- | ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.worker.enabled`                                | `false`                                                 | Enables Worker mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `zeta.worker.max-tracked-keys`                       | `1000000`                                               | Soft admission cap on the distinct keys a Worker tracks at once; the hard headroom makes the OOM bound finite. At the cap, every sampled window untouched for more than a full window is reclaimed first, so one walk can release several seats for concurrent admissions; below the hard bound the pass's own admission reserves one hard-headroom seat, and at the hard bound it admits only from a seat the pass reclaims, so only a full hard ledger that also finds nothing idle rejects it (counted and logged at DEBUG). An admission that arrives while another thread holds the sample is admitted instead of rejected when it can reserve a seat below the hard ledger: the holder may be releasing the very seats it needs, and the cap is approximate. A completed sample that finds none opens a 50 ms saturation window during which other new keys at the cap are rejected without another sample, so the admissions that miss the sample lock cannot outrun it; a sample that still finds the table below the cap (a concurrent sweep released seats) admits instead. A key already holding a HOT seat bypasses the sampling gate and the hard headroom when its window is re-created, so the cap can be exceeded by at most `max-active-hot-keys`. A hard headroom of `max(8, 1%)` above the cap still grants one uncontended sample; only a pass that finds nothing idle refuses the key, and a sample that still leaves the table at the cap keeps the relief scan armed. `0` disables the cap. Memory: each tracked key holds a doubled ring of `2 × alignedSlices` longs plus about 80 B of object/map/string overhead — roughly 350–400 B/key at the default 16 slices, so `1000000` projects ~350–400 MB of heap for the window table alone. Startup logs a WARN when that projection exceeds half the max heap. |
| `zeta.worker.node-id`                                | `""` (auto)                                             | Stable Worker node id used in the report queue name (`zeta.reportToWorker.<app-name>.<node-id>`) and as the default epoch-file name. Resolution order: this property, `INSTANCE_ID`, `zeta.local.instance-id`, then a random id generated per start. The random fallback resets epoch persistence and creates a new 7-day report queue on every restart, so a production Worker should always configure one of the first three.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `zeta.worker.epoch-file`                             | `""` (auto → `${java.io.tmpdir}/zeta-epoch-<workerId>`) | Epoch file for restart detection. It is incremented under a file lock on every start and must survive restarts. One binary `long` (8 bytes); a file with any other size is treated as absent and repaired on the next start. A sibling `.lock` file carries the cross-process lock, and the new value is written to a same-directory temporary file and renamed over the target, so the path must name a file inside a writable parent directory. Wiping the temp directory resets the epoch, and Apps reject the new incarnation's decisions until the cached entries expire. Filesystem-significant characters in the id are replaced by `_` in the default name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `zeta.worker.messaging.report-exchange`              | `zeta.reportToWorker.exchange`                          | DirectExchange for App reports; must match the App's `zeta.local.report-exchange`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `zeta.worker.messaging.broadcast-exchange`           | `zeta.send.exchange`                                    | DirectExchange for HOT/COOL broadcasts, published under `send.<app-name>`; must match the App's `zeta.worker-listener.exchange-name`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `zeta.worker.messaging.heartbeat-exchange`           | `zeta.heartbeat.exchange`                               | TopicExchange for heartbeats; must match the App's `zeta.local.heartbeat.exchange-name`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `zeta.worker.report-consumer.concurrent-consumers`   | `8`                                                     | Concurrent consumers of the report queue, minimum 1.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `zeta.worker.report-consumer.prefetch-count`         | `50`                                                    | Prefetch count per consumer, between 1 and 65535 (the AMQP 0-9-1 `basicQos` range); validates at startup instead of silently degrading.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `zeta.worker.report-consumer.staleness-threshold-ms` | `0`                                                     | Consumer-side staleness filter; `0` disables it and leaves staleness to the queue's `x-message-ttl`. When enabled, only reports with a positive age beyond the threshold are dropped; a negative age (reporter clock ahead) always passes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `zeta.worker.report-queue.max-length`                | `10000`                                                 | Maximum messages per shard's report queue; beyond it the broker drops the oldest messages (`drop-head`) and keeps the freshest statistical signal.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `zeta.worker.report-queue.message-ttl-ms`            | `60000`                                                 | TTL of a message in the queue; the broker discards it on expiry.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `zeta.worker.broadcast.hot-queue-capacity`           | `10000`                                                 | Pending HOT decisions. A decision arriving when it is full is dropped immediately, counted in `zeta.worker.decisions.dropped{type="hot"}` and never retried; the promotion that produced it is undone so the seat returns. The same hand-back applies to a HOT that expires, fails to publish, or is discarded at shutdown.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `zeta.worker.broadcast.cool-queue-capacity`          | `10000`                                                 | Pending COOL decisions. A burst buffer, not a per-seat guarantee: a seat is released before its COOL is queued and can be reused for another lifecycle, so a smaller queue is valid. A dropped COOL converges through the App's hard TTL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `zeta.worker.broadcast.cool-burst`                   | `32`                                                    | Most COOL decisions <b>sent</b> in one drain round before yielding to HOT. Terminal items (elided/superseded/expired) are bounded separately by the per-lane work budget, so they cannot monopolize the drain thread. The token bucket's ceiling is the larger of `max-cool-broadcasts-per-second` and this burst, so a burst above the per-second rate is still reachable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `zeta.worker.broadcast.hot-burst`                    | `8`                                                     | Most HOT decisions <b>sent</b> in one drain round; the floor that keeps a COOL wave from starving HOT. Terminal items use the separate per-lane work budget. The token bucket's ceiling is the larger of `max-hot-broadcasts-per-second` and this burst, so a burst above the per-second rate is still reachable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `zeta.worker.broadcast.max-decision-age-ms`          | `30000`                                                 | Maximum queue residency (1ms–1h). The drain drops an older decision as expired (`zeta.worker.decisions.expired`, tagged `type=hot or type=cool`) instead of sending it; an expired HOT also returns its seat. The broadcaster keeps a COOL-state accepted record for twice this bound with no size cap, so no still-travel-eligible decision loses its ordering predecessor; a record whose last accepted decision is HOT is kept until a COOL is accepted, because it is the only proof the cluster still holds the key hot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `zeta.worker.heartbeat.ping-interval-ms`             | `1000`                                                  | Interval between structured heartbeats.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `zeta.worker.heartbeat.ready-grace-ms`               | `3000`                                                  | How long a Worker with no tracked keys still reports `ready`. It gates ring admission only, breaking the cold-start cycle of no ring slot → no reports → no keys.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

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
| `zeta.worker.detection.cooling.low-qps-check-interval`        | `1s`                | LOW_QPS re-check period. Bounded between one slice and the configured window: it sets the check rate `max-active-hot-keys × 1000 / interval` checks/s. Used and validated only when at least one rule selects `COOL_FOR_LOW_QPS`; an EXPIRE-only configuration ignores this interval.                               |
| `zeta.worker.detection.cooling.sweep-interval`                | `30s`               | Reclaims idle windows and re-evaluates overdue cool-down timers; minimum 1s. A lost check is late by at most one interval plus a one-second grace, so a check that only just fell due is left to its wheel thread.                                                                                                  |
| `zeta.worker.detection.cooling.threads`                       | `2`                 | Striped cool-down wheels, one advancing thread each (1..128); keys are partitioned across them by hash.                                                                                                                                                                                                             |
| `zeta.worker.detection.limits.max-active-hot-keys`            | `10000`             | HOT seats. At the cap new promotions are refused (`zeta.worker.hot.rejected`) and existing keys are never evicted.                                                                                                                                                                                                  |
| `zeta.worker.detection.limits.max-hot-broadcasts-per-second`  | `500`               | HOT send budget, enforced on the drain thread with a token bucket.                                                                                                                                                                                                                                                  |
| `zeta.worker.detection.limits.max-cool-broadcasts-per-second` | `500`               | COOL send budget. COOL is drained first with a burst floor, so a cool wave cannot starve HOT.                                                                                                                                                                                                                       |

Startup validation collects every violation into one failure. Hard errors: QPS/geometry bounds, `coolCount < hotCount`,
`low-qps-for < effective window`, one-slice lower bound on the re-check interval when a LOW_QPS rule exists,
no-effect fields under the wrong strategy, incomplete `defaults` or rules (only while detection is enabled),
duplicate or reserved rule ids, a sweep interval below 1s. Warnings: seats above `max-tracked-keys`, a LOW_QPS
re-check rate above 200,000/s per wheel thread, thresholds under 10 accesses (Poisson
noise), a cool band less than 10% below `hot-qps`, and EXPIRE refresh rate above the narrowest per-lane broadcast
budget (`min(max-hot-broadcasts-per-second, max-cool-broadcasts-per-second)`), because a refresh spends one HOT and
one COOL token on separate lanes.

## 12. Scheduling

Zeta uses one shared scheduler pool for its periodic work — sketch decay, expelled-key draining and the
retention sweeps. Tasks never overlap; a failure is reported through `ZetaExceptionHandler` and the next run is
still scheduled. Background warming has its own opt-in (`zeta.local.warm.*`), so a deployment that never declares a
trigger spends no scheduler slot on renewal.

```yaml
zeta:
  local:
    scheduler-pool-size: 8 # shared scheduler pool
```

| Property                         | Default | Notes                                                                                                                                                                                                                                   |
| -------------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.scheduler-pool-size` | `8`     | Size of Zeta's shared scheduler pool. Tasks never overlap; a failure is reported through `ZetaExceptionHandler` and the next run is still scheduled. The Worker control plane uses its own single-threaded scheduler and is unaffected. |
