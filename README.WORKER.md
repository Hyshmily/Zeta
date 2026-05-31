[← Back to Home](README.md)

# Worker Mode

Worker Mode is an optional deployment topology where a dedicated node aggregates access reports from all application instances, runs cluster-wide hot key detection via a sliding-window + state-machine pipeline, and broadcasts HOT/COOL decisions back to every instance.

This approach solves the **single-instance blind spot** — an app instance's local HeavyKeeper cannot distinguish between "accessed 100 times by one pod" and "accessed once by 100 pods." Worker Mode provides cluster-consensus hot key detection without a centralized proxy.

## Architecture

```
┌────────────────────┐      RabbitMQ fanout      ┌──────────────────────┐
│  App Instance 1    │  ─── report (periodic) ──→│  Worker Node         │
│  HotKeyReporter    │                           │                      │
├────────────────────┤                           │  ┌────────────────┐  │
│  App Instance 2    │  ─── report (periodic) ──→│  │ ReportConsumer │  │
│  HotKeyReporter    │                           │  │ (AMQP consumer)│  │
├────────────────────┤                           │  └───────┬────────┘  │
│  App Instance N    │                           │          │           │
│  HotKeyReporter    │  ─── report (periodic) ──→│          ↓           │
└────────────────────┘                           │  ┌────────────────┐  │
                                                 │  │HotKeyStateMach │  │
                                                 │  │  (per-key FSM) │  │
                                                 │  └───────┬────────┘  │
                                                 │          │           │
                                                 │  ┌────────────────┐  │
                                                 │  │WorkerBroadcaste│  │
                                                 │  │ (HOT/COOL via  │  │
                                                 │  │  RabbitMQ)     │  │
                                                 │  └────────┬───────┘  │
                                                 └───────────┼──────────┘
                                                             │
                        RabbitMQ fanout (hotkey.worker.exchange)
                                                             │
                                                             ↓
                          ┌──────────────────────────────────────────┐
                          │   All App Instances (WorkerListener)     │
                          │  ┌──────────┐  ┌──────────┐  ┌────────┐  │
                          │  │Instance 1│  │Instance 2│  │  ...   │  │
                          │  └──────────┘  └──────────┘  └────────┘  │
                          └──────────────────────────────────────────┘
```

## Report Flow

1. Every `get()` / `getWithSoftExpire()` call triggers `hotKeyReporter.record(key)` on both L1 hit and L2 miss paths.
2. `HotKeyReporter` aggregates per-key counts locally (Caffeine, 30s expiry, max 100k keys) and periodically publishes `ReportMessage` records to the `hotkey.report.exchange` RabbitMQ exchange, routed by `app-name` and `shard-index`.
3. The Worker node's `ReportConsumer` receives reports and feeds access counts into `workerTopK.add()`, discarding stale reports (>5s old).
4. The Worker processes reports asynchronously — it does not block app instances.

### Sharding

When `shard-count > 1`, reports are distributed across multiple Worker instances. Each app instance computes `abs(hash(cacheKey)) % shard-count` and publishes to the corresponding shard routing key. Each Worker binds to its own queue (`hotkey.report.{appName}.{shardIndex}`), enabling horizontal scaling of the Worker plane.

## Sliding Window Detector

The `SlidingWindowDetector` is a lock-free time-series counter that tracks per-key access counts within a configurable window.

| Property                           | Default | Description                                   |
| ---------------------------------- | ------- | --------------------------------------------- |
| `hotkey.worker.window-duration-ms` | `1000`  | Total window duration (1 second)              |
| `hotkey.worker.window-slices`      | `10`    | Number of time slices per window (100ms each) |

Each key maintains an array of `long` counters indexed by time slice. On each report tick, the oldest slice is recycled and the effective count for each key is recomputed as the sum across all slices. This gives an accurate QPS estimate without per-access locking.

## Hot Key State Machine

Each tracked key follows a lifecycle managed by `HotKeyStateMachine`:

```
          ┌──────────────────────────────────────────────────┐
          │                                                  │
          ↓                                                  │
     ┌─────────┐     above threshold     ┌──────┐            │
     │ NORMAL  │ ──────────────────────→ │ HOT  │            │
     └─────────┘                         └──┬───┘            │
          ↑                                  │               │
          │         below threshold          │               │
          │     ┌────────────────────────────┘               │
          │     ↓                                            │
          │  ┌──────────┐     grace expired    ┌──────────┐  │
          │  │ PRE_COOL │ ──────────────────→  │   COOL   │  │
          │  └──────────┘                      └──────────┘  │
          │         ↑                                        │
          │         | (silent re-heat during grace)          │
          └─────────┴────────────────────────────────────────|
```

- **NORMAL**: Key exists but below hot threshold. Access tracked but no broadcast sent.
- **HOT**: Key exceeds threshold for `confirm-duration-ms`. Worker broadcasts `TYPE_HOT` to all instances. Stays HOT until sustained cooldown.
- **PRE_COOL**: Key dropped below threshold but is in a grace period. If it rises again during this period, it silently returns to HOT without a broadcast, avoiding toggle storms.
- **COOL**: Key has been below threshold for `cool-duration-ms`. Worker broadcasts `TYPE_COOL` and the key returns to NORMAL.

### State Machine Configuration

| Property                            | Default | Description                                            |
| ----------------------------------- | ------- | ------------------------------------------------------ |
| `hotkey.worker.hot-threshold`       | `-1`    | Absolute hot threshold (use `-1` to use ratio instead) |
| `hotkey.worker.hot-threshold-ratio` | `0.01`  | Hot threshold as fraction of estimated global QPS      |
| `hotkey.worker.confirm-duration-ms` | `2000`  | Duration above threshold to confirm HOT (2s)           |
| `hotkey.worker.cool-duration-ms`    | `15000` | Duration below threshold to confirm COOL (15s)         |
| `hotkey.worker.pre-cool-grace-ms`   | `5000`  | Grace period for silent re-heat (5s)                   |

## Dynamic Threshold

The Worker adapts to traffic patterns by periodically recalculating the hot threshold based on estimated global QPS:

```
hotThreshold = max(minCount, estimatedGlobalQPS * hotThresholdRatio)
```

| Property                                | Default | Description                                          |
| --------------------------------------- | ------- | ---------------------------------------------------- |
| `hotkey.worker.recalculate-interval-ms` | `60000` | Recalculation interval (60s)                         |
| `hotkey.worker.qps-change-tolerance`    | `0.5`   | ±50% QPS change required to trigger threshold update |

The `qps-change-tolerance` prevents threshold churn during normal traffic fluctuations — only significant QPS shifts trigger a recalculation.

## Top-K Cross-Validation

The Worker periodically validates the app-side HeavyKeeper Top-K against its own cluster-wide Top-K, ensuring consistency and enabling pre-warming.

| Property                                      | Default | Description                              |
| --------------------------------------------- | ------- | ---------------------------------------- |
| `hotkey.worker.topk-validate-interval-ms`     | `60000` | Cross-validation interval (60s)          |
| `hotkey.worker.topk-pre-warm-count`           | `5`     | Top-K entries eligible for pre-warming   |
| `hotkey.worker.topk-pre-warm-min-appearances` | `2`     | Min consecutive appearances for pre-warm |

Top-K entries appearing in the Worker's Top-K across consecutive validation intervals are candidates for pre-warming. The Worker can proactively push these keys to app instances before they would naturally be detected locally.

## Deployment Modes

| Mode        | `worker.enabled`  | `worker.exclusive-mode` | Active Beans                                                           |
| ----------- | ----------------- | ----------------------- | ---------------------------------------------------------------------- |
| App-only    | `false` (default) | —                       | `HotKeyCache`, TopK detector, actuator, sync                           |
| Worker-only | `true`            | `true` (default)        | Worker-only (SlidingWindow, StateMachine, ReportConsumer, Broadcaster) |
| Coexistence | `true`            | `false`                 | All beans (App + Worker) — useful for dev/testing                      |

In **Worker-only** mode, `HotKey.isHotKey()` / `get()` / `putThrough()` throw `UnsupportedOperationException` — these operations require the app-side cache. Worker-TopK queries (`returnWorkerHotKeys()`) remain available.

## Configuration Example

```yaml
hotkey:
  worker:
    enabled: true
    app-name: my-service
    shard-count: 1
    shard-index: 0

    window-duration-ms: 1000
    window-slices: 10
    hot-threshold-ratio: 0.01
    confirm-duration-ms: 2000
    cool-duration-ms: 15000
    pre-cool-grace-ms: 5000
    recalculate-interval-ms: 60000
    qps-change-tolerance: 0.5

    topk-validate-interval-ms: 60000
    topk-pre-warm-count: 5
    topk-pre-warm-min-appearances: 2

    worker-top-k: 100
    worker-width: 20000
    worker-depth: 10
    worker-decay: 0.9
    worker-min-count: 10

spring:
  rabbitmq:
    host: localhost
    port: 5672
```

### Worker Listener (on app instances)

```yaml
hotkey:
  worker-listener:
    enabled: true
```

Each app instance must enable the `worker-listener` to receive HOT/COOL decisions from the Worker. The listener binds to queue `hotkey.worker:{instanceId}` and processes incoming decisions.

### Report Configuration (on app instances)

```yaml
hotkey:
  app-name: my-service
  report-interval-ms: 100
  shard-count: 1
```

## Failure Behavior

| Failure                   | Impact                                                                     | Recovery                                          |
| ------------------------- | -------------------------------------------------------------------------- | ------------------------------------------------- |
| Worker crashes            | App instances continue with local TopK; no cluster-wide HOT/COOL decisions | Restart Worker; instances reconnect automatically |
| Report channel fails      | App reports queued/buffered (RabbitMQ persistence)                         | Auto-recover on RabbitMQ restoration              |
| Worker broadcast fails    | No cross-instance HOT/COOL sync; local TopK still functional               | Restart Worker broadcaster                        |

