# App-Side Report Burst Absorption & Backpressure

**Date:** 2026-06-02  
**Status:** Draft  
**Reference:** v0.0.4 `KeyProducer → LinkedBlockingQueue(2M) → KeyConsumer × N`

## Problem

`HotKeyReporter.flush()` runs on a single scheduler thread and synchronously calls
`ReportPublisher.publish()` (RabbitMQ `convertAndSend`) for every shard. When
RabbitMQ is slow or unreachable:

1. The scheduler thread blocks on the first shard's publish, delaying all remaining
   shards and subsequent flush cycles.
2. There is no backpressure — the Caffeine buffer silently evicts entries when
   `maximumSize(100_000)` is exceeded, with no warning or rate adaptation.
3. There is no burst absorption — a sudden traffic spike produces a large flush
   that hits RabbitMQ as a synchronous wave.

## Design

### Data Flow

```
record(cacheKey)
       │
       ▼
HotKeyReporter.counters ──→ Caffeine (30s, 100k)
       │
       ▼  flush() every reportIntervalMs (scheduler thread)
group by shard
       │
       ▼
ShardBatch(shard, timestamp, Map<key, count>)
       │
       ▼  offer(timeout) ── queue full → drop + WARN
LinkedBlockingQueue<ShardBatch> (capacity=10k, configurable)
       │
       ├── ConsumerThread-1 ──→ stale(>5s)? drop ──→ ReportPublisher.publish()
       ├── ConsumerThread-2 ──→ stale(>5s)? drop ──→ ReportPublisher.publish()
       ├── ...
       └── ConsumerThread-N ──→ stale(>5s)? drop ──→ ReportPublisher.publish()
```

### Components (all inside `HotKeyReporter`)

#### 1. `ShardBatch` (new inner record)

```
record ShardBatch(int shard, long timestamp, Map<String, Long> counts)
```

#### 2. `ReportDispatcher` (new inner component)

- Holds `BlockingQueue<ShardBatch> queue` and a `List<Thread>` of consumers
- `start()`: creates `consumerCount` daemon threads, starts each
- `enqueue(ShardBatch)`: `offer(batch, timeout, MILLISECONDS)` — returns false when full
- `shutdown()`: sets `running=false`, interrupts consumers, waits for termination

#### 3. Consumer loop

```java
while (running) {
    ShardBatch batch = queue.poll(1, SECONDS);
    if (batch == null) continue;

    // 5s stale check: data queued too long → discard
    if (now() - batch.timestamp() > 5_000) {
        expiredCount.increment();
        continue;
    }

    reportPublisher.publish(batch.shard(), new ReportMessage(appName, now(), batch.counts()));
}
```

#### 4. Modified `flush()`

```java
private void flush() {
    // ... existing shard grouping ...
    sharded.forEach((shard, counts) -> {
        if (!dispatcher.enqueue(new ShardBatch(shard, now(), counts))) {
            queueFullCount.increment();
            log.warn("report queue full, dropped shard={} keys={}, queueDepth={}",
                shard, counts.size(), dispatcher.depth());
        }
    });
}
```

### Backpressure Chain

```
RabbitMQ slow/down
  → consumer publish() blocked
  → queue fills up
  → flush() offer() timeout
  → ShardBatch dropped + WARN
  → scheduler thread returns quickly (not blocked)
  → Caffeine entries remain (not drained by flush)
  → Caffeine 30s expiry drops oldest entries
  → natural rate-limiting
```

When RabbitMQ recovers, consumers drain the queue, `flush()` enqueues succeed
again, and normal operation resumes.

## Configuration (`HotKeyProperties`, under `hotkey.local.report`)

```yaml
hotkey:
  local:
    report:
      enabled: true                              # existing
      report-interval-ms: 1000                    # existing
      queue-capacity: 10000                       # default
      queue-offer-timeout-ms: 100                 # default
      consumer-count: 0                           # 0 = auto (max(1, shardCount/2))
```

## Monitoring (`HotKeyActuator`)

| Metric | Source | Description |
|--------|--------|-------------|
| `reportQueueDepth` | `queue.size()` | Current queue occupancy |
| `reportQueueCapacity` | config | Queue capacity |
| `reportExpiredCount` | `AtomicLong` | 5s stale discards |
| `reportQueueFullCount` | `AtomicLong` | Offer-timeout discards |

## Logging Rules

| Level | Event | Frequency |
|-------|-------|-----------|
| INFO | Startup parameters | Once |
| WARN | Queue full, batch dropped | Each drop |
| DEBUG | Stale discard | Aggregate per 1000 |

## Changes Summary

### Files Modified

| File | Change |
|------|--------|
| `report/HotKeyReporter.java` | Major: add ShardBatch record, ReportDispatcher, consumer threads, 5s stale check, modified flush() |
| `autoconfigure/HotKeyProperties.java` | Minor: add 3 new config fields |
| `hotkeycache/HotKeySchedulingConfiguration.java` | Review: flush() no longer blocking, verify scheduling |
| `actuator/HotKeyEndpoint.java` | Minor: add queue metrics |

### No Changes

- `ReportPublisher.java` — unchanged
- `ReportMessage.java` — unchanged
- `HotKeyReportAutoConfiguration.java` — unchanged (dispatcher starts in `HotKeyReporter.start()`)
- Worker module — unaffected
- `record(String cacheKey)` API — unchanged

## Open Questions

- None. All parameters are configurable with reasonable defaults.
