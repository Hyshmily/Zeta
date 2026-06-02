# App-Side Report Backpressure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add burst absorption and backpressure to `HotKeyReporter` via a `LinkedBlockingQueue<ShardBatch>` between `flush()` and RabbitMQ publish, plus stale-data discard at dequeue time.

**Architecture:** `flush()` (scheduler thread) groups Caffeine counters by shard as before, but enqueues into a bounded `LinkedBlockingQueue<ShardBatch>` instead of publishing directly. N daemon consumer threads drain the queue, check 5s staleness, and publish via `ReportPublisher`. Queue full → `offer()` timeout → batch dropped + WARN → Caffeine eviction provides natural backpressure.

**Tech Stack:** Java, `LinkedBlockingQueue`, `AtomicLong`, `Thread`

**Files:**
- Modify: `common/src/main/java/io/github/hyshmily/hotkey/report/HotKeyReporter.java`
- Modify: `common/src/main/java/io/github/hyshmily/hotkey/hotkeycache/HotKeyProperties.java`
- Modify: `common/src/main/java/io/github/hyshmily/hotkey/autoconfigure/HotKeyReportAutoConfiguration.java`
- Modify: `common/src/main/java/io/github/hyshmily/hotkey/actuator/HotKeyEndpoint.java`

---

### Task 1: Add config fields to HotKeyProperties

**Files:**
- Modify: `common/src/main/java/io/github/hyshmily/hotkey/hotkeycache/HotKeyProperties.java` (after line 145, before `instanceId`)

- [ ] **Step 1: Add queue/consumer config fields**

Add after `shardCount` field and before `instanceId` field:

```java
  @Min(1)
  private int queueCapacity = 10_000;

  @Min(1)
  private int queueOfferTimeoutMs = 100;

  @Min(0)
  private int consumerCount = 0;
```

- [ ] **Step 2: Add consumerCount accessor with auto-default**

Add after the other effective-methods (around line 134):

```java
  /** Effective consumer thread count: configured value, or max(1, shardCount/2). */
  public int effectiveConsumerCount() {
    return consumerCount > 0 ? consumerCount : Math.max(1, shardCount / 2);
  }
```

- [ ] **Step 3: Verify compilation**

Run: `mvn compile -pl common -q`

---

### Task 2: Refactor HotKeyReporter — add ShardBatch, ReportDispatcher, async flush

**Files:**
- Modify: `common/src/main/java/io/github/hyshmily/hotkey/report/HotKeyReporter.java`

- [ ] **Step 1: Add ShardBatch record and new fields**

Add `ShardBatch` record, `reportDispatcher`, `expiredCount`, `queueFullCount` to `HotKeyReporter`:

```java
@Slf4j
public class HotKeyReporter {

  // ...existing fields...

  private final int queueCapacity;
  private final int queueOfferTimeoutMs;
  private final int consumerCount;
  private ReportDispatcher dispatcher;

  record ShardBatch(int shard, long timestamp, Map<String, Long> counts) {}
```

- [ ] **Step 2: Change constructor to accept new params**

Replace the existing constructor with:

```java
  public HotKeyReporter(
    ReportPublisher reportPublisher,
    ScheduledExecutorService scheduler,
    long reportIntervalMs,
    int shardCount,
    String appName,
    int queueCapacity,
    int queueOfferTimeoutMs,
    int consumerCount
  ) {
    this.reportPublisher = reportPublisher;
    this.scheduler = scheduler;
    this.reportIntervalMs = reportIntervalMs;
    this.shardCount = shardCount;
    this.appName = appName;
    this.queueCapacity = queueCapacity;
    this.queueOfferTimeoutMs = queueOfferTimeoutMs;
    this.consumerCount = consumerCount;
  }
```

- [ ] **Step 3: Modify start() to init the dispatcher**

Add to `start()` after the `started` CAS success, before scheduling:

```java
    dispatcher = new ReportDispatcher();
    dispatcher.start();
    log.info("HotKeyReporter started: appName={}, shardCount={}, intervalMs={}, queueCapacity={}, consumers={}",
      appName, shardCount, reportIntervalMs, queueCapacity, dispatcher.consumerCount());
```

Also add a `stop()` method:

```java
  /** Stop the dispatcher. Called via @PreDestroy or destroyMethod. */
  public void stop() {
    if (dispatcher != null) {
      dispatcher.shutdown();
    }
  }
```

- [ ] **Step 4: Rewrite flush() to enqueue instead of publish**

```java
  private void flush() {
    if (counters.estimatedSize() == 0) {
      return;
    }

    Map<Integer, Map<String, Long>> sharded = new HashMap<>();
    long now = System.currentTimeMillis();
    counters
      .asMap()
      .forEach((key, adder) -> {
        long val = adder.sumThenReset();
        if (val > 0) {
          int shard = Math.floorMod(key.hashCode(), shardCount);
          sharded.computeIfAbsent(shard, _ -> new HashMap<>()).put(key, val);
        }
      });

    sharded.forEach((shard, counts) -> {
      if (!dispatcher.enqueue(new ShardBatch(shard, now, counts))) {
        log.warn("report queue full, dropped shard={} keys={}, depth={}/{}",
          shard, counts.size(), dispatcher.depth(), queueCapacity);
      }
    });
  }
```

- [ ] **Step 5: Add ReportDispatcher inner class**

Add as an inner class of `HotKeyReporter`:

```java
  class ReportDispatcher {
    private final BlockingQueue<ShardBatch> queue = new LinkedBlockingQueue<>(queueCapacity);
    private final AtomicLong expiredCount = new AtomicLong();
    private final AtomicLong droppedCount = new AtomicLong();
    private final List<Thread> consumers = new ArrayList<>();
    private volatile boolean running;

    void start() {
      running = true;
      int count = consumerCount;
      for (int i = 0; i < count; i++) {
        Thread t = new Thread(this::consumeLoop,
          "report-consumer-" + i);
        t.setDaemon(true);
        t.start();
        consumers.add(t);
      }
    }

    boolean enqueue(ShardBatch batch) {
      try {
        return queue.offer(batch, queueOfferTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    void shutdown() {
      running = false;
      for (Thread t : consumers) {
        t.interrupt();
      }
      for (Thread t : consumers) {
        try { t.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      }
      consumers.clear();
      log.info("ReportDispatcher stopped, remaining queue={}, expired={}, dropped={}",
        queue.size(), expiredCount.get(), droppedCount.get());
    }

    int depth() { return queue.size(); }
    int capacity() { return queueCapacity; }
    int consumerCount() { return consumers.size(); }
    long expired() { return expiredCount.get(); }
    long dropped() { return droppedCount.get(); }

    private void consumeLoop() {
      while (running) {
        try {
          ShardBatch batch = queue.poll(1, TimeUnit.SECONDS);
          if (batch == null) continue;

          // 5s stale check: data queued too long → discard
          if (System.currentTimeMillis() - batch.timestamp() > 5_000) {
            expiredCount.increment();
            continue;
          }

          reportPublisher.publish(batch.shard(),
            new ReportMessage(appName, batch.timestamp(), batch.counts()));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
```

- [ ] **Step 6: Update imports**

Verify `HotKeyReporter.java` imports include:
```java
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
```

Also remove `import lombok.RequiredArgsConstructor;` (no longer needed since we have a manual constructor).

- [ ] **Step 7: Compile and verify**

Run: `mvn compile -pl common -q`
Expected: Only Lombok/Unsafe deprecation warnings.

---

### Task 3: Update HotKeyReportAutoConfiguration to pass new params + destroyMethod

**Files:**
- Modify: `common/src/main/java/io/github/hyshmily/hotkey/autoconfigure/HotKeyReportAutoConfiguration.java`

- [ ] **Step 1: Update hotKeyReporter bean creation**

Change the bean annotation to add `destroyMethod` and update the constructor call:

```java
  @Bean(initMethod = "start", destroyMethod = "stop")
  @ConditionalOnMissingBean
  public HotKeyReporter hotKeyReporter(
    ReportPublisher reportPublisher,
    ScheduledExecutorService hotKeyReportScheduler,
    HotKeyProperties properties
  ) {
    return new HotKeyReporter(
      reportPublisher,
      hotKeyReportScheduler,
      properties.getReportIntervalMs(),
      properties.getShardCount(),
      properties.getAppName(),
      properties.getQueueCapacity(),
      properties.getQueueOfferTimeoutMs(),
      properties.effectiveConsumerCount()
    );
  }
```

- [ ] **Step 2: Compile and verify**

Run: `mvn compile -pl common -q`

---

### Task 4: Add queue metrics to HotKeyEndpoint

**Files:**
- Modify: `common/src/main/java/io/github/hyshmily/hotkey/actuator/HotKeyEndpoint.java`

- [ ] **Step 1: Add HotKeyReporter constructor parameter**

Add import and field, update constructor:

```java
import io.github.hyshmily.hotkey.report.HotKeyReporter;
```

```java
  private final HotKeyReporter hotKeyReporter;
```

Update constructor to accept it:

```java
  public HotKeyEndpoint(
    TopK hotKeyDetector,
    TopK workerTopK,
    Cache<String, Object> caffeineCache,
    SingleFlight singleFlight,
    HotKeyProperties properties,
    HotKeyReporter hotKeyReporter
  ) {
    // ...existing assignments...
    this.hotKeyReporter = hotKeyReporter;
  }
```

- [ ] **Step 2: Add queue metrics to hotKeyInfo()**

Add at the end of `hotKeyInfo()` before the return statement:

```java
    if (hotKeyReporter != null) {
      info.put("reportQueueDepth", hotKeyReporter.dispatcherDepth());
      info.put("reportQueueCapacity", hotKeyReporter.dispatcherCapacity());
      info.put("reportExpiredCount", hotKeyReporter.dispatcherExpired());
      info.put("reportQueueFullCount", hotKeyReporter.dispatcherDropped());
    }
```

- [ ] **Step 3: Add accessor methods to HotKeyReporter**

Add to `HotKeyReporter`:

```java
  int dispatcherDepth() { return dispatcher == null ? -1 : dispatcher.depth(); }
  int dispatcherCapacity() { return dispatcher == null ? -1 : dispatcher.capacity(); }
  long dispatcherExpired() { return dispatcher == null ? -1 : dispatcher.expired(); }
  long dispatcherDropped() { return dispatcher == null ? -1 : dispatcher.dropped(); }
```

- [ ] **Step 4: Update HotKeyEndpoint bean wiring**

Check `HotKeyActuatorAutoConfiguration.java` for how the endpoint is created and add `hotKeyReporter` parameter there as well.

- [ ] **Step 5: Compile and verify**

Run: `mvn compile -q`

---

### Spec Coverage Check

| Spec Section | Task |
|-------------|------|
| ShardBatch record | Task 2 Step 1 |
| ReportDispatcher inner class | Task 2 Step 5 |
| Consumer loop + 5s stale check | Task 2 Step 5 |
| Modified flush() | Task 2 Step 4 |
| Backpressure (offer timeout → drop) | Task 2 Step 5 |
| Config fields (queueCapacity, queueOfferTimeoutMs, consumerCount) | Task 1 |
| Metrics endpoint | Task 4 |
| Auto-configuration update | Task 3 |
| Consumer count auto-default | Task 1 Step 2 |
| Graceful shutdown (destroyMethod) | Task 3 Step 1 |
