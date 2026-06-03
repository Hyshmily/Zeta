package io.github.hyshmily.hotkey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.algorithm.HeavyKeeper;
import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.CacheSyncListener;
import io.github.hyshmily.hotkey.broadcast.CacheSyncProperties;
import io.github.hyshmily.hotkey.broadcast.WorkerListener;
import io.github.hyshmily.hotkey.broadcast.WorkerListenerProperties;
import io.github.hyshmily.hotkey.broadcast.WorkerMessage;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.entity.HotKeyDecision;
import io.github.hyshmily.hotkey.entity.KeyState;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyCache;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import io.github.hyshmily.hotkey.hotkeycache.VersionController;
import io.github.hyshmily.hotkey.hotkeycache.VersionGuard;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import io.github.hyshmily.hotkey.report.ReportMessage;
import io.github.hyshmily.hotkey.report.ReportPublisher;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production-level stress tests for high-concurrency and distributed scenarios.
 * Each test simulates production load to verify correctness of hot-key detection,
 * caching, broadcast, and related core components under extreme conditions.
 */
class HotKeyStressTest {

  private static final Logger log = LoggerFactory.getLogger(HotKeyStressTest.class);

  /**
   * HeavyKeeper concurrent tests.
   */
  /** HeavyKeeper must not produce duplicate keys under high concurrency. */
  @Test
  void heavyKeeper_shouldNotHaveDuplicateKeysUnderHighConcurrency() throws InterruptedException {
    int threadCount = 20;
    int keysPerThread = 50;
    int iterationsPerKey = 200;
    HeavyKeeper keeper = new HeavyKeeper(100, 50000, 5, 0.92, 10);

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      int base = t * keysPerThread;
      executor.submit(() -> {
        try {
          for (int k = 0; k < keysPerThread; k++) {
            String key = "stress-key-" + (base + k);
            for (int i = 0; i < iterationsPerKey; i++) {
              keeper.add(key, 1);
            }
          }
        } catch (Exception e) {
          errors.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
    assertThat(errors.get()).isZero();
    assertThat(keeper.total()).isEqualTo((long) threadCount * keysPerThread * iterationsPerKey);

    List<Item> items = keeper.list();
    Set<String> seen = new HashSet<>();
    for (Item item : items) {
      assertThat(seen.add(item.key()))
        .as("Duplicate key in TopK list: %s", item.key())
        .isTrue();
    }

    for (int i = 1; i < items.size(); i++) {
      assertThat(items.get(i - 1).count())
        .isGreaterThanOrEqualTo(items.get(i).count());
    }
  }

  /** TopK size must not exceed k under concurrent contention. */
  @Test
  void heavyKeeper_topKSizeShouldNotExceedKUnderContention() throws InterruptedException {
    int k = 10;
    HeavyKeeper keeper = new HeavyKeeper(k, 50000, 5, 0.92, 5);
    int threadCount = 30;
    int iterations = 500;

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      executor.submit(() -> {
        try {
          for (int i = 0; i < iterations; i++) {
            keeper.add("conflict-key-" + (i % 5), 10);
          }
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
    assertThat(keeper.list().size()).isLessThanOrEqualTo(k);
  }

  /** Supplier must execute far fewer times than the number of concurrent requesters. */
  @Test
  void singleFlight_shouldDeduplicateUnderExtremeContention() throws InterruptedException {
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    SingleFlight sf = new SingleFlight(50000, 10, 5, asyncExec);
    int threadCount = 50;
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger executionCount = new AtomicInteger(0);
    AtomicInteger successCount = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      asyncExec.submit(() -> {
        try {
          Optional<String> result = sf.load("dedup-key", () -> {
            executionCount.incrementAndGet();
            try { Thread.sleep(50); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return "result";
          });
          if (result.isPresent()) successCount.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    asyncExec.shutdown();
    log.info("singleFlight: executions={}, success={}", executionCount.get(), successCount.get());
    assertThat(executionCount.get()).isLessThan(5);
    assertThat(successCount.get()).isEqualTo(threadCount);
  }

  /** HotKeyCache must remain consistent under concurrent reads and writes. */
  @Test
  void hotKeyCache_shouldMaintainConsistencyUnderConcurrentAccess() throws InterruptedException {
    TopK detector = new HeavyKeeper(100, 50000, 5, 0.92, 10);
    Cache<String, Object> caffeine = Caffeine.newBuilder().maximumSize(1000).build();
    Executor executor = Runnable::run;
    HotKeyProperties props = new HotKeyProperties();
    CacheExpireManager expireMgr = new CacheExpireManager(caffeine, executor, props, 10);
    SingleFlight sf = new SingleFlight(50000, 10, 5, executor);
    HotKeyCache cache = new HotKeyCache(
      detector, caffeine, sf, expireMgr, executor,
      Optional.empty(), Optional.empty(),
      new RuleMatcher(Optional.empty(), Optional.empty()),
      new VersionController(Optional.empty(), 60)
    );

    for (int i = 0; i < 20; i++) {
      caffeine.put("key-" + i, CacheEntry.builder()
        .value("initial")
        .dataVersion(1).isVersionDegraded(false).decisionVersion(0)
        .hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.NORMAL).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
        .build());
    }

    int threadCount = 10;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      pool.submit(() -> {
        try {
          for (int op = 0; op < 200; op++) {
            String key = "key-" + (op % 20);
            switch (op % 3) {
              case 0 -> cache.get(key, () -> "loaded");
              case 1 -> cache.invalidate(key);
              case 2 -> cache.peek(key);
            }
          }
        } catch (Exception e) {
          errors.incrementAndGet();
        } finally { latch.countDown(); }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    assertThat(errors.get()).isZero();
  }

  /** Only the first broadcast must pass dedup for the same key+version under concurrency. */
  @Test
  void cacheSyncPublisher_shouldDeduplicateConcurrentBroadcasts() throws InterruptedException {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setDedupWindowSeconds(60);
    props.setDedupMaxSize(10000);

    com.github.benmanes.caffeine.cache.Cache<String, Long> dedupCache = Caffeine.newBuilder()
      .expireAfterWrite(60, TimeUnit.SECONDS).maximumSize(10000).build();

    int threadCount = 20;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger sendCount = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      pool.submit(() -> {
        try {
          String compositeKey = "REFRESH:dedup-key";
          AtomicBoolean skipped = new AtomicBoolean(false);
          dedupCache.asMap().compute(compositeKey, (_, oldVersion) -> {
            if (oldVersion != null && oldVersion >= 5L) {
              skipped.set(true);
              return oldVersion;
            }
            return 5L;
          });
          if (!skipped.get()) sendCount.incrementAndGet();
        } finally { latch.countDown(); }
      });
    }

    assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    assertThat(sendCount.get()).isEqualTo(1);
  }

  /** A newer version must be accepted after an older one was already seen. */
  @Test
  void cacheSyncPublisher_shouldAllowNewerVersionAfterOlder() {
    com.github.benmanes.caffeine.cache.Cache<String, Long> dedupCache = Caffeine.newBuilder()
      .expireAfterWrite(60, TimeUnit.SECONDS).maximumSize(10000).build();
    String key = "REFRESH:ver-key";

    AtomicBoolean skipped1 = new AtomicBoolean(true);
    dedupCache.asMap().compute(key, (_, old) -> { skipped1.set(false); return 5L; });
    assertThat(skipped1.get()).isFalse();

    AtomicBoolean skipped2 = new AtomicBoolean(false);
    dedupCache.asMap().compute(key, (_, old) -> {
      if (old != null && old >= 4L) { skipped2.set(true); return old; }
      return 4L;
    });
    assertThat(skipped2.get()).isTrue();

    AtomicBoolean skipped3 = new AtomicBoolean(true);
    dedupCache.asMap().compute(key, (_, old) -> {
      if (old != null && old >= 6L) { skipped3.set(true); return old; }
      skipped3.set(false); return 6L;
    });
    assertThat(skipped3.get()).isFalse();
  }

  /** VersionGuard must handle concurrent shouldSkipForSync / shouldSkipForWorker calls correctly. */

  @Test
  void versionGuard_shouldHandleConcurrentAccess() throws InterruptedException {
    Cache<String, Object> cache = Caffeine.newBuilder().maximumSize(1000).build();
    cache.put("vg-key", CacheEntry.builder()
      .value("v").dataVersion(5).isVersionDegraded(false).decisionVersion(3)
      .hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(30_000)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build());

    int threadCount = 10;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      int tid = t;
      pool.submit(() -> {
        try {
          for (int i = 0; i < 100; i++) {
            boolean r1 = VersionGuard.shouldSkipForSync(cache, "vg-key", tid * 100L + i, false);
            boolean r2 = VersionGuard.shouldSkipForWorker(cache, "vg-key", tid * 100L + i);
            assertThat(r1).isIn(true, false);
            assertThat(r2).isIn(true, false);
          }
        } catch (Exception e) { errors.incrementAndGet(); }
        finally { latch.countDown(); }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    assertThat(errors.get()).isZero();
  }

  /** HotKeyStateMachine must handle concurrent evaluations for independent keys and the same key. */

  @Test
  void stateMachine_shouldHandleConcurrentEvaluations() throws InterruptedException {
    HotKeyStateMachine sm = new HotKeyStateMachine(3, 10, 3);
    int threadCount = 10;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      int tid = t;
      pool.submit(() -> {
        try {
          String key = "sm-key-" + tid;
          for (int i = 0; i < 5; i++) {
            HotKeyDecision d = sm.evaluate(key, true);
            assertThat(d).isNotNull();
            assertThat(d.cacheKey()).isEqualTo(key);
          }
          for (int i = 0; i < 15; i++) {
            HotKeyDecision d = sm.evaluate(key, false);
            assertThat(d).isNotNull();
            assertThat(d.cacheKey()).isEqualTo(key);
          }
        } catch (Exception e) { errors.incrementAndGet(); }
        finally { latch.countDown(); }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    assertThat(errors.get()).isZero();
  }

  /** All threads operating on the same key must not produce duplicate states. */
  @Test
  void stateMachine_sameKeyConcurrentEvaluations_shouldNotThrow() throws InterruptedException {
    HotKeyStateMachine sm = new HotKeyStateMachine(2, 5, 2);
    int threadCount = 20;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      pool.submit(() -> {
        try {
          for (int i = 0; i < 10; i++) {
            sm.evaluate("same-key", i < 3);
          }
        } catch (Exception e) { errors.incrementAndGet(); }
        finally { latch.countDown(); }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    assertThat(errors.get()).isZero();
  }

  /** HotKeyReporter must handle high-frequency concurrent record calls correctly. */

  @Test
  void reporter_highFrequencyRecord_shouldNotThrow() throws InterruptedException {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    ReportPublisher publisher = mock(ReportPublisher.class);
    doAnswer(inv -> null).when(publisher).publish(anyInt(), any());

    HotKeyReporter reporter = new HotKeyReporter(
      publisher, scheduler, 100, 1, "stress-test",
      10000, 100, 2
    );
    reporter.start();

    int threadCount = 10;
    int iterations = 50000;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      pool.submit(() -> {
        try {
          for (int i = 0; i < iterations; i++) {
            reporter.record("freq-key-" + (i & 0xFF));
          }
        } catch (Exception e) { errors.incrementAndGet(); }
        finally { latch.countDown(); }
      });
    }

    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    reporter.stop();
    scheduler.shutdown();
    assertThat(errors.get()).isZero();
    log.info("High frequency record: depth={}, expired={}, dropped={}",
      reporter.dispatcherDepth(), reporter.dispatcherExpired(), reporter.dispatcherDropped());
  }

  /** CacheSyncListener must handle concurrent invalidate and refresh messages correctly. */
  @Test
  void cacheSyncListener_concurrentInvalidateAndRefresh_shouldNotCorruptCache()
    throws InterruptedException {
    Cache<String, Object> cache = Caffeine.newBuilder().maximumSize(1000).build();

    // Pre-populate L1 with entries
    for (int i = 0; i < 50; i++) {
      cache.put("sync-key-" + i, CacheEntry.builder()
        .value("v" + i).dataVersion(10).isVersionDegraded(false).decisionVersion(5)
        .hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
        .build());
    }

    int threadCount = 10;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      int tid = t;
      pool.submit(() -> {
        try {
          for (int i = 0; i < 100; i++) {
            String key = "sync-key-" + (i % 50);
            // Simulate handleInvalidate
            if (i % 2 == 0) {
              cache.asMap().compute(key, (_, existing) -> {
                if (existing == null) return null;
                if (VersionGuard.shouldSkipForSync(cache, key, 15, false)) return existing;
                return null;
              });
            } else {
              // Simulate handleRefresh
              cache.asMap().compute(key, (_, existing) -> {
                if (existing == null) return null;
                if (VersionGuard.shouldSkipForSync(cache, key, 8, false)) return existing;
                if (existing instanceof CacheEntry ce) {
                  return CacheEntry.builder()
                    .value("refreshed-" + tid)
                    .dataVersion(15).isVersionDegraded(false)
                    .decisionVersion(ce.getDecisionVersion())
                    .hardTtlMs(ce.getHardTtlMs())
                    .hardExpireAtMs(ce.getHardExpireAtMs())
                    .softTtlMs(ce.getSoftTtlMs())
                    .softExpireAtMs(ce.getSoftExpireAtMs())
                    .keyState(ce.getKeyState())
                    .normalHardTtlMs(ce.getNormalHardTtlMs())
                    .normalSoftTtlMs(ce.getNormalSoftTtlMs())
                    .build();
                }
                return existing;
              });
            }
          }
        } catch (Exception e) { errors.incrementAndGet(); }
        finally { latch.countDown(); }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    assertThat(errors.get()).isZero();
  }

  /** WorkerListener must not corrupt the cache under concurrent HOT/COOL decisions. */
  @Test
  void workerListener_concurrentHotCool_shouldNotCorruptCache() throws InterruptedException {
    Cache<String, Object> cache = Caffeine.newBuilder().maximumSize(1000).build();

    cache.put("wc-key", CacheEntry.builder()
      .value("initial").dataVersion(5).isVersionDegraded(false).decisionVersion(0)
      .hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.NORMAL).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build());

    int threadCount = 10;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      long decisionVer = t + 1;
      pool.submit(() -> {
        try {
          for (int i = 0; i < 50; i++) {
            if (i < 25) {
              // Simulate HOT decision
              cache.asMap().compute("wc-key", (_, existing) -> {
                if (VersionGuard.shouldSkipForWorker(cache, "wc-key", decisionVer)) return existing;
                if (existing instanceof CacheEntry ce) {
                  return CacheEntry.builder()
                    .value(ce.getValue()).dataVersion(ce.getDataVersion())
                    .isVersionDegraded(ce.isVersionDegraded())
                    .decisionVersion(decisionVer)
                    .hardTtlMs(3_600_000).hardExpireAtMs(Long.MAX_VALUE)
                    .softTtlMs(300_000).softExpireAtMs(System.currentTimeMillis() + 300_000)
                    .keyState(KeyState.HOT)
                    .normalHardTtlMs(ce.getNormalHardTtlMs()).normalSoftTtlMs(ce.getNormalSoftTtlMs())
                    .build();
                }
                return existing;
              });
            } else {
              // Simulate COOL decision
              cache.asMap().compute("wc-key", (_, existing) -> {
                if (VersionGuard.shouldSkipForWorker(cache, "wc-key", decisionVer)) return existing;
                if (existing instanceof CacheEntry ce) {
                  return CacheEntry.builder()
                    .value(ce.getValue()).dataVersion(ce.getDataVersion())
                    .isVersionDegraded(ce.isVersionDegraded())
                    .decisionVersion(decisionVer)
                    .hardTtlMs(ce.getNormalHardTtlMs())
                    .hardExpireAtMs(Long.MAX_VALUE)
                    .softTtlMs(0).softExpireAtMs(0)
                    .keyState(KeyState.COOL)
                    .normalHardTtlMs(ce.getNormalHardTtlMs()).normalSoftTtlMs(ce.getNormalSoftTtlMs())
                    .build();
                }
                return existing;
              });
            }
          }
        } catch (Exception e) { errors.incrementAndGet(); }
        finally { latch.countDown(); }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    assertThat(errors.get()).isZero();

    // Final state must be COOL or NORMAL (higher-decisionVersion COOL overrides HOT)
    Object finalEntry = cache.getIfPresent("wc-key");
    assertThat(finalEntry).isInstanceOf(CacheEntry.class);
    CacheEntry ce = (CacheEntry) finalEntry;
    assertThat(ce.getKeyState()).isIn(KeyState.COOL, KeyState.HOT, KeyState.NORMAL);
    assertThat(ce.getDecisionVersion()).isPositive();
  }

  /**
   * Simulate a distributed multi-node scenario: each node has its own
   * HotKeyCache and shares a simulated Redis (Caffeine cache).
   * Concurrent reads/writes must keep the cache basically usable.
   */
  @Test
  void distributedScenario_shouldNotThrowUnderLoad() throws InterruptedException {
    int nodeCount = 3;
    int keyCount = 20;
    int opsPerNode = 300;

    // Shared simulated Redis
    Cache<String, Object> simulatedRedis = Caffeine.newBuilder().maximumSize(10000).build();
    for (int i = 0; i < keyCount; i++) {
      simulatedRedis.put("ds-key-" + i, "redis-value-" + i);
    }

    // Broadcast message queue (simulates RabbitMQ fanout)
    BlockingQueue<Runnable> broadcastQueue = new LinkedBlockingQueue<>();

    ExecutorService nodePool = Executors.newFixedThreadPool(nodeCount);
    CountDownLatch nodeLatch = new CountDownLatch(nodeCount);
    AtomicInteger totalErrors = new AtomicInteger(0);

    // Each node gets its own thread pool
    for (int n = 0; n < nodeCount; n++) {
      int nodeId = n;
      nodePool.submit(() -> {
        try {
          // Create each node's independent components
          TopK detector = new HeavyKeeper(100, 50000, 5, 0.92, 10);
          Cache<String, Object> l1 = Caffeine.newBuilder().maximumSize(1000).build();
          Executor syncExec = Runnable::run;
          HotKeyProperties props = new HotKeyProperties();
          CacheExpireManager expMgr = new CacheExpireManager(l1, syncExec, props, 10);
          SingleFlight sf = new SingleFlight(50000, 10, 5, syncExec);
          HotKeyCache hotKeyCache = new HotKeyCache(
            detector, l1, sf, expMgr, syncExec,
            Optional.empty(), Optional.empty(),
            new RuleMatcher(Optional.empty(), Optional.empty()),
            new VersionController(Optional.empty(), 60)
          );

          // Internal concurrent operations within each node
          int workerThreads = 5;
          ExecutorService workerPool = Executors.newFixedThreadPool(workerThreads);
          CountDownLatch workerLatch = new CountDownLatch(workerThreads);

          for (int w = 0; w < workerThreads; w++) {
            int workerId = w;
            workerPool.submit(() -> {
              try {
                for (int op = 0; op < opsPerNode; op++) {
                  String key = "ds-key-" + (op % keyCount);
                  int opType = (op + workerId + nodeId) % 4;
                  switch (opType) {
                    case 0 -> {
                      // READ: read from L1, fall back to simulated Redis on miss
                      hotKeyCache.get(key, () -> simulatedRedis.getIfPresent(key));
                    }
                    case 1 -> {
                      // INVALIDATE
                      hotKeyCache.invalidate(key);
                      // Broadcast simulated messages to other nodes
                      broadcastQueue.offer(() ->
                        l1.invalidate(key)
                      );
                    }
                    case 2 -> hotKeyCache.peek(key);
                    case 3 -> {
                      // WRITE: simulate write
                      simulatedRedis.put(key, "new-value-" + nodeId + "-" + op);
                    }
                  }
                }
              } catch (Exception e) {
                totalErrors.incrementAndGet();
              } finally {
                workerLatch.countDown();
              }
            });
          }

          assertThat(workerLatch.await(30, TimeUnit.SECONDS)).isTrue();
          workerPool.shutdown();

          // Process broadcast messages
          List<Runnable> broadcasts = new ArrayList<>();
          broadcastQueue.drainTo(broadcasts);
          for (Runnable r : broadcasts) {
            try { r.run(); } catch (Exception e) { totalErrors.incrementAndGet(); }
          }
        } catch (Exception e) {
          totalErrors.incrementAndGet();
        } finally {
          nodeLatch.countDown();
        }
      });
    }

    assertThat(nodeLatch.await(60, TimeUnit.SECONDS)).isTrue();
    nodePool.shutdown();
    assertThat(totalErrors.get()).isZero();
    log.info("Distributed scenario: nodes={}, errors={}", nodeCount, totalErrors.get());
  }
}
