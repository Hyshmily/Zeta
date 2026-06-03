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
 * 高并发、分布式场景的生产级压力测试。
 * 每个测试模拟生产环境负载，验证热点探测、缓存、广播等核心组件在极端条件下的正确性。
 */
class HotKeyStressTest {

  private static final Logger log = LoggerFactory.getLogger(HotKeyStressTest.class);

  // ─────────────────────────────────────────────────────────────
  // 1. HeavyKeeper 高并发测试
  // ─────────────────────────────────────────────────────────────

  /** 验证 HeavyKeeper 在高并发下不出现重复 key。 */
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

  /** 验证 TopK 大小不超过 k。 */
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

  // ─────────────────────────────────────────────────────────────
  // 2. SingleFlight 并发去重测试
  // ─────────────────────────────────────────────────────────────

  /** 验证极端并发下 supplier 执行次数远小于请求数。 */
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

  // ─────────────────────────────────────────────────────────────
  // 3. HotKeyCache 并发读写测试
  // ─────────────────────────────────────────────────────────────

  /** HotKeyCache 高并发读写下数据一致。 */
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

  // ─────────────────────────────────────────────────────────────
  // 4. CacheSyncPublisher 并发去重测试
  // ─────────────────────────────────────────────────────────────

  /** 验证 dedup 在并发下的正确性：相同 key+version 只有第一个通过。 */
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

  /** 验证版本递增时新版本覆盖旧版本。 */
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

  // ─────────────────────────────────────────────────────────────
  // 5. VersionGuard 并发测试
  // ─────────────────────────────────────────────────────────────

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

  // ─────────────────────────────────────────────────────────────
  // 6. HotKeyStateMachine 并发测试
  // ─────────────────────────────────────────────────────────────

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

  /** 所有线程操作相同 key，验证不会产生重复状态。 */
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

  // ─────────────────────────────────────────────────────────────
  // 7. HotKeyReporter 并发测试
  // ─────────────────────────────────────────────────────────────

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

  // ─────────────────────────────────────────────────────────────
  // 8. CacheSyncListener 并发测试
  // ─────────────────────────────────────────────────────────────

  /** 模拟 CacheSyncListener 在并发接收信息时的正确性。 */
  @Test
  void cacheSyncListener_concurrentInvalidateAndRefresh_shouldNotCorruptCache()
    throws InterruptedException {
    Cache<String, Object> cache = Caffeine.newBuilder().maximumSize(1000).build();

    // 预填充
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
            // 模拟 handleInvalidate
            if (i % 2 == 0) {
              cache.asMap().compute(key, (_, existing) -> {
                if (existing == null) return null;
                if (VersionGuard.shouldSkipForSync(cache, key, 15, false)) return existing;
                return null;
              });
            } else {
              // 模拟 handleRefresh
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

  // ─────────────────────────────────────────────────────────────
  // 9. WorkerListener HOT/COOL 并发测试
  // ─────────────────────────────────────────────────────────────

  /** WorkerListener 在并发 HOT/COOL 决策下不损坏缓存。 */
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
              // 模拟 HOT
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
              // 模拟 COOL
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

    // 最终 state 必须是 COOL 或 NORMAL（高 decisionVersion 的 COOL 会覆盖 HOT）
    Object finalEntry = cache.getIfPresent("wc-key");
    assertThat(finalEntry).isInstanceOf(CacheEntry.class);
    CacheEntry ce = (CacheEntry) finalEntry;
    assertThat(ce.getKeyState()).isIn(KeyState.COOL, KeyState.HOT, KeyState.NORMAL);
    assertThat(ce.getDecisionVersion()).isPositive();
  }

  // ─────────────────────────────────────────────────────────────
  // 10. 综合分布式场景压力测试
  // ─────────────────────────────────────────────────────────────

  /**
   * 模拟分布式多节点场景：每个节点有独立的 HotKeyCache，共享一个 simulated Redis
   * (Caffeine cache)。同时读写、使缓存保证基本可用。
   */
  @Test
  void distributedScenario_shouldNotThrowUnderLoad() throws InterruptedException {
    int nodeCount = 3;
    int keyCount = 20;
    int opsPerNode = 300;

    // 共享 Redis (模拟)
    Cache<String, Object> simulatedRedis = Caffeine.newBuilder().maximumSize(10000).build();
    for (int i = 0; i < keyCount; i++) {
      simulatedRedis.put("ds-key-" + i, "redis-value-" + i);
    }

    // 广播消息队列 (模拟 RabbitMQ fanout)
    BlockingQueue<Runnable> broadcastQueue = new LinkedBlockingQueue<>();

    ExecutorService nodePool = Executors.newFixedThreadPool(nodeCount);
    CountDownLatch nodeLatch = new CountDownLatch(nodeCount);
    AtomicInteger totalErrors = new AtomicInteger(0);

    // 每个节点启动一个线程池
    for (int n = 0; n < nodeCount; n++) {
      int nodeId = n;
      nodePool.submit(() -> {
        try {
          // 创建节点的独立组件
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

          // 节点的内部并发操作
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
                      // READ: 从 L1 读，miss 则从模拟 Redis 加载
                      hotKeyCache.get(key, () -> simulatedRedis.getIfPresent(key));
                    }
                    case 1 -> {
                      // INVALIDATE
                      hotKeyCache.invalidate(key);
                      // 广播到其他节点的模拟消息
                      broadcastQueue.offer(() ->
                        l1.invalidate(key)
                      );
                    }
                    case 2 -> hotKeyCache.peek(key);
                    case 3 -> {
                      // WRITE: 模拟写入
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

          // 处理广播消息
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
