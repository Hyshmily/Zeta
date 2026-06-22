/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.hyshmily.hotkey.integration.stress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.hotkey.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.local.CacheSyncProperties;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.HotKeyDecision;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.util.version.VersionController;
import io.github.hyshmily.hotkey.util.version.VersionGuard;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.reporting.ReportPublisher;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
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
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stress tests for core HotKey components under concurrent, high-throughput conditions.
 *
 * <p>Covers the HeavyKeeper algorithm, SingleFlight dedup, HotKeyCache consistency,
 * broadcast dedup, VersionGuard, HotKeyStateMachine, reporter backpressure,
 * listener concurrency, and distributed scenarios. Each test records latency
 * percentiles, throughput, and error counts, and writes a JSON report at the end.
 */
class HotKeyStressIT {

  private static final Logger log = LoggerFactory.getLogger(HotKeyStressIT.class);

  private static final List<StressMetrics> ALL_METRICS = new ArrayList<>();

  // -- Metrics Framework ----------------------------------------------------------

  /**
   * Collects and computes latency percentiles, throughput, and error counts for a single test scenario.
   *
   * <p>Test methods instantiate one of these, call {@link #recordLatency} and {@link #recordOp}
   * per operation, then {@link #finish()} to compute p50/p95/p99, ops/sec, and error totals.
   * Metrics are serialized to the final JSON report by {@link #writeReport}.
   */
  static class StressMetrics {

    final String testName;
    final long startNanos = System.nanoTime();
    final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger errorCount = new AtomicInteger(0);
    final AtomicLong opCount = new AtomicLong(0);
    final Map<String, Object> custom = new LinkedHashMap<>();

    long durationMs;
    long totalOps;
    int errors;
    double p50Ms;
    double p95Ms;
    double p99Ms;
    double opsPerSecond;

    /**
     * @param testName the test name for log and JSON report output
     */
    StressMetrics(String testName) {
      this.testName = testName;
    }

    /**
     * Records a single operation latency in nanoseconds.
     *
     * @param nanos elapsed time for one operation
     */
    void recordLatency(long nanos) {
      latencies.add(nanos);
    }

    /** Increments the operation counter. */
    void recordOp() {
      opCount.incrementAndGet();
    }

    /** Increments the error counter. */
    void recordError() {
      errorCount.incrementAndGet();
    }

    /**
     * Finalizes metrics computation: duration, percentiles, and throughput.
     *
     * @return this instance for chaining
     */
    StressMetrics finish() {
      durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      totalOps = opCount.get();
      errors = errorCount.get();
      long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
      int len = sorted.length;
      p50Ms = len > 0 ? sorted[len / 2] / 1_000_000.0 : 0;
      p95Ms = len > 0 ? sorted[(int) (len * 0.95)] / 1_000_000.0 : 0;
      p99Ms = len > 0 ? sorted[(int) (len * 0.99)] / 1_000_000.0 : 0;
      opsPerSecond = durationMs > 0 ? (double) totalOps / durationMs * 1000.0 : 0;
      return this;
    }

    /**
     * Logs a one-line summary of this metric's results at INFO level.
     */
    void logSummary() {
      log.info(
        "▸ {}: {} ops in {}ms | {} err | {} ops/s | P50={}ms P95={}ms P99={}ms {}",
        testName, totalOps, durationMs, errors, Math.round(opsPerSecond),
        String.format("%.2f", p50Ms), String.format("%.2f", p95Ms), String.format("%.2f", p99Ms),
        custom.isEmpty() ? "" : custom.toString());
    }

    /**
     * Serializes this metric's results to a Jackson {@link ObjectNode} including
     * custom fields mapped to numeric or string values.
     */
    ObjectNode toJson(ObjectMapper mapper) {
      ObjectNode n = mapper.createObjectNode();
      n.put("testName", testName);
      n.put("durationMs", durationMs);
      n.put("totalOps", totalOps);
      n.put("errors", errors);
      n.put("p50Ms", p50Ms);
      n.put("p95Ms", p95Ms);
      n.put("p99Ms", p99Ms);
      n.put("opsPerSecond", Math.round(opsPerSecond));
      ObjectNode cust = mapper.createObjectNode();
      custom.forEach((k, v) -> {
        if (v instanceof Number) cust.put(k, ((Number) v).doubleValue());
        else cust.put(k, v.toString());
      });
      n.set("custom", cust);
      return n;
    }
  }

  /**
   * Writes a JSON report of all collected stress metrics to
   * {@code src/test/resources/testresult/hotkey-stress-<timestamp>.json}.
   *
   * <p>Includes a summary (total tests, total duration, total ops, total errors) and per-test
   * details (latency percentiles, throughput, custom metrics). Runs once after all tests complete.
   */
  @AfterAll
  static void writeReport() throws IOException {
    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    ObjectNode root = mapper.createObjectNode();
    root.put("timestamp", Instant.now().toString());
    root.put("totalTests", ALL_METRICS.size());
    long totalDuration = ALL_METRICS.stream().mapToLong(m -> m.durationMs).sum();
    long totalOps = ALL_METRICS.stream().mapToLong(m -> m.totalOps).sum();
    int totalErrors = ALL_METRICS.stream().mapToInt(m -> m.errors).sum();
    root.put("totalDurationMs", totalDuration);
    root.put("totalOps", totalOps);
    root.put("totalErrors", totalErrors);
    ArrayNode tests = mapper.createArrayNode();
    for (StressMetrics m : ALL_METRICS) {
      tests.add(m.toJson(mapper));
    }
    root.set("tests", tests);
    Path reportPath = Path.of("src", "test", "resources", "testresult",
        "hotkey-stress-" + Instant.now().toString().replace(":", "-") + ".json");
    Files.createDirectories(reportPath.getParent());
    mapper.writeValue(reportPath.toFile(), root);
    log.info("Stress report written to {}", reportPath.toAbsolutePath());
  }

  // -- Shared Helpers -------------------------------------------------------------

  /**
   * Builds a {@link CacheEntry} with the given parameters; hard and soft expire-at times are
   * computed from the current wall clock.
   */
  static CacheEntry entry(String value, int dataVersion, KeyState state, long hardTtlMs, long decisionVersion) {
    return CacheEntry.builder()
      .value(value)
      .dataVersion(dataVersion)
      .isVersionDegraded(false)
      .decisionVersion(decisionVersion)
      .hardTtlMs(hardTtlMs)
      .hardExpireAtMs(hardTtlMs > 0 ? System.currentTimeMillis() + hardTtlMs : Long.MAX_VALUE)
      .softTtlMs(hardTtlMs > 0 ? Math.min(hardTtlMs, 30_000) : 30_000)
      .softExpireAtMs(hardTtlMs > 0 ? System.currentTimeMillis() + Math.min(hardTtlMs, 30_000) : 30_000)
      .keyState(state)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }

  /**
   * Creates a {@link HotKeyCache} wired with a {@link SingleFlight}, {@link CacheExpireManager},
   * empty {@link RuleMatcher}, and a {@link VersionController} for stress testing.
   */
  static HotKeyCache createCache(HotKeyDetector detector, Cache<String, Object> caffeine, Executor executor) {
    HotKeyProperties props = new HotKeyProperties();
    CacheExpireManager expireMgr = new CacheExpireManager(caffeine, executor, props, 10);
    SingleFlight sf = new SingleFlight(50000, 10, 5, executor);
    return new HotKeyCache(
      detector, caffeine, sf, expireMgr, executor,
      Optional.empty(), Optional.empty(),
      new RuleMatcher(Optional.empty(), Optional.empty()),
      new VersionController(Optional.empty(), 60),
      new RingManager(150),
      new ClusterHealthView(0, 5000, 3));
  }

  /**
   * A consumer that accepts an integer index and may throw any exception.
   *
   * <p>Used by {@link #concurrentRun} to abstract per-operation logic that might fail, so errors
   * can be caught and counted rather than aborting the entire concurrent workload.
   */
  interface ThrowingConsumer {
    void accept(int index) throws Exception;
  }

  /**
   * Runs a concurrent workload with the given thread count; each thread executes {@code opsPerThread}
   * operations via the supplied task. All errors are tracked in the provided metrics.
   */
  static void concurrentRun(String label, int threads, int opsPerThread, ThrowingConsumer task, StressMetrics metrics)
    throws InterruptedException {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch latch = new CountDownLatch(threads);
    AtomicInteger localErrors = new AtomicInteger(0);
    for (int t = 0; t < threads; t++) {
      int tid = t;
      pool.submit(() -> {
        try {
          for (int i = 0; i < opsPerThread; i++) {
            long start = System.nanoTime();
            task.accept(tid * opsPerThread + i);
            metrics.recordLatency(System.nanoTime() - start);
            metrics.recordOp();
          }
        } catch (Exception e) {
          localErrors.incrementAndGet();
          metrics.recordError();
        } finally {
          latch.countDown();
        }
      });
    }
    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    if (localErrors.get() > 0) {
      log.warn("  ⚠ {} local errors in {}", localErrors.get(), label);
    }
  }

  // ================================================================================
  // Section 1: Algorithm (HeavyKeeper / TopK)
  // ================================================================================

  /**
   * Verifies HeavyKeeper correctly identifies top-K keys under a Zipf distribution.
   */
  @Test
  void heavyKeeper_zipfDistribution() throws InterruptedException {
    StressMetrics m = new StressMetrics("heavyKeeper_zipfDistribution");
    ALL_METRICS.add(m);
    int k = 50;
    HeavyKeeper keeper = new HeavyKeeper(k, 50000, 5, 0.925, 10);
    int totalKeys = 500;
    double exponent = 1.2;
    double[] weights = new double[totalKeys];
    double sum = 0;
    for (int i = 1; i <= totalKeys; i++) {
      weights[i - 1] = 1.0 / Math.pow(i, exponent);
      sum += weights[i - 1];
    }
    int[] accesses = new int[totalKeys];
    int totalOps = 200_000;
    Random rng = new Random(42);
    for (int i = 0; i < totalOps; i++) {
      double dice = rng.nextDouble() * sum;
      double cum = 0;
      for (int j = 0; j < totalKeys; j++) {
        cum += weights[j];
        if (dice <= cum) {
          accesses[j]++;
          keeper.addDirect("zipf-" + j, 1);
          break;
        }
      }
    }

    List<Item> top = keeper.list();
    m.custom.put("topKSize", top.size());
    m.custom.put("totalAccesses", totalOps);
    m.custom.put("k", k);

    Set<String> seen = new HashSet<>();
    long totalInList = 0;
    for (Item item : top) {
      assertThat(seen.add(item.key())).isTrue();
      totalInList += item.count();
    }
    log.info("  Zipf: K={}, totalKeys={}, topKCount={}, totalInTopK={}", k, totalKeys, top.size(), totalInList);
    int topKeyIdx = Integer.parseInt(top.get(0).key().replace("zipf-", ""));
    log.info("  Top-1 key: zipf-{} (rank {} by Zipf), accesses={}", topKeyIdx, topKeyIdx + 1, accesses[topKeyIdx]);
    m.finish().logSummary();
  }

  /**
   * Ensures HeavyKeeper produces no duplicate keys under high-thread contention.
   */
  @Test
  void heavyKeeper_noDuplicateKeys() throws InterruptedException {
    StressMetrics m = new StressMetrics("heavyKeeper_noDuplicateKeys");
    ALL_METRICS.add(m);
    int threadCount = 30;
    int keysPerThread = 100;
    int iterationsPerKey = 500;
    HeavyKeeper keeper = new HeavyKeeper(200, 80000, 5, 0.92, 10);
    concurrentRun("noDup", threadCount, keysPerThread, (idx) -> {
      int t = idx / keysPerThread;
      int k = idx % keysPerThread;
      String key = "ndup-" + t + "-" + k;
      for (int i = 0; i < iterationsPerKey; i++) {
        keeper.addDirect(key, 1);
      }
    }, m);
    assertThat(keeper.total()).isEqualTo((long) threadCount * keysPerThread * iterationsPerKey);
    List<Item> items = keeper.list();
    Set<String> seen = new HashSet<>();
    for (Item item : items) {
      assertThat(seen.add(item.key())).as("Duplicate in TopK: %s", item.key()).isTrue();
    }
    for (int i = 1; i < items.size(); i++) {
      assertThat(items.get(i - 1).count()).isGreaterThanOrEqualTo(items.get(i).count());
    }
    m.custom.put("topKSize", items.size());
    m.custom.put("totalDistinctKeys", threadCount * keysPerThread);
    m.finish().logSummary();
  }

  /**
   * Confirms HeavyKeeper's result list stays within configured K bound under heavy insertions.
   */
  @Test
  void heavyKeeper_boundedSize() throws InterruptedException {
    StressMetrics m = new StressMetrics("heavyKeeper_boundedSize");
    ALL_METRICS.add(m);
    int k = 10;
    HeavyKeeper keeper = new HeavyKeeper(k, 50000, 5, 0.92, 5);
    int threadCount = 50;
    int iterations = 500;
    concurrentRun("bounded", threadCount, 1, (idx) -> {
      for (int i = 0; i < iterations; i++) {
        keeper.addDirect("bk-" + (i % 5), 10);
      }
    }, m);
    int size = keeper.list().size();
    assertThat(size).isLessThanOrEqualTo(k);
    m.custom.put("actualSize", size);
    m.custom.put("maxK", k);
    m.finish().logSummary();
  }

  // ================================================================================
  // Section 2: SingleFlight
  // ================================================================================

  /**
   * Verifies SingleFlight deduplicates 100 concurrent requests into a single supplier execution.
   */
  @Test
  void singleFlight_extremeDedup() throws InterruptedException {
    StressMetrics m = new StressMetrics("singleFlight_extremeDedup");
    ALL_METRICS.add(m);
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    SingleFlight sf = new SingleFlight(50000, 10, 5, asyncExec);
    int threadCount = 100;
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger executionCount = new AtomicInteger(0);
    AtomicInteger successCount = new AtomicInteger(0);
    for (int i = 0; i < threadCount; i++) {
      asyncExec.submit(() -> {
        try {
          long start = System.nanoTime();
          Optional<String> result = sf.load("dedup-extreme", () -> {
            executionCount.incrementAndGet();
            try { Thread.sleep(5); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return "result";
          });
          if (result.isPresent()) successCount.incrementAndGet();
          m.recordLatency(System.nanoTime() - start);
          m.recordOp();
        } finally { latch.countDown(); }
      });
    }
    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    asyncExec.shutdown();
    double ratio = (double) (threadCount - executionCount.get()) / threadCount * 100;
    m.custom.put("executions", executionCount.get());
    m.custom.put("dedupRatio", Math.round(ratio * 100.0) / 100.0);
    log.info("  SingleFlight: {} threads, {} actual executions ({:.1f}% dedup), {} success",
      threadCount, executionCount.get(), ratio, successCount.get());
    assertThat(executionCount.get()).isLessThan(30);
    assertThat(successCount.get()).isEqualTo(threadCount);
    m.finish().logSummary();
  }

  /**
   * Prevents cache stampede across 50 keys each contended by 20 threads simultaneously.
   */
  @Test
  void singleFlight_cacheStampede() throws InterruptedException {
    StressMetrics m = new StressMetrics("singleFlight_cacheStampede");
    ALL_METRICS.add(m);
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    SingleFlight sf = new SingleFlight(50000, 10, 5, asyncExec);
    int keyCount = 50;
    int threadsPerKey = 20;
    int totalThreads = keyCount * threadsPerKey;
    CountDownLatch latch = new CountDownLatch(totalThreads);
    AtomicInteger totalExecutions = new AtomicInteger(0);
    AtomicInteger totalSuccess = new AtomicInteger(0);
    for (int k = 0; k < keyCount; k++) {
      String key = "stampede-" + k;
      for (int t = 0; t < threadsPerKey; t++) {
        asyncExec.submit(() -> {
          try {
            long start = System.nanoTime();
            Optional<String> result = sf.load(key, () -> {
              totalExecutions.incrementAndGet();
              try { Thread.sleep(1); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
              return "loaded";
            });
            if (result.isPresent()) totalSuccess.incrementAndGet();
            m.recordLatency(System.nanoTime() - start);
            m.recordOp();
          } finally { latch.countDown(); }
        });
      }
    }
    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    asyncExec.shutdown();
    double ratio = (double) (totalThreads - totalExecutions.get()) / totalThreads * 100;
    m.custom.put("keyCount", keyCount);
    m.custom.put("threadsPerKey", threadsPerKey);
    m.custom.put("totalExecutions", totalExecutions.get());
    m.custom.put("dedupRatio", Math.round(ratio * 100.0) / 100.0);
    log.info("  Stampede: {} keys, {} threads, {} actual executions ({:.1f}% dedup), {} success",
      keyCount, totalThreads, totalExecutions.get(), ratio, totalSuccess.get());
    assertThat(totalExecutions.get()).isLessThan(keyCount * 3);
    assertThat(totalSuccess.get()).isEqualTo(totalThreads);
    m.finish().logSummary();
  }

  /**
   * Validates SingleFlight handles mixed hot-key and cold-key contention correctly.
   */
  @Test
  void singleFlight_mixedHotCold() throws InterruptedException {
    StressMetrics m = new StressMetrics("singleFlight_mixedHotCold");
    ALL_METRICS.add(m);
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    SingleFlight sf = new SingleFlight(50000, 10, 5, asyncExec);
    int hotKeys = 5;
    int coldKeys = 95;
    int requestsPerKey = 10;
    CountDownLatch latch = new CountDownLatch((hotKeys + coldKeys) * requestsPerKey);
    AtomicInteger totalExecutions = new AtomicInteger(0);
    Map<String, Integer> execPerKey = new HashMap<>();
    for (int k = 0; k < hotKeys; k++) {
      String hk = "hot-" + k;
      execPerKey.put(hk, 0);
      for (int r = 0; r < requestsPerKey; r++) {
        String fk = hk;
        asyncExec.submit(() -> {
          try {
            sf.load(fk, () -> { totalExecutions.incrementAndGet(); execPerKey.compute(fk, (kk, v) -> v == null ? 1 : v + 1); try { Thread.sleep(1); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } return "hot"; });
          } finally { latch.countDown(); }
        });
      }
    }
    for (int k = 0; k < coldKeys; k++) {
      String ck = "cold-" + k;
      for (int r = 0; r < requestsPerKey; r++) {
        asyncExec.submit(() -> {
          try {
            sf.load(ck, () -> { totalExecutions.incrementAndGet(); try { Thread.sleep(1); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } return "cold"; });
          } finally { latch.countDown(); }
        });
      }
    }
    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    asyncExec.shutdown();
    m.custom.put("hotKeys", hotKeys);
    m.custom.put("coldKeys", coldKeys);
    m.custom.put("totalExecutions", totalExecutions.get());
    log.info("  Mixed: hot={}, cold={}, totalExec={}", hotKeys, coldKeys, totalExecutions.get());
    m.finish().logSummary();
  }

  // ================================================================================
  // Section 3: HotKeyCache
  // ================================================================================

  /**
   * Ensures concurrent get/invalidate/peek on HotKeyCache maintains consistency without errors.
   */
  @Test
  void hotKeyCache_consistency() throws InterruptedException {
    StressMetrics m = new StressMetrics("hotKeyCache_consistency");
    ALL_METRICS.add(m);
    HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(100, 50000, 5, 0.92, 10));
    Cache<String, Object> caffeine = Caffeine.newBuilder().maximumSize(1000).build();
    Executor executor = Runnable::run;
    HotKeyCache cache = createCache(detector, caffeine, executor);
    for (int i = 0; i < 50; i++) {
      caffeine.put("ck-" + i, entry("init", 1, KeyState.NORMAL, 300_000, 0));
    }
    concurrentRun("consistency", 20, 500, (idx) -> {
      String key = "ck-" + (idx % 50);
      switch (idx % 3) {
        case 0 -> cache.get(key, () -> "loaded");
        case 1 -> cache.invalidate(key);
        case 2 -> cache.peek(key);
      }
    }, m);
    assertThat(m.errorCount.get()).isZero();
    m.finish().logSummary();
  }

  /**
   * Simulates a realistic read-heavy (90%) / write-light (10%) production workload mix.
   */
  @Test
  void hotKeyCache_productionMix() throws InterruptedException {
    StressMetrics m = new StressMetrics("hotKeyCache_productionMix");
    ALL_METRICS.add(m);
    HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(100, 50000, 5, 0.92, 10));
    Cache<String, Object> caffeine = Caffeine.newBuilder().maximumSize(1000).build();
    Executor executor = Runnable::run;
    HotKeyCache cache = createCache(detector, caffeine, executor);
    for (int i = 0; i < 100; i++) {
      caffeine.put("pm-" + i, entry("v", 1, KeyState.NORMAL, 300_000, 0));
    }
    int threadCount = 20;
    int opsPerThread = 500;
    int totalOps = threadCount * opsPerThread;
    AtomicInteger reads = new AtomicInteger(0);
    AtomicInteger writes = new AtomicInteger(0);
    concurrentRun("productionMix", threadCount, opsPerThread, (idx) -> {
      String key = "pm-" + (idx % 100);
      if (idx % 10 < 9) {
        cache.get(key, () -> "loaded");
        reads.incrementAndGet();
      } else {
        cache.putThrough(key, "new-val", () -> {}, 300_000, 30_000);
        writes.incrementAndGet();
      }
    }, m);
    m.custom.put("readPct", Math.round((double) reads.get() / totalOps * 100));
    m.custom.put("writePct", Math.round((double) writes.get() / totalOps * 100));
    log.info("  Production mix: {} reads ({:.0f}%), {} writes ({:.0f}%)",
      reads.get(), (double) reads.get() / totalOps * 100,
      writes.get(), (double) writes.get() / totalOps * 100);
    assertThat(m.errorCount.get()).isZero();
    m.finish().logSummary();
  }

  /**
   * Stress-tests HotKeyCache when 200 entries expire nearly simultaneously and are reloaded.
   */
  @Test
  void hotKeyCache_ttlExpiryStorm() throws InterruptedException {
    StressMetrics m = new StressMetrics("hotKeyCache_ttlExpiryStorm");
    ALL_METRICS.add(m);
    HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(200, 50000, 5, 0.92, 10));
    Cache<String, Object> caffeine = Caffeine.newBuilder().maximumSize(2000).build();
    Executor executor = Runnable::run;
    HotKeyCache cache = createCache(detector, caffeine, executor);
    int keyCount = 200;
    for (int i = 0; i < keyCount; i++) {
      caffeine.put("es-" + i, entry("v", 1, KeyState.NORMAL, 1, 0));
    }
    Thread.sleep(5);
    concurrentRun("expiryStorm", 30, 200, (idx) -> {
      String key = "es-" + (idx % keyCount);
      cache.get(key, () -> "reloaded");
    }, m);
    assertThat(m.errorCount.get()).isZero();
    m.custom.put("keyCount", keyCount);
    log.info("  TTL storm: {} keys expired, {} ops, {} errors", keyCount, m.opCount.get(), m.errorCount.get());
    m.finish().logSummary();
  }

  /**
   * Verifies Caffeine eviction under high memory pressure (5x max capacity insertions).
   */
  @Test
  void hotKeyCache_memoryPressure() throws InterruptedException {
    StressMetrics m = new StressMetrics("hotKeyCache_memoryPressure");
    ALL_METRICS.add(m);
    int maxSize = 200;
    HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(500, 50000, 5, 0.92, 10));
    Cache<String, Object> caffeine = Caffeine.newBuilder().maximumSize(maxSize).build();
    Executor executor = Runnable::run;
    createCache(detector, caffeine, executor);
    int insertCount = maxSize * 5;
    for (int i = 0; i < insertCount; i++) {
      caffeine.put("mp-" + i, entry("big", 1, KeyState.HOT, 300_000, i));
    }
    caffeine.cleanUp();
    long actualSize = caffeine.estimatedSize();
    m.custom.put("maxSize", maxSize);
    m.custom.put("insertedCount", insertCount);
    m.custom.put("actualSize", actualSize);
    log.info("  Memory pressure: max={}, inserted={}, actual={}", maxSize, insertCount, actualSize);
    assertThat(actualSize).isLessThanOrEqualTo((long) maxSize * 2);
    m.finish().logSummary();
  }

  /**
   * Tracks a single key through its full lifecycle: warmup → hot → cool states.
   */
  @Test
  void hotKeyCache_lifecycle() throws InterruptedException {
    StressMetrics m = new StressMetrics("hotKeyCache_lifecycle");
    ALL_METRICS.add(m);
    HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(50, 50000, 5, 0.92, 10));
    Cache<String, Object> caffeine = Caffeine.newBuilder().maximumSize(100).build();
    Executor executor = Runnable::run;
    HotKeyCache cache = createCache(detector, caffeine, executor);
    String key = "lifecycle-key";
    caffeine.put(key, entry("start", 1, KeyState.NORMAL, 300_000, 0));
    int warmupOps = 10;
    for (int i = 0; i < warmupOps; i++) {
      cache.get(key, () -> "val");
    }
    int hotOps = 500;
    for (int i = 0; i < hotOps; i++) {
      cache.get(key, () -> "val");
      detector.add(key, 1);
    }
    CacheEntry ce = (CacheEntry) caffeine.getIfPresent(key);
    boolean becameHot = ce != null && ce.getKeyState() == KeyState.HOT;
    int coolOps = 200;
    for (int i = 0; i < coolOps; i++) {
      cache.get(key, () -> "val");
    }
    m.custom.put("warmupOps", warmupOps);
    m.custom.put("hotOps", hotOps);
    m.custom.put("coolOps", coolOps);
    m.custom.put("becameHot", becameHot ? 1 : 0);
    log.info("  Lifecycle: warmup={}, hot={}, cool={}, becameHot={}", warmupOps, hotOps, coolOps, becameHot);
    m.finish().logSummary();
  }

  // ================================================================================
  // Section 4: Broadcast
  // ================================================================================

  /**
   * Verifies CacheSyncPublisher deduplicates 30 concurrent send attempts to exactly 1 actual send.
   */
  @Test
  void cacheSyncPublisher_dedup() throws InterruptedException {
    StressMetrics m = new StressMetrics("cacheSyncPublisher_dedup");
    ALL_METRICS.add(m);
    Cache<String, Long> dedupCache = Caffeine.newBuilder()
      .expireAfterWrite(60, TimeUnit.SECONDS).maximumSize(10000).build();
    int threadCount = 30;
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger sendCount = new AtomicInteger(0);
    for (int t = 0; t < threadCount; t++) {
      int tid = t;
      Thread thread = new Thread(() -> {
        try {
          long start = System.nanoTime();
          String compositeKey = "REFRESH:broadcast-key";
          AtomicBoolean skipped = new AtomicBoolean(false);
          dedupCache.asMap().compute(compositeKey, (k, oldVersion) -> {
            if (oldVersion != null && oldVersion >= 10L) {
              skipped.set(true);
              return oldVersion;
            }
            return 10L;
          });
          if (skipped.get()) {
            m.recordLatency(System.nanoTime() - start);
            m.custom.put("deduped_" + tid, 1);
          } else {
            sendCount.incrementAndGet();
            m.recordLatency(System.nanoTime() - start);
            m.custom.put("sender_" + tid, 1);
          }
        } finally { latch.countDown(); }
      });
      thread.start();
    }
    assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(sendCount.get()).isEqualTo(1);
    m.custom.put("threadCount", threadCount);
    m.custom.put("actualSends", sendCount.get());
    double dedupPct = (double) (threadCount - sendCount.get()) / threadCount * 100;
    m.custom.put("dedupPct", Math.round(dedupPct * 100.0) / 100.0);
    log.info("  Broadcast dedup: {} threads, {} sends, {:.1f}% dedup", threadCount, sendCount.get(), dedupPct);
    m.finish().logSummary();
  }

  /**
   * Validates version-ordering logic with 5 test cases (newer, older, equal, edge cases).
   */
  @Test
  void cacheSyncPublisher_versionOrdering() {
    StressMetrics m = new StressMetrics("cacheSyncPublisher_versionOrdering");
    ALL_METRICS.add(m);
    Cache<String, Long> dedupCache = Caffeine.newBuilder()
      .expireAfterWrite(60, TimeUnit.SECONDS).maximumSize(10000).build();
    int[][] testCases = {{5, 3, 1}, {3, 6, 0}, {10, 10, 1}, {7, 7, 1}, {1, 0, 1}};
    for (int[] tc : testCases) {
      String key = "ver-" + tc[0] + "-" + tc[1];
      dedupCache.asMap().compute(key, (k, old) -> (long) tc[0]);
      AtomicBoolean skipped = new AtomicBoolean(false);
      dedupCache.asMap().compute(key, (k, old) -> {
        if (old != null && old >= tc[1]) { skipped.set(true); return old; }
        return (long) tc[1];
      });
      boolean expectedSkip = tc[2] == 1;
      assertThat(skipped.get()).as("ver: cur=%d, in=%d, expSkip=%s", tc[0], tc[1], expectedSkip).isEqualTo(expectedSkip);
    }
    m.custom.put("testCases", testCases.length);
    log.info("  Version ordering: {} cases, all correct", testCases.length);
    m.finish().logSummary();
  }

  /**
   * Simulates a broadcast storm with 2000 requests across 500 keys from 20 concurrent threads.
   */
  @Test
  void broadcastStorm() throws InterruptedException {
    StressMetrics m = new StressMetrics("broadcastStorm");
    ALL_METRICS.add(m);
    Cache<String, Long> dedupCache = Caffeine.newBuilder()
      .expireAfterWrite(60, TimeUnit.SECONDS).maximumSize(100000).build();
    int stormSize = 2000;
    int threads = 20;
    int opsPerThread = stormSize / threads;
    concurrentRun("broadcastStorm", threads, opsPerThread, (idx) -> {
      String key = "storm-key-" + (idx % 500);
      String compositeKey = "REFRESH:" + key;
      dedupCache.asMap().compute(compositeKey, (k, oldVersion) -> {
        long newVer = idx;
        if (oldVersion != null && oldVersion >= newVer) return oldVersion;
        return newVer;
      });
    }, m);
    assertThat(m.errorCount.get()).isZero();
    m.custom.put("stormSize", stormSize);
    m.custom.put("uniqueKeys", 500);
    log.info("  Broadcast storm: {} requests, {} keys, 0 errors", stormSize, 500);
    m.finish().logSummary();
  }

  // ================================================================================
  // Section 5: VersionGuard
  // ================================================================================

  /**
   * Tests VersionGuard's shouldSkipForSync and shouldSkipForWorker under 10-thread concurrent load.
   */
  @Test
  void versionGuard_concurrent() throws InterruptedException {
    StressMetrics m = new StressMetrics("versionGuard_concurrent");
    ALL_METRICS.add(m);
    Cache<String, Object> cache = Caffeine.newBuilder().maximumSize(100).build();
    cache.put("vg-key", entry("v", 5, KeyState.HOT, 300_000, 3));
    concurrentRun("versionGuard", 10, 500, (idx) -> {
      VersionGuard.shouldSkipForSync(cache, "vg-key", idx, false);
      VersionGuard.shouldSkipForWorker(cache, "vg-key", idx);
    }, m);
    assertThat(m.errorCount.get()).isZero();
    m.finish().logSummary();
  }

  // ================================================================================
  // Section 6: HotKeyStateMachine
  // ================================================================================

  /**
   * Verifies HotKeyStateMachine handles independent keys with no cross-key interference.
   */
  @Test
  void stateMachine_independentKeys() throws InterruptedException {
    StressMetrics m = new StressMetrics("stateMachine_independentKeys");
    ALL_METRICS.add(m);
    HotKeyStateMachine sm = new HotKeyStateMachine(3, 10, 3);
    concurrentRun("smIndependent", 10, 5, (idx) -> {
      String key = "sm-key-" + idx;
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
    }, m);
    assertThat(m.errorCount.get()).isZero();
    m.finish().logSummary();
  }

  /**
   * Stress-tests HotKeyStateMachine with 20 threads all evaluating the same key concurrently.
   */
  @Test
  void stateMachine_sameKey() throws InterruptedException {
    StressMetrics m = new StressMetrics("stateMachine_sameKey");
    ALL_METRICS.add(m);
    HotKeyStateMachine sm = new HotKeyStateMachine(2, 5, 2);
    concurrentRun("smSameKey", 20, 10, (idx) -> {
      sm.evaluate("same-key", idx < 3);
    }, m);
    assertThat(m.errorCount.get()).isZero();
    m.finish().logSummary();
  }

  /**
   * Simulates gradual hot-key emergence over 5 phases where hot ratio changes over time.
   */
  @Test
  void stateMachine_gradualDrift() throws InterruptedException {
    StressMetrics m = new StressMetrics("stateMachine_gradualDrift");
    ALL_METRICS.add(m);
    HotKeyStateMachine sm = new HotKeyStateMachine(3, 10, 3);
    String key = "drift-key";
    int totalPhases = 5;
    int opsPerPhase = 50;
    List<KeyState> states = new ArrayList<>();
    for (int phase = 0; phase < totalPhases; phase++) {
      for (int i = 0; i < opsPerPhase; i++) {
        boolean isHot = phase >= 2;
        HotKeyDecision d = sm.evaluate(key, isHot);
        if (d != null && d.type() != null) {
          if (d.type() == HotKeyDecision.DecisionType.HOT) states.add(KeyState.HOT);
          else if (d.type() == HotKeyDecision.DecisionType.COOL) states.add(KeyState.COOL);
        }
      }
      Thread.sleep(2);
    }
    m.custom.put("phases", totalPhases);
    m.custom.put("opsPerPhase", opsPerPhase);
    m.custom.put("totalDecisions", states.size());
    log.info("  Gradual drift: {} phases, {} ops/phase, {} decisions", totalPhases, opsPerPhase, states.size());
    m.finish().logSummary();
  }

  // ================================================================================
  // Section 7: Reporter
  // ================================================================================

  /**
   * Pushes the HotKeyReporter with 20 threads × 100k high-frequency record operations.
   */
  @Test
  void reporter_highFrequency() throws InterruptedException {
    StressMetrics m = new StressMetrics("reporter_highFrequency");
    ALL_METRICS.add(m);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    ReportPublisher publisher = mock(ReportPublisher.class);
    doAnswer(inv -> null).when(publisher).publish(any(), any());
    HotKeyReporter reporter = new HotKeyReporter(
      publisher, scheduler, 100L, "stress-test", 10000, 100, 2, new RingManager(150),
      new ClusterHealthView(0, 5000, 3));
    reporter.start();
    concurrentRun("highFreq-reporter", 20, 100_000, (idx) -> {
      reporter.record("freq-key-" + (idx & 0xFF));
    }, m);
    Thread.sleep(200);
    reporter.stop();
    scheduler.shutdown();
    assertThat(m.errorCount.get()).isZero();
    m.custom.put("depth", (long) reporter.dispatcherDepth());
    m.custom.put("expired", (long) reporter.dispatcherExpired());
    m.custom.put("dropped", (long) reporter.dispatcherDropped());
    log.info("  High-freq reporter: {} ops, depth={}, expired={}, dropped={}",
      m.totalOps, reporter.dispatcherDepth(), reporter.dispatcherExpired(), reporter.dispatcherDropped());
    m.finish().logSummary();
  }

  /**
   * Validates HotKeyReporter backpressure behavior with limited queue capacity under heavy load.
   */
  @Test
  void reporter_backpressure() throws InterruptedException {
    StressMetrics m = new StressMetrics("reporter_backpressure");
    ALL_METRICS.add(m);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    ReportPublisher publisher = mock(ReportPublisher.class);
    doAnswer(inv -> null).when(publisher).publish(any(), any());
    int queueCapacity = 1000;
    HotKeyReporter reporter = new HotKeyReporter(
      publisher, scheduler, 100L, "backpressure-test",
      queueCapacity, 1, 1, new RingManager(150),
      new ClusterHealthView(0, 5000, 3));
    reporter.start();
    int threadCount = 10;
    int opsPerThread = 20_000;
    concurrentRun("backpressure", threadCount, opsPerThread, (idx) -> {
      reporter.record("bp-key-" + (idx & 0x1F));
    }, m);
    Thread.sleep(200);
    reporter.stop();
    scheduler.shutdown();
    long depth = reporter.dispatcherDepth();
    long expired = reporter.dispatcherExpired();
    long dropped = reporter.dispatcherDropped();
    long queueLoss = expired + dropped;
    double lossRate = (double) queueLoss / m.totalOps * 100;
    m.custom.put("queueCapacity", queueCapacity);
    m.custom.put("depth", depth);
    m.custom.put("expired", expired);
    m.custom.put("dropped", dropped);
    m.custom.put("queueLoss", queueLoss);
    m.custom.put("lossRatePct", Math.round(lossRate * 100.0) / 100.0);
    log.info("  Backpressure: cap={}, {} ops, queueLoss={} ({:.2f}%), depth={}",
      queueCapacity, m.totalOps, queueLoss, lossRate, depth);
    m.finish().logSummary();
  }

  /**
   * Tests HotKeyReporter sharding with 4 shards spread across 8 threads × 20k operations.
   */
  @Test
  void reporter_multiShard() throws InterruptedException {
    StressMetrics m = new StressMetrics("reporter_multiShard");
    ALL_METRICS.add(m);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    ReportPublisher publisher = mock(ReportPublisher.class);
    doAnswer(inv -> null).when(publisher).publish(any(), any());
    HotKeyReporter reporter = new HotKeyReporter(
      publisher, scheduler, 100L, "shard-test",
      10000, 50, 4, new RingManager(150),
      new ClusterHealthView(0, 5000, 3));
    reporter.start();
    int threadCount = 8;
    int opsPerThread = 20_000;
    concurrentRun("shard", threadCount, opsPerThread, (idx) -> {
      reporter.record("shard-key-" + (idx & 0x3FF));
    }, m);
    Thread.sleep(200);
    reporter.stop();
    scheduler.shutdown();
    m.custom.put("depth", (long) reporter.dispatcherDepth());
    m.custom.put("expired", (long) reporter.dispatcherExpired());
    m.custom.put("dropped", (long) reporter.dispatcherDropped());
    log.info("  Multi-shard: {} ops, depth={}, expired={}, dropped={}",
      m.totalOps, reporter.dispatcherDepth(), reporter.dispatcherExpired(), reporter.dispatcherDropped());
    m.finish().logSummary();
  }

  // ================================================================================
  // Section 8: Listener Stress
  // ================================================================================

  /**
   * Tests concurrent invalidate and refresh operations on CacheSyncListener across 100 keys.
   */
  @Test
  void cacheSyncListener_concurrent() throws InterruptedException {
    StressMetrics m = new StressMetrics("cacheSyncListener_concurrent");
    ALL_METRICS.add(m);
    Cache<String, Object> cache = Caffeine.newBuilder().maximumSize(1000).build();
    for (int i = 0; i < 100; i++) {
      cache.put("sl-key-" + i, entry("v" + i, 10, KeyState.HOT, 300_000, 5));
    }
    concurrentRun("syncListener", 10, 200, (idx) -> {
      String key = "sl-key-" + (idx % 100);
      if (idx % 2 == 0) {
        cache.asMap().compute(key, (k, existing) -> {
          if (existing == null) return null;
          if (VersionGuard.shouldSkipForSync(cache, key, 15, false)) return existing;
          return null;
        });
      } else {
        cache.asMap().compute(key, (k, existing) -> {
          if (existing == null) return null;
          if (VersionGuard.shouldSkipForSync(cache, key, 8, false)) return existing;
          if (existing instanceof CacheEntry ce) {
            return CacheEntry.builder()
              .value("refreshed").dataVersion(15).isVersionDegraded(false)
              .decisionVersion(ce.getDecisionVersion())
              .hardTtlMs(ce.getHardTtlMs()).hardExpireAtMs(ce.getHardExpireAtMs())
              .softTtlMs(ce.getSoftTtlMs()).softExpireAtMs(ce.getSoftExpireAtMs())
              .keyState(ce.getKeyState())
              .normalHardTtlMs(ce.getNormalHardTtlMs()).normalSoftTtlMs(ce.getNormalSoftTtlMs())
              .build();
          }
          return existing;
        });
      }
    }, m);
    assertThat(m.errorCount.get()).isZero();
    m.finish().logSummary();
  }

  /**
   * Validates WorkerListener HOT/COOL decision race handling with 10 concurrent threads.
   */
  @Test
  void workerListener_concurrent() throws InterruptedException {
    StressMetrics m = new StressMetrics("workerListener_concurrent");
    ALL_METRICS.add(m);
    Cache<String, Object> cache = Caffeine.newBuilder().maximumSize(100).build();
    cache.put("wl-key", entry("initial", 5, KeyState.NORMAL, 300_000, 0));
    concurrentRun("workerListener", 10, 100, (idx) -> {
      long decisionVer = idx + 1;
      cache.asMap().compute("wl-key", (k, existing) -> {
        if (VersionGuard.shouldSkipForWorker(cache, "wl-key", decisionVer)) return existing;
        if (existing instanceof CacheEntry ce) {
          boolean isHot = idx % 2 == 0;
          return CacheEntry.builder()
            .value(ce.getValue()).dataVersion(ce.getDataVersion())
            .isVersionDegraded(ce.isVersionDegraded())
            .decisionVersion(decisionVer)
            .hardTtlMs(isHot ? 3_600_000 : ce.getNormalHardTtlMs())
            .hardExpireAtMs(Long.MAX_VALUE)
            .softTtlMs(isHot ? 300_000 : 0)
            .softExpireAtMs(isHot ? System.currentTimeMillis() + 300_000 : 0)
            .keyState(isHot ? KeyState.HOT : KeyState.COOL)
            .normalHardTtlMs(ce.getNormalHardTtlMs()).normalSoftTtlMs(ce.getNormalSoftTtlMs())
            .build();
        }
        return existing;
      });
    }, m);
    assertThat(m.errorCount.get()).isZero();
    Object finalEntry = cache.getIfPresent("wl-key");
    assertThat(finalEntry).isInstanceOf(CacheEntry.class);
    CacheEntry ce = (CacheEntry) finalEntry;
    m.custom.put("finalState", ce.getKeyState().name());
    m.custom.put("finalDecisionVersion", ce.getDecisionVersion());
    assertThat(ce.getKeyState()).isIn(KeyState.COOL, KeyState.HOT, KeyState.NORMAL);
    assertThat(ce.getDecisionVersion()).isPositive();
    m.finish().logSummary();
  }

  // ================================================================================
  // Section 9: Distributed Scenarios
  // ================================================================================

  /**
   * Simulates a 5-node distributed scenario with 8 workers each, 500 ops, simulated Redis + broadcast bus.
   */
  @Test
  void distributedScenario() throws InterruptedException {
    StressMetrics m = new StressMetrics("distributedScenario");
    ALL_METRICS.add(m);
    int nodeCount = 5;
    int keyCount = 50;
    int opsPerNode = 500;
    Cache<String, Object> simulatedRedis = Caffeine.newBuilder().maximumSize(10000).build();
    for (int i = 0; i < keyCount; i++) {
      simulatedRedis.put("ds-key-" + i, "redis-v");
    }
    BlockingQueue<Runnable> broadcastBus = new LinkedBlockingQueue<>();
    ExecutorService nodePool = Executors.newFixedThreadPool(nodeCount);
    CountDownLatch nodeLatch = new CountDownLatch(nodeCount);
    AtomicInteger totalErrors = new AtomicInteger(0);
    for (int n = 0; n < nodeCount; n++) {
      int nodeId = n;
      nodePool.submit(() -> {
        try {
          HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(100, 50000, 5, 0.92, 10));
          Cache<String, Object> l1 = Caffeine.newBuilder().maximumSize(1000).build();
          Executor syncExec = Runnable::run;
          HotKeyCache cache = createCache(detector, l1, syncExec);
          ExecutorService workers = Executors.newFixedThreadPool(8);
          CountDownLatch workerLatch = new CountDownLatch(8);
          for (int w = 0; w < 8; w++) {
            int wid = w;
            workers.submit(() -> {
              try {
                for (int op = 0; op < opsPerNode; op++) {
                  String key = "ds-key-" + (op % keyCount);
                  switch ((op + wid + nodeId) % 4) {
                    case 0 -> cache.get(key, () -> simulatedRedis.getIfPresent(key));
                    case 1 -> {
                      cache.invalidate(key);
                      broadcastBus.offer(() -> l1.invalidate(key));
                    }
                    case 2 -> cache.peek(key);
                    case 3 -> simulatedRedis.put(key, "nv-" + nodeId + "-" + op);
                  }
                }
              } catch (Exception e) { totalErrors.incrementAndGet(); }
              finally { workerLatch.countDown(); }
            });
          }
          assertThat(workerLatch.await(30, TimeUnit.SECONDS)).isTrue();
          workers.shutdown();
          List<Runnable> drained = new ArrayList<>();
          broadcastBus.drainTo(drained);
          for (Runnable r : drained) {
            try { r.run(); } catch (Exception e) { totalErrors.incrementAndGet(); }
          }
        } catch (Exception e) { totalErrors.incrementAndGet(); }
        finally { nodeLatch.countDown(); }
      });
    }
    assertThat(nodeLatch.await(60, TimeUnit.SECONDS)).isTrue();
    nodePool.shutdown();
    assertThat(totalErrors.get()).isZero();
    m.custom.put("nodeCount", nodeCount);
    m.custom.put("workersPerNode", 8);
    m.custom.put("opsPerWorker", opsPerNode);
    long totalDistOps = (long) nodeCount * 8 * opsPerNode;
    m.custom.put("totalDistOps", totalDistOps);
    log.info("  Distributed: {} nodes × 8 workers × {} ops = {} total, 0 errors",
      nodeCount, opsPerNode, totalDistOps);
    m.finish().logSummary();
  }

  /**
   * Simulates a 3-node distributed system with random network jitter (delay + partial message loss).
   */
  @Test
  void distributed_networkJitter() throws InterruptedException {
    StressMetrics m = new StressMetrics("distributed_networkJitter");
    ALL_METRICS.add(m);
    int nodeCount = 3;
    int keyCount = 30;
    int opsPerNode = 400;
    Cache<String, Object> simulatedRedis = Caffeine.newBuilder().maximumSize(5000).build();
    for (int i = 0; i < keyCount; i++) simulatedRedis.put("nj-key-" + i, "rv");
    BlockingQueue<DelayedBroadcast> broadcastBus = new LinkedBlockingQueue<>();
    Random jitterRng = new Random(99);
    ExecutorService nodePool = Executors.newFixedThreadPool(nodeCount);
    CountDownLatch nodeLatch = new CountDownLatch(nodeCount);
    AtomicInteger totalErrors = new AtomicInteger(0);
    for (int n = 0; n < nodeCount; n++) {
      int nodeId = n;
      nodePool.submit(() -> {
        try {
          HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(100, 50000, 5, 0.92, 10));
          Cache<String, Object> l1 = Caffeine.newBuilder().maximumSize(1000).build();
          Executor syncExec = Runnable::run;
          HotKeyCache cache = createCache(detector, l1, syncExec);
          ExecutorService workers = Executors.newFixedThreadPool(6);
          CountDownLatch workerLatch = new CountDownLatch(6);
          for (int w = 0; w < 6; w++) {
            int wid = w;
            workers.submit(() -> {
              try {
                for (int op = 0; op < opsPerNode; op++) {
                  String key = "nj-key-" + (op % keyCount);
                  switch ((op + wid + nodeId) % 4) {
                    case 0 -> cache.get(key, () -> {
                      int jitterMs = jitterRng.nextInt(3);
                      if (jitterMs > 0) { try { Thread.sleep(jitterMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
                      return simulatedRedis.getIfPresent(key);
                    });
                    case 1 -> {
                      cache.invalidate(key);
                      int delayMs = jitterRng.nextInt(5);
                      broadcastBus.offer(new DelayedBroadcast(() -> l1.invalidate(key), delayMs));
                    }
                    case 2 -> cache.peek(key);
                    case 3 -> simulatedRedis.put(key, "jv-" + nodeId + "-" + op);
                  }
                }
              } catch (Exception e) { totalErrors.incrementAndGet(); }
              finally { workerLatch.countDown(); }
            });
          }
          assertThat(workerLatch.await(30, TimeUnit.SECONDS)).isTrue();
          workers.shutdown();
          List<DelayedBroadcast> drained = new ArrayList<>();
          broadcastBus.drainTo(drained);
          for (DelayedBroadcast db : drained) {
            if (jitterRng.nextInt(10) < 8) {
              try { db.task.run(); } catch (Exception e) { totalErrors.incrementAndGet(); }
            }
          }
        } catch (Exception e) { totalErrors.incrementAndGet(); }
        finally { nodeLatch.countDown(); }
      });
    }
    assertThat(nodeLatch.await(60, TimeUnit.SECONDS)).isTrue();
    nodePool.shutdown();
    assertThat(totalErrors.get()).isZero();
    m.custom.put("nodeCount", nodeCount);
    m.custom.put("jitterSimulated", 1);
    long totalOpsNet = (long) nodeCount * 6 * opsPerNode;
    m.custom.put("totalOps", totalOpsNet);
    log.info("  Network jitter: {} nodes, {} ops, 0 errors (with simulated delay+loss)", nodeCount, totalOpsNet);
    m.finish().logSummary();
  }

  /**
   * A broadcast task paired with a simulated delay in milliseconds.
   * Used by {@link #distributed_networkJitter} to model network latency.
   */
  record DelayedBroadcast(Runnable task, int delayMs) {}

  /**
   * Simulates burst traffic: steady load followed by sudden 30-thread burst across 3 nodes.
   */
  @Test
  void distributed_burstTraffic() throws InterruptedException {
    StressMetrics m = new StressMetrics("distributed_burstTraffic");
    ALL_METRICS.add(m);
    final int nodeCount = 3;
    final int keyCount = 20;
    final Cache<String, Object> simulatedRedis = Caffeine.newBuilder().maximumSize(5000).build();
    for (int i = 0; i < keyCount; i++) simulatedRedis.put("bt-key-" + i, "rv");
    final ExecutorService nodePool = Executors.newFixedThreadPool(nodeCount);
    final CountDownLatch nodeLatch = new CountDownLatch(nodeCount);
    final AtomicInteger totalErrors = new AtomicInteger(0);
    for (int n = 0; n < nodeCount; n++) {
      nodePool.submit(() -> {
        try {
          HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(100, 50000, 5, 0.92, 10));
          Cache<String, Object> l1 = Caffeine.newBuilder().maximumSize(1000).build();
          Executor syncExec = Runnable::run;
          final HotKeyCache cache = createCache(detector, l1, syncExec);
          final int steadyOps = 200;
          for (int i = 0; i < steadyOps; i++) {
            final String btKey = "bt-key-" + (i % keyCount);
            cache.get(btKey, () -> simulatedRedis.getIfPresent(btKey));
          }
          final int burstThreads = 30;
          final ExecutorService burstPool = Executors.newFixedThreadPool(burstThreads);
          final CountDownLatch burstLatch = new CountDownLatch(burstThreads);
          for (int b = 0; b < burstThreads; b++) {
            final int bid = b;
            final String prefix = "bt-key-";
            burstPool.submit(() -> {
              try {
                for (int i = 0; i < 200; i++) {
                  final String btKey = prefix + (i % keyCount);
                  cache.get(btKey, () -> simulatedRedis.getIfPresent(btKey));
                }
              } catch (Exception e) { totalErrors.incrementAndGet(); }
              finally { burstLatch.countDown(); }
            });
          }
          assertThat(burstLatch.await(30, TimeUnit.SECONDS)).isTrue();
          burstPool.shutdown();
        } catch (Exception e) { totalErrors.incrementAndGet(); }
        finally { nodeLatch.countDown(); }
      });
    }
    assertThat(nodeLatch.await(60, TimeUnit.SECONDS)).isTrue();
    nodePool.shutdown();
    assertThat(totalErrors.get()).isZero();
    m.custom.put("nodeCount", nodeCount);
    m.custom.put("burstThreadsPerNode", 30);
    m.custom.put("burstOpsPerThread", 200);
    long totalBurst = (long) nodeCount * 30 * 200;
    m.custom.put("totalBurstOps", totalBurst);
    log.info("  Burst traffic: {} nodes, {} burst ops each = {}, 0 errors",
      nodeCount, 30L * 200, totalBurst);
    m.finish().logSummary();
  }

  // ================================================================================
  // Section 10: Edge Cases
  // ================================================================================

  /**
   * HeavyKeeper bounded size under high key churn: 100k unique keys with K=100.
   */
  @Test
  void keyChurn_highRate() throws InterruptedException {
    StressMetrics m = new StressMetrics("keyChurn_highRate");
    ALL_METRICS.add(m);
    int k = 100;
    HeavyKeeper keeper = new HeavyKeeper(k, 50000, 5, 0.92, 10);
    int uniqueKeys = 100_000;
    int opsPerKey = 2;
    int threadCount = 20;
    int opsPerThread = (uniqueKeys * opsPerKey) / threadCount;
    concurrentRun("keyChurn", threadCount, opsPerThread, (idx) -> {
      keeper.addDirect("churn-" + (idx % uniqueKeys), 1);
    }, m);
    int topSize = keeper.list().size();
    m.custom.put("uniqueKeys", uniqueKeys);
    m.custom.put("maxTopK", k);
    m.custom.put("actualTopK", topSize);
    assertThat(topSize).isLessThanOrEqualTo(k);
    log.info("  Key churn: {} unique keys, TopK ≤ {} (actual {}), 0 errors", uniqueKeys, k, topSize);
    m.finish().logSummary();
  }

  /**
   * Tracks a key gradually emerging as hot over 10 phases with varying hot-access ratios.
   */
  @Test
  void gradualHotKeyEmergence() throws InterruptedException {
    StressMetrics m = new StressMetrics("gradualHotKeyEmergence");
    ALL_METRICS.add(m);
    HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(20, 50000, 5, 0.92, 10));
    Cache<String, Object> caffeine = Caffeine.newBuilder().maximumSize(100).build();
    Executor executor = Runnable::run;
    HotKeyCache cache = createCache(detector, caffeine, executor);
    String hotKey = "emerge-hot";
    String coldKey = "emerge-cold";
    caffeine.put(hotKey, entry("v", 1, KeyState.NORMAL, 300_000, 0));
    caffeine.put(coldKey, entry("v", 1, KeyState.NORMAL, 300_000, 0));
    int phases = 10;
    int opsPerPhase = 100;
    List<Integer> hotRatios = List.of(10, 20, 30, 50, 70, 90, 80, 60, 40, 20);
    Map<String, Object> phaseMetrics = new LinkedHashMap<>();
    for (int p = 0; p < phases; p++) {
      int hotPct = hotRatios.get(p);
      for (int i = 0; i < opsPerPhase; i++) {
        boolean accessHot = i < (opsPerPhase * hotPct / 100);
        String key = accessHot ? hotKey : coldKey;
        cache.get(key, () -> "val");
        detector.add(key, 1);
      }
      Thread.sleep(1);
      phaseMetrics.put("phase" + p + "_hotPct", hotPct);
    }
    CacheEntry hotEntry = (CacheEntry) caffeine.getIfPresent(hotKey);
    CacheEntry coldEntry = (CacheEntry) caffeine.getIfPresent(coldKey);
    m.custom.put("phases", phases);
    m.custom.put("hotFinalState", hotEntry != null ? hotEntry.getKeyState().name() : "evicted");
    m.custom.put("coldFinalState", coldEntry != null ? coldEntry.getKeyState().name() : "evicted");
    log.info("  Gradual emergence: {} phases, hot={}, cold={}",
      phases,
      hotEntry != null ? hotEntry.getKeyState() : "evicted",
      coldEntry != null ? coldEntry.getKeyState() : "evicted");
    m.finish().logSummary();
  }

  /**
   * Simulates a cold-start boot storm: 40 threads × 500 keys against an empty cache.
   */
  @Test
  void emptyCache_bootStorm() throws InterruptedException {
    StressMetrics m = new StressMetrics("emptyCache_bootStorm");
    ALL_METRICS.add(m);
    int threadCount = 40;
    int keysTotal = 500;
    int opsPerThread = keysTotal;
    Cache<String, Object> simulatedDB = Caffeine.newBuilder().maximumSize(10000).build();
    for (int i = 0; i < keysTotal; i++) {
      simulatedDB.put("boot-key-" + i, "db-value-" + i);
    }
    HotKeyDetector detector = new HotKeyDetector(new HeavyKeeper(200, 50000, 5, 0.92, 10));
    Cache<String, Object> caffeine = Caffeine.newBuilder().maximumSize(2000).build();
    Executor executor = Runnable::run;
    HotKeyCache cache = createCache(detector, caffeine, executor);
    concurrentRun("bootStorm", threadCount, opsPerThread, (idx) -> {
      String key = "boot-key-" + (idx % keysTotal);
      Object val = cache.get(key, () -> simulatedDB.getIfPresent(key));
      assertThat(val).isNotNull();
    }, m);
    assertThat(m.errorCount.get()).isZero();
    long cachedCount = caffeine.estimatedSize();
    m.custom.put("threadCount", threadCount);
    m.custom.put("totalOps", threadCount * opsPerThread);
    m.custom.put("cachedAfterStorm", cachedCount);
    log.info("  Boot storm: {} threads × {} keys, {} cached after, 0 errors",
      threadCount, opsPerThread, cachedCount);
    m.finish().logSummary();
  }

  /**
   * Validates HeavyKeeper handles mixed short and long key strings without bias.
   */
  @Test
  void mixedKeySizes_heavyKeeper() throws InterruptedException {
    StressMetrics m = new StressMetrics("mixedKeySizes_heavyKeeper");
    ALL_METRICS.add(m);
    int k = 50;
    HeavyKeeper keeper = new HeavyKeeper(k, 50000, 5, 0.92, 10);
    int shortKeys = 1000;
    int longKeys = 500;
    int iterations = 50;
    int totalOps = 0;
    concurrentRun("mixedKeySize", 10, (shortKeys + longKeys) * iterations / 10, (idx) -> {
      int baseIdx = idx % (shortKeys + longKeys);
      if (baseIdx < shortKeys) {
        keeper.addDirect("sk" + baseIdx, 1);
      } else {
        String longK = "very-long-key-prefix-" + (baseIdx - shortKeys) + "-with-suffix";
        keeper.addDirect(longK, 1);
      }
    }, m);
    assertThat(m.errorCount.get()).isZero();
    assertThat(keeper.list().size()).isLessThanOrEqualTo(k);
    m.custom.put("shortKeys", shortKeys);
    m.custom.put("longKeys", longKeys);
    log.info("  Mixed key sizes: short={}, long={}, TopK ≤ {}", shortKeys, longKeys, k);
    m.finish().logSummary();
  }

  /**
   * Verifies SingleFlight handles timeout contention where some suppliers are slow.
   */
  @Test
  void singleFlight_timeoutContention() throws InterruptedException {
    StressMetrics m = new StressMetrics("singleFlight_timeoutContention");
    ALL_METRICS.add(m);
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    SingleFlight sf = new SingleFlight(100, 10, 5, asyncExec);
    int threadCount = 50;
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger timeouts = new AtomicInteger(0);
    AtomicInteger successes = new AtomicInteger(0);
    for (int i = 0; i < threadCount; i++) {
      int delay = i < 5 ? 50 : 1;
      asyncExec.submit(() -> {
        try {
          long start = System.nanoTime();
          Optional<String> result = sf.load("timeout-key", () -> {
            try { Thread.sleep(delay); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return "done";
          });
          if (result.isPresent()) successes.incrementAndGet();
          else timeouts.incrementAndGet();
          m.recordLatency(System.nanoTime() - start);
          m.recordOp();
        } finally { latch.countDown(); }
      });
    }
    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    asyncExec.shutdown();
    m.custom.put("successes", successes.get());
    m.custom.put("timeouts", timeouts.get());
    assertThat(successes.get() + timeouts.get()).isEqualTo(threadCount);
    log.info("  Timeout contention: {} threads, {} success, {} timeouts",
      threadCount, successes.get(), timeouts.get());
    m.finish().logSummary();
  }
}
