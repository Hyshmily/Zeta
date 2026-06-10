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
package io.github.hyshmily.hotkey.integration.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Benchmark for distributed cache operations across simulated nodes.
 *
 * <p>Measures throughput and latency for hot-read, cold-read, mixed-read/write/invalidate
 * phases under multi-threaded concurrency with real Redis + RabbitMQ infrastructure.
 * Outputs a JSON report with latency percentiles and L1/L2 hit rates.
 */
@Testcontainers
@Tag("docker")
@Tag("benchmark")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DistributedBenchmarkIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(DistributedBenchmarkIT.class);

  // --- Containers ---

  /** Redis container for L2 cache and version tracking. */
  @Container
  static GenericContainer<?> redis = new GenericContainer<>(
      DockerImageName.parse("redis:7-alpine"))
    .withExposedPorts(6379);

  /** RabbitMQ container for broadcast sync and cross-instance messaging. */
  @Container
  static GenericContainer<?> rabbitmq = new GenericContainer<>(
      DockerImageName.parse("rabbitmq:4.1-management"))
    .withExposedPorts(5672, 15672)
    .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1))
    .withStartupTimeout(Duration.ofMinutes(2));

  /**
   * Overrides Spring properties to point Redis and RabbitMQ to Testcontainers endpoints.
   *
   * @param r the dynamic property registry
   */
  @DynamicPropertySource
  static void overrideProps(DynamicPropertyRegistry r) {
    r.add("spring.data.redis.host", redis::getHost);
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    r.add("spring.rabbitmq.host", rabbitmq::getHost);
    r.add("spring.rabbitmq.port", () -> rabbitmq.getMappedPort(5672));
    r.add("spring.rabbitmq.username", () -> "guest");
    r.add("spring.rabbitmq.password", () -> "guest");
  }

  // --- Injection ---

  @Autowired
  HotKey hotKey;

  @Autowired
  StringRedisTemplate redisTemplate;

  @Autowired
  RabbitTemplate rabbitTemplate;

  // --- Benchmark config ---

  /** Number of keys pre-warmed and treated as hot in read phases. */
  static final int HOT_KEY_COUNT = 10_000;
  /** Number of keys used as cold (never locally cached) in cold-read phases. */
  static final int COLD_KEY_COUNT = 40_000;
  /** Number of concurrent threads for all benchmark phases. */
  static final int THREAD_COUNT = 8;
  /** Operations executed per thread per phase. */
  static final int OPS_PER_THREAD = 2_500;
  /** Hard TTL in milliseconds applied to all cached entries. */
  static final int HARD_TTL = 600_000;
  /** Soft TTL in milliseconds for logical expiry. */
  static final int SOFT_TTL = 300_000;

  // --- Main test ---

  /** Runs warmup, hot-read, cold-read, mixed, and post-sync read phases and writes a JSON report. */
  @Test
  void fullChainBenchmark() throws Exception {
    List<Map<String, Object>> phases = new ArrayList<>();

    phases.add(warmup());
    phases.add(readPhase("hot-read-only", 0, HOT_KEY_COUNT));
    phases.add(readPhase("cold-read-only", HOT_KEY_COUNT, HOT_KEY_COUNT + COLD_KEY_COUNT));
    phases.add(mixedPhase());
    phases.add(readPhase("hot-read-after-sync", 0, HOT_KEY_COUNT));

    // aggregate
    long totalOps = phases.stream().filter(p -> !p.get("name").equals("warmup"))
        .mapToLong(p -> ((Number) p.get("totalOps")).longValue()).sum();
    double totalDuration = phases.stream().filter(p -> !p.get("name").equals("warmup"))
        .mapToDouble(p -> ((Number) p.get("durationMs")).doubleValue()).sum();

    Map<String, Object> report = new HashMap<>();
    report.put("testName", "HotKey Distributed Benchmark");
    report.put("timestamp", Instant.now().toString());
    report.put("config", Map.of(
        "hotKeyCount", HOT_KEY_COUNT,
        "coldKeyCount", COLD_KEY_COUNT,
        "threadCount", THREAD_COUNT,
        "opsPerThread", OPS_PER_THREAD,
        "hardTtlMs", HARD_TTL,
        "softTtlMs", SOFT_TTL
    ));
    report.put("phases", phases);
    report.put("summary", Map.of(
        "totalOps", totalOps,
        "totalDurationMs", Math.round(totalDuration * 100.0) / 100.0,
        "overallThroughputOpsPerSec", Math.round(totalOps / (totalDuration / 1000.0))
    ));

    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    String json = mapper.writeValueAsString(report);

    Path reportPath = Path.of("src", "test", "resources", "testresult",
        "benchmark-distributed-" + Instant.now().toString().replace(":", "-") + ".json");
    Files.createDirectories(reportPath.getParent());
    Files.writeString(reportPath, json);

    log.info("\n=========================================\nBenchmark report -> {}\n{}",
        reportPath.toAbsolutePath(), json);

    assertThat(phases.stream().mapToLong(p -> ((Number) p.get("errors")).longValue()).sum()).isZero();
  }

  /** Seeds all hot and cold keys into Redis and L1. */
  private Map<String, Object> warmup() throws Exception {
    log.info("Warmup: putting {} keys into Redis + L1 ...", HOT_KEY_COUNT + COLD_KEY_COUNT);
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);

    runConcurrent(THREAD_COUNT, HOT_KEY_COUNT + COLD_KEY_COUNT, (tid, i, key, value) -> {
      hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    }, errors);

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    int total = HOT_KEY_COUNT + COLD_KEY_COUNT;
    log.info("Warmup done: {} keys in {} ms ({} ops/sec), errors={}",
        total, String.format("%.1f", ms),
        String.format("%.0f", total / (ms / 1000.0)), errors.get());

    return Map.of(
        "name", "warmup",
        "totalOps", total,
        "durationMs", ms,
        "throughputOpsPerSec", Math.round(total / (ms / 1000.0)),
        "errors", errors.get()
    );
  }

  /** Runs a concurrent read-only phase for keys in [from, to) range and returns metrics. */
  private Map<String, Object> readPhase(String name, int from, int to) throws Exception {
    log.info("Phase [{}]: keys [{},{}) ...", name, from, to);
    int range = to - from;
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    AtomicLong l2Calls = new AtomicLong(0);
    List<List<Long>> threadLatencies = new ArrayList<>();
    for (int t = 0; t < THREAD_COUNT; t++) {
      threadLatencies.add(new ArrayList<>());
    }

    runConcurrentWithLatency(THREAD_COUNT, OPS_PER_THREAD, range, from,
        (tid, i, key, value) -> {
          long ns = PerfSupport.measure(() ->
              hotKey.get(key, () -> {
                l2Calls.incrementAndGet();
                return redisTemplate.opsForValue().get(key);
              }, HARD_TTL, SOFT_TTL)
          );
          threadLatencies.get(tid).add(ns);
        }, errors, threadLatencies);

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    long totalOps = (long) THREAD_COUNT * OPS_PER_THREAD;
    long l1Hits = totalOps - l2Calls.get();

    // flatten latencies
    double[] latMs = threadLatencies.stream()
        .flatMapToLong(list -> list.stream().mapToLong(Long::longValue))
        .mapToDouble(ns -> ns / 1_000_000.0)
        .toArray();
    Arrays.sort(latMs);

    Map<String, Object> phase = buildPhase(name, totalOps, ms, l1Hits, l2Calls.get(), errors.get(), latMs);
    logPhase(name, totalOps, ms, l1Hits, l2Calls.get(), errors.get(), latMs);
    return phase;
  }

  /** Runs a mixed workload (80% read, 10% write, 10% invalidate) concurrently. */
  private Map<String, Object> mixedPhase() throws Exception {
    log.info("Phase [mixed-with-sync]: 80% read + 10% write + 10% invalidate ...");
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    AtomicLong l2Calls = new AtomicLong(0);
    AtomicLong writes = new AtomicLong(0);
    AtomicLong invals = new AtomicLong(0);
    List<List<Long>> threadLatencies = new ArrayList<>();
    for (int t = 0; t < THREAD_COUNT; t++) {
      threadLatencies.add(new ArrayList<>());
    }

    CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
    ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

    for (int t = 0; t < THREAD_COUNT; t++) {
      int tid = t;
      pool.submit(() -> {
        List<Long> myLats = threadLatencies.get(tid);
        try {
          for (int i = 0; i < OPS_PER_THREAD; i++) {
            int op = i % 10;
            long ns;
            if (op < 8) {
              int idx = (tid * OPS_PER_THREAD + i) % (HOT_KEY_COUNT + COLD_KEY_COUNT);
              String key = keyFor(idx);
              ns = PerfSupport.measure(() ->
                  hotKey.get(key, () -> {
                    l2Calls.incrementAndGet();
                    return redisTemplate.opsForValue().get(key);
                  }, HARD_TTL, SOFT_TTL)
              );
            } else if (op < 9) {
              String k = "benchmark:mixed:w:" + UUID.randomUUID();
              String v = "w-" + tid + "-" + i;
              ns = PerfSupport.measure(() ->
                  hotKey.putThrough(k, v, () -> redisTemplate.opsForValue().set(k, v))
              );
              writes.incrementAndGet();
            } else {
              int idx = (tid * OPS_PER_THREAD + i) % (HOT_KEY_COUNT + COLD_KEY_COUNT);
              String key = keyFor(idx);
              MessageProperties props = new MessageProperties();
              props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
              props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
              props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
              Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
              ns = PerfSupport.measure(() ->
                  rabbitTemplate.send("hotkey.sync.exchange", "", msg)
              );
              invals.incrementAndGet();
            }
            myLats.add(ns);
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Mixed phase error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    long totalOps = (long) THREAD_COUNT * OPS_PER_THREAD;
    long reads = totalOps - writes.get() - invals.get();
    long l1Hits = reads - l2Calls.get();

    double[] latMs = threadLatencies.stream()
        .flatMapToLong(list -> list.stream().mapToLong(Long::longValue))
        .mapToDouble(ns -> ns / 1_000_000.0)
        .toArray();
    Arrays.sort(latMs);

    Map<String, Object> phase = buildPhase("mixed-with-sync", totalOps, ms, l1Hits, l2Calls.get(), errors.get(), latMs);
    phase.put("reads", reads);
    phase.put("writes", writes.get());
    phase.put("invalidations", invals.get());

    log.info("Phase [mixed-with-sync] done: {} ops ({}r+{}w+{}i) in {} ms, L1={}, L2={}, errors={}",
        totalOps, reads, writes.get(), invals.get(),
        String.format("%.1f", ms), l1Hits, l2Calls.get(), errors.get());
    return phase;
  }

  /**
   * Generates a deterministic key name from an integer index.
   *
   * @param idx the key index
   * @return the key string in the format "benchmark:key:{idx}"
   */
  private static String keyFor(int idx) {
    return "benchmark:key:" + idx;
  }

  /**
   * Computes the p-th percentile from a sorted array of latencies.
   *
   * @param sorted sorted latency values (nanoseconds, ascending)
   * @param p      the percentile to compute (e.g. 50, 90, 99)
   * @return the p-th percentile value
   */
  private static double percentile(double[] sorted, double p) {
    if (sorted.length == 0) return 0;
    double rank = p / 100.0 * (sorted.length - 1);
    int lo = (int) Math.floor(rank);
    int hi = (int) Math.ceil(rank);
    if (lo == hi || hi >= sorted.length) return sorted[lo];
    double frac = rank - lo;
    return sorted[lo] * (1 - frac) + sorted[hi] * frac;
  }

  /**
   * Builds a phase result map with latency percentiles, L1/L2 hit rates, and throughput.
   *
   * @param name    the phase name
   * @param totalOps total operations performed
   * @param ms      elapsed time in milliseconds
   * @param l1Hits  number of L1 cache hits
   * @param l2Calls number of L2 (Redis) calls
   * @param errors  count of errors encountered
   * @param latMs   sorted latency values in milliseconds
   * @return a phase result map suitable for JSON serialization
   */
  private static Map<String, Object> buildPhase(
      String name, long totalOps, double ms, long l1Hits, long l2Calls,
      long errors, double[] latMs) {
    double p50 = percentile(latMs, 50);
    double p90 = percentile(latMs, 90);
    double p99 = percentile(latMs, 99);
    double p999 = percentile(latMs, 99.9);
    double max = latMs.length > 0 ? latMs[latMs.length - 1] : 0;
    double mean = latMs.length > 0 ? Arrays.stream(latMs).average().orElse(0) : 0;

    Map<String, Object> phase = new HashMap<>();
    phase.put("name", name);
    phase.put("totalOps", totalOps);
    phase.put("durationMs", Math.round(ms * 100.0) / 100.0);
    phase.put("throughputOpsPerSec", Math.round(totalOps / (ms / 1000.0)));
    phase.put("l1Hits", l1Hits);
    phase.put("l1HitRate", Math.round((double) l1Hits / totalOps * 10000.0) / 10000.0);
    phase.put("l2Calls", l2Calls);
    phase.put("errors", errors);
    phase.put("latencyMs", Map.of(
        "p50", round3(p50),
        "p90", round3(p90),
        "p99", round3(p99),
        "p999", round3(p999),
        "max", round3(max),
        "mean", round3(mean)
    ));
    return phase;
  }

  /**
   * Logs a phase summary line with duration, ops, L1/L2 counts, and p50/p99 latency.
   *
   * @param name    the phase name
   * @param totalOps total operations
   * @param ms      elapsed time in milliseconds
   * @param l1Hits  L1 cache hit count
   * @param l2Calls L2 (Redis) call count
   * @param errors  error count
   * @param latMs   sorted latency values in milliseconds
   */
  private static void logPhase(
      String name, long totalOps, double ms, long l1Hits, long l2Calls,
      long errors, double[] latMs) {
    double p50 = percentile(latMs, 50);
    double p99 = percentile(latMs, 99);
    log.info("Phase [{}] done: {} ops in {} ms, L1={}, L2={}, p50={} ms, p99={} ms, errors={}",
        name, totalOps, String.format("%.1f", ms), l1Hits, l2Calls,
        String.format("%.3f", p50), String.format("%.3f", p99), errors);
  }

  /**
   * Rounds a double to 3 decimal places.
   *
   * @param v the value to round
   * @return the value rounded to 3 decimal places
   */
  private static double round3(double v) {
    return Math.round(v * 1000.0) / 1000.0;
  }

  /**
   * A concurrent cache operation accepting thread ID, iteration index, key, and value.
   */
  @FunctionalInterface
  interface Op {
    void run(int tid, int i, String key, String value) throws Throwable;
  }

  /**
   * Runs a concurrent key-based workload across a fixed thread pool.
   *
   * @param threads  number of concurrent threads
   * @param keyCount total number of keys to process
   * @param op       the operation to execute per key
   * @param errors   accumulator for error count
   */
  private static void runConcurrent(int threads, int keyCount, Op op, AtomicInteger errors)
      throws Exception {
    int keysPerThread = keyCount / threads;
    CountDownLatch latch = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);

    for (int t = 0; t < threads; t++) {
      int tid = t;
      int from = tid * keysPerThread;
      int to = (tid == threads - 1) ? keyCount : (tid + 1) * keysPerThread;
      pool.submit(() -> {
        try {
          for (int i = from; i < to; i++) {
            String key = keyFor(i);
            op.run(tid, i, key, "v-" + i);
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.warn("Concurrent error at thread {}: {}", tid, ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
  }

  /**
   * Runs a concurrent workload with per-operation latency measurement.
   *
   * @param threads         number of concurrent threads
   * @param opsPerThread    operations per thread
   * @param keyRange        number of distinct keys
   * @param keyOffset       starting key index offset
   * @param op              the operation to execute
   * @param errors          accumulator for error count
   * @param threadLatencies per-thread list of measured latencies (nanoseconds)
   */
  private static void runConcurrentWithLatency(
      int threads, int opsPerThread, int keyRange, int keyOffset,
      Op op, AtomicInteger errors, List<List<Long>> threadLatencies) throws Exception {
    CountDownLatch latch = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);

    for (int t = 0; t < threads; t++) {
      int tid = t;
      List<Long> myLats = threadLatencies.get(tid);
      pool.submit(() -> {
        try {
          for (int i = 0; i < opsPerThread; i++) {
            int idx = (tid * opsPerThread + i) % keyRange + keyOffset;
            String key = keyFor(idx);
            int iter = i;
            long ns = PerfSupport.measure(() ->
                op.run(tid, iter, key, null)
            );
            myLats.add(ns);
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Latency phase error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
  }

  // --- Micro-benchmark helper ---

  /**
   * A runnable that may throw checked exceptions.
   */
  @FunctionalInterface
  interface CheckedRunnable {
    void run() throws Throwable;
  }

  /**
   * Micro-benchmark utility for measuring execution time in nanoseconds.
   */
  static class PerfSupport {
    /**
     * Executes a runnable and returns the elapsed time in nanoseconds.
     *
     * @param r the runnable to measure
     * @return elapsed time in nanoseconds
     */
    static long measure(CheckedRunnable r) {
      long t0 = System.nanoTime();
      try {
        r.run();
      } catch (RuntimeException e) {
        throw e;
      } catch (Error e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
      return System.nanoTime() - t0;
    }
  }
}
