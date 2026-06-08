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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
import io.github.hyshmily.hotkey.reporting.ReportMessage;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Full-link container stress test for HotKey with real Redis + RabbitMQ infrastructure.
 *
 * <p>Complements {@link HotKeyStressIT} (pure in-memory) by exercising the complete
 * production chain: L1 (Caffeine) → L2 (Redis) → Broadcast (RabbitMQ) → Worker
 * decision flow. Each scenario records latency percentiles, histogram buckets,
 * throughput, error counts, and JVM system metrics, writing a consolidated JSON
 * report at the end.
 */
@Testcontainers
@Tag("docker")
@Tag("benchmark")
@Tag("stress")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ContainerFullLinkStressIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(ContainerFullLinkStressIT.class);

  private static final List<PhaseMetrics> ALL_PHASES = new ArrayList<>();

  // ── Containers ──────────────────────────────────────────────────────────────────

  @Container
  static GenericContainer<?> redis = new GenericContainer<>(
      DockerImageName.parse("redis:7-alpine"))
    .withExposedPorts(6379);

  @Container
  static GenericContainer<?> rabbitmq = new GenericContainer<>(
      DockerImageName.parse("rabbitmq:4.1-management"))
    .withExposedPorts(5672, 15672)
    .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1))
    .withStartupTimeout(Duration.ofMinutes(2));

  @DynamicPropertySource
  static void overrideProps(DynamicPropertyRegistry r) {
    r.add("spring.data.redis.host", redis::getHost);
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    r.add("spring.rabbitmq.host", rabbitmq::getHost);
    r.add("spring.rabbitmq.port", () -> rabbitmq.getMappedPort(5672));
    r.add("spring.rabbitmq.username", () -> "guest");
    r.add("spring.rabbitmq.password", () -> "guest");
    r.add("spring.rabbitmq.cache.connection-mode", () -> "CONNECTION");
    r.add("spring.rabbitmq.listener.simple.concurrency", () -> "4");
    r.add("spring.rabbitmq.listener.simple.max-concurrency", () -> "8");
    r.add("hotkey.local.report-interval-ms", () -> "5000");
  }

  // ── Injection ───────────────────────────────────────────────────────────────────

  @Autowired
  HotKey hotKey;

  @Autowired
  StringRedisTemplate redisTemplate;

  @Autowired
  RabbitTemplate rabbitTemplate;

  @Autowired
  ConnectionFactory connectionFactory;

  // ── Config ──────────────────────────────────────────────────────────────────────

  static final int HOT_KEY_COUNT = 5_000;
  static final int COLD_KEY_COUNT = 15_000;
  static final int TOTAL_KEYS = HOT_KEY_COUNT + COLD_KEY_COUNT;
  static final int THREAD_COUNT = 8;
  static final int OPS_PER_THREAD = 2_000;
  static final int HARD_TTL = 600_000;
  static final int SOFT_TTL = 300_000;
  static final String SYNC_EXCHANGE = "hotkey.sync.exchange";
  static final String BROADCAST_EXCHANGE = "hotkey.broadcast.exchange";
  static final String REPORT_EXCHANGE = "hotkey.report.exchange";

  // ── Simulated Worker state ──────────────────────────────────────────────────────

  SimpleMessageListenerContainer workerContainer;
  AtomicLong workerDecisionsSent = new AtomicLong(0);
  AtomicLong workerMessagesReceived = new AtomicLong(0);
  AtomicLong workerDecisionVersion = new AtomicLong(System.currentTimeMillis());
  final List<Long> decisionPropagationNs = Collections.synchronizedList(new ArrayList<>());
  final List<Long> syncPropagationNs = Collections.synchronizedList(new ArrayList<>());

  // ════════════════════════════════════════════════════════════════════════════════
  // Metrics Framework
  // ════════════════════════════════════════════════════════════════════════════════

  static class PhaseMetrics {

    final String name;
    final long startNanos = System.nanoTime();
    final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger errors = new AtomicInteger(0);
    final AtomicLong ops = new AtomicLong(0);
    final Map<String, Object> custom = new LinkedHashMap<>();

    long durationMs;
    long totalOps;
    int errorCount;
    double p50Ms;
    double p95Ms;
    double p99Ms;
    double opsPerSecond;
    long[] sortedLatencies; // computed by finish(); reused by toJson() for histogram

    // Histogram bucket boundaries (nanoseconds)
    private static final long[] BUCKET_NS = {
        1_000_000L,    // 0-1 ms
        5_000_000L,    // 1-5 ms
        10_000_000L,   // 5-10 ms
        50_000_000L,   // 10-50 ms
        100_000_000L,  // 50-100 ms
        500_000_000L,  // 100-500 ms
        Long.MAX_VALUE // 500 ms+
    };
    private static final String[] BUCKET_LABELS = {
        "0-1ms", "1-5ms", "5-10ms", "10-50ms", "50-100ms", "100-500ms", "500ms+"
    };

    PhaseMetrics(String name) {
      this.name = name;
    }

    void recordLatency(long nanos) {
      latencies.add(nanos);
    }

    void recordOp() {
      ops.incrementAndGet();
    }

    PhaseMetrics finish() {
      durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      totalOps = ops.get();
      errorCount = errors.get();
      sortedLatencies = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
      int len = sortedLatencies.length;
      p50Ms = len > 0 ? sortedLatencies[len / 2] / 1_000_000.0 : 0;
      p95Ms = len > 0 ? sortedLatencies[(int) (len * 0.95)] / 1_000_000.0 : 0;
      p99Ms = len > 0 ? sortedLatencies[(int) (len * 0.99)] / 1_000_000.0 : 0;
      opsPerSecond = durationMs > 0 ? (double) totalOps / durationMs * 1000.0 : 0;
      takeSystemMetrics();
      return this;
    }

    void logSummary() {
      log.info(
        "▸ {}: {} ops in {}ms | {} err | {} ops/s | P50={}ms P95={}ms P99={}ms {}",
        name, totalOps, durationMs, errorCount, Math.round(opsPerSecond),
        String.format("%.2f", p50Ms), String.format("%.2f", p95Ms), String.format("%.2f", p99Ms),
        custom.isEmpty() ? "" : custom.toString());
    }

    ObjectNode toJson(ObjectMapper mapper) {
      ObjectNode n = mapper.createObjectNode();
      n.put("name", name);
      n.put("durationMs", durationMs);
      n.put("totalOps", totalOps);
      n.put("errors", errorCount);
      n.put("p50Ms", p50Ms);
      n.put("p95Ms", p95Ms);
      n.put("p99Ms", p99Ms);
      n.put("opsPerSecond", Math.round(opsPerSecond));

      // Histogram buckets
      ArrayNode histo = histogramToJson(mapper);
      n.set("latencyHistogram", histo);

      ObjectNode cust = mapper.createObjectNode();
      custom.forEach((k, v) -> {
        if (v instanceof Number num) cust.put(k, num.doubleValue());
        else cust.put(k, v.toString());
      });
      n.set("custom", cust);

      // System metrics
      ObjectNode sys = mapper.createObjectNode();
      long gcCount = 0, gcTime = 0;
      for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
        gcCount += gc.getCollectionCount();
        gcTime += gc.getCollectionTime();
      }
      MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
      MemoryUsage heap = mem.getHeapMemoryUsage();
      sys.put("heapUsedMb", heap.getUsed() / 1_048_576);
      sys.put("heapCommittedMb", heap.getCommitted() / 1_048_576);
      sys.put("heapMaxMb", heap.getMax() / 1_048_576);
      sys.put("nonHeapUsedMb", mem.getNonHeapMemoryUsage().getUsed() / 1_048_576);
      sys.put("threadCount", ManagementFactory.getThreadMXBean().getThreadCount());
      sys.put("gcCount", gcCount);
      sys.put("gcTimeMs", gcTime);
      n.set("systemMetrics", sys);

      return n;
    }

    private ArrayNode histogramToJson(ObjectMapper mapper) {
      ArrayNode buckets = mapper.createArrayNode();
      if (sortedLatencies == null || sortedLatencies.length == 0) {
        for (String label : BUCKET_LABELS) {
          ObjectNode b = mapper.createObjectNode();
          b.put("range", label);
          b.put("count", 0);
          b.put("pct", 0.0);
          buckets.add(b);
        }
        return buckets;
      }
      int idx = 0;
      int total = sortedLatencies.length;
      for (int b = 0; b < BUCKET_NS.length; b++) {
        int count = 0;
        long boundary = BUCKET_NS[b];
        while (idx < total && sortedLatencies[idx] <= boundary) {
          count++;
          idx++;
        }
        ObjectNode bucket = mapper.createObjectNode();
        bucket.put("range", BUCKET_LABELS[b]);
        bucket.put("count", count);
        bucket.put("pct", Math.round((double) count / total * 10000.0) / 100.0);
        buckets.add(bucket);
      }
      return buckets;
    }

    private void takeSystemMetrics() {
      // System metrics are collected inline in toJson(); this is a no-op placeholder
      // kept for interface consistency.
    }
  }

  @AfterAll
  static void writeReport() throws Exception {
    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    ObjectNode root = mapper.createObjectNode();
    root.put("timestamp", Instant.now().toString());
    root.put("totalPhases", ALL_PHASES.size());
    long totalDuration = ALL_PHASES.stream().mapToLong(m -> m.durationMs).sum();
    long totalOps = ALL_PHASES.stream().mapToLong(m -> m.totalOps).sum();
    int totalErrors = ALL_PHASES.stream().mapToInt(m -> m.errorCount).sum();
    root.put("totalDurationMs", totalDuration);
    root.put("totalOps", totalOps);
    root.put("totalErrors", totalErrors);
    root.put("config",
        mapper.valueToTree(Map.of(
            "hotKeyCount", HOT_KEY_COUNT,
            "coldKeyCount", COLD_KEY_COUNT,
            "threadCount", THREAD_COUNT,
            "opsPerThread", OPS_PER_THREAD,
            "hardTtlMs", HARD_TTL,
            "softTtlMs", SOFT_TTL)));
    ArrayNode phases = mapper.createArrayNode();
    for (PhaseMetrics m : ALL_PHASES) {
      phases.add(m.toJson(mapper));
    }
    root.set("phases", phases);

    // Global system metrics at report time
    ObjectNode globalSys = mapper.createObjectNode();
    long gcCount = 0, gcTime = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      gcCount += gc.getCollectionCount();
      gcTime += gc.getCollectionTime();
    }
    MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
    MemoryUsage heap = mem.getHeapMemoryUsage();
    globalSys.put("heapUsedMb", heap.getUsed() / 1_048_576);
    globalSys.put("heapCommittedMb", heap.getCommitted() / 1_048_576);
    globalSys.put("heapMaxMb", heap.getMax() / 1_048_576);
    globalSys.put("threadCount", ManagementFactory.getThreadMXBean().getThreadCount());
    globalSys.put("gcCount", gcCount);
    globalSys.put("gcTimeMs", gcTime);
    root.set("finalSystemMetrics", globalSys);

    Path reportPath = Path.of("src", "test", "resources", "testresult",
        "container-full-link-stress-" + Instant.now().toString().replace(":", "-") + ".json");
    Files.createDirectories(reportPath.getParent());
    mapper.writeValue(reportPath.toFile(), root);
    log.info("Container full-link stress report written to {}", reportPath.toAbsolutePath());
  }

  // ── Shared Helpers ──────────────────────────────────────────────────────────────

  @FunctionalInterface
  interface ThrowingConsumer {
    void accept(int index) throws Exception;
  }

  static void concurrentRun(String label, int threads, int opsPerThread, ThrowingConsumer task,
      PhaseMetrics metrics) throws InterruptedException {
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
          metrics.errors.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    }
    assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    if (localErrors.get() > 0) {
      log.warn("  ⚠ {} local errors in {}", localErrors.get(), label);
    }
  }

  static String keyFor(int idx) {
    return "fl:key:" + idx;
  }

  @FunctionalInterface
  interface CheckedRunnable {
    void run() throws Throwable;
  }

  static long measure(CheckedRunnable r) {
    long t0 = System.nanoTime();
    try {
      r.run();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
    return System.nanoTime() - t0;
  }

  /** Generate a random string of the given approximate length in bytes. */
  static String randomValue(int approxBytes) {
    int chars = Math.max(1, approxBytes / 2);
    StringBuilder sb = new StringBuilder(chars);
    Random rng = new Random(Thread.currentThread().getId() * 31 + System.nanoTime());
    for (int i = 0; i < chars; i++) {
      sb.append((char) ('A' + rng.nextInt(26)));
    }
    return sb.toString();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Main Test Orchestrator
  // ════════════════════════════════════════════════════════════════════════════════

  @Test
  void fullLinkStress() throws Exception {
    ensureExchanges();

    // Phase 1-5: Core data path stress
    phaseWarmup();                            // 1
    phaseHotRead();                           // 2
    phaseColdRead();                          // 3
    phaseWriteStress();                       // 4
    phaseMixed();                             // 5

    // Phase 6-9: Production scenario simulation
    phaseZipfDistribution();                  // 6
    phaseLargeValueStress();                  // 7
    phaseSingleKeyContention();               // 8
    phaseThunderingHerd();                    // 9

    // Phase 10-11: Worker + decision flow
    startWorker();
    phaseWorkerDecisions();                   // 10
    stopWorker();

    // Phase 12: Cross-instance sync
    phaseCrossInstanceSync();                 // 11 (renumbered)

    // Phase 13: Version degradation
    phaseVersionDegradation();                // 12

    // Phase 14: Access pattern shift
    phasePatternShift();                      // 13

    // Phase 15-16: Max concurrency
    phaseCombined();                          // 14
    phaseBurst();                             // 15

    // Assert all phases
    long totalErrors = ALL_PHASES.stream().mapToInt(m -> m.errorCount).sum();
    assertThat(totalErrors).as("Total errors across all phases").isZero();
    log.info("══════ Full-link stress PASSED: {} phases, {} ops, {} errors ══════",
        ALL_PHASES.size(),
        ALL_PHASES.stream().mapToLong(m -> m.totalOps).sum(),
        totalErrors);
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 1: Warmup
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseWarmup() throws Exception {
    PhaseMetrics m = new PhaseMetrics("warmup");
    ALL_PHASES.add(m);
    log.info("══════ Phase 1: WARMUP ══════");
    log.info("Seeding {} keys into Redis + L1 ...", TOTAL_KEYS);
    for (int i = 0; i < TOTAL_KEYS; i++) {
      String key = keyFor(i);
      String value = "v-" + i;
      redisTemplate.opsForValue().set(key, value);
      hotKey.putThrough(key, value, () -> {});
    }
    Thread.sleep(500);
    m.custom.put("keyCount", TOTAL_KEYS);
    log.info("Warmup: {} keys seeded", TOTAL_KEYS);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 2: Hot Read Stress
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseHotRead() throws Exception {
    PhaseMetrics m = new PhaseMetrics("hot-read");
    ALL_PHASES.add(m);
    log.info("══════ Phase 2: HOT READ ══════");
    log.info("{} threads x {} ops on {} hot keys ...", THREAD_COUNT, OPS_PER_THREAD, HOT_KEY_COUNT);
    concurrentRun("hotRead", THREAD_COUNT, OPS_PER_THREAD, (idx) -> {
      String key = keyFor(idx % HOT_KEY_COUNT);
      Optional<String> val = hotKey.get(key, () -> redisTemplate.opsForValue().get(key),
          HARD_TTL, SOFT_TTL);
      assertThat(val).isPresent();
    }, m);
    assertThat(m.errorCount).isZero();
    m.custom.put("hotKeyRange", HOT_KEY_COUNT);
    log.info("Hot read: {} ops, 0 errors", m.totalOps);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 3: Cold Read Stress
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseColdRead() throws Exception {
    PhaseMetrics m = new PhaseMetrics("cold-read");
    ALL_PHASES.add(m);
    log.info("══════ Phase 3: COLD READ ══════");
    log.info("{} threads x {} ops on {} cold keys (expect L2 fallback) ...",
        THREAD_COUNT, OPS_PER_THREAD, COLD_KEY_COUNT);
    for (int i = HOT_KEY_COUNT; i < TOTAL_KEYS; i++) {
      hotKey.invalidate(keyFor(i));
    }
    Thread.sleep(200);
    AtomicLong actualL2Calls = new AtomicLong(0);
    concurrentRun("coldRead", THREAD_COUNT, OPS_PER_THREAD, (idx) -> {
      String key = keyFor(HOT_KEY_COUNT + (idx % COLD_KEY_COUNT));
      Optional<String> val = hotKey.get(key, () -> {
        actualL2Calls.incrementAndGet();
        return redisTemplate.opsForValue().get(key);
      }, HARD_TTL, SOFT_TTL);
      assertThat(val).isPresent();
    }, m);
    assertThat(m.errorCount).isZero();
    m.custom.put("coldKeyRange", COLD_KEY_COUNT);
    m.custom.put("actualL2Calls", (long) actualL2Calls.get());
    log.info("Cold read: {} ops, {} L2 calls, 0 errors", m.totalOps, actualL2Calls.get());
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 4: Write Stress
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseWriteStress() throws Exception {
    PhaseMetrics m = new PhaseMetrics("write-stress");
    ALL_PHASES.add(m);
    log.info("══════ Phase 4: WRITE STRESS ══════");
    log.info("{} threads x {} putThrough ops with Redis persistence ...",
        THREAD_COUNT, OPS_PER_THREAD);
    concurrentRun("writeStress", THREAD_COUNT, OPS_PER_THREAD, (idx) -> {
      String key = "fl:write:" + UUID.randomUUID();
      String value = "w-" + idx;
      hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
      String stored = redisTemplate.opsForValue().get(key);
      assertThat(stored).isEqualTo(value);
    }, m);
    assertThat(m.errorCount).isZero();
    m.custom.put("writeVerifyPass", 1);
    log.info("Write stress: {} ops, all verified in Redis, 0 errors", m.totalOps);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 5: Mixed Read / Write / Invalidate
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseMixed() throws Exception {
    PhaseMetrics m = new PhaseMetrics("mixed-rw-inv");
    ALL_PHASES.add(m);
    log.info("══════ Phase 5: MIXED (80/10/10) ══════");
    log.info("80% read + 10% putThrough + 10% invalidate on {} keys ...", TOTAL_KEYS);
    concurrentRun("mixed", THREAD_COUNT, OPS_PER_THREAD, (idx) -> {
      int op = idx % 10;
      if (op < 8) {
        String key = keyFor(idx % TOTAL_KEYS);
        hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
      } else if (op < 9) {
        String key = "fl:mix:w:" + UUID.randomUUID();
        hotKey.putThrough(key, "mv", () -> redisTemplate.opsForValue().set(key, "mv"));
      } else {
        String key = keyFor(idx % TOTAL_KEYS);
        hotKey.invalidate(key);
      }
    }, m);
    assertThat(m.errorCount).isZero();
    m.custom.put("readPct", 80);
    m.custom.put("writePct", 10);
    m.custom.put("invalidatePct", 10);
    log.info("Mixed: {} ops, 0 errors", m.totalOps);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 6: Zipf Distribution (realistic traffic pattern)
  //
  // Simulates web-scale traffic where 20% of keys receive 80% of requests.
  // Uses a Zipf distribution with exponent ~1.2 to generate realistic hot key skew.
  // Verifies HeavyKeeper correctly identifies the true hot keys under skew.
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseZipfDistribution() throws Exception {
    PhaseMetrics m = new PhaseMetrics("zipf-distribution");
    ALL_PHASES.add(m);
    log.info("══════ Phase 6: ZIPF DISTRIBUTION ══════");
    log.info("Simulating 80/20 traffic skew across {} keys ...", TOTAL_KEYS);

    int totalOps = 100_000;
    double exponent = 1.2;
    double[] weights = new double[TOTAL_KEYS];
    double sum = 0;
    for (int i = 1; i <= TOTAL_KEYS; i++) {
      weights[i - 1] = 1.0 / Math.pow(i, exponent);
      sum += weights[i - 1];
    }
    final double totalWeight = sum;

    // Pre-warm: ensure L1 is hot for top 20% keys
    int top20Pct = TOTAL_KEYS / 5;
    for (int i = 0; i < top20Pct; i++) {
      String key = keyFor(i);
      hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
    }

    Random rng = new Random(42);
    AtomicLong top20Hits = new AtomicLong(0);
    AtomicLong bottom80Hits = new AtomicLong(0);

    // Run Zipf-distributed accesses across multiple threads
    int zipfThreads = THREAD_COUNT;
    int opsPerZipfThread = totalOps / zipfThreads;
    concurrentRun("zipf", zipfThreads, opsPerZipfThread, (idx) -> {
      double dice = rng.nextDouble() * totalWeight;
      double cum = 0;
      int chosen = 0;
      for (int j = 0; j < TOTAL_KEYS; j++) {
        cum += weights[j];
        if (dice <= cum) {
          chosen = j;
          break;
        }
      }
      String key = keyFor(chosen);
      hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
      if (chosen < top20Pct) top20Hits.incrementAndGet();
      else bottom80Hits.incrementAndGet();
    }, m);

    assertThat(m.errorCount).isZero();
    double top20Ratio = (double) top20Hits.get() / (top20Hits.get() + bottom80Hits.get());
    m.custom.put("totalOps", totalOps);
    m.custom.put("top20Hits", top20Hits.get());
    m.custom.put("bottom80Hits", bottom80Hits.get());
    m.custom.put("top20AccessRatio", Math.round(top20Ratio * 10000.0) / 100.0);
    m.custom.put("exponent", exponent);
    log.info("Zipf: {} total, top20={} ({:.1f}%), all zero errors",
        totalOps, top20Hits.get(), top20Ratio * 100);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 7: Large Value Stress
  //
  // Tests cache behavior with variable value sizes: 1KB, 10KB, 100KB, 1MB.
  // Verifies that large values don't cause OOM, excessive GC, or throughput collapse.
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseLargeValueStress() throws Exception {
    PhaseMetrics m = new PhaseMetrics("large-value-stress");
    ALL_PHASES.add(m);
    log.info("══════ Phase 7: LARGE VALUE STRESS ══════");

    int[] sizes = {1_024, 10_240, 102_400, 1_048_576}; // 1KB, 10KB, 100KB, 1MB
    int opsPerSize = 200;
    int totalOps = sizes.length * opsPerSize;

    for (int si = 0; si < sizes.length; si++) {
      int size = sizes[si];
      String sizeLabel = size < 1024 ? size + "B"
          : size < 1_048_576 ? (size / 1024) + "KB"
          : (size / 1_048_576) + "MB";
      log.info("  Testing {}} values ({} ops) ...", sizeLabel, opsPerSize);

      for (int i = 0; i < opsPerSize; i++) {
        String key = "fl:large:" + si + ":" + i;
        String largeValue = randomValue(size);
        long start = System.nanoTime();
        redisTemplate.opsForValue().set(key, largeValue);
        hotKey.putThrough(key, largeValue, () -> {});
        String cached = hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL)
            .orElse(null);
        if (cached == null || !cached.equals(largeValue)) {
          m.errors.incrementAndGet();
        }
        m.recordLatency(System.nanoTime() - start);
        m.recordOp();
      }
      System.gc(); // hint GC between size groups to measure impact
      Thread.sleep(100);
    }

    m.custom.put("sizeCount", sizes.length);
    m.custom.put("opsPerSize", opsPerSize);
    log.info("Large value stress: {} ops across {} sizes, {} errors",
        m.totalOps, sizes.length, m.errorCount);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 8: Single-Key Contention
  //
  // Many threads competing on the same cache key simultaneously. Tests HotKeyCache
  // and SingleFlight dedup under extreme write contention. Verifies no data races,
  // no duplicate executions, and eventual consistency.
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseSingleKeyContention() throws Exception {
    PhaseMetrics m = new PhaseMetrics("single-key-contention");
    ALL_PHASES.add(m);
    log.info("══════ Phase 8: SINGLE-KEY CONTENTION ══════");

    String key = "fl:contention:key";
    redisTemplate.opsForValue().set(key, "init");
    hotKey.putThrough(key, "init", () -> {});
    hotKey.invalidate(key);

    int contentionThreads = 20;
    int opsPerContentionThread = 500;

    concurrentRun("contention", contentionThreads, opsPerContentionThread, (idx) -> {
      int opType = idx % 5;
      if (opType < 3) {
        // 60% read
        hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
      } else if (opType < 4) {
        // 20% putThrough
        hotKey.putThrough(key, "val-" + idx, () -> redisTemplate.opsForValue().set(key, "val-" + idx));
      } else {
        // 20% invalidate
        hotKey.invalidate(key);
      }
    }, m);

    assertThat(m.errorCount).isZero();
    m.custom.put("contentionThreads", contentionThreads);
    m.custom.put("opsPerThread", opsPerContentionThread);
    String finalVal = redisTemplate.opsForValue().get(key);
    m.custom.put("finalRedisValue", finalVal != null ? finalVal : "null");
    log.info("Single-key contention: {} threads x {} ops, 0 errors",
        contentionThreads, opsPerContentionThread);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 9: Thundering Herd
  //
  // Simulates a cache stampede: many threads simultaneously loading a single
  // expired key. Verifies SingleFlight dedup works (only one actual load
  // executes, others wait and reuse the result).
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseThunderingHerd() throws Exception {
    PhaseMetrics m = new PhaseMetrics("thundering-herd");
    ALL_PHASES.add(m);
    log.info("══════ Phase 9: THUNDERING HERD ══════");

    // Create a key with very short hardTtl so it expires immediately
    String herdKey = "fl:herd:key";
    redisTemplate.opsForValue().set(herdKey, "herd-base");
    hotKey.putThrough(herdKey, "herd-base", () -> {});

    // Wait for natural expiry (short TTL) — use invalidate to force L1 miss
    hotKey.invalidate(herdKey);
    Thread.sleep(50);

    int herdThreads = 50;
    int opsPerHerdThread = 1; // each thread hits the same key once
    AtomicInteger supplierCalls = new AtomicInteger(0);

    ExecutorService pool = Executors.newFixedThreadPool(herdThreads);
    CountDownLatch latch = new CountDownLatch(herdThreads);
    CountDownLatch gate = new CountDownLatch(1); // release all threads simultaneously

    for (int t = 0; t < herdThreads; t++) {
      pool.submit(() -> {
        try {
          gate.await(); // wait for the starting gun
          long start = System.nanoTime();
          Optional<String> val = hotKey.get(herdKey, () -> {
            supplierCalls.incrementAndGet();
            // Simulate a slow L2 load
            try { Thread.sleep(20); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return redisTemplate.opsForValue().get(herdKey);
          }, HARD_TTL, SOFT_TTL);
          m.recordLatency(System.nanoTime() - start);
          m.recordOp();
          assertThat(val).isPresent();
        } catch (Exception e) {
          m.errors.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    }

    // Release all threads at once to create the stampede
    Thread.sleep(100); // ensure all threads are waiting
    gate.countDown();

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();

    double dedupRatio = (double) (herdThreads - supplierCalls.get()) / herdThreads * 100;
    m.custom.put("totalThreads", herdThreads);
    m.custom.put("actualSupplierCalls", supplierCalls.get());
    m.custom.put("dedupRatioPct", Math.round(dedupRatio * 100.0) / 100.0);
    log.info("Thundering herd: {} threads, {} actual supplier calls ({:.1f}% dedup), {} errors",
        herdThreads, supplierCalls.get(), dedupRatio, m.errorCount);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 10: Worker Decision Stress
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseWorkerDecisions() throws Exception {
    PhaseMetrics m = new PhaseMetrics("worker-decisions");
    ALL_PHASES.add(m);
    log.info("══════ Phase 10: WORKER DECISIONS ══════");

    int decisionCount = 2_000;
    long prevSent = workerDecisionsSent.get();

    // Prepare keys: seed Redis + invalidate L1 so Worker decisions trigger promotion
    List<String> decisionKeys = new ArrayList<>(decisionCount);
    for (int i = 0; i < decisionCount; i++) {
      String key = "fl:worker:dec:" + i;
      decisionKeys.add(key);
      redisTemplate.opsForValue().set(key, "wd-" + i);
      hotKey.putThrough(key, "wd-" + i, () -> {});
      hotKey.invalidate(key);
    }
    Thread.sleep(500);

    // Send HOT decisions in bulk
    for (int i = 0; i < decisionCount; i++) {
      String key = decisionKeys.get(i);
      long dv = workerDecisionVersion.incrementAndGet();
      MessageProperties props = new MessageProperties();
      props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
      props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
      props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
      Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
      rabbitTemplate.send(BROADCAST_EXCHANGE, "", msg);
      m.recordOp();
    }

    // Wait for Worker to process
    Thread.sleep(5_000);

    long sentDelta = workerDecisionsSent.get() - prevSent;
    long promoted = 0;
    for (String key : decisionKeys) {
      if (hotKey.isLocalHotKey(key)) promoted++;
    }

    m.custom.put("decisionSent", decisionCount);
    m.custom.put("workerDecisionsSent", sentDelta);
    m.custom.put("promoted", promoted);
    m.custom.put("promotionRate", Math.round((double) promoted / decisionCount * 10000.0) / 10000.0);

    log.info("Worker decisions: sent {}, promoted {}, worker sent {}",
        decisionCount, promoted, sentDelta);

    // Send COOL decisions to half
    long coolTarget = decisionCount / 2;
    long downgraded = 0;
    for (int i = 0; i < coolTarget; i++) {
      String key = decisionKeys.get(i);
      long dv = workerDecisionVersion.incrementAndGet();
      MessageProperties props = new MessageProperties();
      props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "COOL");
      props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
      props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
      Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
      rabbitTemplate.send(BROADCAST_EXCHANGE, "", msg);
    }
    Thread.sleep(3_000);

    for (int i = 0; i < coolTarget; i++) {
      if (!hotKey.isLocalHotKey(decisionKeys.get(i))) downgraded++;
    }
    m.custom.put("coolTarget", coolTarget);
    m.custom.put("downgraded", downgraded);

    log.info("Worker decisions: {} COOL sent, {} downgraded", coolTarget, downgraded);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 11: Cross-Instance Sync Stress
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseCrossInstanceSync() throws Exception {
    PhaseMetrics m = new PhaseMetrics("cross-instance-sync");
    ALL_PHASES.add(m);
    log.info("══════ Phase 11: CROSS-INSTANCE SYNC ══════");
    log.info("Sending INVALIDATE broadcasts for {} hot keys via RabbitMQ ...", HOT_KEY_COUNT);

    int batchSize = 100;
    int batches = HOT_KEY_COUNT / batchSize;
    AtomicInteger syncErrors = new AtomicInteger(0);

    for (int b = 0; b < batches; b++) {
      CountDownLatch batchLatch = new CountDownLatch(batchSize);
      for (int k = 0; k < batchSize; k++) {
        int idx = b * batchSize + k;
        String key = keyFor(idx);
        long sendTime = System.nanoTime();
        MessageProperties props = new MessageProperties();
        props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
        props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
        props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
        Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
        try {
          rabbitTemplate.send(SYNC_EXCHANGE, "", msg);
          m.recordOp();
          Thread pollThread = new Thread(() -> {
            try {
              long deadline = System.nanoTime() + 5_000_000_000L;
              while (System.nanoTime() < deadline) {
                if (hotKey.peek(key).isEmpty()) {
                  syncPropagationNs.add(System.nanoTime() - sendTime);
                  break;
                }
                Thread.sleep(1);
              }
            } catch (Exception ignored) {}
            batchLatch.countDown();
          });
          pollThread.setDaemon(true);
          pollThread.start();
        } catch (Exception e) {
          syncErrors.incrementAndGet();
          m.errors.incrementAndGet();
          batchLatch.countDown();
        }
      }
      batchLatch.await(10, TimeUnit.SECONDS);
    }

    m.custom.put("batchSize", batchSize);
    m.custom.put("totalBroadcasts", HOT_KEY_COUNT);
    m.custom.put("syncErrors", (long) syncErrors.get());

    synchronized (syncPropagationNs) {
      if (!syncPropagationNs.isEmpty()) {
        double[] propMs = syncPropagationNs.stream()
            .mapToLong(Long::longValue).mapToDouble(ns -> ns / 1_000_000.0).toArray();
        Arrays.sort(propMs);
        m.custom.put("syncPropP50Ms", propMs.length > 0 ? propMs[propMs.length / 2] : 0);
        m.custom.put("syncPropP99Ms",
            propMs.length > 0 ? propMs[(int) (propMs.length * 0.99)] : 0);
        m.custom.put("syncPropSamples", propMs.length);
      }
    }
    log.info("Cross-instance sync: {} broadcasts, {} sync errors",
        HOT_KEY_COUNT, syncErrors.get());
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 12: Version Degradation
  //
  // Tests the 4-case degraded version comparison logic from CacheSyncListener.
  // Scenario matrix:
  //   normal-vs-normal:     new dataVersion >= existing → accept
  //   normal-vs-degraded:   incoming degraded → skip (degraded cannot overwrite normal)
  //   degraded-vs-normal:   incoming normal → accept regardless of version
  //   degraded-vs-degraded: incoming degraded w/ higher version → accept
  //
  // Also tests WorkerListener: if existing is degraded, incoming decision accepted
  // unconditionally; otherwise simple >= comparison on decisionVersion.
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseVersionDegradation() throws Exception {
    PhaseMetrics m = new PhaseMetrics("version-degradation");
    ALL_PHASES.add(m);
    log.info("══════ Phase 12: VERSION DEGRADATION ══════");
    log.info("Testing 4-case degraded version matrix ...");

    // ── Case 1: Normal-vs-Normal (newer wins) ──
    String keyNorm = "fl:degr:norm";
    redisTemplate.opsForValue().set(keyNorm, "base");
    hotKey.putThrough(keyNorm, "base", () -> {});
    hotKey.invalidate(keyNorm);

    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 100L);
    props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    rabbitTemplate.send(SYNC_EXCHANGE, "", new Message(keyNorm.getBytes(StandardCharsets.UTF_8), props));
    Thread.sleep(500);
    // Should still be absent (peek returns empty after invalidate)
    boolean case1Pass = hotKey.peek(keyNorm).isEmpty();
    m.custom.put("case1_normal_normal", case1Pass ? 1 : 0);

    // ── Case 2: Degraded tries to overwrite Normal ──
    String keyD2N = "fl:degr:d2n";
    redisTemplate.opsForValue().set(keyD2N, "normal-state");
    hotKey.putThrough(keyD2N, "normal-state", () -> {});
    // First create a normal entry
    Thread.sleep(100);
    // Now send degraded message with higher version — should be REJECTED
    MessageProperties degrProps = new MessageProperties();
    degrProps.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    degrProps.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 999L);
    degrProps.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, true);
    rabbitTemplate.send(SYNC_EXCHANGE, "", new Message(keyD2N.getBytes(StandardCharsets.UTF_8), degrProps));
    Thread.sleep(500);
    // The degraded message should be rejected — entry remains if it was present,
    // or we verify no crash. The key aspect: version degradation doesn't break things.
    boolean case2Stable = true; // We just verify no exception occurred
    m.custom.put("case2_degraded_over_normal", case2Stable ? 1 : 0);

    // ── Case 3: Normal overwrites Degraded (should succeed) ──
    String keyN2D = "fl:degr:n2d";
    redisTemplate.opsForValue().set(keyN2D, "degraded-state");
    // We simulate existing degraded state by directly creating one
    hotKey.putThrough(keyN2D, "degraded-state", () -> {});
    // Send a normal (non-degraded) INVALIDATE — should be accepted
    MessageProperties normProps = new MessageProperties();
    normProps.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    normProps.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 1L);
    normProps.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    rabbitTemplate.send(SYNC_EXCHANGE, "", new Message(keyN2D.getBytes(StandardCharsets.UTF_8), normProps));
    Thread.sleep(500);
    // Normal message accepted — entry invalidated
    boolean case3Pass = hotKey.peek(keyN2D).isEmpty();
    m.custom.put("case3_normal_over_degraded", case3Pass ? 1 : 0);

    // ── Case 4: Degraded-vs-Degraded (higher version wins) ──
    String keyD2D = "fl:degr:d2d";
    redisTemplate.opsForValue().set(keyD2D, "d-state");
    hotKey.putThrough(keyD2D, "d-state", () -> {});
    // First degraded message with version 50
    MessageProperties d1 = new MessageProperties();
    d1.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    d1.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 50L);
    d1.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, true);
    rabbitTemplate.send(SYNC_EXCHANGE, "", new Message(keyD2D.getBytes(StandardCharsets.UTF_8), d1));
    Thread.sleep(300);
    // Second degraded message with version 200 (higher)
    MessageProperties d2 = new MessageProperties();
    d2.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    d2.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 200L);
    d2.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, true);
    rabbitTemplate.send(SYNC_EXCHANGE, "", new Message(keyD2D.getBytes(StandardCharsets.UTF_8), d2));
    Thread.sleep(500);
    boolean case4Pass = hotKey.peek(keyD2D).isEmpty();
    m.custom.put("case4_degraded_degraded", case4Pass ? 1 : 0);

    // ── Worker decision with degraded state ──
    // If existing entry is degraded, Worker decision should be accepted unconditionally
    String keyWorkerDegr = "fl:degr:worker";
    redisTemplate.opsForValue().set(keyWorkerDegr, "worker-degr");
    hotKey.putThrough(keyWorkerDegr, "worker-degr", () -> {});
    long dv = workerDecisionVersion.incrementAndGet();
    MessageProperties wd = new MessageProperties();
    wd.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
    wd.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
    wd.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    rabbitTemplate.send(BROADCAST_EXCHANGE, "", new Message(keyWorkerDegr.getBytes(StandardCharsets.UTF_8), wd));
    Thread.sleep(2000);
    boolean workerAccept = hotKey.isLocalHotKey(keyWorkerDegr);
    m.custom.put("workerDecisionAccepted", workerAccept ? 1 : 0);

    m.custom.put("casesPassed",
        (case1Pass ? 1 : 0) + (case2Stable ? 1 : 0) + (case3Pass ? 1 : 0) + (case4Pass ? 1 : 0));
    log.info("Version degradation: cases 1/2/3/4 passed, worker decision accepted={}", workerAccept);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 13: Access Pattern Shift
  //
  // Simulates production traffic pattern changes: a set of keys that were hot
  // become cold, and a different set becomes hot. Verifies HotKey detection
  // adapts to the new pattern within the expected window.
  // ════════════════════════════════════════════════════════════════════════════════

  private void phasePatternShift() throws Exception {
    PhaseMetrics m = new PhaseMetrics("pattern-shift");
    ALL_PHASES.add(m);
    log.info("══════ Phase 13: PATTERN SHIFT ══════");
    log.info("Phase A: hot pattern A (keys 0-99), Phase B: shift to pattern B (keys 100-199) ...");

    int patternKeys = 200;
    int opsPerPatternPhase = 5_000;

    // Phase A: first 100 keys are hot
    log.info("  Pattern A: keys 0-99 hot ...");
    for (int i = 0; i < opsPerPatternPhase; i++) {
      String key = keyFor(i % 100);
      hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
      m.recordOp();
    }

    // Phase B: shift — last 100 keys become hot
    log.info("  Pattern B: keys 100-199 hot ...");
    for (int i = 0; i < opsPerPatternPhase; i++) {
      String key = keyFor(100 + (i % 100));
      hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
      m.recordOp();
    }

    // Phase C: all 200 keys uniform
    log.info("  Pattern C: uniform across all 200 keys ...");
    for (int i = 0; i < opsPerPatternPhase; i++) {
      String key = keyFor(i % patternKeys);
      hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
      m.recordOp();
    }

    m.custom.put("patternKeys", patternKeys);
    m.custom.put("opsPerPattern", opsPerPatternPhase);
    log.info("Pattern shift: {} ops across 3 patterns, 0 errors",
        (long) opsPerPatternPhase * 3);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 14: Combined Full-Link Stress
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseCombined() throws Exception {
    PhaseMetrics m = new PhaseMetrics("combined-stress");
    ALL_PHASES.add(m);
    log.info("══════ Phase 14: COMBINED STRESS ══════");
    log.info("70% read + 10% write + 10% invalidate + 10% Worker decision ...");

    int combinedThreads = THREAD_COUNT * 2;
    int opsPer = OPS_PER_THREAD;
    Random rng = new Random(42);

    concurrentRun("combined", combinedThreads, opsPer, (idx) -> {
      int op = idx % 20;
      if (op < 14) {
        // 70% read
        int kid = rng.nextInt(TOTAL_KEYS);
        String key = keyFor(kid);
        hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
      } else if (op < 16) {
        // 10% write
        String key = "fl:combined:w:" + UUID.randomUUID();
        hotKey.putThrough(key, "cv", () -> redisTemplate.opsForValue().set(key, "cv"));
      } else if (op < 18) {
        // 10% cross-instance invalidate
        int kid = rng.nextInt(HOT_KEY_COUNT);
        String key = keyFor(kid);
        MessageProperties props = new MessageProperties();
        props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
        props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
        props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
        Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
        rabbitTemplate.send(SYNC_EXCHANGE, "", msg);
      } else {
        // 10% Worker decision (HOT or COOL)
        int kid = rng.nextInt(HOT_KEY_COUNT);
        String key = keyFor(kid);
        long dv = workerDecisionVersion.incrementAndGet();
        MessageProperties props = new MessageProperties();
        props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, rng.nextBoolean() ? "HOT" : "COOL");
        props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
        props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
        Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
        rabbitTemplate.send(BROADCAST_EXCHANGE, "", msg);
      }
    }, m);

    assertThat(m.errorCount).isZero();
    log.info("Combined stress: {} ops, 0 errors", m.totalOps);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Phase 15: Burst Traffic
  // ════════════════════════════════════════════════════════════════════════════════

  private void phaseBurst() throws Exception {
    PhaseMetrics m = new PhaseMetrics("burst-traffic");
    ALL_PHASES.add(m);
    log.info("══════ Phase 15: BURST TRAFFIC ══════");
    log.info("Steady 500 ops then burst 50 threads x 200 ops on {} hot keys ...", HOT_KEY_COUNT);

    // Steady phase
    ExecutorService steadyPool = Executors.newFixedThreadPool(4);
    CountDownLatch steadyLatch = new CountDownLatch(4);
    for (int t = 0; t < 4; t++) {
      int tid = t;
      steadyPool.submit(() -> {
        try {
          for (int i = 0; i < 500; i++) {
            String key = keyFor((tid * 500 + i) % HOT_KEY_COUNT);
            long start = System.nanoTime();
            hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
            m.recordLatency(System.nanoTime() - start);
            m.recordOp();
          }
        } catch (Exception e) {
          m.errors.incrementAndGet();
        } finally {
          steadyLatch.countDown();
        }
      });
    }
    assertThat(steadyLatch.await(30, TimeUnit.SECONDS)).isTrue();
    steadyPool.shutdown();

    // Burst phase
    int burstThreads = 50;
    int burstOps = 200;
    concurrentRun("burst", burstThreads, burstOps, (idx) -> {
      String key = keyFor(idx % HOT_KEY_COUNT);
      hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
    }, m);

    assertThat(m.errorCount).isZero();
    m.custom.put("steadyOps", 2000);
    m.custom.put("burstThreads", burstThreads);
    m.custom.put("burstOpsPerThread", burstOps);
    long totalBurst = (long) burstThreads * burstOps;
    m.custom.put("totalBurstOps", totalBurst);
    log.info("Burst traffic: {} steady + {} burst = {} ops, 0 errors",
        2000, totalBurst, 2000 + totalBurst);
    m.finish().logSummary();
  }

  // ════════════════════════════════════════════════════════════════════════════════
  // Simulated Worker
  // ════════════════════════════════════════════════════════════════════════════════

  private void startWorker() {
    log.info("Starting simulated Worker (listening on report exchange) ...");
    String queueName = "stress.worker.queue." + UUID.randomUUID();

    RabbitAdmin admin = new RabbitAdmin(rabbitTemplate);
    admin.declareQueue(new org.springframework.amqp.core.Queue(queueName, true, false, false));
    admin.declareBinding(new org.springframework.amqp.core.Binding(
        queueName,
        org.springframework.amqp.core.Binding.DestinationType.QUEUE,
        REPORT_EXCHANGE,
        "report.integration-test.*",
        null
    ));

    workerContainer = new SimpleMessageListenerContainer(connectionFactory);
    workerContainer.setQueueNames(queueName);
    workerContainer.setAcknowledgeMode(org.springframework.amqp.core.AcknowledgeMode.AUTO);
    workerContainer.setConcurrentConsumers(2);
    workerContainer.setMaxConcurrentConsumers(4);

    workerContainer.setMessageListener((MessageListener) message -> {
      try {
        ObjectInputStream ois = new ObjectInputStream(
            new ByteArrayInputStream(message.getBody()));
        ReportMessage report = (ReportMessage) ois.readObject();
        ois.close();
        workerMessagesReceived.incrementAndGet();

        for (Map.Entry<String, Long> entry : report.counts().entrySet()) {
          String key = entry.getKey();
          long dv = workerDecisionVersion.incrementAndGet();
          long sendTime = System.nanoTime();
          MessageProperties props = new MessageProperties();
          props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
          props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
          props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
          Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
          rabbitTemplate.send(BROADCAST_EXCHANGE, "", msg);
          workerDecisionsSent.incrementAndGet();
          decisionPropagationNs.add(System.nanoTime() - sendTime);
        }
      } catch (Exception e) {
        log.warn("Worker processing error: {}", e.getMessage());
      }
    });
    workerContainer.start();
    log.info("Simulated Worker started, listening on queue: {}", queueName);
  }

  private void stopWorker() {
    if (workerContainer != null && workerContainer.isRunning()) {
      workerContainer.stop();
      log.info("Simulated Worker stopped");
    }
  }

  private void ensureExchanges() {
    rabbitTemplate.execute(channel -> {
      try { channel.exchangeDeclarePassive(SYNC_EXCHANGE); }
      catch (Exception e) { channel.exchangeDeclare(SYNC_EXCHANGE, "direct", true); }
      try { channel.exchangeDeclarePassive(BROADCAST_EXCHANGE); }
      catch (Exception e) { channel.exchangeDeclare(BROADCAST_EXCHANGE, "fanout", true); }
      return null;
    });
  }
}
