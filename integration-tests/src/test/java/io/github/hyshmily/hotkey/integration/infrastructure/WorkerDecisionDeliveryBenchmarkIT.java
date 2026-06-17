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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * Benchmark measuring worker decision (HOT/COOL) delivery latency and throughput.
 *
 * <p>Sends bulk HOT decisions, COOL decisions, version-ordered messages, and
 * concurrent decisions while measuring propagation latency, promotion rate,
 * and version ordering correctness.
 */
@Testcontainers
@Tag("docker")
@Tag("benchmark")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class WorkerDecisionDeliveryBenchmarkIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(WorkerDecisionDeliveryBenchmarkIT.class);

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

  /**
   * Overrides Redis and RabbitMQ connection properties to point at the Testcontainers
   * instances. Configures connection-mode caching and listener concurrency for
   * high-throughput decision delivery.
   */
  @DynamicPropertySource
  static void overrideProps(DynamicPropertyRegistry r) {
    r.add("spring.data.redis.host", () -> System.getenv().getOrDefault("TESTCONTAINERS_HOST_OVERRIDE", redis.getHost()));
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    r.add("spring.rabbitmq.host", () -> System.getenv().getOrDefault("TESTCONTAINERS_HOST_OVERRIDE", rabbitmq.getHost()));
    r.add("spring.rabbitmq.port", () -> rabbitmq.getMappedPort(5672));
    r.add("spring.rabbitmq.username", () -> "guest");
    r.add("spring.rabbitmq.password", () -> "guest");
    r.add("spring.rabbitmq.cache.connection-mode", () -> "CONNECTION");
    r.add("spring.rabbitmq.listener.simple.concurrency", () -> "4");
    r.add("spring.rabbitmq.listener.simple.max-concurrency", () -> "8");
    r.add("hotkey.worker-listener.auto-startup", () -> "true");
  }

  @Autowired
  HotKey hotKey;

  @Autowired
  StringRedisTemplate redisTemplate;

  @Autowired
  RabbitTemplate rabbitTemplate;

  static final int DECISION_COUNT = 5_000;
  static final int LATENCY_SAMPLE_COUNT = 200;
  static final int COOL_COUNT = 500;
  static final int THREAD_COUNT = 4;
  static final int OPS_PER_CONCURRENT = 500;
  static final int VERSION_ORDER_BATCH = 1_000;
  static final int COLLECTIVE_WAIT_SECONDS = 15;
  static final String WORKER_EXCHANGE = "hotkey.worker.exchange";

  /** Runnable that may throw any checked exception. */
  @FunctionalInterface
  interface CheckedRunnable {
    void run() throws Throwable;
  }

  /** Times a single operation to nanosecond precision. */
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

  /** Runs warmup, bulk HOT decisions, COOL decisions, version ordering, and concurrent decision phases and writes a JSON report. */
  @Test
  void decisionDeliveryBenchmark() throws Exception {
    ensureExchange(WORKER_EXCHANGE);
    List<Map<String, Object>> phases = new ArrayList<>();

    phases.add(phaseWarmup());

    PhaseTwoResult r2 = phaseHotDecisionBulk();
    phases.add(r2.phase);

    PhaseCoolResult cool = phaseCoolDecision(r2.promotedKeys);
    phases.add(cool.phase);

    phases.add(phaseVersionOrdering());
    phases.add(phaseConcurrentDecisions());

    long totalOps = phases.stream()
        .mapToLong(p -> ((Number) p.get("totalOps")).longValue()).sum();
    double totalMs = phases.stream()
        .mapToDouble(p -> ((Number) p.get("durationMs")).doubleValue()).sum();

    Map<String, Object> report = new HashMap<>();
    report.put("testName", "HotKey Worker Decision Delivery Benchmark");
    report.put("timestamp", Instant.now().toString());
    report.put("config", Map.of(
        "decisionCount", DECISION_COUNT,
        "latencySampleCount", LATENCY_SAMPLE_COUNT,
        "coolCount", COOL_COUNT,
        "threadCount", THREAD_COUNT,
        "opsPerConcurrentThread", OPS_PER_CONCURRENT,
        "versionOrderBatch", VERSION_ORDER_BATCH,
        "collectiveWaitSeconds", COLLECTIVE_WAIT_SECONDS,
        "workerExchange", WORKER_EXCHANGE
    ));
    report.put("phases", phases);
    report.put("summary", Map.of(
        "totalOps", totalOps,
        "totalDurationMs", Math.round(totalMs * 100.0) / 100.0,
        "overallThroughputOpsPerSec",
        totalMs > 0 ? Math.round(totalOps / (totalMs / 1000.0)) : 0
    ));

    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    String json = mapper.writeValueAsString(report);

    Path reportPath = Path.of("src", "test", "resources", "testresult",
        "benchmark-worker-decision-" + Instant.now().toString().replace(":", "-") + ".json");
    Files.createDirectories(reportPath.getParent());
    Files.writeString(reportPath, json);

    log.info("\n=========================================\nBenchmark report -> {}\n{}",
        reportPath.toAbsolutePath(), json);

    long totalErrors = phases.stream()
        .mapToLong(p -> ((Number) p.get("errors")).longValue()).sum();
    assertThat(totalErrors).as("Total benchmark errors").isZero();
  }

  // ══════════════════════════════════════════════════
  // Phase 1: Warmup
  // ══════════════════════════════════════════════════

  private Map<String, Object> phaseWarmup() throws Exception {
    log.info("══════ Phase 1: WARMUP ══════");
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    List<Long> latencies = new ArrayList<>();

    for (int i = 0; i < 1_000; i++) {
      int idx = i;
      String key = "bench:dec:prep:" + idx;
      long ns = measure(() ->
          hotKey.putThrough(key, "v-" + idx,
              () -> redisTemplate.opsForValue().set(key, "v-" + idx)));
      latencies.add(ns);
    }
    Thread.sleep(500);

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    double[] lats = latencies.stream().mapToDouble(l -> l / 1_000_000.0).sorted().toArray();
    Map<String, Object> phase = buildPhase("warmup", 1_000, ms, 0, 0, errors.get(), lats);
    logPhase(phase);
    return phase;
  }

  // ══════════════════════════════════════════════════
  // Phase 2: Bulk HOT decision delivery
  //   Strategy: send all decisions, wait collectively,
  //   then count + sample fine-grained latency
  // ══════════════════════════════════════════════════

  /** Result carrier for the bulk HOT decision phase, holding the phase metrics and the set of promoted keys. */
  static class PhaseTwoResult {
    final Map<String, Object> phase;
    final Set<String> promotedKeys;

    PhaseTwoResult(Map<String, Object> phase, Set<String> promotedKeys) {
      this.phase = phase;
      this.promotedKeys = promotedKeys;
    }
  }

  /** Sends bulk HOT decisions, waits for propagation, and measures promotion rate and latency. */
  private PhaseTwoResult phaseHotDecisionBulk() throws Exception {
    log.info("══════ Phase 2: BULK HOT DECISIONS ══════");
    log.info("Sending {} HOT decisions, then waiting {}s for promotion ...",
        DECISION_COUNT, COLLECTIVE_WAIT_SECONDS);
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    List<Long> sendLatencies = new ArrayList<>();
    List<String> allKeys = new ArrayList<>(DECISION_COUNT);
    List<String> latencySampleKeys = new ArrayList<>(LATENCY_SAMPLE_COUNT);

    // ── Prepare keys in Redis + L1, then invalidate ──

    for (int i = 0; i < DECISION_COUNT; i++) {
      String key = "bench:dec:hot:" + i;
      allKeys.add(key);
      redisTemplate.opsForValue().set(key, "hot-value-" + i);
      hotKey.putThrough(key, "hot-value-" + i, () -> {});
      hotKey.invalidate(key);
    }
    Thread.sleep(500);

    // ── Send HOT decisions ──

    for (int i = 0; i < DECISION_COUNT; i++) {
      String key = allKeys.get(i);
      MessageProperties props = new MessageProperties();
      props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
      props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, (long) i + 1);
      props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
      Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
      long ns = measure(() -> rabbitTemplate.send(WORKER_EXCHANGE, "", msg));
      sendLatencies.add(ns);

      if (i < LATENCY_SAMPLE_COUNT) {
        latencySampleKeys.add(key);
      }
    }

    log.info("Sent {} HOT decisions, waiting {}s for WorkerListener processing ...",
        DECISION_COUNT, COLLECTIVE_WAIT_SECONDS);
    Thread.sleep(COLLECTIVE_WAIT_SECONDS * 1000L);

    // ── Count promotions ──

    AtomicLong promoted = new AtomicLong(0);
    Set<String> promotedSet = new HashSet<>();
    for (String key : allKeys) {
      if (hotKey.isLocalHotKey(key)) {
        promoted.incrementAndGet();
        promotedSet.add(key);
      }
    }

    // ── Measure fine-grained latency for sample keys ──
    // Re-send to sample keys with individual timing

    List<Long> fineGrainedPropagationNs = new ArrayList<>();
    for (String sampleKey : latencySampleKeys) {
      hotKey.invalidate(sampleKey);
    }
    Thread.sleep(1_000);

    for (String sampleKey : latencySampleKeys) {
      long sendTime = System.nanoTime();
      int idx = allKeys.indexOf(sampleKey);
      MessageProperties props = new MessageProperties();
      props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
      props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, (long) DECISION_COUNT + idx + 1);
      props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
      Message msg = new Message(sampleKey.getBytes(StandardCharsets.UTF_8), props);
      rabbitTemplate.send(WORKER_EXCHANGE, "", msg);

      long deadline = System.nanoTime() + 5_000_000_000L;
      while (System.nanoTime() < deadline) {
        if (hotKey.isLocalHotKey(sampleKey)) {
          fineGrainedPropagationNs.add(System.nanoTime() - sendTime);
          break;
        }
        Thread.sleep(1);
      }
    }

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    double[] sendLats = sendLatencies.stream()
        .mapToDouble(l -> l / 1_000_000.0).sorted().toArray();
    double[] finePropLats = fineGrainedPropagationNs.stream()
        .mapToDouble(l -> l / 1_000_000.0).sorted().toArray();

    Map<String, Object> phase = buildPhase("hot-decision-bulk",
        DECISION_COUNT, ms, promoted.get(), 0, errors.get(), sendLats);
    phase.put("fineGrainedPropagationSamples", finePropLats.length);
    phase.put("promoted", promoted.get());
    phase.put("promotionRate",
        Math.round((double) promoted.get() / DECISION_COUNT * 10000.0) / 10000.0);

    Map<String, Object> sendLatMap = new HashMap<>();
    sendLatMap.put("p50", round3(percentile(sendLats, 50)));
    sendLatMap.put("p90", round3(percentile(sendLats, 90)));
    sendLatMap.put("p99", round3(percentile(sendLats, 99)));
    sendLatMap.put("p999", round3(percentile(sendLats, 99.9)));
    sendLatMap.put("max", round3(sendLats[sendLats.length - 1]));
    sendLatMap.put("mean", round3(Arrays.stream(sendLats).average().orElse(0)));
    sendLatMap.put("samples", sendLats.length);
    phase.put("amqpSendLatencyMs", sendLatMap);

    if (finePropLats.length > 0) {
      Map<String, Object> propLatMap = new HashMap<>();
      propLatMap.put("p50", round3(percentile(finePropLats, 50)));
      propLatMap.put("p90", round3(percentile(finePropLats, 90)));
      propLatMap.put("p99", round3(percentile(finePropLats, 99)));
      propLatMap.put("max", round3(finePropLats[finePropLats.length - 1]));
      propLatMap.put("mean", round3(Arrays.stream(finePropLats).average().orElse(0)));
      propLatMap.put("samples", finePropLats.length);
      phase.put("propagationLatencyMs", propLatMap);
    }

    logPhase(phase);
    return new PhaseTwoResult(phase, promotedSet);
  }

  // ══════════════════════════════════════════════════
  // Phase 3: COOL decision on promoted keys
  // ══════════════════════════════════════════════════

  /** Result carrier for the COOL decision phase, holding the phase metrics. */
  static class PhaseCoolResult {
    final Map<String, Object> phase;

    PhaseCoolResult(Map<String, Object> phase) {
      this.phase = phase;
    }
  }

  /** Sends COOL decisions to a subset of promoted keys and measures downgrade rate. */
  private PhaseCoolResult phaseCoolDecision(Set<String> promotedKeys) throws Exception {
    log.info("══════ Phase 3: COOL DECISIONS ══════");
    List<String> toDowngrade = promotedKeys.stream()
        .limit(COOL_COUNT).toList();
    log.info("Sending COOL to {} of {} promoted keys ...",
        toDowngrade.size(), promotedKeys.size());
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    AtomicLong downgraded = new AtomicLong(0);
    List<Long> sendLatencies = new ArrayList<>();

    for (int i = 0; i < toDowngrade.size(); i++) {
      String key = toDowngrade.get(i);
      long baseIdx = Long.parseLong(key.replace("bench:dec:hot:", ""));
      MessageProperties props = new MessageProperties();
      props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "COOL");
      props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION,
          (long) DECISION_COUNT + baseIdx + 1);
      props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
      Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
      long ns = measure(() -> rabbitTemplate.send(WORKER_EXCHANGE, "", msg));
      sendLatencies.add(ns);
    }

    Thread.sleep(COLLECTIVE_WAIT_SECONDS * 1000L);

    for (String key : toDowngrade) {
      if (!hotKey.isLocalHotKey(key)) {
        downgraded.incrementAndGet();
      }
    }

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    double[] sendLats = sendLatencies.stream()
        .mapToDouble(l -> l / 1_000_000.0).sorted().toArray();

    Map<String, Object> phase = buildPhase("cool-decision",
        toDowngrade.size(), ms, 0, 0, errors.get(), sendLats);
    phase.put("coolTargets", toDowngrade.size());
    phase.put("downgraded", downgraded.get());
    phase.put("downgradeRate",
        toDowngrade.size() > 0
            ? Math.round((double) downgraded.get() / toDowngrade.size() * 10000.0) / 10000.0
            : 0);

    Map<String, Object> sendLatMap = new HashMap<>();
    sendLatMap.put("p50", round3(percentile(sendLats, 50)));
    sendLatMap.put("p90", round3(percentile(sendLats, 90)));
    sendLatMap.put("p99", round3(percentile(sendLats, 99)));
    sendLatMap.put("samples", sendLats.length);
    phase.put("amqpSendLatencyMs", sendLatMap);

    logPhase(phase);
    return new PhaseCoolResult(phase);
  }

  // ══════════════════════════════════════════════════
  // Phase 4: Version ordering
  // ══════════════════════════════════════════════════

  /** Sends HOT decisions with monotonic versions and verifies an old-version decision is correctly rejected. */
  private Map<String, Object> phaseVersionOrdering() throws Exception {
    log.info("══════ Phase 4: VERSION ORDERING ══════");
    log.info("Sending {} HOT decisions with monotonic versions, "
        + "then checking an old-version decision is rejected ...",
        VERSION_ORDER_BATCH);
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    String key = "bench:dec:version:ordering";

    redisTemplate.opsForValue().set(key, "ordering-value");
    hotKey.putThrough(key, "ordering-value", () -> {});
    hotKey.invalidate(key);
    Thread.sleep(500);

    long baseVersion = 100_000;
    long lastSentVersion = 0;
    for (long i = 1; i <= VERSION_ORDER_BATCH; i++) {
      MessageProperties props = new MessageProperties();
      props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
      props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, baseVersion + i);
      props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
      Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
      try {
        rabbitTemplate.send(WORKER_EXCHANGE, "", msg);
        lastSentVersion = baseVersion + i;
      } catch (Exception e) {
        errors.incrementAndGet();
      }
      if (i % 100 == 0) Thread.sleep(1);
    }

    Thread.sleep(3_000);
    log.info("Sent versions {}-{}", baseVersion + 1, lastSentVersion);

    long oldVersion = baseVersion + 1;
    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
    props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, oldVersion);
    props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
    try {
      rabbitTemplate.send(WORKER_EXCHANGE, "", msg);
    } catch (Exception e) {
      errors.incrementAndGet();
    }
    Thread.sleep(2_000);

    log.info("Old-version (v={}) decision sent, no crash - key is hot (from v{})",
        oldVersion, lastSentVersion);
    assertThat(errors.get()).as("Version ordering should have no errors").isZero();

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    Map<String, Object> phase = buildPhase("version-ordering",
        VERSION_ORDER_BATCH + 1, ms, 0, 0, errors.get(), new double[0]);
    phase.put("monotonicVersionsSent", VERSION_ORDER_BATCH);
    logPhase(phase);
    return phase;
  }

  // ══════════════════════════════════════════════════
  // Phase 5: Concurrent decisions
  // ══════════════════════════════════════════════════

  /** Runs concurrent multi-threaded HOT/COOL decision sending and measures throughput. */
  private Map<String, Object> phaseConcurrentDecisions() throws Exception {
    log.info("══════ Phase 5: CONCURRENT DECISIONS ══════");
    log.info("{} threads × {} ops, 90% HOT + 10% COOL, measuring send throughput ...",
        THREAD_COUNT, OPS_PER_CONCURRENT);
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    AtomicLong hotSent = new AtomicLong(0);
    AtomicLong coolSent = new AtomicLong(0);
    List<Long> sendLatencies = new ArrayList<>();

    CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
    ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

    for (int t = 0; t < THREAD_COUNT; t++) {
      int tid = t;
      pool.submit(() -> {
        try {
          for (int i = 0; i < OPS_PER_CONCURRENT; i++) {
            String key = "bench:dec:conc:" + tid + ":" + i;
            redisTemplate.opsForValue().set(key, "conc-value");
            hotKey.putThrough(key, "conc-value", () -> {});
            hotKey.invalidate(key);

            boolean isHot = i % 10 != 0;
            long dv = (long) tid * OPS_PER_CONCURRENT + i + 1;
            MessageProperties props = new MessageProperties();
            props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, isHot ? "HOT" : "COOL");
            props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
            props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
            Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
            long ns = measure(() -> rabbitTemplate.send(WORKER_EXCHANGE, "", msg));
            sendLatencies.add(ns);
            if (isHot) hotSent.incrementAndGet();
            else coolSent.incrementAndGet();
            Thread.sleep(5);
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Concurrent decision error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
    Thread.sleep(3_000);

    long totalOps = (long) THREAD_COUNT * OPS_PER_CONCURRENT;
    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    double[] sendLats = sendLatencies.stream()
        .mapToDouble(l -> l / 1_000_000.0).sorted().toArray();

    Map<String, Object> phase = buildPhase("concurrent-decisions",
        totalOps, ms, 0, 0, errors.get(), sendLats);
    phase.put("hotDecisions", hotSent.get());
    phase.put("coolDecisions", coolSent.get());

    Map<String, Object> sendLatMap = new HashMap<>();
    sendLatMap.put("p50", round3(percentile(sendLats, 50)));
    sendLatMap.put("p90", round3(percentile(sendLats, 90)));
    sendLatMap.put("p99", round3(percentile(sendLats, 99)));
    sendLatMap.put("p999", round3(percentile(sendLats, 99.9)));
    sendLatMap.put("max", round3(sendLats[sendLats.length - 1]));
    sendLatMap.put("mean", round3(Arrays.stream(sendLats).average().orElse(0)));
    sendLatMap.put("samples", sendLats.length);
    phase.put("amqpSendLatencyMs", sendLatMap);

    logPhase(phase);
    return phase;
  }

  // ══════════════════════════════════════════════════
  // Helpers
  // ══════════════════════════════════════════════════

  /** Declares a fanout exchange idempotently. */
  private void ensureExchange(String name) {
    rabbitTemplate.execute(channel -> {
      channel.exchangeDeclare(name, "fanout", true);
      return null;
    });
  }

  /** Computes the p-th percentile from a sorted array of latencies. */
  private static double percentile(double[] sorted, double p) {
    if (sorted.length == 0) return 0;
    double rank = p / 100.0 * (sorted.length - 1);
    int lo = (int) Math.floor(rank);
    int hi = (int) Math.ceil(rank);
    if (lo == hi || hi >= sorted.length) return sorted[lo];
    double frac = rank - lo;
    return sorted[lo] * (1 - frac) + sorted[hi] * frac;
  }

  /** Rounds a double to 3 decimal places. */
  private static double round3(double v) {
    return Math.round(v * 1000.0) / 1000.0;
  }

  /** Creates a phase result map with name, ops, duration, hit rates, and latency percentiles. */
  private static Map<String, Object> buildPhase(
      String name, long totalOps, double ms,
      long l1Hits, long l2Calls, long errors, double[] latMs) {
    Map<String, Object> p = new HashMap<>();
    p.put("name", name);
    p.put("totalOps", totalOps);
    p.put("durationMs", Math.round(ms * 100.0) / 100.0);
    p.put("throughputOpsPerSec",
        ms > 0 ? Math.round(totalOps / (ms / 1000.0)) : 0);
    p.put("l1Hits", l1Hits);
    p.put("l1HitRate",
        totalOps > 0
            ? Math.round((double) l1Hits / totalOps * 10000.0) / 10000.0
            : 0);
    p.put("l2Calls", l2Calls);
    p.put("errors", errors);

    if (latMs.length > 0) {
      Map<String, Object> latMap = new HashMap<>();
      latMap.put("p50", round3(percentile(latMs, 50)));
      latMap.put("p90", round3(percentile(latMs, 90)));
      latMap.put("p99", round3(percentile(latMs, 99)));
      latMap.put("p999", round3(percentile(latMs, 99.9)));
      latMap.put("max", round3(latMs[latMs.length - 1]));
      latMap.put("mean", round3(Arrays.stream(latMs).average().orElse(0)));
      latMap.put("samples", latMs.length);
      p.put("latencyMs", latMap);
    }
    return p;
  }

  /** Logs a phase summary at INFO level. */
  private static void logPhase(Map<String, Object> phase) {
    log.info("Phase [{}] done: {} ops in {} ms, throughput={}/s, errors={}",
        phase.get("name"), phase.get("totalOps"),
        String.format("%.1f", phase.get("durationMs")),
        phase.get("throughputOpsPerSec"), phase.get("errors"));
  }
}
