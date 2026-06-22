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
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
import io.github.hyshmily.hotkey.reporting.ReportMessage;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
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
 * Benchmark for multi-instance cache consistency under concurrent access.
 *
 * <p>Simulates a multi-node deployment with a local Worker (HeavyKeeper) consuming
 * reports and issuing HOT decisions. Measures throughput, latency, and propagation
 * delay across warmup, hot-read, worker decision, cross-instance sync, and combined
 * stress phases. Outputs a JSON report with per-phase metrics.
 */
@Testcontainers
@Tag("docker")
@Tag("benchmark")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class MultiInstanceBenchmarkIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(MultiInstanceBenchmarkIT.class);

  // --- Containers ---

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
   * Registers dynamic Testcontainers host/port properties for Redis and RabbitMQ.
   *
   * @param r the dynamic property registry supplied by Spring
   */
  @DynamicPropertySource
  static void overrideProps(DynamicPropertyRegistry r) {
    r.add("spring.data.redis.host", redis::getHost);
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    r.add("spring.rabbitmq.host", rabbitmq::getHost);
    r.add("spring.rabbitmq.port", () -> rabbitmq.getMappedPort(5672));
    r.add("spring.rabbitmq.username", () -> "guest");
    r.add("spring.rabbitmq.password", () -> "guest");
    r.add("hotkey.local.report-interval-ms", () -> "1000");
    r.add("hotkey.worker-listener.exchange", () -> "hotkey.broadcast.exchange");
  }

  // --- Injection ---

  @Autowired
  HotKey hotKey;

  @Autowired
  StringRedisTemplate redisTemplate;

  @Autowired
  RabbitTemplate rabbitTemplate;

  @Autowired
  ConnectionFactory connectionFactory;

  // --- Benchmark config ---

  static final int HOT_KEY_COUNT = 10_000;
  static final int COLD_KEY_COUNT = 40_000;
  static final int TOTAL_KEYS = HOT_KEY_COUNT + COLD_KEY_COUNT;
  static final int THREAD_COUNT = 8;
  static final int OPS_PER_THREAD = 2_500;
  static final int HARD_TTL = 600_000;
  static final int SOFT_TTL = 300_000;
  static final int WORKER_HOT_THRESHOLD = 50;

  // --- Simulated Worker state ---

  HeavyKeeper workerTopK;
  SimpleMessageListenerContainer workerContainer;
  AtomicLong workerDecisionsSent = new AtomicLong(0);
  AtomicLong workerDecisionsFailed = new AtomicLong(0);
  AtomicLong workerMessagesReceived = new AtomicLong(0);
  AtomicLong workerDecisionVersion = new AtomicLong(System.currentTimeMillis());
  final List<Long> decisionPropagationNs = Collections.synchronizedList(new ArrayList<>());

  // --- App-2 simulation state ---

  final List<Long> syncPropagationNs = Collections.synchronizedList(new ArrayList<>());

  // --- Phase metrics ---

  static final String REPORT_EXCHANGE = "hotkey.report.exchange";
  static final String SYNC_EXCHANGE = "hotkey.sync.exchange";
  static final String BROADCAST_EXCHANGE = "hotkey.broadcast.exchange";

  // ================================================

  /** Runs warmup, hot-read, worker decision, cross-instance sync, and combined stress phases and writes a JSON report. */
  @Test
  void fullChainBenchmark() throws Exception {
    declareReportExchange();

    List<Map<String, Object>> phases = new ArrayList<>();

    phases.add(phaseWarmup());
    phases.add(phaseHotRead());
    startWorker();
    phases.add(phaseWorkerDecision());
    phases.add(phaseCrossInstanceSync());
    phases.add(phaseCombinedStress());
    stopWorker();

    long totalOps = phases.stream()
        .mapToLong(p -> ((Number) p.get("totalOps")).longValue()).sum();
    double totalMs = phases.stream()
        .mapToDouble(p -> ((Number) p.get("durationMs")).doubleValue()).sum();

    Map<String, Object> report = new HashMap<>();
    report.put("testName", "HotKey Multi-Instance Distributed Benchmark");
    report.put("timestamp", Instant.now().toString());
    report.put("config", Map.of(
        "hotKeyCount", HOT_KEY_COUNT,
        "coldKeyCount", COLD_KEY_COUNT,
        "threadCount", THREAD_COUNT,
        "opsPerThread", OPS_PER_THREAD,
        "hardTtlMs", HARD_TTL,
        "softTtlMs", SOFT_TTL,
        "workerHotThreshold", WORKER_HOT_THRESHOLD
    ));
    report.put("phases", phases);
    report.put("summary", Map.of(
        "totalOps", totalOps,
        "totalDurationMs", Math.round(totalMs * 100.0) / 100.0,
        "overallThroughputOpsPerSec", Math.round(totalOps / (totalMs / 1000.0))
    ));

    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    String json = mapper.writeValueAsString(report);

    Path reportPath = Path.of("src", "test", "resources", "testresult",
        "benchmark-multi-instance-" + Instant.now().toString().replace(":", "-") + ".json");
    Files.createDirectories(reportPath.getParent());
    Files.writeString(reportPath, json);

    log.info("\n=========================================\nBenchmark report -> {}\n{}",
        reportPath.toAbsolutePath(), json);

    long totalErrors = phases.stream()
        .mapToLong(p -> ((Number) p.get("errors")).longValue()).sum();
    assertThat(totalErrors).as("Total benchmark errors (≤5 tolerated)").isLessThanOrEqualTo(5);
  }

  // ================================================
  // Phase 1: Warmup
  // ================================================

  /** Seeds all hot and cold keys into Redis and L1. */
  private Map<String, Object> phaseWarmup() throws Exception {
    log.info("══════ Phase 1: WARMUP ══════");
    log.info("Putting {} keys via app-1 (L1 + Redis) ...", TOTAL_KEYS);
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);

    runConcurrent(THREAD_COUNT, TOTAL_KEYS, (tid, i, key, value) -> {
      hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    }, errors);

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    log.info("Warmup: {} keys in {} ms ({} ops/sec), errors={}",
        TOTAL_KEYS, String.format("%.1f", ms),
        String.format("%.0f", TOTAL_KEYS / (ms / 1000.0)), errors.get());

    return phaseMap("warmup", TOTAL_KEYS, ms, errors.get());
  }

  // ================================================
  // Phase 2: Hot read (app-1 only)
  // ================================================

  /** Reads hot keys via app-1 and measures L1 hit rate. */
  private Map<String, Object> phaseHotRead() throws Exception {
    log.info("══════ Phase 2: APP-1 HOT READ ══════");
    log.info("Reading {} hot keys via app-1 should hit L1 ...", HOT_KEY_COUNT);
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    AtomicLong l2Calls = new AtomicLong(0);
    List<List<Long>> threadLats = initLatencyBuckets(THREAD_COUNT);

    runReadWithLatency(THREAD_COUNT, OPS_PER_THREAD, HOT_KEY_COUNT, 0,
        (tid, i, key, value) -> {
          hotKey.get(key, () -> {
            l2Calls.incrementAndGet();
            return redisTemplate.opsForValue().get(key);
          }, HARD_TTL, SOFT_TTL);
        }, errors, threadLats);

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    long totalOps = (long) THREAD_COUNT * OPS_PER_THREAD;
    long l1Hits = totalOps - l2Calls.get();
    double[] lats = flattenLatencies(threadLats);

    Map<String, Object> phase = phaseWithLatency("app-1-hot-read", totalOps, ms,
        l1Hits, l2Calls.get(), errors.get(), lats);
    logPhase(phase);
    return phase;
  }

  // ================================================
  // Phase 3: Worker decision flow
  // ================================================

  /** Triggers reports via reads, simulates Worker consuming and issuing HOT decisions. */
  private Map<String, Object> phaseWorkerDecision() throws Exception {
    log.info("══════ Phase 3: WORKER DECISION ══════");
    log.info("Triggering reports via reads → Worker consumes → HOT decisions → app-1 promotes ...");
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    AtomicLong l2Calls = new AtomicLong(0);
    List<List<Long>> threadLats = initLatencyBuckets(THREAD_COUNT);
    long prevDecisions = workerDecisionsSent.get();
    long prevReceived = workerMessagesReceived.get();

    runReadWithLatency(THREAD_COUNT, OPS_PER_THREAD, HOT_KEY_COUNT, 0,
        (tid, i, key, value) -> {
          hotKey.get(key, () -> {
            l2Calls.incrementAndGet();
            return redisTemplate.opsForValue().get(key);
          }, HARD_TTL, SOFT_TTL);
        }, errors, threadLats);

    Thread.sleep(3000);

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    long totalOps = (long) THREAD_COUNT * OPS_PER_THREAD;
    long l1Hits = totalOps - l2Calls.get();
    double[] lats = flattenLatencies(threadLats);

    Map<String, Object> phase = phaseWithLatency("worker-decision", totalOps, ms,
        l1Hits, l2Calls.get(), errors.get(), lats);
    phase.put("workerMessagesReceived", workerMessagesReceived.get() - prevReceived);
    phase.put("workerDecisionsSent", workerDecisionsSent.get() - prevDecisions);
    phase.put("workerDecisionsFailed", workerDecisionsFailed.get());

    synchronized (decisionPropagationNs) {
      if (!decisionPropagationNs.isEmpty()) {
        double[] propMs = decisionPropagationNs.stream()
            .mapToLong(Long::longValue).mapToDouble(ns -> ns / 1_000_000.0).toArray();
        Arrays.sort(propMs);
        phase.put("decisionPropagationLatencyMs", Map.of(
            "p50", round3(percentile(propMs, 50)),
            "p90", round3(percentile(propMs, 90)),
            "p99", round3(percentile(propMs, 99)),
            "samples", propMs.length
        ));
      }
    }
    logPhase(phase);
    return phase;
  }

  // ================================================
  // Phase 4: Cross-instance sync (simulate app-2)
  // ================================================

  /** Simulates app-2 sending INVALIDATE broadcasts and measures propagation latency. */
  private Map<String, Object> phaseCrossInstanceSync() throws Exception {
    log.info("══════ Phase 4: CROSS-INSTANCE SYNC ══════");
    log.info("Simulating app-2 sending INVALIDATE broadcasts to {} keys ...", HOT_KEY_COUNT);
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    syncPropagationNs.clear();
    AtomicLong syncCount = new AtomicLong(0);

    int batchSize = 100;
    int batches = HOT_KEY_COUNT / batchSize;
    for (int b = 0; b < batches; b++) {
      List<String> batchKeys = new ArrayList<>(batchSize);
      for (int k = 0; k < batchSize; k++) {
        int idx = b * batchSize + k;
        String key = keyFor(idx);
        batchKeys.add(key);
      }
      CountDownLatch latch = new CountDownLatch(batchSize);
      for (String key : batchKeys) {
        long sendTime = System.nanoTime();
        MessageProperties props = new MessageProperties();
        props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
        props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
        props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
        Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
        try {
          rabbitTemplate.send(SYNC_EXCHANGE, "", msg);
          syncCount.incrementAndGet();
          int finalIdx = b * batchSize + batchKeys.indexOf(key);
          Thread pollThread = new Thread(() -> {
            try {
              long deadline = System.nanoTime() + 5_000_000_000L;
              while (System.nanoTime() < deadline) {
                if (hotKey.peek(keyFor(finalIdx)).isEmpty()) {
                  syncPropagationNs.add(System.nanoTime() - sendTime);
                  break;
                }
                Thread.sleep(1);
              }
            } catch (Exception ignored) {}
            latch.countDown();
          });
          pollThread.setDaemon(true);
          pollThread.start();
        } catch (Exception e) {
          errors.incrementAndGet();
          latch.countDown();
        }
      }
      latch.await(10, TimeUnit.SECONDS);
    }

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    Map<String, Object> phase = phaseMap("cross-instance-sync", syncCount.get(), ms, errors.get());

    synchronized (syncPropagationNs) {
      if (!syncPropagationNs.isEmpty()) {
        double[] propMs = syncPropagationNs.stream()
            .mapToLong(Long::longValue).mapToDouble(ns -> ns / 1_000_000.0).toArray();
        Arrays.sort(propMs);
        phase.put("syncPropagationLatencyMs", Map.of(
            "p50", round3(percentile(propMs, 50)),
            "p90", round3(percentile(propMs, 90)),
            "p99", round3(percentile(propMs, 99)),
            "max", round3(propMs[propMs.length - 1]),
            "samples", propMs.length
        ));
      }
    }

    logPhase(phase);
    return phase;
  }

  // ================================================
  // Phase 5: Combined stress (all paths simultaneously)
  // ================================================

  /** Runs a combined workload (70% read, 15% write, 10% sync invalidate, 5% Worker decision). */
  private Map<String, Object> phaseCombinedStress() throws Exception {
    log.info("══════ Phase 5: COMBINED STRESS ══════");
    log.info("70% read + 15% write + 10% app-2 invalidate + 5% Worker decision ...");
    long t0 = System.nanoTime();
    AtomicInteger errors = new AtomicInteger(0);
    AtomicLong l2Calls = new AtomicLong(0);
    AtomicLong writes = new AtomicLong(0);
    AtomicLong syncInvals = new AtomicLong(0);
    AtomicLong workerDecs = new AtomicLong(0);
    List<List<Long>> threadLats = initLatencyBuckets(THREAD_COUNT);

    CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
    ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

    for (int t = 0; t < THREAD_COUNT; t++) {
      int tid = t;
      List<Long> myLats = threadLats.get(tid);
      pool.submit(() -> {
        try {
          for (int i = 0; i < OPS_PER_THREAD; i++) {
            int op = i % 20;
            long ns;
            if (op < 14) {
              int idx = (tid * OPS_PER_THREAD + i) % TOTAL_KEYS;
              String key = keyFor(idx);
              ns = measure(() ->
                  hotKey.get(key, () -> {
                    l2Calls.incrementAndGet();
                    return redisTemplate.opsForValue().get(key);
                  }, HARD_TTL, SOFT_TTL)
              );
            } else if (op < 17) {
              String k = "stress:write:" + UUID.randomUUID();
              String v = "w-" + tid + "-" + i;
              ns = measure(() ->
                  hotKey.putThrough(k, v, () -> redisTemplate.opsForValue().set(k, v))
              );
              writes.incrementAndGet();
            } else if (op < 19) {
              int idx = (tid * OPS_PER_THREAD + i) % HOT_KEY_COUNT;
              String key = keyFor(idx);
              MessageProperties props = new MessageProperties();
              props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
              props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
              props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
              Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
              ns = measure(() -> rabbitTemplate.send(SYNC_EXCHANGE, "", msg));
              syncInvals.incrementAndGet();
            } else {
              int idx = (tid * OPS_PER_THREAD + i) % HOT_KEY_COUNT;
              String key = keyFor(idx);
              long dv = workerDecisionVersion.incrementAndGet();
              MessageProperties props = new MessageProperties();
              props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
              props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
              props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
              Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
              ns = measure(() ->
                  rabbitTemplate.send(BROADCAST_EXCHANGE, "hot.integration-test", msg));
              workerDecisionsSent.incrementAndGet();
              workerDecs.incrementAndGet();
            }
            myLats.add(ns);
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Combined stress error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();

    double ms = (System.nanoTime() - t0) / 1_000_000.0;
    long totalOps = (long) THREAD_COUNT * OPS_PER_THREAD;
    long reads = totalOps - writes.get() - syncInvals.get() - workerDecs.get();
    long l1Hits = reads - l2Calls.get();
    double[] lats = flattenLatencies(threadLats);

    Map<String, Object> phase = phaseWithLatency("combined-stress", totalOps, ms,
        l1Hits, l2Calls.get(), errors.get(), lats);
    phase.put("reads", reads);
    phase.put("writes", writes.get());
    phase.put("syncInvalidations", syncInvals.get());
    phase.put("workerDecisions", workerDecs.get());

    logPhase(phase);
    return phase;
  }

  // ================================================
  // Simulated Worker
  // ================================================

  /** Starts a simulated Worker using HeavyKeeper to consume reports and issue HOT decisions. */
  private void startWorker() {
    log.info("Starting simulated Worker (HeavyKeeper + decision loop) ...");
    workerTopK = new HeavyKeeper(200, 20000, 10, 0.9, 10);

    String queueName = "benchmark.worker.queue." + UUID.randomUUID();

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
    workerContainer.setConcurrentConsumers(1);
    workerContainer.setMaxConcurrentConsumers(2);

    workerContainer.setMessageListener((MessageListener) message -> {
      try {
        ObjectInputStream ois = new ObjectInputStream(
            new ByteArrayInputStream(message.getBody()));
        ReportMessage report = (ReportMessage) ois.readObject();
        ois.close();
        workerMessagesReceived.incrementAndGet();

        for (Map.Entry<String, Long> entry : report.counts().entrySet()) {
          String key = entry.getKey();
          long count = entry.getValue();
          workerTopK.addDirect(key, (int) Math.min(count, Integer.MAX_VALUE));
          long hotCount = workerTopK.contains(key)
              ? workerTopK.list().stream()
                  .filter(item -> item.key().equals(key))
                  .mapToLong(io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.Item::count)
                  .findFirst().orElse(0)
              : 0;
          if (hotCount >= WORKER_HOT_THRESHOLD) {
            long dv = workerDecisionVersion.incrementAndGet();
            long sendTime = System.nanoTime();
            MessageProperties props = new MessageProperties();
            props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
            props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
            props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
            Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
            rabbitTemplate.send(BROADCAST_EXCHANGE, "hot.integration-test", msg);
            workerDecisionsSent.incrementAndGet();
            decisionPropagationNs.add(System.nanoTime() - sendTime);
          }
        }
      } catch (Exception e) {
        workerDecisionsFailed.incrementAndGet();
        log.warn("Worker processing error: {}", e.getMessage());
      }
    });
    workerContainer.start();
    log.info("Simulated Worker started, listening on queue: {}", queueName);
  }

  /** Stops the simulated Worker listener container. */
  private void stopWorker() {
    if (workerContainer != null && workerContainer.isRunning()) {
      workerContainer.stop();
      log.info("Simulated Worker stopped");
    }
  }

  /**
   * Declares the report direct exchange idempotently.
   */
  private void declareReportExchange() {
    rabbitTemplate.execute(channel -> {
      channel.exchangeDeclare(REPORT_EXCHANGE, "direct", true);
      return null;
    });
  }

  // ================================================
  // Helpers
  // ================================================

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
   * @param sorted sorted latency values (milliseconds, ascending)
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
   * Rounds a double to 3 decimal places.
   *
   * @param v the value to round
   * @return the value rounded to 3 decimal places
   */
  private static double round3(double v) {
    return Math.round(v * 1000.0) / 1000.0;
  }

  /**
   * Initialises per-thread latency buckets for the given thread count.
   *
   * @param n number of threads
   * @return a list of n empty latency lists
   */
  private static List<List<Long>> initLatencyBuckets(int n) {
    List<List<Long>> buckets = new ArrayList<>(n);
    for (int i = 0; i < n; i++) buckets.add(new ArrayList<>());
    return buckets;
  }

  /**
   * Flattens per-thread latency buckets into a single sorted array (ms).
   *
   * @param buckets per-thread latency lists (nanoseconds)
   * @return sorted array of latency values in milliseconds
   */
  private static double[] flattenLatencies(List<List<Long>> buckets) {
    double[] arr = buckets.stream()
        .flatMapToLong(list -> list.stream().mapToLong(Long::longValue))
        .mapToDouble(ns -> ns / 1_000_000.0)
        .toArray();
    Arrays.sort(arr);
    return arr;
  }

  /**
   * Creates a basic phase result map with name, ops, duration, and throughput.
   *
   * @param name     the phase name
   * @param totalOps total operations performed
   * @param ms       elapsed time in milliseconds
   * @param errors   error count
   * @return a phase result map with basic metrics
   */
  private static Map<String, Object> phaseMap(
      String name, long totalOps, double ms, long errors) {
    Map<String, Object> p = new HashMap<>();
    p.put("name", name);
    p.put("totalOps", totalOps);
    p.put("durationMs", Math.round(ms * 100.0) / 100.0);
    p.put("throughputOpsPerSec", Math.round(totalOps / (ms / 1000.0)));
    p.put("errors", errors);
    return p;
  }

  /**
   * Creates a phase result map including latency percentiles and L1/L2 hit rates.
   *
   * @param name     the phase name
   * @param totalOps total operations performed
   * @param ms       elapsed time in milliseconds
   * @param l1Hits   number of L1 cache hits
   * @param l2Calls  number of L2 (Redis) calls
   * @param errors   error count
   * @param lats     sorted latency values in milliseconds
   * @return a phase result map with latency percentiles and hit rates
   */
  private static Map<String, Object> phaseWithLatency(
      String name, long totalOps, double ms, long l1Hits, long l2Calls,
      long errors, double[] lats) {
    Map<String, Object> p = phaseMap(name, totalOps, ms, errors);
    p.put("l1Hits", l1Hits);
    p.put("l2Calls", l2Calls);
    p.put("l1HitRate", totalOps > 0
        ? Math.round((double) l1Hits / totalOps * 10000.0) / 10000.0 : 0);

    Map<String, Object> latMap = new HashMap<>();
    latMap.put("p50", round3(percentile(lats, 50)));
    latMap.put("p90", round3(percentile(lats, 90)));
    latMap.put("p99", round3(percentile(lats, 99)));
    latMap.put("p999", round3(percentile(lats, 99.9)));
    latMap.put("max", lats.length > 0 ? round3(lats[lats.length - 1]) : 0);
    latMap.put("mean", lats.length > 0
        ? round3(Arrays.stream(lats).average().orElse(0)) : 0);
    latMap.put("samples", lats.length);
    p.put("latencyMs", latMap);
    return p;
  }

  /** Logs a phase summary line with name, ops, duration, and errors. */
  private static void logPhase(Map<String, Object> phase) {
    log.info("Phase [{}] done: {} ops in {} ms, errors={}",
        phase.get("name"), phase.get("totalOps"),
        String.format("%.1f", phase.get("durationMs")), phase.get("errors"));
  }

  // --- Helpers: measure ---

  /** A runnable that may throw any checked exception. */
  @FunctionalInterface
  interface CheckedRunnable {
    void run() throws Throwable;
  }

  /** Measures execution time of a runnable in nanoseconds. */
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

  // --- Helpers: concurrent runner (no latency) ---

  /** A concurrent key-value operation that may throw any checked exception. */
  @FunctionalInterface
  interface Op {
    void run(int tid, int i, String key, String value) throws Throwable;
  }

  /** Runs a concurrent key-based workload across a fixed thread pool. */
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
            op.run(tid, i, keyFor(i), "v-" + i);
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

  // --- Helpers: read phase with latency ---

  /** Runs a concurrent read workload with per-operation latency tracking. */
  private static void runReadWithLatency(
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
            long ns = measure(() -> op.run(tid, iter, key, null));
            myLats.add(ns);
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Read phase error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(120, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
  }
}
