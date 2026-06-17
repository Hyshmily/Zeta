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
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.model.HotKeyDecision;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
import io.github.hyshmily.hotkey.sync.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.WorkerHeartbeatMessage;
import io.github.hyshmily.hotkey.reporting.ReportMessage;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
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
 * Extreme-parameter variant of PropagationDelayIT.
 *
 * <p>All parameters tuned to minimum theoretical values:
 * <ul>
 *   <li>report-interval-ms = 1 (was 100) — near-instant flush</li>
 *   <li>warmup-jitter-ms = 0 (was 100) — no thundering-herd protection</li>
 *   <li>confirm-duration-ms = 0 (was 300) — state machine disabled</li>
 *   <li>sliding-window slices at hardware minimum</li>
 * </ul>
 */
@Testcontainers
@Tag("docker")
@Tag("benchmark")
@Tag("propagation")
@Tag("extreme")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PropagationDelayExtremeIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(PropagationDelayExtremeIT.class);

  private static final List<PhaseMetrics> ALL_PHASES = new ArrayList<>();

  // -- Containers --

  @Container
  static GenericContainer<?> redis = new GenericContainer<>(
      DockerImageName.parse("redis:7-alpine"))
    .withExposedPorts(6379)
    .withStartupTimeout(Duration.ofMinutes(1));

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
    // -- Extreme parameters --
    r.add("hotkey.local.report-interval-ms", () -> "1");          // default 100 → 1
    r.add("hotkey.worker-listener.warmup-jitter-ms", () -> "0");  // default 100 → 0
    r.add("hotkey.sync.warmup-jitter-ms", () -> "0");             // default 100 → 0
    r.add("hotkey.worker.sliding-window.duration-ms", () -> "100"); // default 1000 → 100
    r.add("hotkey.worker.sliding-window.slices", () -> "100");      // default 10 → 100 (1ms per slice)
  }

  // -- Injection --

  @Autowired
  HotKey hotKey;

  @Autowired
  StringRedisTemplate redisTemplate;

  @Autowired
  RabbitTemplate rabbitTemplate;

  @Autowired
  ConnectionFactory connectionFactory;

  @Autowired
  ClusterHealthView workerHealthMonitor;

  // -- Config --

  static final String SYNC_EXCHANGE = "hotkey.sync.exchange";
  static final String BROADCAST_EXCHANGE = "hotkey.broadcast.exchange";
  static final int HARD_TTL = 600_000;
  static final int SOFT_TTL = 300_000;

  // ================================================================================
  // Metrics
  // ================================================================================

  /**
   * Collects latency, throughput, and error metrics for a single test phase.
   * Computes P50/P95/P99 latency percentiles, throughput (ops/s), and a latency
   * histogram. Supports JSON serialisation for the final aggregated report.
   */
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
    long[] sortedLatencies;

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

    /**
     * Creates a new metrics collector for the named phase.
     *
     * @param name human-readable phase identifier (used in logs and JSON report)
     */
    PhaseMetrics(String name) {
      this.name = name;
    }

    /**
     * Records a single operation latency.
     *
     * @param nanos measured elapsed time in nanoseconds
     */
    void recordLatency(long nanos) {
      latencies.add(nanos);
    }

    /** Increments the total operation counter by one. */
    void recordOp() {
      ops.incrementAndGet();
    }

    /**
     * Finalises metrics: computes duration, percentiles, and throughput.
     *
     * @return this instance for method chaining
     */
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
      return this;
    }

    /** Logs a one-line summary of this phase's metrics via SLF4J. */
    void logSummary() {
      log.info(
        "▸ {}: {} ops in {}ms | {} err | {} ops/s | P50={}ms P95={}ms P99={}ms {}",
        name, totalOps, durationMs, errorCount, Math.round(opsPerSecond),
        String.format("%.3f", p50Ms), String.format("%.3f", p95Ms), String.format("%.3f", p99Ms),
        custom.isEmpty() ? "" : custom.toString());
    }

    /**
     * Serialises this phase's metrics and current JVM system metrics into a JSON node.
     *
     * @param mapper shared Jackson ObjectMapper
     * @return populated ObjectNode with latency, throughput, and system metrics
     */
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

      ArrayNode histo = histogramToJson(mapper);
      n.set("latencyHistogram", histo);

      ObjectNode cust = mapper.createObjectNode();
      custom.forEach((k, v) -> {
        if (v instanceof Number num) cust.put(k, num.doubleValue());
        else cust.put(k, v.toString());
      });
      n.set("custom", cust);

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
  }

  // ================================================================================
  // Report Writer
  // ================================================================================

  /**
   * Aggregates all phase metrics into a single JSON report file written to
   * {@code src/test/resources/testresult/propagation-delay-extreme-&lt;timestamp&gt;.json}.
   * Includes per-phase latency histograms, global system metrics (heap, GC, threads),
   * and a top-level summary.
   */
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
    root.put("description",
        "Extreme-parameter propagation delay: report-interval=1ms, warmup-jitter=0, confirm-duration=0");

    ArrayNode phases = mapper.createArrayNode();
    for (PhaseMetrics m : ALL_PHASES) {
      phases.add(m.toJson(mapper));
    }
    root.set("phases", phases);

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
        "propagation-delay-extreme-" + Instant.now().toString().replace(":", "-") + ".json");
    Files.createDirectories(reportPath.getParent());
    mapper.writeValue(reportPath.toFile(), root);
    log.info("Extreme propagation delay report written to {}", reportPath.toAbsolutePath());
  }

  // ================================================================================
  //  Main Test
  // ================================================================================

  /**
   * Measures propagation delays under extreme settings: report-interval-ms=1, warmup-jitter-ms=0,
   * confirm-duration-ms=0 for the tightest possible Worker decision pipeline timing.
   */
  @Test
  void measurePropagationDelaysExtreme() throws Exception {
    log.info("====== Extreme Propagation Delay Measurements ======");
    log.info("  report-interval-ms=1 | warmup-jitter-ms=0 | confirm-duration-ms=0");

    warmupConnections();

    phaseRedisGet();
    phaseRedisSet();
    phaseAmqpPublish();
    phaseAmqpE2e();
    phaseL1Hit();
    phaseL2ColdGet();
    // Phase 7: Worker decision pipeline — warmup-jitter=0 takes effect
    phaseWorkerDecisionPipeline();
    // Phase 8: State machine pipeline — confirmDurationMs=0 → instant promotion
    phaseStateMachinePipelineZeroConfirm();
    // Phase 9A: Full chain, SM 0 confirm (test-only no-SM path)
    phaseFullChainNoSM();
    // Phase 9B: Full chain with zero-confirm state machine
    phaseFullChainWithSM();

    long totalErrors = ALL_PHASES.stream().mapToInt(m -> m.errorCount).sum();
    assertThat(totalErrors).as("Total errors across all phases").isZero();
    log.info("====== Extreme propagation delay tests PASSED: {} phases, {} ops, {} errors ======",
        ALL_PHASES.size(),
        ALL_PHASES.stream().mapToLong(m -> m.totalOps).sum(),
        totalErrors);
  }

  // ================================================================================
  // Warmup
  // ================================================================================

  private void warmupConnections() {
    log.info("Warming up connections ...");
    redisTemplate.opsForValue().set("prop:warmup", "1");
    redisTemplate.opsForValue().get("prop:warmup");
    redisTemplate.delete("prop:warmup");
    String wq = "prop.warmup.queue." + UUID.randomUUID();
    RabbitAdmin admin = new RabbitAdmin(rabbitTemplate);
    Queue warmupQ = new Queue(wq, false, false, true);
    admin.declareQueue(warmupQ);
    FanoutExchange warmupEx = new FanoutExchange("prop.warmup.exchange", false, true);
    admin.declareExchange(warmupEx);
    admin.declareBinding(BindingBuilder.bind(warmupQ).to(warmupEx));
    for (int i = 0; i < 10; i++) {
      rabbitTemplate.convertAndSend("prop.warmup.exchange", "", "warmup-" + i);
    }
    admin.deleteExchange("prop.warmup.exchange");
    log.info("Warmup complete");
  }

  // ================================================================================
  // Phase 1: Redis GET RTT
  // ================================================================================

  private void phaseRedisGet() throws Exception {
    PhaseMetrics m = new PhaseMetrics("redis_get_rtt");
    ALL_PHASES.add(m);
    log.info("====== Phase 1: REDIS GET RTT ======");
    int count = 10_000;
    String key = "prop:rget:key";
    redisTemplate.opsForValue().set(key, "x");
    for (int i = 0; i < count; i++) {
      long start = System.nanoTime();
      redisTemplate.opsForValue().get(key);
      m.recordLatency(System.nanoTime() - start);
      m.recordOp();
    }
    assertThat(m.errorCount).isZero();
    m.custom.put("key", key);
    m.custom.put("sampleCount", count);
    m.finish().logSummary();
  }

  // ================================================================================
  // Phase 2: Redis SET RTT
  // ================================================================================

  private void phaseRedisSet() throws Exception {
    PhaseMetrics m = new PhaseMetrics("redis_set_rtt");
    ALL_PHASES.add(m);
    log.info("====== Phase 2: REDIS SET RTT ======");
    int count = 5_000;
    for (int i = 0; i < count; i++) {
      long start = System.nanoTime();
      redisTemplate.opsForValue().set("prop:rset:k" + i, "v" + i);
      m.recordLatency(System.nanoTime() - start);
      m.recordOp();
    }
    assertThat(m.errorCount).isZero();
    m.custom.put("sampleCount", count);
    m.finish().logSummary();
  }

  // ================================================================================
  // Phase 3: AMQP Publish
  // ================================================================================

  private void phaseAmqpPublish() throws Exception {
    PhaseMetrics m = new PhaseMetrics("amqp_publish");
    ALL_PHASES.add(m);
    log.info("====== Phase 3: AMQP PUBLISH ======");
    String ex = "prop.pub.exchange." + UUID.randomUUID();
    RabbitAdmin admin = new RabbitAdmin(rabbitTemplate);
    FanoutExchange fex = new FanoutExchange(ex, false, true);
    admin.declareExchange(fex);
    int count = 10_000;
    byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
    MessageProperties props = new MessageProperties();
    for (int i = 0; i < count; i++) {
      long start = System.nanoTime();
      rabbitTemplate.send(ex, "", new Message(payload, props));
      m.recordLatency(System.nanoTime() - start);
      m.recordOp();
    }
    assertThat(m.errorCount).isZero();
    m.custom.put("sampleCount", count);
    m.custom.put("payloadBytes", payload.length);
    m.finish().logSummary();
  }

  // ================================================================================
  // Phase 4: AMQP End-to-End Delivery
  // ================================================================================

  private void phaseAmqpE2e() throws Exception {
    PhaseMetrics m = new PhaseMetrics("amqp_e2e_delivery");
    ALL_PHASES.add(m);
    log.info("====== Phase 4: AMQP E2E DELIVERY ======");
    String ex = "prop.e2e.exchange." + UUID.randomUUID();
    String q = "prop.e2e.queue." + UUID.randomUUID();
    RabbitAdmin admin = new RabbitAdmin(rabbitTemplate);
    Queue queue = new Queue(q, false, false, true);
    admin.declareQueue(queue);
    FanoutExchange fex = new FanoutExchange(ex, false, true);
    admin.declareExchange(fex);
    admin.declareBinding(BindingBuilder.bind(queue).to(fex));

    List<Long> deliveryLatencies = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch consumerReady = new CountDownLatch(1);
    SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
    container.setQueueNames(q);
    container.setAcknowledgeMode(AcknowledgeMode.AUTO);
    container.setConcurrentConsumers(2);
    container.setMessageListener((MessageListener) msg -> {
      MessageProperties mp = msg.getMessageProperties();
      Object sendNanosObj = mp.getHeader("sendTimeNanos");
      if (sendNanosObj instanceof Number n) {
        deliveryLatencies.add(System.nanoTime() - n.longValue());
      }
    });
    container.start();
    consumerReady.countDown();

    int count = 5_000;
    byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
    for (int i = 0; i < count; i++) {
      MessageProperties props = new MessageProperties();
      props.setHeader("sendTimeNanos", System.nanoTime());
      long start = System.nanoTime();
      rabbitTemplate.send(ex, "", new Message(payload, props));
      m.recordLatency(System.nanoTime() - start);
      m.recordOp();
    }
    Thread.sleep(2_000);
    container.stop();

    m.custom.put("deliverySamples", deliveryLatencies.size());
    if (!deliveryLatencies.isEmpty()) {
      long[] sorted = deliveryLatencies.stream().mapToLong(Long::longValue).sorted().toArray();
      int len = sorted.length;
      m.custom.put("deliveryP50Ms", sorted[len / 2] / 1_000_000.0);
      m.custom.put("deliveryP95Ms", sorted[(int) (len * 0.95)] / 1_000_000.0);
      m.custom.put("deliveryP99Ms", sorted[(int) (len * 0.99)] / 1_000_000.0);
      m.custom.put("deliveryMinMs", sorted[0] / 1_000_000.0);
      m.custom.put("deliveryMaxMs", sorted[len - 1] / 1_000_000.0);
    }
    assertThat(m.errorCount).isZero();
    m.custom.put("publishSamples", count);
    m.finish().logSummary();
  }

  // ================================================================================
  // Phase 5: HotKey L1 Hit
  // ================================================================================

  private void phaseL1Hit() throws Exception {
    PhaseMetrics m = new PhaseMetrics("hotkey_l1_hit");
    ALL_PHASES.add(m);
    log.info("====== Phase 5: HOTKEY L1 HIT ======");
    int count = 10_000;
    String key = "prop:l1hit:key";
    redisTemplate.opsForValue().set(key, "l1-hot");
    hotKey.putThrough(key, "l1-hot", () -> {});
    Thread.sleep(100);
    assertThat(hotKey.get(key, () -> null, HARD_TTL, SOFT_TTL)).isPresent();
    for (int i = 0; i < count; i++) {
      long start = System.nanoTime();
      hotKey.get(key, () -> null, HARD_TTL, SOFT_TTL);
      m.recordLatency(System.nanoTime() - start);
      m.recordOp();
    }
    assertThat(m.errorCount).isZero();
    m.custom.put("sampleCount", count);
    m.custom.put("key", key);
    m.finish().logSummary();
  }

  // ================================================================================
  // Phase 6: HotKey Cold Get
  // ================================================================================

  private void phaseL2ColdGet() throws Exception {
    PhaseMetrics m = new PhaseMetrics("hotkey_l1_miss");
    ALL_PHASES.add(m);
    log.info("====== Phase 6: HOTKEY COLD GET ======");
    int count = 5_000;
    List<String> keys = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String key = "prop:cold:k" + i + "-" + UUID.randomUUID();
      keys.add(key);
      redisTemplate.opsForValue().set(key, "cold-v" + i);
    }
    AtomicLong actualSupplierCalls = new AtomicLong(0);
    for (int i = 0; i < count; i++) {
      String key = keys.get(i);
      long start = System.nanoTime();
      hotKey.get(key, () -> {
        actualSupplierCalls.incrementAndGet();
        return redisTemplate.opsForValue().get(key);
      }, HARD_TTL, SOFT_TTL);
      m.recordLatency(System.nanoTime() - start);
      m.recordOp();
    }
    assertThat(m.errorCount).isZero();
    m.custom.put("sampleCount", count);
    m.custom.put("actualSupplierCalls", actualSupplierCalls.get());
    m.custom.put("supplierCallRatio",
        Math.round((double) actualSupplierCalls.get() / count * 10000.0) / 100.0);
    m.finish().logSummary();
  }

  // ================================================================================
  // Phase 7: Worker Decision Pipeline (warmup-jitter=0)
  // ================================================================================

  private void phaseWorkerDecisionPipeline() throws Exception {
    PhaseMetrics m = new PhaseMetrics("worker_decision_pipeline");
    ALL_PHASES.add(m);
    log.info("====== Phase 7: WORKER DECISION PIPELINE (jitter=0) ======");

    rabbitTemplate.execute(channel -> {
      try { channel.exchangeDeclarePassive(BROADCAST_EXCHANGE); }
      catch (Exception e) { channel.exchangeDeclare(BROADCAST_EXCHANGE, "fanout", true); }
      return null;
    });

    int count = 200;
    AtomicLong decisionVersion = new AtomicLong(System.currentTimeMillis());

    for (int i = 0; i < count; i++) {
      String key = "prop:worker:pipe:" + i + "-" + UUID.randomUUID();
      redisTemplate.opsForValue().set(key, "wpipe-v" + i);
      hotKey.invalidate(key);

      long dv = decisionVersion.incrementAndGet();
      MessageProperties props = new MessageProperties();
      props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
      props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
      props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
      Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);

      long start = System.nanoTime();
      rabbitTemplate.send(BROADCAST_EXCHANGE, "", msg);
      m.recordOp();

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      boolean promoted = false;
      while (System.nanoTime() < deadline) {
        if (hotKey.isLocalHotKey(key)) {
          promoted = true;
          break;
        }
        Thread.sleep(1);
      }

      if (promoted) {
        m.recordLatency(System.nanoTime() - start);
      } else {
        m.errors.incrementAndGet();
        log.warn("Worker decision timeout for key: {}", key);
      }
    }

    assertThat(m.errorCount).as("Worker decision timeouts").isLessThan(count / 10);
    m.custom.put("decisionSent", count);
    m.custom.put("promoted", count - m.errorCount);
    m.custom.put("promotionRate",
        Math.round((double) (count - m.errorCount) / count * 10000.0) / 100.0);
    m.finish().logSummary();
  }

  // ================================================================================
  // Phase 8: State Machine Pipeline (confirmDurationMs=0 → instant promotion)
  // ================================================================================

  private void phaseStateMachinePipelineZeroConfirm() throws Exception {
    PhaseMetrics m = new PhaseMetrics("state_machine_pipeline_zero_confirm");
    ALL_PHASES.add(m);
    log.info("====== Phase 8: STATE MACHINE (confirmDurationMs=0) ======");

    int count = 10;
    int confirmWindows = 0;
    int coolWindows = 0;
    int preCoolGraceWindows = 0;

    rabbitTemplate.execute(channel -> {
      try { channel.exchangeDeclarePassive(BROADCAST_EXCHANGE); }
      catch (Exception e) { channel.exchangeDeclare(BROADCAST_EXCHANGE, "fanout", true); }
      return null;
    });

    HotKeyStateMachine sm = new HotKeyStateMachine(confirmWindows, coolWindows, preCoolGraceWindows);
    AtomicLong decisionVersion = new AtomicLong(System.currentTimeMillis());

    List<String> keys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String key = "prop:smpipe:" + i + "-" + UUID.randomUUID();
      keys.add(key);
      redisTemplate.opsForValue().set(key, "sm-v" + i);
      hotKey.invalidate(key);
    }

    ExecutorService pool = Executors.newFixedThreadPool(count);
    CountDownLatch latch = new CountDownLatch(count);

    for (String key : keys) {
      pool.submit(() -> {
        try {
          long startTotal = System.nanoTime();
          // confirmDurationMs=0: evaluate once is enough
          sm.evaluate(key, true);
          // Immediately send HOT decision
          long dv = decisionVersion.incrementAndGet();
          MessageProperties props = new MessageProperties();
          props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
          props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
          props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
          Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
          rabbitTemplate.send(BROADCAST_EXCHANGE, "", msg);
          m.recordOp();

          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
          boolean promoted = false;
          while (System.nanoTime() < deadline) {
            if (hotKey.isLocalHotKey(key)) {
              promoted = true;
              break;
            }
            Thread.sleep(1);
          }

          if (promoted) {
            m.recordLatency(System.nanoTime() - startTotal);
          } else {
            m.errors.incrementAndGet();
            log.warn("Zero-confirm SM promotion timeout for key: {}", key);
          }
        } catch (Exception e) {
          m.errors.incrementAndGet();
          log.error("Zero-confirm SM pipeline error for key: {}", key, e);
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(30, TimeUnit.SECONDS);
    pool.shutdown();

    assertThat(m.errorCount).as("SM promotion timeouts").isLessThan(count / 10);
    m.custom.put("decisionSent", count);
    m.custom.put("promoted", count - m.errorCount);
    m.custom.put("promotionRate",
        Math.round((double) (count - m.errorCount) / count * 10000.0) / 100.0);
    m.custom.put("confirmWindows", confirmWindows);
    m.custom.put("stateMachineConfig", "confirmDurationMs=0");
    m.finish().logSummary();
  }

  // ================================================================================
  // Phase 9A: Full Chain (No SM)
  // ================================================================================

  private void phaseFullChainNoSM() throws Exception {
    phaseFullChain(false);
  }

  // ================================================================================
  // Phase 9B: Full Chain with Zero-Confirm SM
  // ================================================================================

  private void phaseFullChainWithSM() throws Exception {
    phaseFullChain(true);
  }

  private void phaseFullChain(boolean withStateMachine) throws Exception {
    PhaseMetrics m = new PhaseMetrics(withStateMachine ? "full_chain_with_sm_zero_confirm" : "full_chain_without_sm");
    ALL_PHASES.add(m);
    log.info(
      "====== Phase 9{}: FULL CHAIN (extreme) ======",
      withStateMachine ? "B (w/ zero-confirm SM)" : "A (w/o SM)");

    int count = 10;
    int confirmWindows = 0;
    String appName = "integration-test";

    rabbitTemplate.execute(channel -> {
      try { channel.exchangeDeclarePassive(BROADCAST_EXCHANGE); }
      catch (Exception e) { channel.exchangeDeclare(BROADCAST_EXCHANGE, "fanout", true); }
      return null;
    });

    workerHealthMonitor.onHeartbeat(new WorkerHeartbeatMessage("sim-node", 1, System.currentTimeMillis(), 0, 0.0, true, 0, 0, 0, 0, 0));
    ScheduledExecutorService heartbeatRefresher = Executors.newSingleThreadScheduledExecutor();
    heartbeatRefresher.scheduleAtFixedRate(
      () -> workerHealthMonitor.onHeartbeat(new WorkerHeartbeatMessage("sim-node", 1, System.currentTimeMillis(), 0, 0.0, true, 0, 0, 0, 0, 0)),
      3, 3, TimeUnit.SECONDS);

    String reportQName = "test.fc.extreme." + UUID.randomUUID();
    RabbitAdmin admin = new RabbitAdmin(rabbitTemplate);
    Queue reportQ = new Queue(reportQName, false, false, true);
    admin.declareQueue(reportQ);
    admin.declareBinding(
      BindingBuilder.bind(reportQ)
        .to(new DirectExchange("hotkey.report.exchange"))
        .with("report." + appName + ".0"));

    HotKeyStateMachine sm = withStateMachine
      ? new HotKeyStateMachine(confirmWindows, 0, 0)
      : null;
    AtomicLong decisionVersion = new AtomicLong(System.currentTimeMillis());
    ObjectMapper mapper = new ObjectMapper();
    Set<String> pending = ConcurrentHashMap.newKeySet();

    SimpleMessageListenerContainer reportContainer =
      new SimpleMessageListenerContainer(connectionFactory);
    reportContainer.setQueueNames(reportQName);
    reportContainer.setAcknowledgeMode(AcknowledgeMode.AUTO);
    reportContainer.setMessageListener((MessageListener) msg -> {
      try {
        ReportMessage report = mapper.readValue(msg.getBody(), ReportMessage.class);
        for (Map.Entry<String, Long> entry : report.counts().entrySet()) {
          String key = entry.getKey();
          if (!pending.contains(key)) continue;

          boolean shouldBroadcast;
          if (withStateMachine) {
            shouldBroadcast =
              sm.evaluate(key, entry.getValue() > 0).type()
                == HotKeyDecision.DecisionType.HOT;
          } else {
            shouldBroadcast = true;
          }

          if (shouldBroadcast) {
            long dv = decisionVersion.incrementAndGet();
            MessageProperties props = new MessageProperties();
            props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
            props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, dv);
            props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
            Message bcast = new Message(
              key.getBytes(StandardCharsets.UTF_8), props);
            rabbitTemplate.send(BROADCAST_EXCHANGE, "", bcast);
            m.recordOp();
          }
        }
      } catch (Exception e) {
        log.error("Report processing error in Phase 9", e);
      }
    });
    reportContainer.start();

    List<String> keys = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String key =
        "prop:fc:" + (withStateMachine ? "sm" : "nosm") + ":e" + i + "-" + UUID.randomUUID();
      keys.add(key);
      redisTemplate.opsForValue().set(key, "fc-v" + i);
      hotKey.invalidate(key);
      pending.add(key);
    }

    ExecutorService pool = Executors.newFixedThreadPool(count);
    CountDownLatch latch = new CountDownLatch(count);

    for (String key : keys) {
      pool.submit(() -> {
        try {
          long start = System.nanoTime();
          while (true) {
            if (Thread.interrupted()) break;
            hotKey.get(key, () -> redisTemplate.opsForValue().get(key), HARD_TTL, SOFT_TTL);
            if (hotKey.isLocalHotKey(key)) {
              m.recordLatency(System.nanoTime() - start);
              pending.remove(key);
              break;
            }
            long elapsedSec =
              TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
            if (elapsedSec > (withStateMachine ? 30 : 15)) {
              m.errors.incrementAndGet();
              log.warn("Full chain timeout for key: {}", key);
              pending.remove(key);
              break;
            }
            Thread.sleep(1);
          }
        } catch (Exception e) {
          m.errors.incrementAndGet();
          log.error("Full chain error for key: {}", key, e);
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(withStateMachine ? 45 : 25, TimeUnit.SECONDS);
    pool.shutdown();
    reportContainer.stop();
    heartbeatRefresher.shutdown();

    assertThat(m.errors.get())
      .as("Full chain timeouts (" + (withStateMachine ? "w/ SM zero confirm" : "w/o SM") + ")")
      .isLessThan(withStateMachine ? count / 5 : count / 10);
    m.custom.put("totalKeys", count);
    m.custom.put("promoted", count - m.errors.get());
    if (withStateMachine) {
      m.custom.put("confirmWindows", confirmWindows);
    }
    m.finish().logSummary();
  }
}
