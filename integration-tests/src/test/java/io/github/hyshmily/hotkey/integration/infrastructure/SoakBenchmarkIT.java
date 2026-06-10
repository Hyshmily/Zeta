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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
 * Long-running soak benchmark for sustained cache throughput under load.
 *
 * <p>Runs concurrent read, write, and sync operations for several minutes,
 * taking periodic snapshots of heap, GC, thread count, and throughput.
 * Validates post-soak data integrity in Redis.
 */
@Testcontainers
@Tag("docker")
@Tag("benchmark")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SoakBenchmarkIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(SoakBenchmarkIT.class);

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
   * instances. Sets a longer report interval and larger expelled queue capacity for
   * sustained soak operation.
   */
  @DynamicPropertySource
  static void overrideProps(DynamicPropertyRegistry r) {
    r.add("spring.data.redis.host", redis::getHost);
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    r.add("spring.rabbitmq.host", rabbitmq::getHost);
    r.add("spring.rabbitmq.port", () -> rabbitmq.getMappedPort(5672));
    r.add("spring.rabbitmq.username", () -> "guest");
    r.add("spring.rabbitmq.password", () -> "guest");
    r.add("hotkey.local.report-interval-ms", () -> "5000");
    r.add("hotkey.local.expelled-queue-capacity", () -> "500000");
  }

  @Autowired
  HotKey hotKey;

  @Autowired
  StringRedisTemplate redisTemplate;

  @Autowired
  RabbitTemplate rabbitTemplate;

  static final int SOAK_MINUTES = Integer.getInteger("hotkey.soak.minutes", 5);
  static final int SNAPSHOT_INTERVAL_SECONDS = 60;
  static final int READ_THREADS = 4;
  static final int WRITE_THREADS = 2;
  static final int SYNC_THREADS = 1;

  /** Runs concurrent read/write/sync threads for a configured duration and takes periodic snapshots. */
  @Test
  void soakTest() throws Exception {
    log.info("══════ SOAK TEST ══════");
    log.info("Duration: {} minutes, snapshot every {}s", SOAK_MINUTES, SNAPSHOT_INTERVAL_SECONDS);
    log.info("{} read threads, {} write threads, {} sync threads",
        READ_THREADS, WRITE_THREADS, SYNC_THREADS);

    ensureExchange("hotkey.sync.exchange");
    ensureExchange("hotkey.worker.exchange");

    for (int i = 0; i < 500; i++) {
      String key = "soak:prep:" + i;
      redisTemplate.opsForValue().set(key, "prep-" + i);
      hotKey.putThrough(key, "prep-" + i, () -> {});
    }
    log.info("Warmup: 500 keys seeded");

    AtomicInteger readErrors = new AtomicInteger(0);
    AtomicInteger writeErrors = new AtomicInteger(0);
    AtomicInteger syncErrors = new AtomicInteger(0);

    AtomicLong totalReadOps = new AtomicLong(0);
    AtomicLong totalWriteOps = new AtomicLong(0);
    AtomicLong totalSyncOps = new AtomicLong(0);

    List<Map<String, Object>> snapshots = new ArrayList<>();

    ExecutorService readPool = Executors.newFixedThreadPool(READ_THREADS);
    ExecutorService writePool = Executors.newFixedThreadPool(WRITE_THREADS);
    ExecutorService syncPool = Executors.newFixedThreadPool(SYNC_THREADS);
    CountDownLatch doneLatch = new CountDownLatch(READ_THREADS + WRITE_THREADS + SYNC_THREADS);

    long endTime = System.currentTimeMillis() + SOAK_MINUTES * 60_000L;
    Random rnd = new Random(42);

    for (int t = 0; t < READ_THREADS; t++) {
      int tid = t;
      readPool.submit(() -> {
        try {
          while (System.currentTimeMillis() < endTime) {
            int idx = rnd.nextInt(500);
            String key = "soak:prep:" + idx;
            try {
              hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 60000, 30000);
              totalReadOps.incrementAndGet();
            } catch (Exception e) {
              readErrors.incrementAndGet();
            }
          }
        } finally {
          doneLatch.countDown();
        }
      });
    }

    for (int t = 0; t < WRITE_THREADS; t++) {
      int tid = t;
      writePool.submit(() -> {
        try {
          while (System.currentTimeMillis() < endTime) {
            String key = "soak:write:" + UUID.randomUUID();
            String value = "w-" + tid + "-" + System.nanoTime();
            try {
              hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
              totalWriteOps.incrementAndGet();
            } catch (Exception e) {
              writeErrors.incrementAndGet();
            }
            Thread.sleep(10);
          }
        } catch (InterruptedException ignored) {
        } finally {
          doneLatch.countDown();
        }
      });
    }

    for (int t = 0; t < SYNC_THREADS; t++) {
      syncPool.submit(() -> {
        try {
          while (System.currentTimeMillis() < endTime) {
            int idx = rnd.nextInt(500);
            String key = "soak:prep:" + idx;
            try {
              MessageProperties props = new MessageProperties();
              props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE,
                  rnd.nextBoolean() ? "INVALIDATE" : "REFRESH");
              props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
              props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
              Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
              rabbitTemplate.send("hotkey.sync.exchange", "", msg);
              totalSyncOps.incrementAndGet();
            } catch (Exception e) {
              syncErrors.incrementAndGet();
            }
            Thread.sleep(50);
          }
        } catch (InterruptedException ignored) {
        } finally {
          doneLatch.countDown();
        }
      });
    }

    long snapshotEnd = endTime;
    long t0 = System.currentTimeMillis();
    while (System.currentTimeMillis() < snapshotEnd) {
      long elapsed = System.currentTimeMillis() - t0;
      Thread.sleep(SNAPSHOT_INTERVAL_SECONDS * 1000L);

      Map<String, Object> snap = takeSnapshot(elapsed,
          totalReadOps.get(), totalWriteOps.get(), totalSyncOps.get(),
          readErrors.get(), writeErrors.get(), syncErrors.get());
      snapshots.add(snap);
      logSnapshot(snap);
    }

    assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();

    readPool.shutdownNow();
    writePool.shutdownNow();
    syncPool.shutdownNow();

    Map<String, Object> report = new HashMap<>();
    report.put("testName", "HotKey Soak Test");
    report.put("soakMinutes", SOAK_MINUTES);
    report.put("timestamp", Instant.now().toString());
    report.put("snapshotIntervalSeconds", SNAPSHOT_INTERVAL_SECONDS);
    report.put("snapshots", snapshots);
    report.put("summary", Map.of(
        "totalReadOps", totalReadOps.get(),
        "totalWriteOps", totalWriteOps.get(),
        "totalSyncOps", totalSyncOps.get(),
        "totalOps", totalReadOps.get() + totalWriteOps.get() + totalSyncOps.get(),
        "readErrors", readErrors.get(),
        "writeErrors", writeErrors.get(),
        "syncErrors", syncErrors.get(),
        "totalErrors", readErrors.get() + writeErrors.get() + syncErrors.get()
    ));

    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    String json = mapper.writeValueAsString(report);

    Path reportPath = Path.of("src", "test", "resources", "testresult",
        "benchmark-soak-" + Instant.now().toString().replace(":", "-") + ".json");
    Files.createDirectories(reportPath.getParent());
    Files.writeString(reportPath, json);

    log.info("\n=========================================\nSoak report -> {}\n{}",
        reportPath.toAbsolutePath(), json);

    assertThat(readErrors.get() + writeErrors.get() + syncErrors.get())
        .as("Soak test errors").isZero();

    int integrityErrors = 0;
    for (int i = 0; i < 500; i++) {
      String key = "soak:prep:" + i;
      String expected = "prep-" + i;
      try {
        String actual = redisTemplate.opsForValue().get(key);
        if (!expected.equals(actual)) integrityErrors++;
      } catch (Exception e) {
        integrityErrors++;
      }
    }
    report.put("integrityErrors", integrityErrors);
    log.info("Post-soak integrity: {} / 500 keys intact", 500 - integrityErrors);
    assertThat(integrityErrors).as("Post-soak Redis integrity").isZero();
  }

  /** Captures a metrics snapshot including heap, GC, thread count, and operation counts. */
  private Map<String, Object> takeSnapshot(long elapsedMs,
      long readOps, long writeOps, long syncOps,
      long readErrs, long writeErrs, long syncErrs) {
    MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heap = memBean.getHeapMemoryUsage();
    MemoryUsage nonHeap = memBean.getNonHeapMemoryUsage();

    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    long totalGcCount = 0;
    long totalGcTime = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      totalGcCount += gc.getCollectionCount();
      totalGcTime += gc.getCollectionTime();
    }

    Map<String, Object> snap = new HashMap<>();
    snap.put("elapsedSeconds", elapsedMs / 1000);
    snap.put("totalReadOps", readOps);
    snap.put("totalWriteOps", writeOps);
    snap.put("totalSyncOps", syncOps);
    snap.put("readErrors", readErrs);
    snap.put("writeErrors", writeErrs);
    snap.put("syncErrors", syncErrs);

    snap.put("heapUsedMb", heap.getUsed() / 1_048_576);
    snap.put("heapMaxMb", heap.getMax() / 1_048_576);
    snap.put("heapCommittedMb", heap.getCommitted() / 1_048_576);
    snap.put("nonHeapUsedMb", nonHeap.getUsed() / 1_048_576);
    snap.put("threadCount", threadBean.getThreadCount());
    snap.put("daemonThreadCount", threadBean.getDaemonThreadCount());
    snap.put("totalGcCount", totalGcCount);
    snap.put("totalGcTimeMs", totalGcTime);

    return snap;
  }

  /** Logs a single metrics snapshot at INFO level. */
  private void logSnapshot(Map<String, Object> snap) {
    log.info("Snapshot at {}s: heap={}M/{}M, threads={}, GC={}({}ms), "
        + "readOps={}, writeOps={}, syncOps={}, errors={}",
        snap.get("elapsedSeconds"),
        snap.get("heapUsedMb"), snap.get("heapMaxMb"),
        snap.get("threadCount"),
        snap.get("totalGcCount"), snap.get("totalGcTimeMs"),
        snap.get("totalReadOps"), snap.get("totalWriteOps"),
        snap.get("totalSyncOps"),
        ((Number) snap.get("readErrors")).longValue()
            + ((Number) snap.get("writeErrors")).longValue()
            + ((Number) snap.get("syncErrors")).longValue());
  }

  /** Declares a fanout exchange idempotently if it does not already exist. */
  private void ensureExchange(String name) {
    rabbitTemplate.execute(channel -> {
      try {
        channel.exchangeDeclarePassive(name);
      } catch (Exception e) {
        channel.exchangeDeclare(name, "fanout", true);
      }
      return null;
    });
  }
}
