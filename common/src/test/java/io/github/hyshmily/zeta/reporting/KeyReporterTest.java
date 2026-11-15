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
package io.github.hyshmily.zeta.reporting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.cache.AccessCounter;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.sharding.ClusterHealth;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeyReporterTest {

  private static final long REPORT_INTERVAL_MS = 50;
  private static final int QUEUE_CAPACITY = 1000;
  private static final long AWAIT_TIMEOUT_MS = 5000;

  private ScheduledExecutorService scheduler;
  private TestReportPublisher testPublisher;
  private ConsistentHashRing ring;
  private HealthView healthView;
  private KeyReporterImpl reporter;
  private AccessCounter reporterCounter;

  static class TestReportPublisher extends ReportPublisher {

    final List<String> targets = new CopyOnWriteArrayList<>();
    final List<ReportMessage> messages = new CopyOnWriteArrayList<>();
    volatile int publishCount = 0;

    TestReportPublisher() {
      super(null, "test", "testApp");
    }

    @Override
    public void publish(String target, ReportMessage message) {
      targets.add(target);
      messages.add(message);
      publishCount++;
    }
  }

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    testPublisher = new TestReportPublisher();
    ring = new HashRing(150);
    healthView = new ClusterHealth(30000);
    reporter = new KeyReporterImpl(testPublisher, "testApp", QUEUE_CAPACITY, 100, 1, ring, healthView);
    reporterCounter = new AccessCounter(
      mock(io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector.class),
      reporter,
      scheduler,
      50L,
      AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS
    );
    reporterCounter.afterPropertiesSet();
  }

  @AfterEach
  void tearDown() {
    reporter.stop();
    scheduler.shutdownNow();
  }

  private static void registerWorker(HealthView hv, String workerId) {
    hv.onHeartbeat(new WorkerHeartbeatMessage(0L, workerId, 1L, true, null));
  }

  private void awaitPublish(int minCount) throws InterruptedException {
    long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (testPublisher.publishCount >= minCount) {
        return;
      }
      Thread.sleep(10);
    }
  }

  private void awaitDropped(long minCount) throws InterruptedException {
    long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      KeyReporter.DispatcherStats stats = reporter.dispatcherStats();
      if (stats != null && stats.dropped() >= minCount) {
        return;
      }
      Thread.sleep(10);
    }
  }

  @Test
  void record_Report_shouldIncrementPendingCount() {
    reporterCounter.add("key1");
    reporterCounter.add("key1");
    reporterCounter.add("key2");
    assertThat(reporterCounter.pendingKeyCount()).isPositive();
  }

  @Test
  void record_Report_withNullKey_shouldBeIgnored() {
    // The underlying WaveCounter silently ignores null/empty keys (see
    // WaveCounter.add), so the reporter must not throw either.
    reporterCounter.add(null);
    assertThat(reporterCounter.pendingKeyCount()).isZero();
  }

  @Test
  void start_shouldBeIdempotent() {
    reporter.start();
    reporter.start();
    assertThat(reporter.dispatcherStats().depth()).isNotNegative();
  }

  @Test
  void constructorRejectsInvalidBounds() {
    assertThatThrownBy(() -> new KeyReporterImpl(testPublisher, "app", 0, 0, 1, ring, healthView))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("queueCapacity");
    assertThatThrownBy(() -> new KeyReporterImpl(testPublisher, "app", 1, -1, 1, ring, healthView))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("queueOfferTimeoutMs");
    assertThatThrownBy(() -> new KeyReporterImpl(testPublisher, "app", 1, 0, 0, ring, healthView))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("consumerCount");
  }

  /**
   * A throwing health probe must drop the batch, not kill the permanent consumer thread: the
   * fixed pool never replaces a thread whose exception was swallowed by submit().
   */
  @Test
  void healthCheckFailure_dropsTheBatchAndKeepsTheConsumerAlive() throws Exception {
    reporter.stop();
    HealthView flaky = spy(new ClusterHealth(30_000));
    AtomicBoolean failHealth = new AtomicBoolean(true);
    doAnswer(invocation -> {
      if (failHealth.get() && Thread.currentThread().getName().contains("zeta-report-consumer")) {
        throw new IllegalStateException("health probe down");
      }
      return true;
    })
      .when(flaky)
      .isCurrentIncarnation(anyString(), anyLong());
    registerWorker(flaky, "worker-1");

    reporter = new KeyReporterImpl(testPublisher, "testApp", QUEUE_CAPACITY, 100, 1, ring, flaky);
    reporter.start();

    reporter.publishDirect(Map.of("k", 1L));
    awaitDropped(1);
    assertThat(testPublisher.publishCount).isZero();

    failHealth.set(false);
    reporter.publishDirect(Map.of("k", 1L));
    awaitPublish(1);
    assertThat(testPublisher.publishCount).as("the consumer survived the failed probe").isEqualTo(1);
  }

  @Test
  void stop_shouldPreventFurtherPublishing() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporterCounter.add("key-b");
    awaitPublish(1);
    int beforeStop = testPublisher.publishCount;
    reporter.stop();
    reporterCounter.add("key-c");
    Thread.sleep(REPORT_INTERVAL_MS * 3 + 200);
    assertThat(testPublisher.publishCount).isEqualTo(beforeStop);
  }

  @Test
  void dispatcherStats_shouldBeNullBeforeStart() {
    assertThat(reporter.dispatcherStats()).isNull();
  }

  @Test
  void dispatcherStats_shouldReturnNonNegativeDepthAfterStart() {
    reporter.start();
    assertThat(reporter.dispatcherStats().depth()).isNotNegative();
  }

  @Test
  void record_Report_shouldPublishAfterFlush() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporterCounter.add("key-a");
    awaitPublish(1);
    assertThat(testPublisher.publishCount).isPositive();
  }

  @Test
  void multipleRecordsForSameKey_shouldPublishAggregatedCount() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporterCounter.add("agg-key");
    reporterCounter.add("agg-key");
    reporterCounter.add("agg-key");
    awaitPublish(1);
    long total = testPublisher.messages
      .stream()
      .flatMap(m -> m.counts().entrySet().stream())
      .filter(e -> e.getKey().equals("agg-key"))
      .mapToLong(Map.Entry::getValue)
      .sum();
    assertThat(total).isEqualTo(3);
  }

  @Test
  void recordsForDifferentKeys_shouldPublishAsBatch() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporterCounter.add("key-x");
    reporterCounter.add("key-y");
    // Wait until both keys appear across all messages (eager swap may
    // split keys across multiple batches).
    long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    boolean allSeen = false;
    while (System.currentTimeMillis() < deadline) {
      Map<String, Long> merged = testPublisher.messages
        .stream()
        .flatMap(m -> m.counts().entrySet().stream())
        .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
      if (merged.containsKey("key-x") && merged.containsKey("key-y")) {
        allSeen = true;
        break;
      }
      Thread.sleep(10);
    }
    assertThat(allSeen).isTrue();
  }

  @Test
  void gracefulDegradation_shouldDropRecordsWhenClusterUnhealthy() throws Exception {
    reporter.start();
    reporterCounter.add("ghost-key");
    Thread.sleep(REPORT_INTERVAL_MS * 3 + 500);
    assertThat(testPublisher.publishCount).isZero();
  }

  @Test
  void flush_shouldNotPublishWhenNoRecords() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    Thread.sleep(REPORT_INTERVAL_MS * 3 + 200);
    assertThat(testPublisher.publishCount).isZero();
  }

  @Test
  void stop_beforeStart_shouldNotThrow() {
    reporter.stop();
  }

  @Test
  void start_andStopMultipleTimes_shouldBeSafe() {
    reporter.start();
    reporter.stop();
    reporter.start();
    reporter.stop();
  }

  @Test
  void record_Report_withEmptyKey_shouldBeIgnored() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporterCounter.add("");
    // The WaveCounter drops blank keys at add() (documented: "blank keys
    // are silently ignored"), so an empty report must never publish.
    Thread.sleep(REPORT_INTERVAL_MS * 3 + 200);
    assertThat(testPublisher.publishCount).isZero();
  }

  @Test
  void record_Report_withVeryLongKey_shouldWork() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporterCounter.add("k".repeat(10_000));
    awaitPublish(1);
    assertThat(testPublisher.publishCount).isPositive();
  }

  @Test
  void dispatcherExpired_shouldBeZeroInitially() {
    reporter.start();
    KeyReporter.DispatcherStats stats = reporter.dispatcherStats();
    assertThat(stats.expiredDeadTarget() + stats.expiredStale()).isZero();
  }

  @Test
  void dispatcherDropped_shouldBeZeroInitially() {
    reporter.start();
    assertThat(reporter.dispatcherStats().dropped()).isZero();
  }

  @Test
  void flush_withManyKeys_shouldNotLoseCounts() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    for (int i = 0; i < 100; i++) {
      reporterCounter.add("bulk-key-" + i);
    }
    // Wait until all 100 distinct keys appear across all messages (periodic flush may
    // eager swap may split keys across multiple batches).
    long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    boolean allSeen = false;
    long total = 0L;
    while (System.currentTimeMillis() < deadline) {
      total = testPublisher.messages
        .stream()
        .flatMap(m -> m.counts().values().stream())
        .mapToLong(Long::longValue)
        .sum();
      if (total >= 100) {
        allSeen = true;
        break;
      }
      Thread.sleep(10);
    }
    assertThat(allSeen).isTrue();
  }

  @Test
  void start_andStop_shouldHandleGracefully() {
    // design decisions: access counting: the reporter owns no scheduler any more (the counting tide
    // lives on the shared access counter) — start/stop now only manage the
    // routing executor and the publish consumers, and must stay resilient
    // (idempotent start, quiet stop).
    KeyReporter failingReporter = new KeyReporterImpl(
      testPublisher,
      "testApp",
      QUEUE_CAPACITY,
      100,
      1,
      ring,
      healthView
    );
    AccessCounter failingReporterCounter = new AccessCounter(
      mock(io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector.class),
      failingReporter,
      scheduler,
      50L,
      AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS
    );
    failingReporterCounter.afterPropertiesSet();
    failingReporter.start();
    failingReporter.start(); // idempotent
    assertThat(failingReporter.dispatcherStats().depth()).isNotNegative();
    failingReporter.stop();
    failingReporter.stop(); // idempotent
  }

  /**
   * A throwing {@code locateNode} is counted as an unroutable drop on a DEBUG line and the
   * flush loop keeps routing later batches normally.
   */
  @Test
  void flush_locateNodeThrows_shouldCountFailureAndKeepFlushLoopAlive() throws Exception {
    AtomicInteger routeCalls = new AtomicInteger();
    ConsistentHashRing failingRing = mock(ConsistentHashRing.class);
    // onFlush() first materialises the alive set via rebuildIfPossible; without this stub
    // the mock returns null, onFlush NPEs on aliveNodes.isEmpty() and the routing-failure counter
    // path is never exercised.
    when(failingRing.rebuildIfPossible(any())).thenReturn(Set.of("worker-1"));
    // Route to a node the admission did not capture so the reporter exercises the admitted-target
    // fallback, which owns the simulated failure.
    when(failingRing.locateNode(anyString())).thenReturn("new-worker");
    when(failingRing.locateNode(anyString(), any(java.util.function.Predicate.class))).thenAnswer(inv -> {
      if (routeCalls.incrementAndGet() <= 1) {
        throw new IllegalStateException("ring boom");
      }
      return "worker-1";
    });

    KeyReporterImpl failing = new KeyReporterImpl(
      testPublisher,
      "testApp",
      QUEUE_CAPACITY,
      100,
      1,
      failingRing,
      healthView
    );
    AccessCounter failingCounter = new AccessCounter(
      mock(io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector.class),
      failing,
      scheduler,
      50L,
      AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS
    );
    failingCounter.afterPropertiesSet();
    registerWorker(healthView, "worker-1");
    failing.start();
    try {
      // First flush: locateNode throws inside the detached routing task.
      failingCounter.add("boom-key");

      AtomicLong unroutable = new AtomicLong(-1);
      long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
      while (System.currentTimeMillis() < deadline) {
        unroutable.set(unroutableDropCounter(failing).get());
        if (unroutable.get() > 0) {
          break;
        }
        Thread.sleep(10);
      }
      // The failed batch's key was counted, not silently lost.
      assertThat(unroutable.get()).isPositive();
      assertThat(testPublisher.publishCount).isZero();

      // A later batch routes normally — the flush loop survived the failure.
      failingCounter.add("good-key");
      awaitPublish(1);
      assertThat(testPublisher.publishCount).isPositive();
    } finally {
      failing.stop();
    }
  }

  /** Reflection accessor for the private unroutable-drop counter. */
  private static AtomicLong unroutableDropCounter(KeyReporterImpl reporter) throws Exception {
    Field f = KeyReporterImpl.class.getDeclaredField("unroutableDropCounter");
    f.setAccessible(true);
    return (AtomicLong) f.get(reporter);
  }
}
