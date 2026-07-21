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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.reporting.impl.BbrRateLimiterImpl;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.RingManager;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sharding.impl.RingManagerImpl;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.SystemLoadMonitor;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZetaReporterTest {

  private static final long REPORT_INTERVAL_MS = 50;
  private static final int QUEUE_CAPACITY = 1000;
  private static final long AWAIT_TIMEOUT_MS = 5000;

  private ScheduledExecutorService scheduler;
  private TestReportPublisher testPublisher;
  private RingManager ringManager;
  private HealthView healthView;
  private KeyReporterImpl reporter;

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
    ringManager = new RingManagerImpl(150);
    healthView = new HealthViewImpl(3, 30000, 3);
    reporter = new KeyReporterImpl(
      testPublisher,
      scheduler,
      REPORT_INTERVAL_MS,
      "testApp",
      QUEUE_CAPACITY,
      100,
      1,
      ringManager,
      healthView,
      mock(SnowflakeIdGenerator.class)
    );
  }

  @AfterEach
  void tearDown() {
    reporter.stop();
    scheduler.shutdownNow();
  }

  private static void registerWorker(HealthView hv, String workerId) {
    hv.onHeartbeat(new WorkerHeartbeatMessage(0L, workerId, 1, 0, 0.0, true, 0, 0, 0, 0));
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

  @Test
  void record_Report_shouldIncrementPendingCount() {
    reporter.reportToWorker("key1");
    reporter.reportToWorker("key1");
    reporter.reportToWorker("key2");
    assertThat(reporter.getPendingKeyCount()).isPositive();
  }

  @Test
  void record_Report_withNullKey_shouldThrow() {
    assertThatThrownBy(() -> reporter.reportToWorker(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void start_shouldBeIdempotent() {
    reporter.start();
    reporter.start();
    assertThat(reporter.dispatcherCapacity()).isEqualTo(QUEUE_CAPACITY);
  }

  @Test
  void stop_shouldPreventFurtherPublishing() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporter.reportToWorker("key-b");
    awaitPublish(1);
    int beforeStop = testPublisher.publishCount;
    reporter.stop();
    reporter.reportToWorker("key-c");
    Thread.sleep(REPORT_INTERVAL_MS * 3 + 200);
    assertThat(testPublisher.publishCount).isEqualTo(beforeStop);
  }

  @Test
  void dispatcherDepth_shouldReturnMinusOneBeforeStart() {
    assertThat(reporter.dispatcherDepth()).isEqualTo(-1);
  }

  @Test
  void dispatcherDepth_shouldReturnNonNegativeAfterStart() {
    reporter.start();
    assertThat(reporter.dispatcherDepth()).isNotNegative();
  }

  @Test
  void dispatcherCapacity_shouldReturnConfiguredCapacity() {
    assertThat(reporter.dispatcherCapacity()).isEqualTo(-1);
    reporter.start();
    assertThat(reporter.dispatcherCapacity()).isEqualTo(QUEUE_CAPACITY);
  }

  @Test
  void record_Report_shouldPublishAfterFlush() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporter.reportToWorker("key-a");
    awaitPublish(1);
    assertThat(testPublisher.publishCount).isPositive();
  }

  @Test
  void multipleRecordsForSameKey_shouldPublishAggregatedCount() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporter.reportToWorker("agg-key");
    reporter.reportToWorker("agg-key");
    reporter.reportToWorker("agg-key");
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
    reporter.reportToWorker("key-x");
    reporter.reportToWorker("key-y");
    awaitPublish(1);
    assertThat(testPublisher.messages).isNotEmpty();
    ReportMessage first = testPublisher.messages.get(0);
    assertThat(first.counts()).containsOnlyKeys("key-x", "key-y");
  }

  @Test
  void gracefulDegradation_shouldDropRecordsWhenClusterUnhealthy() throws Exception {
    reporter.start();
    reporter.reportToWorker("ghost-key");
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
  void record_Report_withEmptyKey_shouldWork() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporter.reportToWorker("");
    awaitPublish(1);
    assertThat(testPublisher.publishCount).isPositive();
  }

  @Test
  void record_Report_withVeryLongKey_shouldWork() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporter.reportToWorker("k".repeat(10_000));
    awaitPublish(1);
    assertThat(testPublisher.publishCount).isPositive();
  }

  @Test
  void bbrMethods_withoutBbr_shouldReturnMinusOne() {
    assertThat(reporter.bbrPassed()).isEqualTo(-1);
    assertThat(reporter.bbrDropped()).isEqualTo(-1);
    assertThat(reporter.bbrInFlight()).isEqualTo(-1);
    assertThat(reporter.bbrMaxInFlight()).isEqualTo(-1);
  }

  @Test
  void dispatcherExpired_shouldBeZeroInitially() {
    reporter.start();
    assertThat(reporter.dispatcherExpired()).isZero();
  }

  @Test
  void dispatcherDropped_shouldBeZeroInitially() {
    reporter.start();
    assertThat(reporter.dispatcherDropped()).isZero();
  }

  @Test
  void flush_withManyKeys_shouldNotLoseCounts() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    for (int i = 0; i < 100; i++) {
      reporter.reportToWorker("bulk-key-" + i);
    }
    awaitPublish(1);
    assertThat(testPublisher.publishCount).isPositive();
    long total = testPublisher.messages
      .stream()
      .flatMap(m -> m.counts().values().stream())
      .mapToLong(Long::longValue)
      .sum();
    assertThat(total).isEqualTo(100);
  }

  @Test
  void flush_withBbrRateLimiter_shouldWireCorrectly() {
    SystemLoadMonitor cpuMonitor = mock(SystemLoadMonitor.class);
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.9);
    BbrRateLimiterImpl bbr = new BbrRateLimiterImpl(cpuMonitor, 800, 500, 5, 1000);
    reporter.setBbrRateLimiter(bbr);
    assertThat(reporter.bbrPassed()).isZero();
    assertThat(reporter.bbrDropped()).isZero();
    assertThat(reporter.bbrInFlight()).isZero();
    assertThat(reporter.bbrMaxInFlight()).isEqualTo(1);
  }

  @Test
  void flush_withBbrRateLimiterAndRecords_shouldTrackMetrics() throws Exception {
    SystemLoadMonitor cpuMonitor = mock(SystemLoadMonitor.class);
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.5);
    BbrRateLimiterImpl bbr = new BbrRateLimiterImpl(cpuMonitor, 800, 500, 5, 1000);
    reporter.setBbrRateLimiter(bbr);
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporter.reportToWorker("bbr-key");
    awaitPublish(1);
    assertThat(reporter.bbrPassed()).isPositive();
  }

  @Test
  void flush_withHighCpuBbr_shouldDropRecords() throws Exception {
    SystemLoadMonitor cpuMonitor = mock(SystemLoadMonitor.class);
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(1.0);
    BbrRateLimiterImpl bbr = new BbrRateLimiterImpl(cpuMonitor, 800, 500, 5, 1000);
    reporter.setBbrRateLimiter(bbr);
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporter.reportToWorker("high-cpu-key");
    Thread.sleep(REPORT_INTERVAL_MS * 3 + 500);
    assertThat(reporter.bbrDropped()).isNotNegative();
  }

  @Test
  void start_withFailedScheduler_shouldHandleGracefully() {
    ScheduledExecutorService brokenScheduler = mock(ScheduledExecutorService.class);
    when(brokenScheduler.scheduleAtFixedRate(any(), anyLong(), anyLong(), any())).thenThrow(
      new RuntimeException("scheduler failure")
    );
    KeyReporter failingReporter = new KeyReporterImpl(
      testPublisher,
      brokenScheduler,
      REPORT_INTERVAL_MS,
      "testApp",
      QUEUE_CAPACITY,
      100,
      1,
      ringManager,
      healthView,
      mock(SnowflakeIdGenerator.class)
    );
    failingReporter.start();
    assertThat(failingReporter.dispatcherDepth()).isNotNegative();
    failingReporter.stop();
  }
}
