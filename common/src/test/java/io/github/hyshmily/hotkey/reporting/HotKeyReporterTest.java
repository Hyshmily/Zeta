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
package io.github.hyshmily.hotkey.reporting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sync.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.WorkerHeartbeatMessage;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HotKeyReporterTest {

  private static final long REPORT_INTERVAL_MS = 50;
  private static final int QUEUE_CAPACITY = 1000;
  private static final long AWAIT_TIMEOUT_MS = 5000;

  private ScheduledExecutorService scheduler;
  private TestReportPublisher testPublisher;
  private RingManager ringManager;
  private ClusterHealthView healthView;
  private HotKeyReporter reporter;

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
    ringManager = new RingManager(150);
    healthView = new ClusterHealthView(3, 30000, 3);
    reporter = new HotKeyReporter(
      testPublisher, scheduler, REPORT_INTERVAL_MS, "testApp",
      QUEUE_CAPACITY, 100, 1, ringManager, healthView
    );
  }

  @AfterEach
  void tearDown() {
    reporter.stop();
    scheduler.shutdownNow();
  }

  private static void registerWorker(ClusterHealthView hv, String workerId) {
    hv.onHeartbeat(new WorkerHeartbeatMessage(
      workerId, 1, System.currentTimeMillis(), 0, 0.0, true, 0, 0, 0, 0, 0
    ));
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
  void record_shouldIncrementPendingCount() {
    reporter.record("key1");
    reporter.record("key1");
    reporter.record("key2");
    assertThat(reporter.getPendingKeyCount()).isPositive();
  }

  @Test
  void record_withNullKey_shouldThrow() {
    assertThatThrownBy(() -> reporter.record(null)).isInstanceOf(NullPointerException.class);
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
    reporter.record("key-b");
    awaitPublish(1);
    int beforeStop = testPublisher.publishCount;
    reporter.stop();
    reporter.record("key-c");
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
  void record_shouldPublishAfterFlush() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporter.record("key-a");
    awaitPublish(1);
    assertThat(testPublisher.publishCount).isPositive();
  }

  @Test
  void multipleRecordsForSameKey_shouldPublishAggregatedCount() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();
    reporter.record("agg-key");
    reporter.record("agg-key");
    reporter.record("agg-key");
    awaitPublish(1);
    long total = testPublisher.messages.stream()
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
    reporter.record("key-x");
    reporter.record("key-y");
    awaitPublish(1);
    assertThat(testPublisher.messages).isNotEmpty();
    ReportMessage first = testPublisher.messages.get(0);
    assertThat(first.counts()).containsOnlyKeys("key-x", "key-y");
  }

  @Test
  void gracefulDegradation_shouldDropRecordsWhenClusterUnhealthy() throws Exception {
    reporter.start();
    reporter.record("ghost-key");
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
}
