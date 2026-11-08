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
import static org.mockito.Mockito.mock;

import io.github.hyshmily.zeta.cache.AccessCounter;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.ConsistentHashRingImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.HealthViewImpl;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Multi-target routing tests for {@link KeyReporterImpl}: one flush fans out to one batch per
 * alive Worker target, and every enqueued batch is published.
 */
class KeyReporterRoutingTest {

  private static final int QUEUE_CAPACITY = 1000;

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
    ring = new ConsistentHashRingImpl(150);
    healthView = new HealthViewImpl(30000);
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
    long deadline = System.currentTimeMillis() + 8000;
    while (System.currentTimeMillis() < deadline) {
      if (testPublisher.publishCount >= minCount) {
        return;
      }
      Thread.sleep(10);
    }
  }

  @Test
  void flush_fansOutOneBatchPerTarget() throws Exception {
    registerWorker(healthView, "worker-1");
    registerWorker(healthView, "worker-2");
    reporter.start();

    // Deterministically pin one key per Worker so a single flush fans out to
    // two batches.
    Set<String> alive = ring.rebuildIfPossible(healthView);
    assertThat(alive).containsExactlyInAnyOrder("worker-1", "worker-2");
    Map<String, String> keyPerTarget = new HashMap<>();
    for (int i = 0; i < 10_000 && keyPerTarget.size() < 2; i++) {
      String key = "pin-" + i;
      String target = ring.locateNode(key, alive::contains);
      keyPerTarget.putIfAbsent(target, key);
    }
    assertThat(keyPerTarget).as("one key pinned per worker").hasSize(2);
    List<String> pinned = new ArrayList<>(keyPerTarget.values());
    reporterCounter.add(pinned.get(0));
    reporterCounter.add(pinned.get(1));

    awaitPublish(2);

    assertThat(new HashSet<>(testPublisher.targets))
      .as("both worker targets received a batch")
      .containsExactlyInAnyOrder("worker-1", "worker-2");
  }
}
