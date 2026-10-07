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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Routing-saturation and BBR-accounting tests for {@link KeyReporterImpl}.
 *
 * <p>Covers two Phase-1 review findings: the routing-executor rejection path
 * must displace the stalest batch (drop-oldest, freshest signal wins) instead
 * of discarding the just-flushed batch, and the BBR in-flight accounting must
 * balance to zero after quiesce (one gate pass per flush, per-batch
 * enqueue/settle — the gate itself never holds a unit).
 */
class KeyReporterRoutingTest {

  private static final long REPORT_INTERVAL_MS = 50;
  private static final int QUEUE_CAPACITY = 1000;

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
    healthView = new HealthViewImpl(30000, 3);
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
    hv.onHeartbeat(new WorkerHeartbeatMessage(0L, workerId, 1, 0.0, true, 0, 0, 0, 0));
  }

  private static ExecutorService routingExecutor(KeyReporterImpl r) throws Exception {
    Field f = KeyReporterImpl.class.getDeclaredField("routingExecutor");
    f.setAccessible(true);
    return (ExecutorService) f.get(r);
  }

  private static long routingDrops(KeyReporterImpl r) throws Exception {
    Field f = KeyReporterImpl.class.getDeclaredField("routingDropCounter");
    f.setAccessible(true);
    return ((java.util.concurrent.atomic.AtomicLong) f.get(r)).get();
  }

  private static void invokeOnFlush(KeyReporterImpl r, Map<String, Long> keyCounts) throws Exception {
    Method m = KeyReporterImpl.class.getDeclaredMethod("onFlush", Map.class);
    m.setAccessible(true);
    m.invoke(r, keyCounts);
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
  void routingSaturation_shouldDropOldestAndRouteNewest() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.start();

    // Occupy the single routing thread, then fill its 8-slot queue. Any
    // further submit (the flush below) must displace the stalest entry.
    ExecutorService routing = routingExecutor(reporter);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch busy = new CountDownLatch(1);
    routing.submit(() -> {
      busy.countDown();
      try {
        release.await(15, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    assertThat(busy.await(5, TimeUnit.SECONDS)).isTrue();
    for (int i = 0; i < 8; i++) {
      routing.submit(() -> {});
    }

    try {
      invokeOnFlush(reporter, new HashMap<>(Map.of("route-new-1", 5L, "route-new-2", 7L)));
      // Exactly one displacement: the oldest filler, not the new batch.
      assertThat(routingDrops(reporter)).isEqualTo(1);
    } finally {
      release.countDown();
    }

    awaitPublish(1);
    assertThat(testPublisher.publishCount).isPositive();
    boolean newestRouted = testPublisher.messages
      .stream()
      .flatMap(m -> m.counts().keySet().stream())
      .anyMatch("route-new-1"::equals);
    assertThat(newestRouted).as("the just-flushed batch is routed, not discarded").isTrue();
    assertThat(routingDrops(reporter)).as("no further drops after drain").isEqualTo(1);
  }

  @Test
  void bbrAccounting_shouldBalanceToZeroAfterQuiesce() throws Exception {
    SystemLoadMonitor cpu = mock(SystemLoadMonitor.class);
    when(cpu.getCpuLoadEMA()).thenReturn(0.1);
    BbrRateLimiterImpl limiter = new BbrRateLimiterImpl(cpu, 800, 500, 5, 1000, 128L);
    reporter.setBbrRateLimiter(limiter);
    registerWorker(healthView, "worker-1");
    registerWorker(healthView, "worker-2");
    reporter.start();

    // Deterministically pin one key per Worker so a single flush fans out to
    // two batches behind one gate pass (the original P1-4 scenario).
    Set<String> alive = ringManager.reconcileFromHealthView(healthView);
    assertThat(alive).containsExactlyInAnyOrder("worker-1", "worker-2");
    Map<String, String> keyPerTarget = new HashMap<>();
    for (int i = 0; i < 10_000 && keyPerTarget.size() < 2; i++) {
      String key = "bbr-pin-" + i;
      String target = ringManager.routeNode(key, alive::contains);
      keyPerTarget.putIfAbsent(target, key);
    }
    assertThat(keyPerTarget).as("one key pinned per worker").hasSize(2);
    List<String> pinned = new ArrayList<>(keyPerTarget.values());
    reporter.reportToWorker(pinned.get(0));
    reporter.reportToWorker(pinned.get(1));

    awaitPublish(2);
    // Quiesce: no more reports are issued and empty tides return before the
    // gate, so once in-flight is zero and every publish settled it stays so.
    long deadline = System.currentTimeMillis() + 8000;
    while (System.currentTimeMillis() < deadline) {
      if (
        testPublisher.publishCount >= 2
          && reporter.bbrInFlight() == 0
          && reporter.bbrPassed() == testPublisher.publishCount
      ) {
        Thread.sleep(300);
        break;
      }
      Thread.sleep(20);
    }

    assertThat(new HashSet<>(testPublisher.targets))
      .as("both worker targets received a batch")
      .containsExactlyInAnyOrder("worker-1", "worker-2");
    assertThat(reporter.bbrInFlight()).as("every enqueued batch settled exactly once").isZero();
    assertThat(reporter.bbrPassed()).isEqualTo(testPublisher.publishCount);
    assertThat(reporter.bbrDropped()).as("no gate or consumer drops expected").isZero();
  }
}
