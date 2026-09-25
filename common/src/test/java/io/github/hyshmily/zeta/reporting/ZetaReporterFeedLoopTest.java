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
import io.github.hyshmily.zeta.reporting.impl.ReportFeedLoop;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.RingManager;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sharding.impl.RingManagerImpl;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import java.lang.reflect.Field;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * ADR-0078 integration: the feed-loop interval tuner wired into
 * {@link KeyReporterImpl#onFlush}. Pins the four behaviors that matter on the
 * real flush path — shadow computes without applying, apply moves the
 * WaveCounter tide base on sparse batches, and the three censored cycle types
 * (empty snapshot, no alive Worker, BBR gate drop) never produce a sample.
 */
class ZetaReporterFeedLoopTest {

  private static final int QUEUE_CAPACITY = 1000;
  private static final long AWAIT_TIMEOUT_MS = 5000;

  private ScheduledExecutorService scheduler;
  private ZetaReporterTest.TestReportPublisher testPublisher;
  private RingManager ringManager;
  private HealthView healthView;
  private KeyReporterImpl reporter;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    testPublisher = new ZetaReporterTest.TestReportPublisher();
    ringManager = new RingManagerImpl(150);
    healthView = new HealthViewImpl(30000, 3);
    reporter = newReporter();
  }

  private KeyReporterImpl newReporter() {
    return new KeyReporterImpl(
      testPublisher,
      scheduler,
      50,
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

  /** Reads the WaveCounter's current tide base (the value apply-mode moves). */
  private static long tideBase(KeyReporterImpl reporter) throws Exception {
    Field counterField = KeyReporterImpl.class.getDeclaredField("reportWaveCounter");
    counterField.setAccessible(true);
    Object counter = counterField.get(reporter);
    Field intervalField = counter.getClass().getDeclaredField("deliverIntervalMs");
    intervalField.setAccessible(true);
    return intervalField.getLong(counter);
  }

  /**
   * Reports a key every 10ms from a daemon thread so every tide carries a
   * non-empty batch (a once-reported key leaves all later tides empty, and
   * empty tides are censored — the tuner would stop receiving samples).
   */
  private Thread startContinuousReporter(String key) {
    AtomicBoolean stop = new AtomicBoolean(false);
    Thread thread = new Thread(
      () -> {
        while (!stop.get()) {
          reporter.reportToWorker(key);
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            return;
          }
        }
      },
      "feedloop-test-reporter"
    );
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  private boolean waitFor(ThrowingBooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (condition.get()) {
        return true;
      }
      Thread.sleep(20);
    }
    return condition.get();
  }

  /** {@link java.util.function.BooleanSupplier} variant whose body may throw checked exceptions. */
  @FunctionalInterface
  interface ThrowingBooleanSupplier {

    boolean getAsBoolean() throws Exception;

    /** Reflects into {@link #getAsBoolean()} with checked exceptions flattened to false. */
    default boolean get() {
      try {
        return getAsBoolean();
      } catch (Exception e) {
        return false;
      }
    }
  }

  @Test
  void withoutTuner_feedLoopAccessorsReportDisabled() {
    reporter.start();
    assertThat(reporter.feedLoopEnabled()).isFalse();
    assertThat(reporter.feedLoopIntervalMs()).isEqualTo(-1);
    assertThat(reporter.feedLoopScoreBp()).isEqualTo(-1);
    assertThat(reporter.feedLoopBatchSize()).isEqualTo(-1);
  }

  @Test
  void shadowMode_samplesAndExposes_butNeverApplies() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.setIntervalFeedLoop(new ReportFeedLoop(ReportFeedLoop.Mode.SHADOW, 512, 50, 1000, 50));
    reporter.start();

    Thread reporterThread = startContinuousReporter("shadow-key");
    try {
      // The publish pipeline itself must be flowing first (routing -> dispatcher
      // -> consumer is a few thread hops behind the flush that sampled).
      boolean published = waitFor(() -> testPublisher.publishCount > 0);
      assertThat(published).as("flush pipeline delivered at least one batch").isTrue();
      // A sparse batch (1 key vs target 512) pushes the computed trajectory up
      // while the tide base stays untouched.
      assertThat(waitFor(() -> reporter.feedLoopIntervalMs() > 50))
        .as("shadow tuner computed a >50ms interval")
        .isTrue();
      assertThat(reporter.feedLoopScoreBp()).isBetween(0L, 10_000L);
      assertThat(reporter.feedLoopBatchSize()).isPositive();
      assertThat(tideBase(reporter)).as("shadow mode never applies").isEqualTo(50);
    } finally {
      reporterThread.interrupt();
    }
  }

  @Test
  void applyMode_movesTheTideBaseUpOnSparseBatches() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.setIntervalFeedLoop(new ReportFeedLoop(ReportFeedLoop.Mode.ON, 512, 50, 1000, 50));
    reporter.start();

    Thread reporterThread = startContinuousReporter("apply-key");
    try {
      boolean applied = waitFor(() -> tideBase(reporter) > 50);
      assertThat(applied).as("apply mode moved the tide base").isTrue();
      // The gauge mirrors the applied base exactly.
      assertThat(reporter.feedLoopIntervalMs()).isEqualTo(tideBase(reporter));
      assertThat(testPublisher.publishCount).isPositive();
    } finally {
      reporterThread.interrupt();
    }
  }

  @Test
  void noAliveWorker_cyclesAreCensored() throws Exception {
    // No worker registered: every flush drops at the alive-set guard, before
    // the sampling point — the tuner must never see a sample.
    reporter.setIntervalFeedLoop(new ReportFeedLoop(ReportFeedLoop.Mode.ON, 512, 50, 1000, 50));
    reporter.start();

    Thread reporterThread = startContinuousReporter("ghost-key");
    try {
      Thread.sleep(700);
      assertThat(reporter.feedLoopBatchSize()).as("no sample from dropped cycles").isEqualTo(-1);
      assertThat(reporter.feedLoopScoreBp()).isEqualTo(-1);
      assertThat(tideBase(reporter)).isEqualTo(50);
    } finally {
      reporterThread.interrupt();
    }
  }

  @Test
  void bbrGateDrop_cyclesAreCensored() throws Exception {
    registerWorker(healthView, "worker-1");
    // A real BbrRateLimiterImpl with no traffic freeruns (in-flight 0 is inside
    // the freerun band) regardless of CPU, so the drop condition is forced with
    // a stub instead — the censoring contract is the flush loop's code ORDER:
    // a gate-rejected cycle returns before the sampling point.
    BbrRateLimiterImpl rejecting = mock(BbrRateLimiterImpl.class);
    when(rejecting.tryAcquire()).thenReturn(false);
    reporter.setBbrRateLimiter(rejecting);
    reporter.setIntervalFeedLoop(new ReportFeedLoop(ReportFeedLoop.Mode.ON, 512, 50, 1000, 50));
    reporter.start();

    Thread reporterThread = startContinuousReporter("gate-key");
    try {
      Thread.sleep(700);
      org.mockito.Mockito.verify(rejecting, org.mockito.Mockito.atLeastOnce()).onGateDrop();
      assertThat(reporter.feedLoopBatchSize()).as("no sample from gate-dropped cycles").isEqualTo(-1);
      assertThat(tideBase(reporter)).isEqualTo(50);
    } finally {
      reporterThread.interrupt();
    }
  }

  @Test
  void emptyFlush_cyclesAreCensored() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.setIntervalFeedLoop(new ReportFeedLoop(ReportFeedLoop.Mode.ON, 512, 50, 1000, 50));
    reporter.start();

    // No keys ever reported: every tide drains an empty snapshot and returns
    // at the top of onFlush — a "sparse" empty batch must NOT read as signal.
    Thread.sleep(700);
    assertThat(reporter.feedLoopBatchSize()).isEqualTo(-1);
    assertThat(tideBase(reporter)).isEqualTo(50);
  }
}
