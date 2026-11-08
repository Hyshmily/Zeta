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
import io.github.hyshmily.zeta.reporting.impl.ReportFeedLoop;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.ConsistentHashRingImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.HealthViewImpl;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import java.lang.reflect.Field;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * design decisions: reporting integration: the feed-loop interval tuner wired into
 * {@link KeyReporterImpl#publishBatch}. Pins the four behaviors that matter on the
 * real flush path — shadow computes without applying, apply moves the
 * WaveCounter tide base on sparse batches, out-of-band direct reports are never
 * sampled, an empty window is sampled at zero rate, and a no-alive-Worker cycle
 * never produces a sample.
 */
class KeyReporterFeedLoopTest {

  private static final int QUEUE_CAPACITY = 1000;
  private static final long AWAIT_TIMEOUT_MS = 5000;

  private ScheduledExecutorService scheduler;
  private KeyReporterTest.TestReportPublisher testPublisher;
  private ConsistentHashRing ring;
  private HealthView healthView;
  private KeyReporterImpl reporter;
  private AccessCounter reporterCounter;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    testPublisher = new KeyReporterTest.TestReportPublisher();
    ring = new ConsistentHashRingImpl(150);
    healthView = new HealthViewImpl(30000);
    reporter = newReporter();
    // design decisions: access counting: the tide lives on the shared access counter; the feed-loop
    // tuner's apply mode moves it through the cadence-adjuster seam.
    reporterCounter = new AccessCounter(
      mock(io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector.class),
      reporter,
      scheduler,
      50L,
      AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS
    );
    reporter.setCadenceAdjuster(reporterCounter::adjustDeliverIntervalMs);
    reporterCounter.afterPropertiesSet();
  }

  private KeyReporterImpl newReporter() {
    return new KeyReporterImpl(testPublisher, "testApp", QUEUE_CAPACITY, 100, 1, ring, healthView);
  }

  @AfterEach
  void tearDown() {
    reporterCounter.destroy();
    reporter.stop();
    scheduler.shutdownNow();
  }

  private static void registerWorker(HealthView hv, String workerId) {
    hv.onHeartbeat(new WorkerHeartbeatMessage(0L, workerId, 1L, true, null));
  }

  /** Reads the shared counter's current tide base (the value apply-mode moves) — design decisions: access counting. */
  private static long tideBase(AccessCounter counter) throws Exception {
    Field waveField = AccessCounter.class.getDeclaredField("counter");
    waveField.setAccessible(true);
    Object wave = waveField.get(counter);
    Field intervalField = wave.getClass().getDeclaredField("deliverIntervalMs");
    intervalField.setAccessible(true);
    return intervalField.getLong(wave);
  }

  /**
   * Reports a key every 10ms from a daemon thread so every tide carries a
   * non-empty batch (a once-reported key leaves all later tides empty; empty
   * tides now sample zero and would pull the cadence toward the ceiling).
   */
  private Thread startContinuousReporter(String key) {
    AtomicBoolean stop = new AtomicBoolean(false);
    Thread thread = new Thread(
      () -> {
        while (!stop.get()) {
          reporterCounter.add(key);
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
    assertThat(reporter.feedLoop()).isNull();
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
      assertThat(waitFor(() -> reporter.feedLoop().intervalMs() > 50))
        .as("shadow tuner computed a >50ms interval")
        .isTrue();
      assertThat(reporter.feedLoop().lastScoreBp()).isBetween(0L, 10_000L);
      assertThat(reporter.feedLoop().projectedBatchSize()).isPositive();
      assertThat(tideBase(reporterCounter)).as("shadow mode never applies").isEqualTo(50);
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
      boolean applied = waitFor(() -> tideBase(reporterCounter) > 50);
      assertThat(applied).as("apply mode moved the tide base").isTrue();
      // The tide base is applied before the batch finishes its routing →
      // dispatcher hops, so wait for the pipeline instead of sampling once.
      assertThat(waitFor(() -> testPublisher.publishCount > 0))
        .as("flush pipeline delivered at least one batch")
        .isTrue();
      // The tuner mirrors the applied base exactly.
      assertThat(reporter.feedLoop().intervalMs()).isEqualTo(tideBase(reporterCounter));
    } finally {
      reporterThread.interrupt();
    }
  }

  @Test
  void reportOnlyDirectPublish_isNotSampled() throws Exception {
    registerWorker(healthView, "worker-1");
    // A dedicated reporter with no tide loop: publishDirect must not sample even
    // though the shared counter's idle tides do.
    KeyReporterImpl directOnly = newReporter();
    directOnly.setIntervalFeedLoop(new ReportFeedLoop(ReportFeedLoop.Mode.ON, 512, 50, 1000, 50));
    directOnly.start();
    try {
      directOnly.publishDirect(java.util.Map.of("tag-only", 1L));
      // The out-of-band single-key batch reaches the publisher ...
      assertThat(waitFor(() -> testPublisher.publishCount > 0)).as("direct report delivered").isTrue();
      // ... without teaching the tuner anything about a flush window.
      Thread.sleep(200);
      assertThat(directOnly.feedLoop().projectedBatchSize()).as("direct report not sampled").isEqualTo(-1);
    } finally {
      directOnly.stop();
    }
  }

  @Test
  void noAliveWorker_cyclesAreCensored() {
    // No worker registered: the flush drops at the alive-set guard, before the
    // sampling point — the tuner must never see a sample, even for a non-empty
    // batch.
    reporter.setIntervalFeedLoop(new ReportFeedLoop(ReportFeedLoop.Mode.ON, 512, 50, 1000, 50));
    reporter.start();

    reporter.publishBatch(java.util.Map.of("ghost-key", 1L), 50L);

    assertThat(reporter.feedLoop().projectedBatchSize()).as("no sample from dropped cycles").isEqualTo(-1);
    assertThat(reporter.feedLoop().lastScoreBp()).isEqualTo(-1);
  }

  @Test
  void emptyFlush_cyclesAreSampledAsZeroRate() throws Exception {
    registerWorker(healthView, "worker-1");
    reporter.setIntervalFeedLoop(new ReportFeedLoop(ReportFeedLoop.Mode.ON, 512, 50, 1000, 50));
    reporter.start();

    // No keys ever reported: every tide is an empty window and must feed the
    // loop as a zero-rate observation, backing the cadence off toward max-ms.
    assertThat(waitFor(() -> reporter.feedLoop().projectedBatchSize() >= 0))
      .as("an empty window produces a sample")
      .isTrue();
    assertThat(reporter.feedLoop().projectedBatchSize()).isZero();
    assertThat(waitFor(() -> tideBase(reporterCounter) > 50)).as("apply mode backs off on idle").isTrue();
  }
}
