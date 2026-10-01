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
package io.github.hyshmily.zeta.worker.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.worker.detection.Evaluator;
import io.github.hyshmily.zeta.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.zeta.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.zeta.worker.ingest.ReportConsumer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Tests for {@link WorkerDetectionMetrics} — the Worker's detection-plane
 * measurement baseline (ADR-0080): the detector gauge, the emission counters and
 * the monotonic per-batch evaluation timer.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("WorkerDetectionMetrics tests")
class WorkerDetectionMetricsTest {

  @Mock
  private RabbitTemplate rabbitTemplate;

  @Mock
  private Evaluator keyEvaluator;

  @Mock
  private GlobalQpsEstimator globalQpsEstimator;

  @Mock
  private ZetaBayesianSM stateMachine;

  private SimpleMeterRegistry registry;
  private SlidingWindowDetector detector;
  private WorkerDetectionMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    detector = new SlidingWindowDetector(10_000, 10, 1000);
    metrics = new WorkerDetectionMetrics(registry, detector);
  }

  /**
   * Verifies all three meters register when a {@link io.micrometer.core.instrument.MeterRegistry}
   * is supplied, and that exactly the four declared meters exist (no duplicates).
   */
  @Test
  @DisplayName("all three meters register when a MeterRegistry is present")
  void registersAllMetersWhenRegistryPresent() {
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_DETECTOR_KEYS).gauge()).isNotNull();
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_DECISIONS_HOT).counter()).isNotNull();
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_DECISIONS_COOL).counter()).isNotNull();
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_REPORT_EVAL).timer()).isNotNull();

    assertThat(registry.getMeters()).hasSize(4);
  }

  /**
   * Verifies the gauge reads the detector's per-key window map — the state the
   * pre-existing {@code zeta.worker.tracked.keys} gauge cannot see — and follows it
   * down when idle keys are evicted.
   */
  @Test
  @DisplayName("zeta.worker.detector.keys tracks the detector's per-key window map")
  void gaugeTracksDetectorKeyWindowMapSize() {
    assertThat(detectorKeys()).isZero();

    detector.addCount("a", 1);
    detector.addCount("b", 1);
    detector.addCount("c", 1);
    assertThat(detectorKeys()).isEqualTo(3.0);

    // Age the entries past the eviction threshold on the monotonic axis (no sleep)
    // and evict: the gauge must follow the map down.
    TimeSource.setTimeOffsetForTest(0, 60_000);
    try {
      detector.evictStale(1);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
    assertThat(detector.getActiveKeyCount()).isZero();
    assertThat(detectorKeys()).isZero();
  }

  /**
   * Verifies the counters count successful emissions only: a send failure returns
   * {@code false} (the caller rolls the state machine back and the decision is
   * re-emitted later) and must not be counted, or the retry would be counted twice.
   */
  @Test
  @DisplayName("decision counters count successful emissions, never attempts")
  void decisionCountersCountSuccessfulEmissionsOnly() {
    WorkerBroadcaster broadcaster = newBroadcaster(metrics);

    assertThat(broadcaster.broadcastHot("hotKey")).isTrue();
    assertThat(broadcaster.broadcastCool("coolKey")).isTrue();
    assertThat(hotDecisions()).isEqualTo(1.0);
    assertThat(coolDecisions()).isEqualTo(1.0);

    doThrow(new RuntimeException("broker down"))
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));

    assertThat(broadcaster.broadcastHot("failedHotKey")).isFalse();
    assertThat(broadcaster.broadcastCool("failedCoolKey")).isFalse();
    assertThat(hotDecisions()).isEqualTo(1.0);
    assertThat(coolDecisions()).isEqualTo(1.0);
  }

  /**
   * Verifies the 100 ms dedup elision is not counted: it emits nothing (the
   * duplicate decision was already counted when it was sent) even though the
   * caller still sees success and must not roll back.
   */
  @Test
  @DisplayName("a dedup-elided duplicate emits nothing and is not counted again")
  void dedupElidedDuplicateIsNotCountedAgain() {
    WorkerBroadcaster broadcaster = newBroadcaster(metrics);

    assertThat(broadcaster.broadcastHot("sameKey")).isTrue();
    assertThat(broadcaster.broadcastHot("sameKey")).isTrue();

    assertThat(hotDecisions()).isEqualTo(1.0);
    verify(rabbitTemplate, times(1)).send(any(String.class), any(String.class), any(Message.class));
  }

  /**
   * Verifies the legacy broadcaster constructor (no meters collaborator) keeps the
   * legacy path meter-free: the sends still succeed, nothing throws, and no counter
   * moves — the ADR-0037/0061 "null keeps the legacy path" convention.
   */
  @Test
  @DisplayName("the legacy broadcaster constructor leaves the decision counters untouched")
  void legacyConstructorLeavesCountersUnregistered() {
    WorkerBroadcaster legacy = new WorkerBroadcaster(
      rabbitTemplate,
      "zeta.send.exchange",
      "testApp",
      "test-node",
      new AtomicLong(0L),
      mock(SnowflakeIdGenerator.class)
    );

    assertThat(legacy.broadcastHot("legacyKey")).isTrue();
    assertThat(legacy.broadcastCool("legacyCoolKey")).isTrue();

    assertThat(hotDecisions()).isZero();
    assertThat(coolDecisions()).isZero();
  }

  /**
   * Verifies the timer records one monotonic sample per evaluated report batch, in
   * nanoseconds, and that batches which never reach the per-key loop (dropped by the
   * staleness filter, or empty) record nothing.
   */
  @Test
  @DisplayName("zeta.worker.report.eval records one nanosecond sample per evaluated batch")
  void reportEvalTimerRecordsOneSamplePerEvaluatedBatch() throws Exception {
    // A 2 ms evaluation makes the recorded span measurably non-zero without
    // asserting on machine speed.
    when(keyEvaluator.evaluate(anyString(), anyLong(), anyDouble())).thenAnswer(invocation -> {
      Thread.sleep(2);
      return ZetaDecision.none("k", null);
    });

    ReportConsumer consumer = new ReportConsumer(
      keyEvaluator,
      newBroadcaster(metrics),
      globalQpsEstimator,
      stateMachine,
      0L,
      null,
      metrics
    );
    consumer.onReport(batch("k1", 1L));

    Timer timer = registry.find(WorkerDetectionMetrics.METRIC_REPORT_EVAL).timer();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThanOrEqualTo(2_000_000L);

    // Neither a batch dropped by the staleness filter nor an empty batch reaches
    // the evaluation loop, so neither adds a sample.
    ReportConsumer filtered = new ReportConsumer(
      keyEvaluator,
      newBroadcaster(metrics),
      globalQpsEstimator,
      stateMachine,
      5_000L,
      null,
      metrics
    );
    filtered.onReport(new ReportMessage(0L, "testApp", System.currentTimeMillis() - 60_000, Map.of("k2", 1L)));
    filtered.onReport(new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of()));

    assertThat(timer.count()).isEqualTo(1L);
  }

  /**
   * Verifies a meter-less instance (no registry) records nothing and never throws
   * from the recording methods — the path a Worker without micrometer takes. The
   * registry-backed instance of the same detector remains the only one that
   * registers anything.
   */
  @Test
  @DisplayName("a meter-less instance records nothing and never throws")
  void meterlessInstanceRecordsNothing() {
    WorkerDetectionMetrics meterless = new WorkerDetectionMetrics(null, detector);

    assertThatCode(() -> {
      meterless.countHotDecision();
      meterless.countCoolDecision();
      meterless.recordReportEval(meterless.startReportEval());
    }).doesNotThrowAnyException();

    assertThat(registry.getMeters()).hasSize(4);
  }

  private double detectorKeys() {
    return registry.get(WorkerDetectionMetrics.METRIC_DETECTOR_KEYS).gauge().value();
  }

  private double hotDecisions() {
    return registry.get(WorkerDetectionMetrics.METRIC_DECISIONS_HOT).counter().count();
  }

  private double coolDecisions() {
    return registry.get(WorkerDetectionMetrics.METRIC_DECISIONS_COOL).counter().count();
  }

  private WorkerBroadcaster newBroadcaster(WorkerDetectionMetrics meters) {
    return new WorkerBroadcaster(
      rabbitTemplate,
      "zeta.send.exchange",
      "testApp",
      "test-node",
      new AtomicLong(0L),
      mock(SnowflakeIdGenerator.class),
      meters
    );
  }

  private ReportMessage batch(String key, long count) {
    return new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of(key, count));
  }
}
