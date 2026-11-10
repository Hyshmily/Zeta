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

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.worker.detection.lifecycle.CoolingWheels;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import io.github.hyshmily.zeta.worker.detection.rule.HotKeyRule;
import io.github.hyshmily.zeta.worker.detection.rule.MatchType;
import io.github.hyshmily.zeta.worker.detection.rule.RuleBinding;
import io.github.hyshmily.zeta.worker.detection.rule.RuleResolver;
import io.github.hyshmily.zeta.worker.detection.rule.RuleSet;
import io.github.hyshmily.zeta.worker.detection.window.SlidingWindowDetector;
import io.github.hyshmily.zeta.worker.detection.window.WindowGeometry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Meter names, tags and gauge wiring. */
class WorkerDetectionMetricsTest {

  private SimpleMeterRegistry registry;
  private WorkerDetectionMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new WorkerDetectionMetrics(registry);
  }

  @AfterEach
  void tearDown() {
    registry.close();
  }

  private static long addCount(SlidingWindowDetector detector, String key, long count) {
    return detector.addCountAtSlice(
      key,
      count,
      detector.sliceIndex(io.github.hyshmily.zeta.util.TimeSource.monotonicMillis())
    );
  }

  private static void recordCount(SlidingWindowDetector detector, String key, long count) {
    detector.recordCountAtSlice(
      key,
      count,
      detector.sliceIndex(io.github.hyshmily.zeta.util.TimeSource.monotonicMillis())
    );
  }

  @Test
  void countsEveryDeliveryOutcomeUnderItsOwnNameAndType() {
    metrics.countSentDecision(ZetaDecision.DecisionType.HOT);
    metrics.countSentDecision(ZetaDecision.DecisionType.COOL);
    metrics.countDroppedDecision(ZetaDecision.DecisionType.HOT);
    metrics.countElidedDecision(ZetaDecision.DecisionType.COOL);
    metrics.countSupersededDecision(ZetaDecision.DecisionType.HOT);
    metrics.countExpiredDecision(ZetaDecision.DecisionType.COOL);
    metrics.countAbortedPromotion();
    metrics.countRecheck();
    metrics.countDisabledReport(3L);

    assertThat(registry.get(WorkerDetectionMetrics.METRIC_DECISIONS_HOT).counter().count()).isEqualTo(1.0d);
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_DECISIONS_COOL).counter().count()).isEqualTo(1.0d);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_DROPPED, "hot")).isEqualTo(1.0d);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_ELIDED, "cool")).isEqualTo(1.0d);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_SUPERSEDED, "hot")).isEqualTo(1.0d);
    assertThat(counter(WorkerDetectionMetrics.METRIC_DECISIONS_EXPIRED, "cool")).isEqualTo(1.0d);
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_HOT_ABORTED).counter().count()).isEqualTo(1.0d);
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_COOLING_RECHECK).counter().count()).isEqualTo(1.0d);
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_DETECTION_DISABLED_DROPPED).counter().count()).isEqualTo(
      3.0d
    );
  }

  private double counter(String name, String type) {
    return registry.get(name).tag("type", type).counter().count();
  }

  @Test
  void tagsRuleCountersAndCoolReasons() {
    HotKeyRule rule = new HotKeyRule("user-prefix", MatchType.PREFIX, "user:", 0, 0, 1, 0L, 0L, 60_000L, null);
    HotKeyRule defaults = new HotKeyRule(
      "defaults",
      MatchType.REGEX,
      ".*",
      Integer.MIN_VALUE,
      -1,
      100,
      0L,
      0L,
      60_000L,
      null
    );
    RuleResolver resolver = new RuleResolver(RuleSet.of(List.of(rule), defaults), 1, metrics);
    RuleBinding binding = resolver.binding("user-prefix");
    binding.recordMatch();
    binding.recordPromotion();
    binding.recordCool();
    metrics.countCoolReason(WorkerDetectionMetrics.CoolReason.EXPIRE);

    assertThat(
      registry.get(WorkerDetectionMetrics.METRIC_RULE_PROMOTIONS).tag("rule", "user-prefix").counter().count()
    ).isEqualTo(1.0d);
    assertThat(
      registry.get(WorkerDetectionMetrics.METRIC_RULE_COOLS).tag("rule", "user-prefix").counter().count()
    ).isEqualTo(1.0d);
    assertThat(
      registry.get(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "user-prefix").counter().count()
    ).isEqualTo(1.0d);
    assertThat(
      registry.get(WorkerDetectionMetrics.METRIC_COOLING_REASON).tag("reason", "expire").counter().count()
    ).isEqualTo(1.0d);
  }

  @Test
  void bindsGaugesAndFunctionCountersToTheLiveComponents() {
    long base = io.github.hyshmily.zeta.util.TimeSource.monotonicMillis();
    SlidingWindowDetector detector = new SlidingWindowDetector(WindowGeometry.of(64, 4), 1);
    try {
      io.github.hyshmily.zeta.util.TimeSource.setTimeOffsetForTest(0, -base);
      addCount(detector, "resident", 1);
      assertThat(addCount(detector, "second", 1)).as("the pass's own admission uses the hard headroom").isEqualTo(1);

      io.github.hyshmily.zeta.util.TimeSource.setTimeOffsetForTest(0, -base + 200);
      assertThat(addCount(detector, "admitted", 1)).as("the idle windows are reclaimed for the new key").isEqualTo(1);

      // Cap 1 plus the max(8, cap/100) headroom: nine seats. Eight already-hot windows exhaust
      // the ledger, so the next new key is rejected by the hard bound.
      for (int i = 0; i < 8; i++) {
        recordCount(detector, "hot-" + i, 1);
      }
      assertThat(addCount(detector, "rejected", 1)).as("the hard ledger rejects a new key").isZero();
    } finally {
      io.github.hyshmily.zeta.util.TimeSource.setTimeOffsetForTest(0, 0);
    }

    HotKeyRegistry hot = new HotKeyRegistry(4, metrics, new AtomicLong(), detector::windowSum, new CoolingWheels(1));

    metrics.bindMeters(detector, hot, () -> 2, () -> 3, () -> 4);

    assertThat(registry.get(WorkerDetectionMetrics.METRIC_DETECTOR_KEYS).gauge().value()).isEqualTo(9.0d);
    assertThat(
      registry.get(WorkerDetectionMetrics.METRIC_DETECTOR_CAPACITY_EVICTIONS).functionCounter().count()
    ).isEqualTo(2.0d);
    assertThat(
      registry.get(WorkerDetectionMetrics.METRIC_DETECTOR_CAPACITY_REJECTIONS).functionCounter().count()
    ).isEqualTo(1.0d);
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_HOT_ACTIVE).gauge().value()).isZero();
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_HOT_CAPACITY).gauge().value()).isEqualTo(4.0d);
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_BROADCAST_HOT_DEPTH).gauge().value()).isEqualTo(2.0d);
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_BROADCAST_COOL_DEPTH).gauge().value()).isEqualTo(3.0d);
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_COOLING_SCHEDULED).gauge().value()).isEqualTo(4.0d);
  }
}
