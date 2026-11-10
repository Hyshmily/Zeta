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
package io.github.hyshmily.zeta.worker.detection.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.rule.HotKeyRule;
import io.github.hyshmily.zeta.worker.detection.rule.MatchType;
import io.github.hyshmily.zeta.worker.detection.rule.RuleBinding;
import io.github.hyshmily.zeta.worker.detection.rule.RuleResolver;
import io.github.hyshmily.zeta.worker.detection.rule.RuleSet;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The registry's three invariants: seat conservation, event identity and version causality.
 *
 * <p>Time is driven through {@link TimeSource}'s offset hook so cool-down boundaries are exact; the
 * wheels are not advanced, so each due check is fired by calling {@code evaluate} with the entry
 * the wheel would have handed over.
 */
class HotKeyRegistryTest {

  private static final WorkerDetectionMetrics METRICS = new WorkerDetectionMetrics(null);
  private static final HotKeyRule DEFAULT = new HotKeyRule(
    "defaults",
    MatchType.REGEX,
    ".*",
    Integer.MIN_VALUE,
    -1,
    1_000,
    0L,
    0L,
    60_000L,
    null
  );

  private static HotKeyRegistry registry(int capacity, AtomicLong windowSum) {
    return new HotKeyRegistry(capacity, METRICS, new AtomicLong(), key -> windowSum.get(), new CoolingWheels(1));
  }

  private static RuleBinding binding(HotKeyRule rule) {
    RuleSet set = RuleSet.of(List.of(rule), DEFAULT);
    return Objects.requireNonNull(new RuleResolver(set, 1, METRICS).binding(rule.id()));
  }

  private static ZetaDecision promote(HotKeyRegistry registry, String key, HotKeyRule rule) {
    HE entry = registry.promote(key, binding(rule), TimeSource.monotonicMillis());
    return entry == null ? null : entry.decision();
  }

  private static HotKeyRule lowQps(long hotCount, long coolCount, long lowQpsForMs) {
    return new HotKeyRule("low", MatchType.PREFIX, "k", 0, 0, hotCount, coolCount, lowQpsForMs, 100L, null);
  }

  private static void assertConserved(HotKeyRegistry registry) {
    assertThat(registry.availableSeats() + registry.size()).isEqualTo(registry.capacity());
  }

  @Test
  void promotesAnEligibleBinding() {
    HotKeyRule rule = lowQps(5, 2, 1_000);
    HotKeyRegistry registry = registry(2, new AtomicLong(0));
    ZetaDecision decision = promote(registry, "k1", rule);
    assertThat(decision).isNotNull();
    assertThat(decision.type()).isEqualTo(ZetaDecision.DecisionType.HOT);
    assertThat(decision.decisionVersion()).isEqualTo(1L);
    assertThat(registry.isHot("k1")).isTrue();
    assertConserved(registry);
  }

  @Test
  void refusesNewKeysAtCapacityWithoutEvictingExistingOnes() {
    HotKeyRule rule = lowQps(5, 2, 1_000);
    HotKeyRegistry registry = registry(1, new AtomicLong(0));
    assertThat(promote(registry, "a", rule)).isNotNull();
    assertThat(promote(registry, "b", rule)).isNull();
    assertThat(registry.isHot("a")).isTrue();
    assertThat(registry.rejected()).isEqualTo(1);
    assertConserved(registry);
  }

  /**
   * The currency probe a delayed HOT is judged by: the promotion stands only while its own entry owns
   * the seat. A cool releases it, and a re-promotion is a different version.
   */
  @Test
  void aPromotionIsCurrentOnlyWhileItsEntryOwnsTheSeat() {
    HotKeyRule rule = lowQps(5, 2, 1_000);
    HotKeyRegistry registry = registry(2, new AtomicLong(0));
    ZetaDecision first = promote(registry, "k1", rule);
    assertThat(first).isNotNull();
    assertThat(registry.isCurrent("k1", first.decisionVersion())).isTrue();

    registry.abort("k1", first.decisionVersion());
    assertThat(registry.isCurrent("k1", first.decisionVersion())).isFalse();

    ZetaDecision second = promote(registry, "k1", rule);
    assertThat(second).isNotNull();
    assertThat(registry.isCurrent("k1", second.decisionVersion())).isTrue();
    assertThat(registry.isCurrent("k1", first.decisionVersion())).isFalse();
  }

  @Test
  void abortReturnsTheSeatKeepsTheInvariantAndUnschedulesTheCheck() {
    HotKeyRule rule = lowQps(5, 2, 1_000);
    HotKeyRegistry registry = registry(1, new AtomicLong(0));
    ZetaDecision promotion = promote(registry, "a", rule);
    assertThat(promotion).isNotNull();

    registry.abort("a", promotion.decisionVersion());

    assertThat(registry.isHot("a")).isFalse();
    assertConserved(registry);
    assertThat(promote(registry, "a", rule)).as("the key can be promoted again").isNotNull();
  }

  @Test
  void abortOfAStaleVersionLeavesTheEntryAlone() {
    HotKeyRule rule = lowQps(5, 2, 1_000);
    HotKeyRegistry registry = registry(1, new AtomicLong(0));
    promote(registry, "a", rule);
    registry.abort("a", 999L);
    assertThat(registry.isHot("a")).isTrue();
    assertConserved(registry);
  }

  @Test
  void aFiredCheckCoolsTheKeyExactlyOnce() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      HotKeyRule rule = lowQps(5, 2, 0);
      HotKeyRegistry registry = registry(1, new AtomicLong(0));
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE entry = registry.entry("k1");
      TimeSource.setTimeOffsetForTest(0, -base + 100);

      ZetaDecision first = registry.evaluate(entry);
      ZetaDecision second = registry.evaluate(entry);

      assertThat(first).isNotNull();
      assertThat(first.type()).isEqualTo(ZetaDecision.DecisionType.COOL);
      assertThat(second).as("the entry is gone and the seat was released once").isNull();
      assertConserved(registry);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void aCooledEntryCannotReleaseTheSeatTwice() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      HotKeyRule rule = lowQps(5, 2, 0);
      HotKeyRegistry registry = registry(1, new AtomicLong(0));
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE entry = registry.entry("k1");
      TimeSource.setTimeOffsetForTest(0, -base + 100);

      assertThat(registry.evaluate(entry)).isNotNull();
      assertThat(registry.evaluate(entry)).isNull();
      assertConserved(registry);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * The wheel schedules the entry object; after a cool and a re-promotion the map holds a different
   * entry, so the old event fails the identity check and cannot release the new seat.
   */
  @Test
  void aStaleEventForACooledAndRepromotedKeyIsIgnored() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      HotKeyRule rule = lowQps(5, 2, 0);
      HotKeyRegistry registry = registry(1, new AtomicLong(0));
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE previous = registry.entry("k1");
      TimeSource.setTimeOffsetForTest(0, -base + 100);
      assertThat(registry.evaluate(previous)).isNotNull();

      assertThat(promote(registry, "k1", rule)).isNotNull();
      assertThat(registry.evaluate(previous)).as("the previous life's event is stale").isNull();
      assertThat(registry.isHot("k1")).isTrue();
      assertConserved(registry);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void lowQpsDwellStartsOnTheFirstBelowBandCheckAndCoolsWhenItElapses() {
    long base = TimeSource.monotonicMillis();
    AtomicLong windowSum = new AtomicLong(3);
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      HotKeyRule rule = lowQps(5, 3, 1_000);
      HotKeyRegistry registry = registry(1, windowSum);
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE entry = registry.entry("k1");

      // Above the band: no dwell, the check is re-armed and belowSince stays clear. The first
      // check is due one interval after promotion.
      TimeSource.setTimeOffsetForTest(0, -base + 100);
      assertThat(registry.evaluate(entry)).isNull();
      assertThat(entry.belowSince()).isEqualTo(HE.NOT_BELOW);

      // Below the band: the dwell starts at this instant.
      windowSum.set(0);
      TimeSource.setTimeOffsetForTest(0, -base + 200);
      assertThat(registry.evaluate(entry)).isNull();
      long dwellStart = entry.belowSince();
      assertThat(dwellStart).isBetween(200L, 210L);

      // Still inside the dwell window.
      TimeSource.setTimeOffsetForTest(0, -base + 600);
      assertThat(registry.evaluate(entry)).isNull();

      // The dwell elapsed: the key cools.
      TimeSource.setTimeOffsetForTest(0, -base + 1_200);
      ZetaDecision cool = registry.evaluate(entry);
      assertThat(cool).isNotNull();
      assertThat(cool.type()).isEqualTo(ZetaDecision.DecisionType.COOL);
      assertConserved(registry);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A negative monotonic coordinate is a real dwell start. The registry must not clamp it to a
   * positive sentinel, which would make a key that dropped below the band before the epoch wait
   * until the clock crossed zero before its dwell could elapse.
   */
  @Test
  void lowQpsDwellUsesANegativeMonotonicTimestampVerbatim() {
    long base = TimeSource.monotonicMillis();
    AtomicLong windowSum = new AtomicLong(0);
    try {
      TimeSource.setTimeOffsetForTest(0, -base - 10_000L);
      HotKeyRule rule = lowQps(5, 3, 1_000);
      HotKeyRegistry registry = registry(1, windowSum);
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE entry = registry.entry("k1");

      TimeSource.setTimeOffsetForTest(0, -base - 9_900L);
      assertThat(registry.evaluate(entry)).isNull();
      assertThat(entry.belowSince()).isBetween(-9_900L, -9_890L);

      TimeSource.setTimeOffsetForTest(0, -base - 8_900L);
      ZetaDecision cool = registry.evaluate(entry);
      assertThat(cool).isNotNull();
      assertThat(cool.type()).isEqualTo(ZetaDecision.DecisionType.COOL);
      assertConserved(registry);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void recoveryClearsTheDwellClock() {
    long base = TimeSource.monotonicMillis();
    AtomicLong windowSum = new AtomicLong(0);
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      HotKeyRule rule = lowQps(5, 3, 1_000);
      HotKeyRegistry registry = registry(1, windowSum);
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE entry = registry.entry("k1");

      TimeSource.setTimeOffsetForTest(0, -base + 100);
      assertThat(registry.evaluate(entry)).isNull();
      assertThat(entry.belowSince()).isNotEqualTo(HE.NOT_BELOW);

      windowSum.set(3);
      TimeSource.setTimeOffsetForTest(0, -base + 200);
      assertThat(registry.evaluate(entry)).isNull();
      assertThat(entry.belowSince()).as("sum == coolCount is not below the band").isEqualTo(HE.NOT_BELOW);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A deadline that would overflow the monotonic axis saturates at {@link Long#MAX_VALUE}; wrapping
   * negative would make the wheel treat the promotion as already due and cool the key immediately.
   */
  @Test
  void aSaturatedExpireDeadlineStaysFutureInsteadOfCoolingImmediately() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base + 2);
      HotKeyRule rule = new HotKeyRule("expire", MatchType.PREFIX, "k", 0, 0, 5, 0L, 0L, Long.MAX_VALUE - 1L, null);
      HotKeyRegistry registry = registry(1, new AtomicLong(1_000));
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE entry = registry.entry("k1");
      assertThat(entry.nextCheckAt()).isEqualTo(Long.MAX_VALUE);

      assertThat(registry.evaluate(entry)).isNull();
      assertThat(registry.isHot("k1")).isTrue();
      assertConserved(registry);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * The LOW_QPS re-arm takes the earlier of the next check and the dwell deadline. A dwell bound
   * that would overflow must not win that comparison as a past instant and make the wheel fire the
   * same check forever.
   */
  @Test
  void aSaturatedLowQpsDwellDeadlineReArmsAtTheNextCheck() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base + 2);
      HotKeyRule rule = lowQps(5, 2, Long.MAX_VALUE - 1);
      HotKeyRegistry registry = registry(1, new AtomicLong(0));
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE entry = registry.entry("k1");

      TimeSource.setTimeOffsetForTest(0, -base + 102);
      assertThat(registry.evaluate(entry)).isNull();
      assertThat(entry.belowSince()).isNotEqualTo(HE.NOT_BELOW);
      assertThat(entry.nextCheckAt())
        .as("the huge dwell bound must not win the earlier-deadline comparison")
        .isEqualTo(entry.belowSince() + 100L);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void expireCoolsOnTheDeadlineEvenWhileTheWindowIsHot() {
    long base = TimeSource.monotonicMillis();
    AtomicLong windowSum = new AtomicLong(1_000);
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      HotKeyRule rule = new HotKeyRule("expire", MatchType.PREFIX, "k", 0, 0, 5, 0L, 0L, 1_000L, null);
      HotKeyRegistry registry = registry(1, windowSum);
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE entry = registry.entry("k1");

      TimeSource.setTimeOffsetForTest(0, -base + 500);
      assertThat(registry.evaluate(entry)).isNull();

      TimeSource.setTimeOffsetForTest(0, -base + 1_000);
      ZetaDecision cool = registry.evaluate(entry);
      assertThat(cool).isNotNull();
      assertThat(cool.type()).isEqualTo(ZetaDecision.DecisionType.COOL);
      assertConserved(registry);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * The lateness metric belongs to the check the wheel's owner actually evaluates: a wheel-fired
   * check reports the delay against the deadline it replaces, while {@code repair()} only re-links
   * and records no sample of its own.
   */
  @Test
  void aWheelFiredCheckRecordsItsLatenessAndRepairRecordsNone() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      WorkerDetectionMetrics metrics = new WorkerDetectionMetrics(meterRegistry);
      HotKeyRule rule = new HotKeyRule("expire", MatchType.PREFIX, "k", 0, 0, 5, 0L, 0L, 10_000L, null);
      RuleSet set = RuleSet.of(List.of(rule), DEFAULT);
      RuleBinding binding = Objects.requireNonNull(new RuleResolver(set, 2, metrics).binding("expire"));
      HotKeyRegistry registry = new HotKeyRegistry(2, metrics, new AtomicLong(), key -> 0, new CoolingWheels(1));
      assertThat(registry.promote("k1", binding, TimeSource.monotonicMillis())).isNotNull();
      assertThat(registry.promote("k2", binding, TimeSource.monotonicMillis())).isNotNull();
      HE first = registry.entry("k1");

      TimeSource.setTimeOffsetForTest(0, -base + 10_050);
      assertThat(registry.evaluate(first)).isNotNull();
      assertThat(meterRegistry.get(WorkerDetectionMetrics.METRIC_COOLING_LATENESS).timer().count()).isEqualTo(1);
      assertThat(
        meterRegistry.get(WorkerDetectionMetrics.METRIC_COOLING_LATENESS).timer().max(TimeUnit.MILLISECONDS)
      ).isGreaterThanOrEqualTo(50.0d);

      TimeSource.setTimeOffsetForTest(0, -base + 11_050);
      assertThat(registry.repair()).isEqualTo(1);
      assertThat(meterRegistry.get(WorkerDetectionMetrics.METRIC_COOLING_LATENESS).timer().count())
        .as("repair re-links without evaluating, so it records no lateness")
        .isEqualTo(1);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
      meterRegistry.close();
    }
  }

  /**
   * The registry owns the {@code DETACHED -> UNSCHEDULED} hand-off. A repair that runs after the
   * wheel polled the node but before the evaluation takes the stripe must defer to the owner; if it
   * re-linked the node as detached, the evaluation's re-arm would be skipped and the fresh deadline
   * would sit in the pending list for a spurious extra wake.
   */
  @Test
  void aRepairBetweenTheWheelHandoffAndTheEvaluationDefersToTheOwner() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      HotKeyRule rule = lowQps(5, 3, 1_000);
      CoolingWheels wheels = new CoolingWheels(1);
      AtomicLong windowSum = new AtomicLong(1_000);
      HotKeyRegistry registry = new HotKeyRegistry(2, METRICS, new AtomicLong(), key -> windowSum.get(), wheels);
      assertThat(promote(registry, "k1", rule)).isNotNull();
      HE entry = registry.entry("k1");
      TimerWheel wheel = wheels.wheel(0);

      TimeSource.setTimeOffsetForTest(0, -base + 1_200L);
      wheel.advance(1_200L);
      assertThat(wheel.pollDetached()).as("the wheel hands the due node over as detached").isSameAs(entry);
      assertThat(registry.repair()).as("the overdue check is selected").isEqualTo(1);
      assertThat(entry.wheelState).as("repair must not take over the hand-off").isEqualTo(TimerWheel.DETACHED);

      assertThat(registry.evaluate(entry)).isNull();
      assertThat(entry.nextCheckAt()).as("the recovered check re-arms at the next interval").isBetween(1_300L, 1_310L);
      assertThat(entry.wheelState).as("the owner links the fresh deadline").isEqualTo(TimerWheel.SCHEDULED);
      assertThat(wheel.scheduledCount()).isEqualTo(1);
      assertThat(registry.isHot("k1")).isTrue();
      assertConserved(registry);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A check that has only just reached its deadline may still be owned by a wheel thread; the repair
   * leaves it alone. Only a check that stays overdue past the grace is re-linked, and re-linking does
   * not evaluate: the wheel remains the only evaluator.
   */
  @Test
  void repairLeavesAJustDueCheckToTheWheel() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      HotKeyRule rule = new HotKeyRule("expire", MatchType.PREFIX, "k", 0, 0, 5, 0L, 0L, 10_000L, null);
      HotKeyRegistry registry = registry(2, new AtomicLong(1_000));
      promote(registry, "k1", rule);
      promote(registry, "k2", rule);
      HE first = registry.entry("k1");

      assertThat(registry.repair()).isZero();

      TimeSource.setTimeOffsetForTest(0, -base + 10_000);
      assertThat(registry.repair()).as("a check at its deadline belongs to the wheel's thread").isZero();

      TimeSource.setTimeOffsetForTest(0, -base + 11_001);
      assertThat(registry.repair()).isEqualTo(2);
      assertThat(first.wheelState)
        .as("the repair re-links; the wheel owns the evaluation")
        .isEqualTo(TimerWheel.SCHEDULED);
      assertThat(registry.isHot("k1")).isTrue();
      assertConserved(registry);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }
}
