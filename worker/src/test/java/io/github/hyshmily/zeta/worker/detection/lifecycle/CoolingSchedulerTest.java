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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.rule.HotKeyRule;
import io.github.hyshmily.zeta.worker.detection.rule.MatchType;
import io.github.hyshmily.zeta.worker.detection.rule.RuleBinding;
import io.github.hyshmily.zeta.worker.detection.rule.RuleResolver;
import io.github.hyshmily.zeta.worker.detection.rule.RuleSet;
import io.github.hyshmily.zeta.worker.dispatch.DecisionDispatcher;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The wheel thread fires a due check once, submits its COOL, and releases the seat. */
class CoolingSchedulerTest {

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

  private SimpleMeterRegistry meterRegistry;
  private WorkerDetectionMetrics metrics;
  private CoolingWheels wheels;
  private HotKeyRegistry registry;
  private DecisionDispatcher dispatcher;
  private CoolingScheduler scheduler;
  private HotKeyRule rule;
  private RuleBinding binding;

  @BeforeEach
  void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    metrics = new WorkerDetectionMetrics(meterRegistry);
    wheels = new CoolingWheels(1);
    rule = new HotKeyRule("expire", MatchType.PREFIX, "k", 0, 0, 1, 0L, 0L, 50L, null);
    binding = binding(rule);
    registry = new HotKeyRegistry(4, metrics, new AtomicLong(), key -> 0, wheels);
    dispatcher = mock(DecisionDispatcher.class);
  }

  private RuleBinding binding(HotKeyRule hotKeyRule) {
    RuleSet set = RuleSet.of(List.of(hotKeyRule), DEFAULT);
    RuleResolver resolver = new RuleResolver(set, 4, metrics);
    return Objects.requireNonNull(resolver.binding(hotKeyRule.id()));
  }

  @AfterEach
  void tearDown() {
    if (scheduler != null) {
      scheduler.shutdown();
    }
    meterRegistry.close();
  }

  @Test
  void aDueCheckIsEvaluatedOnceAndItsCoolIsSubmitted() {
    scheduler = new CoolingScheduler(wheels, registry, dispatcher);
    assertThat(registry.promote("k1", binding, TimeSource.monotonicMillis())).isNotNull();
    assertThat(scheduler.scheduled()).isEqualTo(1);

    verify(dispatcher, timeout(2_000)).submit(
      argThat((ZetaDecision cool) -> cool != null && cool.type() == ZetaDecision.DecisionType.COOL)
    );
    assertThat(registry.isHot("k1")).isFalse();
    assertThat(scheduler.scheduled()).isZero();
  }

  /**
   * A per-check Error is isolated at the entry boundary: the rest of the due batch still runs and
   * the wheel owner survives. A dead owner leaves every key on its wheel unscheduled, and the
   * repair pass cannot replace it.
   */
  @Test
  void aFailingCheckDoesNotKillTheWheel() {
    wheels = new CoolingWheels(1);
    AtomicBoolean failed = new AtomicBoolean();
    HotKeyRule low = new HotKeyRule("low", MatchType.PREFIX, "k", 0, 0, 5, 1, 0L, 50L, null);
    RuleBinding lowBinding = binding(low);
    registry = new HotKeyRegistry(
      4,
      metrics,
      new AtomicLong(),
      key -> {
        if (failed.compareAndSet(false, true)) {
          throw new AssertionError("poisoned window read");
        }
        return 0L;
      },
      wheels
    );
    dispatcher = mock(DecisionDispatcher.class);
    scheduler = new CoolingScheduler(wheels, registry, dispatcher);

    assertThat(registry.promote("k1", lowBinding, TimeSource.monotonicMillis())).isNotNull();
    assertThat(registry.promote("k2", lowBinding, TimeSource.monotonicMillis())).isNotNull();

    verify(dispatcher, timeout(2_000)).submit(argThat(CoolingSchedulerTest::isCool));
    assertThat(registry.size()).as("the poisoned key stays hot and the other cooled").isEqualTo(1);
  }

  /**
   * A failing submission is isolated at the same boundary: the wheel owner keeps draining the rest
   * of the due batch.
   */
  @Test
  void aFailingSubmissionDoesNotKillTheWheel() {
    wheels = new CoolingWheels(1);
    HotKeyRule low = new HotKeyRule("low", MatchType.PREFIX, "k", 0, 0, 5, 1, 0L, 50L, null);
    RuleBinding lowBinding = binding(low);
    registry = new HotKeyRegistry(4, metrics, new AtomicLong(), key -> 0, wheels);
    dispatcher = mock(DecisionDispatcher.class);
    doThrow(new AssertionError("poisoned submission")).doReturn(false).when(dispatcher).submit(any(ZetaDecision.class));
    scheduler = new CoolingScheduler(wheels, registry, dispatcher);

    assertThat(registry.promote("k1", lowBinding, TimeSource.monotonicMillis())).isNotNull();
    assertThat(registry.promote("k2", lowBinding, TimeSource.monotonicMillis())).isNotNull();

    verify(dispatcher, timeout(2_000).times(2)).submit(any(ZetaDecision.class));
  }

  /**
   * An interrupt observed while the scheduler is running is not a shutdown: the owner clears it and
   * keeps advancing, instead of leaving every key on its wheel unscheduled.
   */
  @Test
  void anUnexpectedInterruptDoesNotKillTheWheel() throws InterruptedException {
    wheels = new CoolingWheels(1);
    AtomicBoolean interrupted = new AtomicBoolean();
    HotKeyRule low = new HotKeyRule("low", MatchType.PREFIX, "k", 0, 0, 5, 1, 0L, 50L, null);
    RuleBinding lowBinding = binding(low);
    registry = new HotKeyRegistry(
      4,
      metrics,
      new AtomicLong(),
      key -> {
        if (interrupted.compareAndSet(false, true)) {
          Thread.currentThread().interrupt();
        }
        return 0L;
      },
      wheels
    );
    dispatcher = mock(DecisionDispatcher.class);
    scheduler = new CoolingScheduler(wheels, registry, dispatcher);

    assertThat(registry.promote("k1", lowBinding, TimeSource.monotonicMillis())).isNotNull();
    verify(dispatcher, timeout(2_000)).submit(argThat(CoolingSchedulerTest::isCool));

    // Let the owner reach the wait and observe the interrupt before the next key becomes due.
    Thread.sleep(100L);
    assertThat(registry.promote("k2", lowBinding, TimeSource.monotonicMillis())).isNotNull();
    verify(dispatcher, timeout(2_000).times(2)).submit(argThat(CoolingSchedulerTest::isCool));
  }

  /** A shutdown wakes a thread waiting on an empty wheel instead of letting it burn the join timeout. */
  @Test
  void shutdownWakesAnIdleThread() {
    scheduler = new CoolingScheduler(wheels, registry, dispatcher);

    long start = System.nanoTime();
    scheduler.shutdown();
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

    assertThat(elapsedMs).isLessThan(1_000L);
  }

  private static boolean isCool(ZetaDecision decision) {
    return decision != null && decision.type() == ZetaDecision.DecisionType.COOL;
  }
}
