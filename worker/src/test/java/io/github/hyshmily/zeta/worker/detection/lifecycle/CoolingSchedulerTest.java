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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
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
    RuleSet set = RuleSet.of(List.of(rule), DEFAULT);
    RuleResolver resolver = new RuleResolver(set, 4, metrics);
    binding = Objects.requireNonNull(resolver.binding(rule.id()));
    registry = new HotKeyRegistry(4, metrics, new AtomicLong(), key -> 0, wheels);
    dispatcher = mock(DecisionDispatcher.class);
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
    assertThat(registry.promoteEntry("k1", binding, TimeSource.monotonicMillis())).isNotNull();
    assertThat(scheduler.scheduled()).isEqualTo(1);

    verify(dispatcher, timeout(2_000)).submit(
      argThat(
        (HotKeyRegistry.CoolDecision cool) ->
          cool != null && cool.decision().type() == ZetaDecision.DecisionType.COOL && cool.replacedPendingHot()
      )
    );
    assertThat(registry.isHot("k1")).isFalse();
    assertThat(scheduler.scheduled()).isZero();
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
}
