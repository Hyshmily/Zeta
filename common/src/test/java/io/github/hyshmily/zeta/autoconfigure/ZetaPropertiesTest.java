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
package io.github.hyshmily.zeta.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ZetaProperties}, verifying default values, validation and TTL ownership.
 */
class ZetaPropertiesTest {

  private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

  private ZetaProperties props() {
    return new ZetaProperties();
  }

  /**
   * Verifies that a freshly created ZetaProperties has the expected default values.
   */
  @Test
  void shouldHaveDefaultValues() {
    ZetaProperties p = props();
    assertThat(p.getTopK()).isEqualTo(100);
    assertThat(p.getWidth()).isEqualTo(50_000);
    assertThat(p.getDepth()).isEqualTo(5);
    assertThat(p.getDecay()).isEqualTo(0.92);
    assertThat(p.getMinCount()).isEqualTo(10);
  }

  /**
   * The default configuration passes Bean Validation. In particular, the floating-point knobs
   * must use decimal constraints: {@code @Min}/{@code @Max} do not support {@code double}, and
   * Hibernate Validator would throw {@code UnexpectedTypeException} during binding when the host
   * application has {@code spring-boot-starter-validation} on the classpath.
   */
  @Test
  void defaultsPassBeanValidation() {
    assertThat(violationMessages(props())).isEmpty();
  }

  /** The collision-decay base is a probability: values above 1 are rejected at binding. */
  @Test
  void decayAboveOneIsRejected() {
    ZetaProperties p = props();
    p.setDecay(1.5);

    assertThat(violationPropertyPaths(p)).contains("decay");
  }

  /** Every positive integer knob rejects 0 instead of failing later in a constructor. */
  @Test
  void positiveIntegerKnobsRejectZero() {
    ZetaProperties p = props();
    p.setReportIntervalMs(0);
    p.setDetectIntervalMs(0);
    p.getCache().setMaxSize(0);
    p.getCache().setTombstoneMaximumSize(0);
    p.getCache().setTombstoneTtlSeconds(0);

    assertThat(violationPropertyPaths(p)).contains(
      "reportIntervalMs",
      "detectIntervalMs",
      "cache.maxSize",
      "cache.tombstoneMaximumSize",
      "cache.tombstoneTtlSeconds"
    );
  }

  /**
   * The breaker divides its window into buckets; {@code windowTimeMs < windowBuckets} would
   * otherwise survive binding and fail at the first {@code SlidingWindowBreaker} construction.
   */
  @Test
  void circuitBreakerWindowGeometryIsValidated() {
    ZetaProperties p = props();
    p.getCircuitBreaker().setWindowTimeMs(5);
    p.getCircuitBreaker().setWindowBuckets(10);

    assertThat(violationMessages(p)).contains(
      "zeta.local.circuit-breaker.window-time-ms must be >= zeta.local.circuit-breaker.window-buckets"
    );
  }

  /**
   * The TTL fields carry both the default and the configured value: Spring's
   * binder overlays a property onto the initialized field, so a second
   * "override" field would only add a branch and make an explicit 0
   * indistinguishable from "unset".
   */
  @Test
  void ttlFieldsCarryDefaultsDirectly() {
    ZetaProperties p = props();
    assertThat(p.getHardTtlMs()).isEqualTo(300_000L);
    assertThat(p.getSoftTtlMs()).isZero();
    assertThat(p.getWarm().getPrewarm().getTrigger()).isEqualTo(HeatTrigger.OFF);
    assertThat(p.getWarm().getKeepWarm().getTrigger()).isEqualTo(HeatTrigger.OFF);
    assertThat(p.getKeyFormat()).isEqualTo(io.github.hyshmily.zeta.policy.KeyFormat.TYPED);
  }

  /**
   * An explicit soft TTL of 0 disables soft expiry; it must not fall back to a
   * default in a later resolution layer.
   */
  @Test
  void softTtlCanBeDisabledWithZero() {
    ZetaProperties p = props();
    p.setSoftTtlMs(0L);
    assertThat(p.getSoftTtlMs()).isZero();
  }

  /**
   * Verifies the default values for the ConsistentHashing nested config.
   */
  @Test
  void consistentHashing_shouldHaveDefaultValues() {
    ZetaProperties p = props();
    assertThat(p.getConsistentHashing().isEnabled()).isTrue();
    assertThat(p.getConsistentHashing().getVirtualNodes()).isEqualTo(500);
  }

  /**
   * Verifies the default values for the Heartbeat nested config.
   */
  @Test
  void heartbeat_shouldHaveDefaultValues() {
    ZetaProperties p = props();
    assertThat(p.getHeartbeat().getExchangeName()).isEqualTo(ZetaConstants.Exchange.HEARTBEAT);
    assertThat(p.getHeartbeat().getTimeoutMs()).isEqualTo(10000);
    assertThat(p.getHeartbeat().getMinAliveWorkers()).isEqualTo(0);
  }

  /**
   * The refresh limiter cannot exceed the shared load pool: a higher configured
   * value is clamped, and the default stays half the pool.
   */
  @Test
  void effectiveRefreshMaxPoolsClampsToTheEffectiveLoadPool() {
    ZetaProperties p = props();
    p.setExecutorMaxPoolSize(16);
    p.setRefreshMaxPools(64);

    assertThat(p.effectiveRefreshMaxPools()).isEqualTo(16);

    p.setRefreshMaxPools(0);
    assertThat(p.effectiveRefreshMaxPools()).isEqualTo(8);

    p.setLoadExecutorMaxPoolSize(10);
    p.setRefreshMaxPools(0);
    assertThat(p.effectiveRefreshMaxPools()).isEqualTo(5);
  }

  @Test
  void dedicatedExecutorSizesInheritTheSharedDefaults() {
    ZetaProperties p = props();
    p.setExecutorCorePoolSize(4);
    p.setExecutorMaxPoolSize(12);
    p.setExecutorQueueCapacity(100);

    assertThat(p.effectiveLoadCorePoolSize()).isEqualTo(4);
    assertThat(p.effectiveLoadMaxPoolSize()).isEqualTo(12);
    assertThat(p.effectiveLoadQueueCapacity()).isEqualTo(100);
    assertThat(p.effectiveSendCorePoolSize()).isEqualTo(4);
    assertThat(p.effectiveSendMaxPoolSize()).isEqualTo(12);
    assertThat(p.effectiveSendQueueCapacity()).isEqualTo(100);

    p.setLoadExecutorCorePoolSize(2);
    p.setLoadExecutorMaxPoolSize(6);
    p.setLoadExecutorQueueCapacity(50);
    p.setSendExecutorCorePoolSize(1);
    p.setSendExecutorMaxPoolSize(3);
    p.setSendExecutorQueueCapacity(20);

    assertThat(p.effectiveLoadCorePoolSize()).isEqualTo(2);
    assertThat(p.effectiveLoadMaxPoolSize()).isEqualTo(6);
    assertThat(p.effectiveLoadQueueCapacity()).isEqualTo(50);
    assertThat(p.effectiveSendCorePoolSize()).isEqualTo(1);
    assertThat(p.effectiveSendMaxPoolSize()).isEqualTo(3);
    assertThat(p.effectiveSendQueueCapacity()).isEqualTo(20);
  }

  /**
   * The executor's core size above its maximum makes the pool constructor
   * reject the configuration; the properties reject it before any bean is
   * created.
   */
  @Test
  void executorPoolSizeValidationRejectsCoreAboveMax() {
    ZetaProperties p = props();
    p.setExecutorCorePoolSize(64);
    p.setExecutorMaxPoolSize(32);

    assertThat(violationMessages(p)).contains(
      "zeta.local.executor-max-pool-size must be >= zeta.local.executor-core-pool-size"
    );
  }

  @Test
  void dedicatedPoolSizeValidationRejectsAnEffectiveCoreAboveMax() {
    ZetaProperties p = props();
    p.setExecutorCorePoolSize(16);
    p.setExecutorMaxPoolSize(32);
    p.setLoadExecutorMaxPoolSize(8);

    assertThat(violationMessages(p)).contains(
      "the effective load and send executor max pool sizes must be >= their effective core pool sizes"
    );
  }

  /**
   * The feed-loop range is checked only while the tuner is enabled: at
   * {@code off} the two values are inert.
   */
  @Test
  void reportIntervalValidationAppliesOnlyWhenTuningIsEnabled() {
    ZetaProperties p = props();
    p.setReportIntervalMinMs(200);
    p.setReportIntervalMaxMs(100);

    assertThat(violationMessages(p)).contains(
      "zeta.local.report-interval-max-ms must be >= zeta.local.report-interval-min-ms"
    );

    p.setReportIntervalTuning(ZetaProperties.ReportIntervalTuning.OFF);
    assertThat(violationMessages(p)).doesNotContain(
      "zeta.local.report-interval-max-ms must be >= zeta.local.report-interval-min-ms"
    );
  }

  /**
   * The adaptive floor is the lower clamp and timeout-ms the ceiling; an
   * inverted pair would make the timeout stop being a bound.
   */
  @Test
  void heartbeatAdaptiveWindowValidationRejectsFloorAboveTimeout() {
    ZetaProperties p = props();
    p.getHeartbeat().setAdaptiveFloorMs(20_000);

    assertThat(violationMessages(p)).contains(
      "zeta.local.heartbeat.adaptive-floor-ms must be <= zeta.local.heartbeat.timeout-ms"
    );
  }

  private static Set<String> violationMessages(ZetaProperties properties) {
    return VALIDATOR.validate(properties).stream().map(ConstraintViolation::getMessage).collect(Collectors.toSet());
  }

  private static Set<String> violationPropertyPaths(ZetaProperties properties) {
    return VALIDATOR.validate(properties)
      .stream()
      .map(violation -> violation.getPropertyPath().toString())
      .collect(Collectors.toSet());
  }
}
