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
package io.github.hyshmily.zeta.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.constants.ZetaConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WorkerProperties}.
 */
class WorkerPropertiesTest {

  private WorkerProperties properties;

  @BeforeEach
  void setUp() {
    properties = new WorkerProperties();
  }

  /**
   * Verifies that the default value of {@code enabled} is {@code false}.
   */
  @Test
  void shouldHaveDefaultEnabledFalse() {
    assertThat(properties.isEnabled()).isFalse();
  }

  /**
   * Verifies the default value of the {@code routing.app-name} sub-property.
   */
  @Test
  void shouldHaveDefaultAppName() {
    assertThat(properties.getRouting().getAppName()).isEqualTo("default");
  }

  /**
   * Verifies the default values of the {@code messaging} sub-properties.
   */
  @Test
  void shouldHaveDefaultMessagingValues() {
    assertThat(properties.getMessaging().getReportExchange()).isEqualTo(ZetaConstants.EXCHANGE_REPORT);
    assertThat(properties.getMessaging().getBroadcastExchange()).isEqualTo(ZetaConstants.EXCHANGE_BROADCAST);
  }

  /**
   * Verifies the default values of the {@code sliding-window} sub-properties.
   */
  @Test
  void shouldHaveDefaultSlidingWindowValues() {
    assertThat(properties.getSlidingWindow().getDurationMs()).isEqualTo(1000);
    assertThat(properties.getSlidingWindow().getSlices()).isEqualTo(10);
  }

  /**
   * Verifies the default values of the {@code threshold} sub-properties.
   */
  @Test
  void shouldHaveDefaultThresholdValues() {
    assertThat(properties.getThreshold().getHotThreshold()).isEqualTo(1000);
    assertThat(properties.getThreshold().getHotThresholdRatio()).isEqualTo(0.01);
  }

  /**
   * Verifies the default values of the {@code state-machine} sub-properties.
   */
  @Test
  void shouldHaveDefaultStateMachineValues() {
    assertThat(properties.getStateMachine().getSmDurationMs()).isEqualTo(500);
    assertThat(properties.getStateMachine().getSmSlices()).isEqualTo(10);
    assertThat(properties.getStateMachine().getConfirmDurationMs()).isEqualTo(100);
    assertThat(properties.getStateMachine().getCoolDurationMs()).isEqualTo(600000);
    assertThat(properties.getStateMachine().getPreCoolGraceMs()).isEqualTo(60000);
  }

  /**
   * Verifies the default values of the {@code heavy-keeper} sub-properties.
   */
  @Test
  void shouldHaveDefaultHeavyKeeperValues() {
    assertThat(properties.getHeavyKeeper().getTopK()).isEqualTo(100);
    assertThat(properties.getHeavyKeeper().getWidth()).isEqualTo(20000);
    assertThat(properties.getHeavyKeeper().getDepth()).isEqualTo(10);
    assertThat(properties.getHeavyKeeper().getDecay()).isEqualTo(0.9);
    assertThat(properties.getHeavyKeeper().getMinCount()).isEqualTo(10);
  }

  /**
   * Verifies that {@code confirmWindows} is correctly derived from {@code confirmDurationMs} and {@code sliceMs}.
   */
  @Test
  void shouldComputeConfirmWindows() {
    // confirmDurationMs=100, sliceMs=500/10=50 => 100/50 = 2
    assertThat(properties.getConfirmWindows()).isEqualTo(2);
  }

  /**
   * Verifies that {@code coolWindows} is correctly derived from {@code coolDurationMs} and {@code sliceMs}.
   */
  @Test
  void shouldComputeCoolWindows() {
    // coolDurationMs=600000, sliceMs=50 => 600000/50 = 12000
    assertThat(properties.getCoolWindows()).isEqualTo(12000);
  }

  /**
   * Verifies that {@code preCoolGraceWindows} is correctly derived from {@code preCoolGraceMs} and {@code sliceMs}.
   */
  @Test
  void shouldComputePreCoolGraceWindows() {
    // preCoolGraceMs=60000, sliceMs=50 => 60000/50 = 1200
    assertThat(properties.getPreCoolGraceWindows()).isEqualTo(1200);
  }

  /**
   * Verifies the default values of the {@code global-qps-dynamic-threshold} sub-properties.
   */
  @Test
  void shouldHaveDefaultGlobalQpsDynamicThreshold() {
    assertThat(properties.getGlobalQpsDynamicThreshold().getQpsChangeTolerance()).isEqualTo(0.5);
    assertThat(properties.getGlobalQpsDynamicThreshold().getLearningPeriodMs()).isEqualTo(30_000);
    assertThat(properties.getGlobalQpsDynamicThreshold().getHotThresholdRatio()).isEqualTo(0.01);
    assertThat(properties.getGlobalQpsDynamicThreshold().getRecalculateIntervalMs()).isEqualTo(60_000);
  }

  /**
   * Verifies the default values of the {@code topk-validation} sub-properties.
   */
  @Test
  void shouldHaveDefaultTopKValidation() {
    assertThat(properties.getTopKValidation().getValidateIntervalMs()).isEqualTo(60000);
    assertThat(properties.getTopKValidation().getPreWarmCount()).isEqualTo(5);
    assertThat(properties.getTopKValidation().getPreWarmMinAppearances()).isEqualTo(2);
  }

  /**
   * Verifies that {@code confirmWindows} with a non-exact division rounds up via ceil.
   * Edge case: confirmDurationMs=350, sliceMs=100 → 350/100 = 3.5 → ceil = 4.
   */
  @Test
  void shouldComputeConfirmWindowsWithRounding() {
    properties.getStateMachine().setConfirmDurationMs(350);
    // sliceMs = 500/10 = 50
    assertThat(properties.getConfirmWindows()).isEqualTo(7);
  }

  /**
   * Verifies that {@code coolWindows} with a non-exact division rounds up via ceil.
   */
  @Test
  void shouldComputeCoolWindowsWithRounding() {
    properties.getStateMachine().setCoolDurationMs(15500);
    // sliceMs = 500/10 = 50
    assertThat(properties.getCoolWindows()).isEqualTo(310);
  }

  /**
   * Verifies that {@code preCoolGraceWindows} with zero preCoolGraceMs produces zero.
   * Edge case: zero duration.
   */
  @Test
  void shouldComputeZeroGraceWindows() {
    properties.getStateMachine().setPreCoolGraceMs(0);
    assertThat(properties.getPreCoolGraceWindows()).isZero();
  }

  /**
   * Verifies the default values of the {@code heartbeat} sub-properties.
   */
  @Test
  void shouldHaveDefaultHeartbeatValues() {
    assertThat(properties.getHeartbeat().getPingIntervalMs()).isEqualTo(1000);
  }

  /**
   * Verifies the default values of the {@code persistence} sub-properties.
   */
  @Test
  void shouldHaveDefaultPersistenceValues() {
    assertThat(properties.getPersistence().isEnabled()).isFalse();
    assertThat(properties.getPersistence().getPersistIntervalMs()).isEqualTo(30_000);
    assertThat(properties.getPersistence().getTopKCount()).isEqualTo(100);
    assertThat(properties.getPersistence().getRedisKeyPrefix()).isEqualTo("zeta:topk:worker:");
    assertThat(properties.getPersistence().getTtlDays()).isEqualTo(3);
  }

  /**
   * Verifies that setting custom values on all nested config objects works correctly.
   */
  @Test
  void shouldAcceptCustomValues() {
    properties.getSlidingWindow().setDurationMs(2000);
    properties.getSlidingWindow().setSlices(20);
    properties.getThreshold().setHotThreshold(5000);
    properties.getThreshold().setHotThresholdRatio(0.05);
    properties.getStateMachine().setEvictIntervalMs(60000);

    assertThat(properties.getSlidingWindow().getDurationMs()).isEqualTo(2000);
    assertThat(properties.getSlidingWindow().getSlices()).isEqualTo(20);
    assertThat(properties.getThreshold().getHotThreshold()).isEqualTo(5000);
    assertThat(properties.getThreshold().getHotThresholdRatio()).isEqualTo(0.05);
    assertThat(properties.getStateMachine().getEvictIntervalMs()).isEqualTo(60000);
  }
}
