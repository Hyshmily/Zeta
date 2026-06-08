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
package io.github.hyshmily.hotkey.worker.config;

import static org.assertj.core.api.Assertions.assertThat;

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

  @Test
  void shouldHaveDefaultEnabledFalse() {
    assertThat(properties.isEnabled()).isFalse();
  }

  @Test
  void shouldHaveDefaultRoutingValues() {
    assertThat(properties.getRouting().getAppName()).isEqualTo("default");
    assertThat(properties.getRouting().getShardCount()).isEqualTo(1);
    assertThat(properties.getRouting().getShardIndex()).isZero();
  }

  @Test
  void shouldHaveDefaultMessagingValues() {
    assertThat(properties.getMessaging().getReportExchange()).isEqualTo("hotkey.report.exchange");
    assertThat(properties.getMessaging().getBroadcastExchange()).isEqualTo("hotkey.broadcast.exchange");
  }

  @Test
  void shouldHaveDefaultSlidingWindowValues() {
    assertThat(properties.getSlidingWindow().getDurationMs()).isEqualTo(1000);
    assertThat(properties.getSlidingWindow().getSlices()).isEqualTo(10);
  }

  @Test
  void shouldHaveDefaultThresholdValues() {
    assertThat(properties.getThreshold().getHotThreshold()).isEqualTo(1000);
    assertThat(properties.getThreshold().getHotThresholdRatio()).isEqualTo(0.01);
  }

  @Test
  void shouldHaveDefaultStateMachineValues() {
    assertThat(properties.getStateMachine().getConfirmDurationMs()).isEqualTo(2000);
    assertThat(properties.getStateMachine().getCoolDurationMs()).isEqualTo(15000);
    assertThat(properties.getStateMachine().getPreCoolGraceMs()).isEqualTo(5000);
  }

  @Test
  void shouldHaveDefaultHeavyKeeperValues() {
    assertThat(properties.getHeavyKeeper().getTopK()).isEqualTo(100);
    assertThat(properties.getHeavyKeeper().getWidth()).isEqualTo(20000);
    assertThat(properties.getHeavyKeeper().getDepth()).isEqualTo(10);
    assertThat(properties.getHeavyKeeper().getDecay()).isEqualTo(0.9);
    assertThat(properties.getHeavyKeeper().getMinCount()).isEqualTo(10);
  }

  @Test
  void shouldComputeConfirmWindows() {
    // confirmDurationMs=2000, sliceMs=1000/10=100 => 2000/100 = 20
    assertThat(properties.getConfirmWindows()).isEqualTo(20);
  }

  @Test
  void shouldComputeCoolWindows() {
    // coolDurationMs=15000, sliceMs=100 => 15000/100 = 150
    assertThat(properties.getCoolWindows()).isEqualTo(150);
  }

  @Test
  void shouldComputePreCoolGraceWindows() {
    // preCoolGraceMs=5000, sliceMs=100 => 5000/100 = 50
    assertThat(properties.getPreCoolGraceWindows()).isEqualTo(50);
  }

  @Test
  void shouldHaveDefaultGlobalQpsDynamicThreshold() {
    assertThat(properties.getGlobalQpsDynamicThreshold().getQpsChangeTolerance()).isEqualTo(0.5);
    assertThat(properties.getGlobalQpsDynamicThreshold().getLearningPeriodMs()).isEqualTo(30_000);
    assertThat(properties.getGlobalQpsDynamicThreshold().getHotThresholdRatio()).isEqualTo(0.01);
    assertThat(properties.getGlobalQpsDynamicThreshold().getRecalculateIntervalMs()).isEqualTo(60_000);
  }

  @Test
  void shouldHaveDefaultTopKValidation() {
    assertThat(properties.getTopKValidation().getValidateIntervalMs()).isEqualTo(60000);
    assertThat(properties.getTopKValidation().getPreWarmCount()).isEqualTo(5);
    assertThat(properties.getTopKValidation().getPreWarmMinAppearances()).isEqualTo(2);
  }
}
