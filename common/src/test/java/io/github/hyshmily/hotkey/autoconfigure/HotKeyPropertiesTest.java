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
package io.github.hyshmily.hotkey.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyProperties}, verifying default values and effective TTL computation.
 */
class HotKeyPropertiesTest {

  private HotKeyProperties props() {
    return new HotKeyProperties();
  }

  /**
   * Verifies that a freshly created HotKeyProperties has the expected default values.
   */
  @Test
  void shouldHaveDefaultValues() {
    HotKeyProperties p = props();
    assertThat(p.getTopK()).isEqualTo(100);
    assertThat(p.getWidth()).isEqualTo(50_000);
    assertThat(p.getDepth()).isEqualTo(5);
    assertThat(p.getDecay()).isEqualTo(0.92);
    assertThat(p.getMinCount()).isEqualTo(10);
  }

  /**
   * Verifies that effectiveHardTtlMs returns the explicitly set value when non-zero.
   */
  @Test
  void effectiveHardTtlMs_shouldReturnOverrideWhenSet() {
    HotKeyProperties p = props();
    assertThat(p.effectiveHardTtlMs()).isEqualTo(300_000L);
    p.setHardTtlMs(600_000L);
    assertThat(p.effectiveHardTtlMs()).isEqualTo(600_000L);
  }

  /**
   * Verifies that effectiveHardTtlMs falls back to the default when the explicit value is zero.
   */
  @Test
  void effectiveHardTtlMs_shouldReturnDefaultWhenOverrideIsZero() {
    HotKeyProperties p = props();
    assertThat(p.effectiveHardTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that effectiveHotHardTtlMs returns the explicitly set non-zero value.
   */
  @Test
  void effectiveHotHardTtlMs_shouldReturnOverrideWhenSet() {
    HotKeyProperties p = props();
    assertThat(p.effectiveHotHardTtlMs()).isEqualTo(3_600_000L);
    p.setHotHardTtlMs(7_200_000L);
    assertThat(p.effectiveHotHardTtlMs()).isEqualTo(7_200_000L);
  }

  /**
   * Verifies that effectiveSoftTtlMs returns the explicitly set non-zero value.
   */
  @Test
  void effectiveSoftTtlMs_shouldReturnOverrideWhenSet() {
    HotKeyProperties p = props();
    assertThat(p.effectiveSoftTtlMs()).isEqualTo(30_000L);
    p.setSoftTtlMs(60_000L);
    assertThat(p.effectiveSoftTtlMs()).isEqualTo(60_000L);
  }

  /**
   * Verifies that effectiveHotSoftTtlMs returns the explicitly set non-zero value.
   */
  @Test
  void effectiveHotSoftTtlMs_shouldReturnOverrideWhenSet() {
    HotKeyProperties p = props();
    assertThat(p.effectiveHotSoftTtlMs()).isEqualTo(300_000L);
    p.setHotSoftTtlMs(600_000L);
    assertThat(p.effectiveHotSoftTtlMs()).isEqualTo(600_000L);
  }

  /**
   * Verifies that soft-expire is enabled when at least one soft TTL is configured with a non-zero value.
   */
  @Test
  void isSoftExpireEnabled_shouldReturnTrueWhenAnySoftTtlConfigured() {
    HotKeyProperties p = props();
    assertThat(p.isSoftExpireEnabled()).isTrue();
  }

  /**
   * Verifies that soft-expire is disabled when all soft TTL values are explicitly set to zero.
   */
  @Test
  void isSoftExpireEnabled_shouldReturnFalseWhenAllSoftTtlsZero() {
    HotKeyProperties p = props();
    p.setDefaultSoftTtlMs(0);
    p.setSoftTtlMs(0);
    p.setDefaultHotSoftTtlMs(0);
    p.setHotSoftTtlMs(0);
    assertThat(p.isSoftExpireEnabled()).isFalse();
  }

  /**
   * Verifies that effectiveConsumerCount returns the configured value when it is positive.
   */
  @Test
  void effectiveConsumerCount_shouldReturnConfiguredWhenPositive() {
    HotKeyProperties p = props();
    p.setConsumerCount(4);
    assertThat(p.effectiveConsumerCount()).isEqualTo(4);
  }

  /**
   * Verifies that effectiveConsumerCount returns the default value of 1 when the configured value is zero.
   */
  @Test
  void effectiveConsumerCount_shouldReturnDefaultWhenZero() {
    HotKeyProperties p = props();
    assertThat(p.effectiveConsumerCount()).isEqualTo(1);
  }

  /**
   * Verifies that effectiveConsumerCount returns max(1, 3/2)=1 when shardCount=3 and consumerCount=0.
   */
  @Test
  void effectiveConsumerCount_shouldReturnMaxOfOneWhenShardCountThree() {
    HotKeyProperties p = props();
    p.setShardCount(3);
    assertThat(p.effectiveConsumerCount()).isEqualTo(1);
  }

  /**
   * Verifies that effectiveConsumerCount returns max(1, 5/2)=2 when shardCount=5 and consumerCount=0.
   */
  @Test
  void effectiveConsumerCount_shouldReturnTwoWhenShardCountFive() {
    HotKeyProperties p = props();
    p.setShardCount(5);
    assertThat(p.effectiveConsumerCount()).isEqualTo(2);
  }

  /**
   * Verifies that effectiveHardTtlMs falls back to default when override is negative (negative > 0 is false).
   */
  @Test
  void effectiveHardTtlMs_shouldUseDefaultWhenOverrideIsNegative() {
    HotKeyProperties p = props();
    p.setHardTtlMs(-100);
    assertThat(p.effectiveHardTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that isSoftExpireEnabled returns true when only normal soft TTL is configured.
   */
  @Test
  void isSoftExpireEnabled_shouldReturnTrueWhenOnlyNormalSoftTtlConfigured() {
    HotKeyProperties p = props();
    p.setDefaultHardTtlMs(0);
    p.setHardTtlMs(0);
    p.setDefaultHotSoftTtlMs(0);
    p.setHotSoftTtlMs(0);
    p.setDefaultSoftTtlMs(30_000);
    assertThat(p.isSoftExpireEnabled()).isTrue();
  }

  /**
   * Verifies the default values for the ConsistentHashing nested config.
   */
  @Test
  void consistentHashing_shouldHaveDefaultValues() {
    HotKeyProperties p = props();
    assertThat(p.getConsistentHashing().isEnabled()).isTrue();
    assertThat(p.getConsistentHashing().getVirtualNodes()).isEqualTo(500);
  }

  /**
   * Verifies the default values for the Heartbeat nested config.
   */
  @Test
  void heartbeat_shouldHaveDefaultValues() {
    HotKeyProperties p = props();
    assertThat(p.getHeartbeat().getExchangeName()).isEqualTo("hotkey.heartbeat.exchange");
    assertThat(p.getHeartbeat().getTimeoutMs()).isEqualTo(3000);
    assertThat(p.getHeartbeat().getVerifyIntervalMs()).isEqualTo(1500);
    assertThat(p.getHeartbeat().getPingTimeoutMs()).isEqualTo(2000);
    assertThat(p.getHeartbeat().getDegradeAfterFailures()).isEqualTo(2);
  }

  /**
   * Verifies the default values for the ReporterLimiter nested config.
   */
  @Test
  void reporterLimiter_shouldHaveDefaultValues() {
    HotKeyProperties p = props();
    assertThat(p.getReporter().getCpuThreshold()).isEqualTo(800);
    assertThat(p.getReporter().getCpuPollIntervalMs()).isEqualTo(500);
    assertThat(p.getReporter().getCpuDecay()).isEqualTo(0.95);
    assertThat(p.getReporter().getBbrWindowMs()).isEqualTo(10_000);
    assertThat(p.getReporter().getBbrWindowBuckets()).isEqualTo(100);
    assertThat(p.getReporter().getBbrCooldownMs()).isEqualTo(1_000);
  }

  /**
   * Verifies the default values for the SpringCache nested config.
   */
  @Test
  void springCache_shouldHaveDefaultValues() {
    HotKeyProperties p = props();
    assertThat(p.getSpringCache().isEnabled()).isFalse();
    assertThat(p.getSpringCache().getKeySeparator()).isEqualTo("::");
  }
}
