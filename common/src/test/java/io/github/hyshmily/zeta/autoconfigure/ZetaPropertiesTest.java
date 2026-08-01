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
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ZetaProperties}, verifying default values and effective TTL computation.
 */
class ZetaPropertiesTest {

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
   * Verifies that effectiveHardTtlMs returns the explicitly set value when non-zero.
   */
  @Test
  void effectiveHardTtlMs_shouldReturnOverrideWhenSet() {
    ZetaProperties p = props();
    assertThat(p.effectiveHardTtlMs()).isEqualTo(300_000L);
    p.setHardTtlMs(600_000L);
    assertThat(p.effectiveHardTtlMs()).isEqualTo(600_000L);
  }

  /**
   * Verifies that effectiveHardTtlMs falls back to the default when the explicit value is zero.
   */
  @Test
  void effectiveHardTtlMs_shouldReturnDefaultWhenOverrideIsZero() {
    ZetaProperties p = props();
    assertThat(p.effectiveHardTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that effectiveHotHardTtlMs returns the explicitly set non-zero value.
   */
  @Test
  void effectiveHotHardTtlMs_shouldReturnOverrideWhenSet() {
    ZetaProperties p = props();
    assertThat(p.effectiveHotHardTtlMs()).isEqualTo(3_600_000L);
    p.setHotHardTtlMs(7_200_000L);
    assertThat(p.effectiveHotHardTtlMs()).isEqualTo(7_200_000L);
  }

  /**
   * Verifies that effectiveSoftTtlMs returns the explicitly set non-zero value.
   */
  @Test
  void effectiveSoftTtlMs_shouldReturnOverrideWhenSet() {
    ZetaProperties p = props();
    assertThat(p.effectiveSoftTtlMs()).isEqualTo(30_000L);
    p.setSoftTtlMs(60_000L);
    assertThat(p.effectiveSoftTtlMs()).isEqualTo(60_000L);
  }

  /**
   * Verifies that effectiveHotSoftTtlMs returns the explicitly set non-zero value.
   */
  @Test
  void effectiveHotSoftTtlMs_shouldReturnOverrideWhenSet() {
    ZetaProperties p = props();
    assertThat(p.effectiveHotSoftTtlMs()).isEqualTo(300_000L);
    p.setHotSoftTtlMs(600_000L);
    assertThat(p.effectiveHotSoftTtlMs()).isEqualTo(600_000L);
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
    assertThat(p.getHeartbeat().getVerifyIntervalMs()).isEqualTo(5000);
    assertThat(p.getHeartbeat().getPingTimeoutMs()).isEqualTo(3000);
    assertThat(p.getHeartbeat().getDegradeAfterFailures()).isEqualTo(3);
    assertThat(p.getHeartbeat().getVerifyMaxBackoffMs()).isEqualTo(600_000);
    assertThat(p.getHeartbeat().getMinAliveWorkers()).isEqualTo(0);
  }

  /**
   * Verifies the default values for the ReporterLimiter nested config.
   */
  @Test
  void reporterLimiter_shouldHaveDefaultValues() {
    ZetaProperties p = props();
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
    ZetaProperties p = props();
    assertThat(p.getSpringCache().getKeySeparator()).isEqualTo("::");
  }
}
