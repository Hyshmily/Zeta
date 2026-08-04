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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.TtlPolicy;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TtlPolicy} — the pure TTL/expiry policy module behind
 * {@link io.github.hyshmily.zeta.cache.cachesupport.ExpireManager}.
 *
 * <p>Covers every stateless lifecycle computation: resolve (override vs
 * default), compute (absolute expire timestamps), getEffective (configured
 * defaults), timestamp conversion with jitter, expiry predicates, and the
 * entry-level TTL transforms. No Caffeine cache required — the policy is
 * tested directly through its own interface.
 */
class TtlPolicyTest {

  private ZetaProperties ttlConfig;
  private TtlPolicy ttlPolicy;

  @BeforeEach
  void setUp() {
    ttlConfig = new ZetaProperties();
    ttlPolicy = new TtlPolicy(ttlConfig, ttlConfig.getTtlJitterRatio());
  }

  /**
   * Verifies that computeHardExpireAt with a positive TTL returns a timestamp in the future.
   */
  @Test
  void computeHardExpireAt_withPositiveTtl_shouldReturnFutureTimestamp() {
    long expireAt = ttlPolicy.computeHardExpireAt(1000);
    assertThat(expireAt).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHardExpireAt falls back to the default TTL when the given value is zero.
   */
  @Test
  void computeHardExpireAt_withZeroTtl_shouldFallbackToDefault() {
    long expireAt = ttlPolicy.computeHardExpireAt(0);
    assertThat(expireAt).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that getEffectiveHardTtlMs returns the configured default hard TTL value.
   */
  @Test
  void getEffectiveHardTtlMs_shouldReturnConfigValue() {
    assertThat(ttlPolicy.getEffectiveHardTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that getEffectiveHotHardTtlMs returns the configured hot-key hard TTL value.
   */
  @Test
  void getEffectiveHotHardTtlMs_shouldReturnConfigValue() {
    assertThat(ttlPolicy.getEffectiveHotHardTtlMs()).isEqualTo(3_600_000L);
  }

  /**
   * Verifies that getEffectiveSoftTtlMs returns the configured default soft TTL value.
   */
  @Test
  void getEffectiveSoftTtlMs_shouldReturnConfigValue() {
    assertThat(ttlPolicy.getEffectiveSoftTtlMs()).isEqualTo(30_000L);
  }

  /**
   * Verifies that getEffectiveHotSoftTtlMs returns the configured hot-key soft TTL value.
   */
  @Test
  void getEffectiveHotSoftTtlMs_shouldReturnConfigValue() {
    assertThat(ttlPolicy.getEffectiveHotSoftTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that a non-CacheEntry plain value is considered soft-expired.
   */
  @Test
  void isSoftExpired_shouldReturnTrueForNonCacheEntry() {
    assertThat(ttlPolicy.isSoftExpired("not-a-cache-entry")).isTrue();
  }

  /**
   * Verifies that a missing cache key is considered soft-expired.
   */
  @Test
  void isSoftExpired_shouldReturnTrueForMissingEntry() {
    assertThat(ttlPolicy.isSoftExpired(null)).isTrue();
  }

  /**
   * Verifies that computeHardExpireAt with zero falls back to default and returns a future timestamp.
   */
  @Test
  void computeHardExpireAt_withZero_shouldFallbackAndReturnFuture() {
    long result = ttlPolicy.computeHardExpireAt(0);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHardExpireAt with a negative TTL falls back to default and returns a future timestamp.
   */
  @Test
  void computeHardExpireAt_withNegative_shouldFallbackAndReturnFuture() {
    long result = ttlPolicy.computeHardExpireAt(-1);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHardExpireAt with Long.MAX_VALUE passes it through unchanged.
   */
  @Test
  void computeHardExpireAt_withMaxValue_shouldPassthrough() {
    assertThat(ttlPolicy.computeHardExpireAt(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that computeHardExpireAt with a positive TTL returns a future timestamp.
   */
  @Test
  void computeHardExpireAt_withPositive_shouldReturnFuture() {
    long result = ttlPolicy.computeHardExpireAt(1000);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeSoftExpireAt returns zero when soft-expire is disabled.
   */
  @Test
  void computeSoftExpireAt_withDisabledConfig_shouldReturnMaxValue() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    TtlPolicy disabled = new TtlPolicy(ttlConfig, ttlConfig.getTtlJitterRatio());
    assertThat(disabled.computeSoftExpireAt(0)).isZero();
  }

  /**
   * Verifies that computeHardExpireAt produces a positive timestamp within a reasonable range.
   */
  @Test
  void hardExpireAt_producesPositiveTimestamp() {
    long hard = ttlPolicy.computeHardExpireAt(5_000);
    assertThat(hard).isGreaterThan(System.currentTimeMillis());
    assertThat(hard).isLessThan(System.currentTimeMillis() + 60_000);
  }

  /**
   * Verifies that computeSoftExpireAt produces a positive timestamp within a reasonable range.
   */
  @Test
  void softExpireAt_producesPositiveTimestamp() {
    long soft = ttlPolicy.computeSoftExpireAt(5_000);
    assertThat(soft).isGreaterThan(System.currentTimeMillis());
    assertThat(soft).isLessThan(System.currentTimeMillis() + 60_000);
  }

  /**
   * Verifies that computeHotHardExpireAt produces a positive future timestamp.
   */
  @Test
  void hotHardExpireAt_producesPositiveTimestamp() {
    long hot = ttlPolicy.computeHotHardExpireAt();
    assertThat(hot).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHotHardExpireAt returns Long.MAX_VALUE when hot hard TTL is disabled (0).
   * This covers toHardExpireTimestamp when hardTtlMs <= 0.
   */
  @Test
  void computeHotHardExpireAt_withDisabledHotHardTtl_shouldReturnMaxValue() {
    ttlConfig.setDefaultHotHardTtlMs(0);
    assertThat(ttlPolicy.computeHotHardExpireAt()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that computeHotSoftExpireAt produces a positive future timestamp.
   */
  @Test
  void hotSoftExpireAt_producesPositiveTimestamp() {
    long hot = ttlPolicy.computeHotSoftExpireAt();
    assertThat(hot).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHotSoftExpireAt returns 0 when hot soft TTL is disabled (0).
   * This covers the toSoftExpireTimestamp branch when softTtlMs <= 0.
   */
  @Test
  void computeHotSoftExpireAt_withZeroHotSoftTtl_shouldReturnZero() {
    ttlConfig.setDefaultHotSoftTtlMs(0);
    assertThat(ttlPolicy.computeHotSoftExpireAt()).isZero();
  }

  /**
   * Verifies that isSoftExpired returns true when softExpireAtMs is a past timestamp
   * (positive but earlier than the current time).
   */
  @Test
  void isSoftExpired_withExpiredEntry_shouldReturnTrue() {
    CacheEntry entry = CacheEntry.builder()
      .value("v")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(1L)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
    assertThat(ttlPolicy.isSoftExpired(entry)).isTrue();
  }

  /**
   * Verifies that isSoftExpired with softExpireAtMs=Long.MAX_VALUE returns false
   * (MAX_VALUE means never soft-expire).
   */
  @Test
  void isSoftExpired_withMaxValue_shouldReturnFalse() {
    CacheEntry entry = CacheEntry.builder()
      .value("v")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(Long.MAX_VALUE)
      .softExpireAtMs(Long.MAX_VALUE)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
    assertThat(ttlPolicy.isSoftExpired(entry)).isFalse();
  }

  /**
   * Verifies that isSoftExpired with softExpireAtMs=0 returns true
   * (zero means immediately expired).
   */
  @Test
  void isSoftExpired_withZeroExpireAt_shouldReturnTrue() {
    CacheEntry entry = CacheEntry.builder()
      .value("v")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(0)
      .softExpireAtMs(0)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
    assertThat(ttlPolicy.isSoftExpired(entry)).isTrue();
  }

  /**
   * Verifies that computeSoftExpireAt passes Long.MAX_VALUE through unchanged.
   */
  @Test
  void computeSoftExpireAt_withMaxValue_shouldPassthrough() {
    assertThat(ttlPolicy.computeSoftExpireAt(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that custom ratio (0.5 = ±50%) produces jitter within the configured range.
   */
  @Test
  void toHardExpireTimestamp_withCustomRatio_shouldJitterWithinRange() {
    ttlConfig.setTtlJitterRatio(0.5);
    TtlPolicy highJitter = new TtlPolicy(ttlConfig, ttlConfig.getTtlJitterRatio());
    long ttl = 10_000;
    Stream.generate(() -> highJitter.computeHardExpireAt(ttl))
      .limit(100)
      .forEach(expireAt -> {
        long diff = expireAt - System.currentTimeMillis();
        assertThat(diff).isBetween(4_900L, 15_100L);
      });
  }

  /**
   * Verifies that the default jitter is applied and within range for both hard and soft TTL paths.
   */
  @Test
  void toHardExpireTimestamp_withDefaultJitter_shouldJitterWithinRange() {
    long ttl = 10_000;
    Stream.generate(() -> ttlPolicy.computeHardExpireAt(ttl))
      .limit(100)
      .forEach(expireAt -> {
        long diff = expireAt - System.currentTimeMillis();
        assertThat(diff).isBetween(9_000L, 11_000L);
      });
    Stream.generate(() -> ttlPolicy.computeSoftExpireAt(ttl))
      .limit(100)
      .forEach(expireAt -> {
        long diff = expireAt - System.currentTimeMillis();
        assertThat(diff).isBetween(9_000L, 11_000L);
      });
  }

  /**
   * Verifies that toHardExpireTimestamp with a custom jitter ratio uses the given ratio.
   */
  @Test
  void toHardExpireTimestamp_withCustomRatio_shouldUseGivenRatio() {
    long ttl = 10_000;
    Stream.generate(() -> ttlPolicy.toHardExpireTimestamp(ttl, 0.5))
      .limit(50)
      .forEach(expireAt -> {
        long diff = expireAt - TimeSource.currentTimeMillis();
        assertThat(diff).isBetween(5_000L, 15_000L);
      });
  }

  /**
   * Verifies that toHardExpireTimestamp with Long.MAX_VALUE and a custom ratio passes through.
   */
  @Test
  void toHardExpireTimestamp_withMaxValueAndCustomRatio_shouldPassthrough() {
    assertThat(ttlPolicy.toHardExpireTimestamp(Long.MAX_VALUE, 0.5)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that toSoftExpireTimestamp with a custom jitter ratio uses the given ratio.
   */
  @Test
  void toSoftExpireTimestamp_withCustomRatio_shouldUseGivenRatio() {
    long ttl = 10_000;
    Stream.generate(() -> ttlPolicy.toSoftExpireTimestamp(ttl, 0.5))
      .limit(50)
      .forEach(expireAt -> {
        long diff = expireAt - TimeSource.currentTimeMillis();
        assertThat(diff).isBetween(5_000L, 15_000L);
      });
  }

  /**
   * Verifies that toSoftExpireTimestamp with non-positive soft TTL returns zero.
   */
  @Test
  void toSoftExpireTimestamp_withNonPositiveTtlAndCustomRatio_shouldReturnZero() {
    assertThat(ttlPolicy.toSoftExpireTimestamp(0, 0.5)).isZero();
    assertThat(ttlPolicy.toSoftExpireTimestamp(-1, 0.5)).isZero();
  }

  /**
   * Verifies that toSoftExpireTimestamp with Long.MAX_VALUE passes through.
   */
  @Test
  void toSoftExpireTimestamp_withMaxValueAndCustomRatio_shouldPassthrough() {
    assertThat(ttlPolicy.toSoftExpireTimestamp(Long.MAX_VALUE, 0.5)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that computeNullExpireAt with positive TTL returns a future timestamp.
   */
  @Test
  void computeNullExpireAt_withPositiveTtl_shouldReturnFuture() {
    assertThat(ttlPolicy.computeNullExpireAt(1_000)).isGreaterThan(TimeSource.currentTimeMillis());
  }

  /**
   * Verifies that computeNullExpireAt with zero TTL falls back to config default.
   */
  @Test
  void computeNullExpireAt_withZeroTtl_shouldFallbackToConfig() {
    assertThat(ttlPolicy.computeNullExpireAt(0)).isGreaterThan(TimeSource.currentTimeMillis());
  }

  /**
   * Verifies that computeNullExpireAt with Long.MAX_VALUE passes through.
   */
  @Test
  void computeNullExpireAt_withMaxValue_shouldPassthrough() {
    assertThat(ttlPolicy.computeNullExpireAt(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  // ── applyHardTtl / applySoftTtl / applyNormalTtl ──────────────

  /**
   * Verifies that applyHardTtl updates the hard TTL and expire-at while
   * preserving the soft TTL and all version/state fields.
   */
  @Test
  void applyHardTtl_shouldUpdateHardTtlAndPreserveSoftTtl() {
    long now = System.currentTimeMillis();
    CacheEntry original = CacheEntry.builder()
      .value("v")
      .dataVersion(10)
      .isVersionDegraded(true)
      .decisionVersion(5)
      .hardTtlMs(60_000)
      .hardExpireAtMs(now + 60_000)
      .softTtlMs(30_000)
      .softExpireAtMs(now + 30_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();

    CacheEntry updated = ttlPolicy.applyHardTtl(original, 120_000);

    assertThat(updated.getHardTtlMs()).isEqualTo(120_000);
    assertThat(updated.getHardExpireAtMs()).isGreaterThan(original.getHardExpireAtMs());
    assertThat(updated.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(updated.getSoftExpireAtMs()).isEqualTo(original.getSoftExpireAtMs());
    assertThat(updated.getDataVersion()).isEqualTo(10);
    assertThat(updated.isVersionDegraded()).isTrue();
    assertThat(updated.getDecisionVersion()).isEqualTo(5);
    assertThat(updated.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies that applySoftTtl updates the soft TTL and expire-at while
   * preserving the hard TTL and all version/state fields.
   */
  @Test
  void applySoftTtl_shouldUpdateSoftTtlAndPreserveHardTtl() {
    long now = System.currentTimeMillis();
    CacheEntry original = CacheEntry.builder()
      .value("v")
      .dataVersion(10)
      .isVersionDegraded(true)
      .decisionVersion(5)
      .hardTtlMs(60_000)
      .hardExpireAtMs(now + 60_000)
      .softTtlMs(30_000)
      .softExpireAtMs(now + 30_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();

    CacheEntry updated = ttlPolicy.applySoftTtl(original, 120_000);

    assertThat(updated.getSoftTtlMs()).isEqualTo(120_000);
    assertThat(updated.getSoftExpireAtMs()).isGreaterThan(original.getSoftExpireAtMs());
    assertThat(updated.getHardTtlMs()).isEqualTo(60_000);
    assertThat(updated.getHardExpireAtMs()).isEqualTo(original.getHardExpireAtMs());
    assertThat(updated.getDataVersion()).isEqualTo(10);
    assertThat(updated.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies that applyNormalTtl updates only the normal TTL fields
   * (normalHardTtlMs, normalSoftTtlMs), leaving all other fields untouched.
   */
  @Test
  void applyNormalTtl_shouldUpdateNormalTtlFields() {
    CacheEntry original = CacheEntry.builder()
      .value("v")
      .dataVersion(10)
      .isVersionDegraded(false)
      .decisionVersion(5)
      .hardTtlMs(60_000)
      .hardExpireAtMs(System.currentTimeMillis() + 60_000)
      .softTtlMs(30_000)
      .softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();

    CacheEntry updated = ttlPolicy.applyNormalTtl(original, 600_000, 60_000);

    assertThat(updated.getNormalHardTtlMs()).isEqualTo(600_000);
    assertThat(updated.getNormalSoftTtlMs()).isEqualTo(60_000);
    assertThat(updated.getHardTtlMs()).isEqualTo(60_000);
    assertThat(updated.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(updated.getDataVersion()).isEqualTo(10);
    assertThat(updated.getKeyState()).isEqualTo(KeyState.HOT);
  }
}
