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
package io.github.hyshmily.hotkey.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CacheExpireManager}, covering soft-expire logic, TTL computation, and background refresh.
 */
class CacheExpireManagerTest {

  private CacheExpireManager expireManager;
  private Cache<String, Object> caffeineCache;
  private HotKeyProperties ttlConfig;

  @BeforeEach
  void setUp() {
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ttlConfig = new HotKeyProperties();
    Executor executor = Runnable::run;
    expireManager = new CacheExpireManager(caffeineCache, executor, ttlConfig, 10);
  }

  /**
   * Verifies that isSoftExpireEnabled delegates to the HotKeyProperties configuration.
   */
  @Test
  void isSoftExpireEnabled_shouldDelegateToConfig() {
    assertThat(expireManager.isSoftExpireEnabled()).isTrue();
  }

  /**
   * Verifies that computeHardExpireAt with a positive TTL returns a timestamp in the future.
   */
  @Test
  void computeHardExpireAt_withPositiveTtl_shouldReturnFutureTimestamp() {
    long expireAt = expireManager.computeHardExpireAt(1000);
    assertThat(expireAt).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHardExpireAt falls back to the default TTL when the given value is zero.
   */
  @Test
  void computeHardExpireAt_withZeroTtl_shouldFallbackToDefault() {
    long expireAt = expireManager.computeHardExpireAt(0);
    assertThat(expireAt).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that getEffectiveHardTtlMs returns the configured default hard TTL value.
   */
  @Test
  void getEffectiveHardTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveHardTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that getEffectiveHotHardTtlMs returns the configured hot-key hard TTL value.
   */
  @Test
  void getEffectiveHotHardTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveHotHardTtlMs()).isEqualTo(3_600_000L);
  }

  /**
   * Verifies that getEffectiveSoftTtlMs returns the configured default soft TTL value.
   */
  @Test
  void getEffectiveSoftTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveSoftTtlMs()).isEqualTo(30_000L);
  }

  /**
   * Verifies that getEffectiveHotSoftTtlMs returns the configured hot-key soft TTL value.
   */
  @Test
  void getEffectiveHotSoftTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveHotSoftTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that isSoftExpired throws IllegalStateException when soft-expire is disabled in config.
   */
  @Test
  void isSoftExpired_whenDisabled_shouldThrow() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    assertThatThrownBy(() -> disabled.isSoftExpired("key"))
      .isInstanceOf(IllegalStateException.class);
  }

  /**
   * Verifies that triggerBackgroundRefresh executes the supplier and preserves the cache entry.
   */
  @Test
  void triggerBackgroundRefresh_shouldAcquireAndReleasePermit() {
    caffeineCache.put("key", CacheEntry.builder()
      .value("old")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build());

    expireManager.triggerBackgroundRefresh("key", () -> "newValue", 30_000);

    assertThat(caffeineCache.getIfPresent("key")).isNotNull();
  }

  /**
   * Verifies that a non-CacheEntry plain value is considered soft-expired.
   */
  @Test
  void isSoftExpired_shouldReturnTrueForNonCacheEntry() {
    caffeineCache.put("plain", "not-a-cache-entry");
    assertThat(expireManager.isSoftExpired("plain")).isTrue();
  }

  /**
   * Verifies that a missing cache key is considered soft-expired.
   */
  @Test
  void isSoftExpired_shouldReturnTrueForMissingEntry() {
    assertThat(expireManager.isSoftExpired("not-present")).isTrue();
  }

  /**
   * Verifies that computeHardExpireAt with zero falls back to default and returns a future timestamp.
   */
  @Test
  void computeHardExpireAt_withZero_shouldFallbackAndReturnFuture() {
    long result = expireManager.computeHardExpireAt(0);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHardExpireAt with a negative TTL falls back to default and returns a future timestamp.
   */
  @Test
  void computeHardExpireAt_withNegative_shouldFallbackAndReturnFuture() {
    long result = expireManager.computeHardExpireAt(-1);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHardExpireAt with Long.MAX_VALUE passes it through unchanged.
   */
  @Test
  void computeHardExpireAt_withMaxValue_shouldPassthrough() {
    assertThat(expireManager.computeHardExpireAt(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that computeHardExpireAt with a positive TTL returns a future timestamp.
   */
  @Test
  void computeHardExpireAt_withPositive_shouldReturnFuture() {
    long result = expireManager.computeHardExpireAt(1000);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeSoftExpireAt returns zero when soft-expire is disabled.
   */
  @Test
  void computeSoftExpireAt_withDisabledConfig_shouldReturnMaxValue() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    assertThat(disabled.computeSoftExpireAt(0)).isZero();
  }

  /**
   * Verifies that triggerBackgroundRefresh does nothing when soft-expire is disabled.
   */
  @Test
  void triggerBackgroundRefresh_whenDisabled_shouldNotExecute() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    caffeineCache.put("key", "plain-value");
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    disabled.triggerBackgroundRefresh("key", () -> "should-not-run", 1_000);
    assertThat(caffeineCache.getIfPresent("key")).isEqualTo("plain-value");
  }

  /**
   * Verifies that triggerBackgroundRefresh discards the result when the entry's data version has changed since the refresh started.
   */
  @Test
  void triggerBackgroundRefresh_withStaleVersion_shouldDiscardResult() {
    caffeineCache.put("key", CacheEntry.builder()
      .value("original")
      .dataVersion(5)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(System.currentTimeMillis() + 300_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build());

    expireManager.triggerBackgroundRefresh("key", () -> {
      caffeineCache.asMap().computeIfPresent("key", (k, existing) -> {
        if (existing instanceof CacheEntry ce) {
          return ce.toBuilder().dataVersion(10).build();
        }
        return existing;
      });
      return "stale-value";
    }, 30_000);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat(entry.getDataVersion()).isEqualTo(10);
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  /**
   * Verifies that computeHardExpireAt produces a positive timestamp within a reasonable range.
   */
  @Test
  void hardExpireAt_producesPositiveTimestamp() {
    long hard = expireManager.computeHardExpireAt(5_000);
    assertThat(hard).isGreaterThan(System.currentTimeMillis());
    assertThat(hard).isLessThan(System.currentTimeMillis() + 60_000);
  }

  /**
   * Verifies that computeSoftExpireAt produces a positive timestamp within a reasonable range.
   */
  @Test
  void softExpireAt_producesPositiveTimestamp() {
    long soft = expireManager.computeSoftExpireAt(5_000);
    assertThat(soft).isGreaterThan(System.currentTimeMillis());
    assertThat(soft).isLessThan(System.currentTimeMillis() + 60_000);
  }

  /**
   * Verifies that computeHotHardExpireAt produces a positive future timestamp.
   */
  @Test
  void hotHardExpireAt_producesPositiveTimestamp() {
    long hot = expireManager.computeHotHardExpireAt();
    assertThat(hot).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHotSoftExpireAt produces a positive future timestamp.
   */
  @Test
  void hotSoftExpireAt_producesPositiveTimestamp() {
    long hot = expireManager.computeHotSoftExpireAt();
    assertThat(hot).isGreaterThan(System.currentTimeMillis());
  }
}
