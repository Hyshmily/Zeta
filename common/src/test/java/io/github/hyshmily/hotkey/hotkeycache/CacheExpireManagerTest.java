package io.github.hyshmily.hotkey.hotkeycache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.entity.KeyState;
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

  @Test
  void isSoftExpireEnabled_shouldDelegateToConfig() {
    assertThat(expireManager.isSoftExpireEnabled()).isTrue();
  }

  @Test
  void computeHardExpireAt_withPositiveTtl_shouldReturnFutureTimestamp() {
    long expireAt = expireManager.computeHardExpireAt(1000);
    assertThat(expireAt).isGreaterThan(System.currentTimeMillis());
  }

  @Test
  void computeHardExpireAt_withZeroTtl_shouldFallbackToDefault() {
    long expireAt = expireManager.computeHardExpireAt(0);
    assertThat(expireAt).isGreaterThan(System.currentTimeMillis());
  }

  @Test
  void getEffectiveHardTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveHardTtlMs()).isEqualTo(300_000L);
  }

  @Test
  void getEffectiveHotHardTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveHotHardTtlMs()).isEqualTo(3_600_000L);
  }

  @Test
  void getEffectiveSoftTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveSoftTtlMs()).isEqualTo(30_000L);
  }

  @Test
  void getEffectiveHotSoftTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveHotSoftTtlMs()).isEqualTo(300_000L);
  }

  @Test
  void isSoftExpired_whenDisabled_shouldThrow() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    assertThatThrownBy(() -> disabled.isSoftExpired("key"))
      .isInstanceOf(IllegalStateException.class);
  }

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

  @Test
  void isSoftExpired_shouldReturnTrueForNonCacheEntry() {
    caffeineCache.put("plain", "not-a-cache-entry");
    assertThat(expireManager.isSoftExpired("plain")).isTrue();
  }

  @Test
  void isSoftExpired_shouldReturnTrueForMissingEntry() {
    assertThat(expireManager.isSoftExpired("not-present")).isTrue();
  }

  @Test
  void toExpireTimestamp_withZero_shouldReturnMaxValue() {
    assertThat(CacheExpireManager.toExpireTimestamp(0)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void toExpireTimestamp_withNegative_shouldReturnMaxValue() {
    assertThat(CacheExpireManager.toExpireTimestamp(-1)).isEqualTo(Long.MAX_VALUE);
    assertThat(CacheExpireManager.toExpireTimestamp(-1000)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void toExpireTimestamp_withMaxValue_shouldPassthrough() {
    assertThat(CacheExpireManager.toExpireTimestamp(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void toExpireTimestamp_withPositive_shouldReturnFuture() {
    long result = CacheExpireManager.toExpireTimestamp(1_000);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  @Test
  void computeSoftExpireAt_withDisabledConfig_shouldReturnMaxValue() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    assertThat(disabled.computeSoftExpireAt(0)).isZero();
  }

  @Test
  void triggerBackgroundRefresh_whenDisabled_shouldNotExecute() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    caffeineCache.put("key", "plain-value");
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    disabled.triggerBackgroundRefresh("key", () -> "should-not-run", 1_000);
    assertThat(caffeineCache.getIfPresent("key")).isEqualTo("plain-value");
  }

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

  @Test
  void hardExpireAt_producesPositiveTimestamp() {
    long hard = expireManager.computeHardExpireAt(5_000);
    assertThat(hard).isGreaterThan(System.currentTimeMillis());
    assertThat(hard).isLessThan(System.currentTimeMillis() + 60_000);
  }

  @Test
  void softExpireAt_producesPositiveTimestamp() {
    long soft = expireManager.computeSoftExpireAt(5_000);
    assertThat(soft).isGreaterThan(System.currentTimeMillis());
    assertThat(soft).isLessThan(System.currentTimeMillis() + 60_000);
  }

  @Test
  void hotHardExpireAt_producesPositiveTimestamp() {
    long hot = expireManager.computeHotHardExpireAt();
    assertThat(hot).isGreaterThan(System.currentTimeMillis());
  }

  @Test
  void hotSoftExpireAt_producesPositiveTimestamp() {
    long hot = expireManager.computeHotSoftExpireAt();
    assertThat(hot).isGreaterThan(System.currentTimeMillis());
  }
}
