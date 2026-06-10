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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.algorithm.AddResult;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.cache.*;
import io.github.hyshmily.hotkey.sync.VersionController;
import io.github.hyshmily.hotkey.monitor.WorkerHealthMonitor;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyCache}, covering peek, get, invalidate, and blacklist behaviors.
 */
class HotKeyCacheTest {

  private TopK hotKeyDetector;
  private Cache<String, Object> caffeineCache;
  private SingleFlight singleFlight;
  private CacheExpireManager expireManager;
  private Executor executor;
  private HotKeyCache hotKeyCache;

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(TopK.class);
    when(hotKeyDetector.add(anyString(), anyInt())).thenReturn(new AddResult(null, false, ""));
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    singleFlight = mock(SingleFlight.class);
    executor = Runnable::run;
    HotKeyProperties ttlConfig = new HotKeyProperties();
    expireManager = new CacheExpireManager(caffeineCache, executor, ttlConfig, 10);

    hotKeyCache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      expireManager,
      executor,
      Optional.empty(),
      Optional.empty(),
      new RuleMatcher(Optional.empty(), Optional.empty()),
      new VersionController(Optional.empty(), 60),
      mock(WorkerHealthMonitor.class)
    );
  }

  /**
   * Verifies that peek returns a cached CacheEntry value.
   */
  @Test
  void peek_shouldReturnCachedValue() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("stored")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.peek("key1")).contains("stored");
  }

  /**
   * Verifies that peek returns empty for null or blank keys.
   */
  @Test
  void peek_shouldReturnEmptyForInvalidKey() {
    assertThat(hotKeyCache.peek(null)).isEmpty();
    assertThat(hotKeyCache.peek("")).isEmpty();
  }

  /**
   * Verifies that a cache entry with KeyState.HOT is identified as a local hot key.
   */
  @Test
  void isLocalHotKey_shouldReturnTrueForHotEntry() {
    caffeineCache.put(
      "hotKey",
      CacheEntry.builder()
        .value("v")
        .dataVersion(0)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(3_600_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(300_000)
        .softExpireAtMs(System.currentTimeMillis() + 300_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.isLocalHotKey("hotKey")).isTrue();
  }

  /**
   * Verifies that a cache entry with KeyState.NORMAL is not identified as a local hot key.
   */
  @Test
  void isLocalHotKey_shouldReturnFalseForNormalEntry() {
    caffeineCache.put(
      "normalKey",
      CacheEntry.builder()
        .value("v")
        .dataVersion(0)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.isLocalHotKey("normalKey")).isFalse();
  }

  /**
   * Verifies that isLocalHotKey returns false for a key not present in the cache.
   */
  @Test
  void isLocalHotKey_shouldReturnFalseForMissingKey() {
    assertThat(hotKeyCache.isLocalHotKey("missing")).isFalse();
  }

  /**
   * Verifies that isLocalHotKey returns false for null keys.
   */
  @Test
  void isLocalHotKey_shouldReturnFalseForInvalidKey() {
    assertThat(hotKeyCache.isLocalHotKey(null)).isFalse();
  }

  /**
   * Verifies that get returns a cached raw value without invoking the loader.
   */
  @Test
  void get_shouldReturnCachedValueOnHit() {
    caffeineCache.put("key1", "rawValue");
    assertThat(hotKeyCache.get("key1", () -> "loaded")).contains("rawValue");
  }

  /**
   * Verifies that get loads and caches a value on cache miss via SingleFlight.
   */
  @Test
  void get_shouldLoadAndCacheOnMiss() {
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("loadedValue"));
    when(hotKeyDetector.add(anyString(), anyInt())).thenReturn(new AddResult(null, false, "key1"));

    Optional<String> result = hotKeyCache.get("key1", () -> "loadedValue");
    assertThat(result).contains("loadedValue");
    assertThat(caffeineCache.getIfPresent("key1")).isNotNull();
  }

  /**
   * Verifies that get returns empty for null or blank keys without loading.
   */
  @Test
  void get_shouldReturnEmptyForInvalidKey() {
    assertThat(hotKeyCache.get(null, () -> "v")).isEmpty();
    assertThat(hotKeyCache.get("", () -> "v")).isEmpty();
  }

  /**
   * Verifies that invalidate removes a cached entry.
   */
  @Test
  void invalidate_shouldRemoveEntry() {
    caffeineCache.put("key1", "value");
    hotKeyCache.invalidate("key1");
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that invalidate handles null and blank keys without throwing.
   */
  @Test
  void invalidate_shouldHandleInvalidKey() {
    hotKeyCache.invalidate(null);
    hotKeyCache.invalidate("");
  }

  /**
   * Verifies that invalidateAll removes multiple cached entries.
   */
  @Test
  void invalidateAll_shouldRemoveEntries() {
    caffeineCache.put("k1", "v1");
    caffeineCache.put("k2", "v2");
    hotKeyCache.invalidateAll(List.of("k1", "k2"));
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
    assertThat(caffeineCache.getIfPresent("k2")).isNull();
  }

  /**
   * Verifies that invalidateAll skips null and blank keys in the input list.
   */
  @Test
  void invalidateAll_shouldSkipInvalidKeys() {
    caffeineCache.put("k1", "v1");
    hotKeyCache.invalidateAll(Arrays.asList("k1", null, ""));
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
  }

  /**
   * Verifies that get throws HotKeyBlockedException for a blacklisted key.
   */
  @Test
  void get_shouldThrowHotKeyBlockedExceptionForBlacklistedKey() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.get("secret", () -> "db")).isInstanceOf(HotKeyBlockedException.class);
  }

  /**
   * Verifies that getWithSoftExpire throws HotKeyBlockedException for a blacklisted key.
   */
  @Test
  void getWithSoftExpire_shouldThrowHotKeyBlockedExceptionForBlacklistedKey() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.getWithSoftExpire("secret", () -> "db")).isInstanceOf(
      HotKeyBlockedException.class
    );
  }
}
