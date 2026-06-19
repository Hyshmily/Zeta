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
package io.github.hyshmily.hotkey.sync;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.sync.VersionGuard;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link VersionGuard}, covering degraded/normal version comparison in sync and worker paths.
 */
class VersionGuardTest {

  private Cache<String, Object> cache;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
  }

  /**
   * Verifies that for two normal (non-degraded) entries, the sync is skipped when the existing version is higher.
   */
  @Test
  void shouldSkipForSync_bothNormal_shouldSkipWhenExistingVersionHigher() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 4, false)).isTrue();
  }

  /**
   * Verifies that for two normal entries, the sync is not skipped when the incoming version is higher.
   */
  @Test
  void shouldSkipForSync_bothNormal_shouldNotSkipWhenIncomingVersionHigher() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 6, false)).isFalse();
  }

  /**
   * Verifies that a degraded incoming message is always skipped when the existing entry is normal.
   */
  @Test
  void shouldSkipForSync_existingNormalIncomingDegraded_shouldAlwaysSkip() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 10, true)).isTrue();
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 0, true)).isTrue();
  }

  /**
   * Verifies that for two degraded entries, the sync is skipped when the existing degraded version is higher.
   */
  @Test
  void shouldSkipForSync_bothDegraded_shouldSkipWhenExistingVersionHigher() {
    cache.put("key", entry(10, true, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 8, true)).isTrue();
  }

  /**
   * Verifies that a normal (non-degraded) incoming message is always accepted even when the existing entry is degraded with a higher version.
   */
  @Test
  void shouldSkipForSync_existingDegradedIncomingNormal_shouldNotSkip() {
    cache.put("key", entry(5, true, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 1, false)).isFalse();
  }

  /**
   * Verifies that a sync is never skipped when there is no existing entry in the cache.
   */
  @Test
  void shouldSkipForSync_noExistingEntry_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForSync(cache, "missing", 1, false)).isFalse();
  }

  /**
   * Verifies that a worker decision is never skipped when the existing entry is degraded.
   */
  @Test
  void shouldSkipForWorker_existingDegraded_shouldNotSkip() {
    cache.put("key", entry(5, true, 0));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 10)).isFalse();
  }

  /**
   * Verifies that a worker decision is not skipped when the incoming decision version is greater than the existing one.
   */
  @Test
  void shouldSkipForWorker_existingNormalLowerVersion_shouldNotSkip() {
    cache.put("key", entry(5, false, 3));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 5)).isFalse();
  }

  /**
   * Verifies that a worker decision is skipped when the incoming decision version is less than or equal to the existing one.
   */
  @Test
  void shouldSkipForWorker_existingNormalEqualOrHigherVersion_shouldSkip() {
    cache.put("key", entry(5, false, 5));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 4)).isTrue();
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 5)).isTrue();
  }

  /**
   * Verifies that a worker decision is not skipped when there is no existing entry in the cache.
   */
  @Test
  void shouldSkipForWorker_noExistingEntry_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForWorker(cache, "missing", 1)).isFalse();
  }

  /**
   * Verifies that for two normal entries with equal version, the sync is skipped.
   */
  @Test
  void shouldSkipForSync_bothNormalEqualVersion_shouldSkip() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 5, false)).isTrue();
  }

  /**
   * Verifies that a degraded incoming message to a non-existent entry should not skip.
   */
  @Test
  void shouldSkipForSync_noExistingEntryDegradedIncoming_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForSync(cache, "missing", 1, true)).isFalse();
  }

  /**
   * Verifies the CacheEntry-overload shouldSkipForSync with null existing entry returns false.
   */
  @Test
  void shouldSkipForSync_cacheEntryOverload_withNull_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForSync((CacheEntry) null, 1, false)).isFalse();
  }

  /**
   * Verifies the CacheEntry-overload shouldSkipForWorker with null existing entry returns false.
   */
  @Test
  void shouldSkipForWorker_cacheEntryOverload_withNull_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForWorker(null, 1)).isFalse();
  }

  /**
   * Verifies that a worker decision with equal decision version to existing normal entry is skipped.
   */
  @Test
  void shouldSkipForWorker_existingNormalEqualVersion_shouldSkip() {
    cache.put("key", entry(5, false, 10));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 10)).isTrue();
  }

  /**
   * Verifies that both-degraded scenario is skipped when existing degraded version equals incoming.
   */
  @Test
  void shouldSkipForSync_bothDegradedEqualVersion_shouldSkip() {
    cache.put("key", entry(10, true, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 10, true)).isTrue();
  }

  /**
   * Verifies that when the cache contains a non-{@link CacheEntry} object, the worker guard
   * returns false (do not skip).
   */
  @Test
  void shouldSkipForWorker_withNonCacheEntryInCache_shouldNotSkip() {
    cache.put("key", "not-a-cache-entry");
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 1)).isFalse();
  }

  /**
   * Verifies that when the cache contains a non-{@link CacheEntry} object, the sync guard
   * returns false (do not skip).
   */
  @Test
  void shouldSkipForSync_withNonCacheEntryInCache_shouldNotSkip() {
    cache.put("key", "not-a-cache-entry");
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 1, false)).isFalse();
  }

  private static CacheEntry entry(long dataVersion, boolean degraded, long decisionVersion) {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(dataVersion)
      .isVersionDegraded(degraded)
      .decisionVersion(decisionVersion)
      .hardTtlMs(300_000L)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000L)
      .softExpireAtMs(30_000L)
      .keyState(KeyState.NORMAL)
      .normalHardTtlMs(300_000L)
      .normalSoftTtlMs(30_000L)
      .build();
  }
}
