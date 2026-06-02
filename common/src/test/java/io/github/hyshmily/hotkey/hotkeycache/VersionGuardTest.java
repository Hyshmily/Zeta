package io.github.hyshmily.hotkey.hotkeycache;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.entity.KeyState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VersionGuardTest {

  private Cache<String, Object> cache;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
  }

  @Test
  void shouldSkipForSync_bothNormal_shouldSkipWhenExistingVersionHigher() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 4, false)).isTrue();
  }

  @Test
  void shouldSkipForSync_bothNormal_shouldNotSkipWhenIncomingVersionHigher() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 6, false)).isFalse();
  }

  @Test
  void shouldSkipForSync_existingNormalIncomingDegraded_shouldAlwaysSkip() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 10, true)).isTrue();
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 0, true)).isTrue();
  }

  @Test
  void shouldSkipForSync_bothDegraded_shouldSkipWhenExistingVersionHigher() {
    cache.put("key", entry(10, true, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 8, true)).isTrue();
  }

  @Test
  void shouldSkipForSync_existingDegradedIncomingNormal_shouldNotSkip() {
    cache.put("key", entry(5, true, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 1, false)).isFalse();
  }

  @Test
  void shouldSkipForSync_noExistingEntry_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForSync(cache, "missing", 1, false)).isFalse();
  }

  @Test
  void shouldSkipForWorker_existingDegraded_shouldNotSkip() {
    cache.put("key", entry(5, true, 0));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 10)).isFalse();
  }

  @Test
  void shouldSkipForWorker_existingNormalLowerVersion_shouldNotSkip() {
    cache.put("key", entry(5, false, 3));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 5)).isFalse();
  }

  @Test
  void shouldSkipForWorker_existingNormalEqualOrHigherVersion_shouldSkip() {
    cache.put("key", entry(5, false, 5));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 4)).isTrue();
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 5)).isTrue();
  }

  @Test
  void shouldSkipForWorker_noExistingEntry_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForWorker(cache, "missing", 1)).isFalse();
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
