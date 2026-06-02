package io.github.hyshmily.hotkey.entity;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CacheEntryTest {

  @Test
  void shouldBuildEntryWithAllFields() {
    CacheEntry entry = CacheEntry.builder()
      .value("testValue")
      .dataVersion(100L)
      .isVersionDegraded(false)
      .decisionVersion(5L)
      .hardTtlMs(300_000L)
      .hardExpireAtMs(System.currentTimeMillis() + 300_000L)
      .softTtlMs(30_000L)
      .softExpireAtMs(System.currentTimeMillis() + 30_000L)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000L)
      .normalSoftTtlMs(30_000L)
      .build();

    assertThat(entry.getValue()).isEqualTo("testValue");
    assertThat(entry.getDataVersion()).isEqualTo(100L);
    assertThat(entry.isVersionDegraded()).isFalse();
    assertThat(entry.getDecisionVersion()).isEqualTo(5L);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  @Test
  void shouldBuildNormalEntry() {
    CacheEntry entry = CacheEntry.builder()
      .value(42)
      .dataVersion(0L)
      .isVersionDegraded(false)
      .decisionVersion(0L)
      .hardTtlMs(300_000L)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(0L)
      .softExpireAtMs(0L)
      .keyState(KeyState.NORMAL)
      .normalHardTtlMs(300_000L)
      .normalSoftTtlMs(30_000L)
      .build();

    assertThat(entry.getValue()).isEqualTo(42);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void shouldBuildDegradedEntry() {
    CacheEntry entry = CacheEntry.builder()
      .value("degraded")
      .dataVersion(Long.MIN_VALUE + 1)
      .isVersionDegraded(true)
      .build();

    assertThat(entry.isVersionDegraded()).isTrue();
    assertThat(entry.getDataVersion()).isEqualTo(Long.MIN_VALUE + 1);
  }
}
