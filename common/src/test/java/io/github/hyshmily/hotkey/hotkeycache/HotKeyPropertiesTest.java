package io.github.hyshmily.hotkey.hotkeycache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyProperties}, verifying default values and effective TTL computation.
 */
class HotKeyPropertiesTest {

  private HotKeyProperties props() {
    return new HotKeyProperties();
  }

  @Test
  void shouldHaveDefaultValues() {
    HotKeyProperties p = props();
    assertThat(p.getTopK()).isEqualTo(100);
    assertThat(p.getWidth()).isEqualTo(50_000);
    assertThat(p.getDepth()).isEqualTo(5);
    assertThat(p.getDecay()).isEqualTo(0.92);
    assertThat(p.getMinCount()).isEqualTo(10);
  }

  @Test
  void effectiveHardTtlMs_shouldReturnOverrideWhenSet() {
    HotKeyProperties p = props();
    assertThat(p.effectiveHardTtlMs()).isEqualTo(300_000L);
    p.setHardTtlMs(600_000L);
    assertThat(p.effectiveHardTtlMs()).isEqualTo(600_000L);
  }

  @Test
  void effectiveHardTtlMs_shouldReturnDefaultWhenOverrideIsZero() {
    HotKeyProperties p = props();
    assertThat(p.effectiveHardTtlMs()).isEqualTo(300_000L);
  }

  @Test
  void effectiveHotHardTtlMs_shouldReturnOverrideWhenSet() {
    HotKeyProperties p = props();
    assertThat(p.effectiveHotHardTtlMs()).isEqualTo(3_600_000L);
    p.setHotHardTtlMs(7_200_000L);
    assertThat(p.effectiveHotHardTtlMs()).isEqualTo(7_200_000L);
  }

  @Test
  void effectiveSoftTtlMs_shouldReturnOverrideWhenSet() {
    HotKeyProperties p = props();
    assertThat(p.effectiveSoftTtlMs()).isEqualTo(30_000L);
    p.setSoftTtlMs(60_000L);
    assertThat(p.effectiveSoftTtlMs()).isEqualTo(60_000L);
  }

  @Test
  void effectiveHotSoftTtlMs_shouldReturnOverrideWhenSet() {
    HotKeyProperties p = props();
    assertThat(p.effectiveHotSoftTtlMs()).isEqualTo(300_000L);
    p.setHotSoftTtlMs(600_000L);
    assertThat(p.effectiveHotSoftTtlMs()).isEqualTo(600_000L);
  }

  @Test
  void isSoftExpireEnabled_shouldReturnTrueWhenAnySoftTtlConfigured() {
    HotKeyProperties p = props();
    assertThat(p.isSoftExpireEnabled()).isTrue();
  }

  @Test
  void isSoftExpireEnabled_shouldReturnFalseWhenAllSoftTtlsZero() {
    HotKeyProperties p = props();
    p.setDefaultSoftTtlMs(0);
    p.setSoftTtlMs(0);
    p.setDefaultHotSoftTtlMs(0);
    p.setHotSoftTtlMs(0);
    assertThat(p.isSoftExpireEnabled()).isFalse();
  }

  @Test
  void effectiveConsumerCount_shouldReturnConfiguredWhenPositive() {
    HotKeyProperties p = props();
    p.setConsumerCount(4);
    assertThat(p.effectiveConsumerCount()).isEqualTo(4);
  }

  @Test
  void effectiveConsumerCount_shouldReturnDefaultWhenZero() {
    HotKeyProperties p = props();
    assertThat(p.effectiveConsumerCount()).isEqualTo(1);
  }
}
