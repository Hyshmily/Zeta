package io.github.hyshmily.hotkey.hotkeycache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CacheKeysPolicyTest {

  @Test
  void invalidCacheKey_null_shouldReturnTrue() {
    assertThat(CacheKeysPolicy.invalidCacheKey(null)).isTrue();
  }

  @Test
  void invalidCacheKey_blank_shouldReturnTrue() {
    assertThat(CacheKeysPolicy.invalidCacheKey("")).isTrue();
    assertThat(CacheKeysPolicy.invalidCacheKey("   ")).isTrue();
  }

  @Test
  void invalidCacheKey_valid_shouldReturnFalse() {
    assertThat(CacheKeysPolicy.invalidCacheKey("validKey")).isFalse();
    assertThat(CacheKeysPolicy.invalidCacheKey("user:123")).isFalse();
  }
}
