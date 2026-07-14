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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CacheKeysPolicy}, covering null, blank, and valid cache key validation.
 */
class CacheKeysPolicyTest {

  /**
   * Verifies that a null cache key is flagged as invalid.
   */
  @Test
  void invalidCacheKey_null_shouldReturnTrue() {
    assertThat(CacheKeysPolicy.invalidCacheKey((String) null)).isTrue();
  }

  /**
   * Verifies that empty or whitespace-only cache keys are flagged as invalid.
   */
  @Test
  void invalidCacheKey_blank_shouldReturnTrue() {
    assertThat(CacheKeysPolicy.invalidCacheKey("")).isTrue();
    assertThat(CacheKeysPolicy.invalidCacheKey("   ")).isTrue();
  }

  /**
   * Verifies that common valid cache keys pass validation.
   */
  @Test
  void invalidCacheKey_valid_shouldReturnFalse() {
    assertThat(CacheKeysPolicy.invalidCacheKey("validKey")).isFalse();
    assertThat(CacheKeysPolicy.invalidCacheKey("user:123")).isFalse();
  }

  /**
   * Verifies that keys containing only tab, newline, or other whitespace are flagged as invalid.
   */
  @Test
  void invalidCacheKey_tabAndNewline_shouldReturnTrue() {
    assertThat(CacheKeysPolicy.invalidCacheKey("\t")).isTrue();
    assertThat(CacheKeysPolicy.invalidCacheKey("\n")).isTrue();
    assertThat(CacheKeysPolicy.invalidCacheKey("\r\n")).isTrue();
    assertThat(CacheKeysPolicy.invalidCacheKey(" \t ")).isTrue();
  }

  /**
   * Verifies that Unicode / non-ASCII keys are valid (boundary: character encoding).
   */
  @Test
  void invalidCacheKey_unicode_shouldReturnFalse() {
    assertThat(CacheKeysPolicy.invalidCacheKey("键")).isFalse();
    assertThat(CacheKeysPolicy.invalidCacheKey("ключ")).isFalse();
    assertThat(CacheKeysPolicy.invalidCacheKey("schlüssel")).isFalse();
    assertThat(CacheKeysPolicy.invalidCacheKey("\u2603")).isFalse(); // snowman
  }

  /**
   * Verifies that a very long cache key is still valid (boundary: extreme length).
   */
  @Test
  void invalidCacheKey_veryLong_shouldReturnFalse() {
    String longKey = "k" + "a".repeat(10_000);
    assertThat(CacheKeysPolicy.invalidCacheKey(longKey)).isFalse();
  }

  /**
   * Verifies that a key with only special characters is valid.
   */
  @Test
  void invalidCacheKey_specialChars_shouldReturnFalse() {
    assertThat(CacheKeysPolicy.invalidCacheKey("!@#$%^&*()")).isFalse();
    assertThat(CacheKeysPolicy.invalidCacheKey("user:123:profile:")).isFalse();
  }

  /**
   * Verifies that the constructor is private and cannot be instantiated.
   */
  @Test
  void constructor_shouldBePrivate() {
    assertThatThrownBy(() -> CacheKeysPolicy.class.getDeclaredConstructor().newInstance()).isInstanceOf(
      IllegalAccessException.class
    );
  }
}
