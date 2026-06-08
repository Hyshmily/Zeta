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

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CacheKeysPolicy}, covering null, blank, and valid cache key validation.
 */
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
