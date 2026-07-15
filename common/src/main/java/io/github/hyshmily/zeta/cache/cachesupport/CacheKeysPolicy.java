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
package io.github.hyshmily.zeta.cache.cachesupport;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import jakarta.annotation.Nullable;
import java.util.Collection;

/**
 * Static utility for validating cache key inputs across the library.
 * <p>
 * Defines the contract for what constitutes a valid cache key:
 * keys must be non-null and non-blank. Invalid keys are silently skipped
 * by all cache operations in {@link HotKeyCache}.
 */
@Internal
public final class CacheKeysPolicy {

  /**
   * Checks whether the given cache key is invalid (null or blank).
   *
   * @param cacheKey the key to validate
   * @return {@code true} if the key is {@code null} or blank
   */
  public static boolean invalidCacheKey(String cacheKey) {
    return cacheKey == null || cacheKey.isBlank();
  }

  public static boolean invalidCacheKey(Collection<String> cacheKeys) {
    return cacheKeys == null || cacheKeys.isEmpty();
  }

  /**
   * Normalize a cache key by stripping query parameters (everything after {@code ?}).
   *
   * @param rawKey the raw cache key (may be {@code null})
   * @return the normalized key, or {@code null} if the input was {@code null}
   */
  @Nullable
  public static String normalizeKey(@Nullable String rawKey) {
    if (rawKey == null) return null;
    int idx = rawKey.indexOf('?');
    return idx >= 0 ? rawKey.substring(0, idx) : rawKey;
  }

  /**
   * Utility class — no instantiation.
   */
  private CacheKeysPolicy() {}
}
