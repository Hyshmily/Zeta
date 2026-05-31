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
package io.github.hyshmily.hotkey.hotkeycache;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import org.jspecify.annotations.Nullable;

/**
 * Shared version comparison logic for broadcast message guards.
 * <p>
 * Used by both {@code WorkerListener} (HOT/COOL, no degraded) and
 * {@code CacheSyncListener} (INVALIDATE/REFRESH, with degraded 4-case comparison).
 */
public final class VersionGuard {

  private VersionGuard() {}

  /**
   * WorkerListener guard: simple version comparison, no degraded logic.
   *
   * @return the existing entry if the guard rejects the update, or {@code null} if the update should proceed
   */
  public static @Nullable Object shouldSkipForWorker(
    Cache<String, Object> cache,
    String cacheKey,
    long incomingVersion
  ) {
    Object existing = cache.getIfPresent(cacheKey);
    if (existing instanceof CacheEntry ce) {
      if (ce.isVersionDegraded()) {
        return null;
      }
      if (ce.getVersion() >= incomingVersion) {
        return existing;
      }
    }
    return null;
  }

  /**
   * CacheSyncListener guard: 4-case degraded version comparison.
   * <p>
   * <ol>
   *   <li>Both normal: skip if existing >= incoming</li>
   *   <li>Existing normal, incoming degraded: always skip (normal wins)</li>
   *   <li>Both degraded: skip if existing >= incoming</li>
   *   <li>Existing degraded, incoming normal: never skip (normal overwrites degraded)</li>
   * </ol>
   *
   * @return the existing entry if the guard rejects the update, or {@code null} if the update should proceed
   */
  public static @Nullable Object shouldSkipForSync(
    Cache<String, Object> cache,
    String cacheKey,
    long incomingVersion,
    boolean incomingDegraded
  ) {
    Object existing = cache.getIfPresent(cacheKey);
    if (existing instanceof CacheEntry existingCacheEntry) {
      boolean existingDegraded = existingCacheEntry.isVersionDegraded();

      // Both normal
      if (!existingDegraded && !incomingDegraded) {
        if (existingCacheEntry.getVersion() >= incomingVersion) {
          return existing;
        }
      }
      // Existing normal, incoming degraded — normal wins
      if (!existingDegraded && incomingDegraded) {
        return existing;
      }
      // Both degraded
      if (existingDegraded && incomingDegraded) {
        if (existingCacheEntry.getVersion() >= incomingVersion) {
          return existing;
        }
      }
      // Existing degraded, incoming normal — normal overwrites
    }
    return null;
  }
}
