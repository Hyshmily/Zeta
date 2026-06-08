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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.model.CacheEntry;

/**
 * Shared version comparison logic for broadcast message guards.
 * <p>
 * <ul>
 *   <li>{@link #shouldSkipForWorker} compares {@code getDecisionVersion()}
 *       — Worker HOT/COOL decisions use {@code isVersionDegraded()} as
 *       a safety net: degraded entries (created during Redis outage) unconditionally
 *       accept decisions to survive Worker restart.</li>
 *   <li>{@link #shouldSkipForSync} compares {@code getDataVersion()}
 *       — data‑mutation broadcasts use the 4‑case degraded comparison.</li>
 * </ul>
 */
public final class VersionGuard {

  private VersionGuard() {}

  /**
   * WorkerListener guard: compares {@code getDecisionVersion()}.
    * When the existing entry has {@code isVersionDegraded()}{@code = true}
   * (i.e. it was created during a Redis outage), the guard unconditionally accepts the
   * incoming decision — the entry was written in an unstable period and should yield to
   * any newer Worker decision, even if the Worker restarted and its {@code AtomicLong}
   * reset.
   */
  public static boolean shouldSkipForWorker(
    Cache<String, Object> cache,
    String cacheKey,
    long incomingDecisionVersion
  ) {
    Object existing = cache.getIfPresent(cacheKey);
    if (existing instanceof CacheEntry existingCacheEntry) {
      boolean existingDegraded = existingCacheEntry.isVersionDegraded();

      if (!existingDegraded) {
        return existingCacheEntry.getDecisionVersion() >= incomingDecisionVersion;
      }
    }
    return false;
  }

  /**
   * CacheSyncListener guard: compares {@code getDataVersion()}.
   * <p>
   * <ol>
   *   <li>Both normal: skip if existing >= incoming</li>
   *   <li>Existing normal, incoming degraded: always skip (normal wins)</li>
   *   <li>Both degraded: skip if existing >= incoming</li>
   *   <li>Existing degraded, incoming normal: never skip (normal overwrites degraded)</li>
   * </ol>
   *
   * @return {@code true} if the incoming refresh should be skipped
   */
  public static Boolean shouldSkipForSync(
    Cache<String, Object> cache,
    String cacheKey,
    long incomingDataVersion,
    boolean incomingDegraded
  ) {
    Object existing = cache.getIfPresent(cacheKey);
    if (existing instanceof CacheEntry existingCacheEntry) {
      boolean existingDegraded = existingCacheEntry.isVersionDegraded();

      // Both normal
      if (!existingDegraded && !incomingDegraded) {
        return existingCacheEntry.getDataVersion() >= incomingDataVersion;
      }
      // Existing normal, incoming degraded — normal wins
      if (!existingDegraded) {
        return true;
      }
      // Both degraded
      if (incomingDegraded) {
        return existingCacheEntry.getDataVersion() >= incomingDataVersion;
      }
      // Existing degraded, incoming normal — normal overwrites
    }
    return false;
  }
}
