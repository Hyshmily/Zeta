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
 * Shared version comparison logic for broadcast message guards used by both
 * the Worker decision listener and the instance-to-instance cache sync listener.
 *
 * <p>This class provides two families of guard methods, each with a fast-path
 * variant (accepting a {@link Cache} reference) and an entry-level variant
 * (accepting an existing {@link CacheEntry}) for use inside atomic
 * {@code compute} blocks:
 *
 * <ul>
 *   <li><b>{@link #shouldSkipForWorker}</b> — Compares {@code decisionVersion}
 *       from Worker HOT/COOL broadcasts. Degraded entries (created during a
 *       Redis outage) unconditionally accept incoming decisions, providing a
 *       safety net against Worker restarts that reset the {@code AtomicLong}
 *       counter (see ADR-0008, ADR-0009).</li>
 *   <li><b>{@link #shouldSkipForSync}</b> — Compares {@code dataVersion}
 *       from application-level data-mutation broadcasts. Uses a 4-case degraded
 *       comparison matrix:
 *       <ol>
 *         <li>Both normal: skip if existing {@code >=} incoming</li>
 *         <li>Existing normal, incoming degraded: always skip (normal wins)</li>
 *         <li>Both degraded: skip if existing {@code >=} incoming</li>
 *         <li>Existing degraded, incoming normal: never skip (normal overwrites degraded)</li>
 *       </ol>
 *   </li>
 * </ul>
 *
 * <p>All methods are stateless and thread-safe. Instances of this utility class
 * must never be created.
 *
 * @see WorkerListener
 * @see CacheSyncListener
 */
public final class VersionGuard {

  /**
   * Utility class — prevent instantiation.
   */
  private VersionGuard() {}

  /**
   * WorkerListener guard for use inside an atomic {@code compute} block: the caller
   * already holds the existing entry reference, so no redundant {@code getIfPresent}
   * is needed.
   *
   * <p>Returns {@code false} (accept) for any of the following:
   * <ul>
   *   <li>No existing entry ({@code null})</li>
   *   <li>Existing entry is degraded ({@code isVersionDegraded == true}) —
   *       yields to any incoming decision, even one with a lower version,
   *       because the degraded entry was written during a Redis outage</li>
   * </ul>
   *
   * Otherwise, skips when the existing entry's {@code decisionVersion} is
   * {@code >=} the incoming version.
   *
   * @param existing                 the existing cache entry; may be {@code null}
   * @param incomingDecisionVersion  the decision version from the incoming Worker message;
   *                                 must be non-negative in normal operation
   * @return {@code true} if the incoming message should be skipped, {@code false}
   *         if the decision should be applied
   */
  public static boolean shouldSkipForWorker(CacheEntry existing, long incomingDecisionVersion) {
    if (existing == null) {
      return false;
    }
    if (existing.isVersionDegraded()) {
      return false;
    }
    return existing.getDecisionVersion() >= incomingDecisionVersion;
  }

  /**
   * WorkerListener guard with a cache-level fast path: fetches the existing entry
   * from the L1 cache and delegates to the entry-level overload.
   *
   * <p>This variant is used <em>outside</em> atomic {@code compute} blocks as a
   * cheap first-pass check. If it returns {@code true} (skip), the caller can avoid
   * the more expensive Redis fetch entirely. A second guard inside the {@code compute}
   * block is still needed for correctness (DCL pattern).
   *
   * @param cache                    the local Caffeine L1 cache; must not be null
   * @param cacheKey                 the cache key to look up; must not be null
   * @param incomingDecisionVersion  the decision version from the incoming Worker message
   * @return {@code true} if the incoming message should be skipped (existing entry
   *         is already up-to-date); {@code false} if the decision may need to be applied
   */
  public static boolean shouldSkipForWorker(
    Cache<String, Object> cache,
    String cacheKey,
    long incomingDecisionVersion
  ) {
    Object existing = cache.getIfPresent(cacheKey);
    if (existing instanceof CacheEntry existingCacheEntry) {
      return shouldSkipForWorker(existingCacheEntry, incomingDecisionVersion);
    }
    return false;
  }

  /**
   * CacheSyncListener guard for use inside an atomic {@code compute} block: the caller
   * already holds the existing entry reference.
   *
   * <p>Applies the 4-case degraded comparison matrix:
   * <ol>
   *   <li>Both normal: skip if existing {@code >=} incoming</li>
   *   <li>Existing normal, incoming degraded: always skip (normal wins)</li>
   *   <li>Both degraded: skip if existing {@code >=} incoming</li>
   *   <li>Existing degraded, incoming normal: never skip (normal overwrites degraded)</li>
   * </ol>
   *
   * <p>This design ensures that a single healthy Redis-backed instance can
   * always overwrite degraded entries from other instances, while preventing
   * degraded broadcasts from reverting healthy entries.
   *
   * @param existing             the existing cache entry; may be {@code null} (returns {@code false})
   * @param incomingDataVersion  the data version from the incoming sync message
   * @param incomingDegraded     {@code true} if the incoming sync message was sent in degraded mode
   * @return {@code true} if the incoming refresh should be skipped;
   *         {@code false} if it should be applied
   */
  public static boolean shouldSkipForSync(CacheEntry existing, long incomingDataVersion, boolean incomingDegraded) {
    if (existing == null) {
      return false;
    }

    boolean existingDegraded = existing.isVersionDegraded();

    // Both normal
    if (!existingDegraded && !incomingDegraded) {
      return existing.getDataVersion() >= incomingDataVersion;
    }
    // Existing normal, incoming degraded — normal wins
    if (!existingDegraded) {
      return true;
    }
    // Both degraded
    if (incomingDegraded) {
      return existing.getDataVersion() >= incomingDataVersion;
    }
    // Existing degraded, incoming normal — normal overwrites
    return false;
  }

  /**
   * CacheSyncListener guard with a cache-level fast path: fetches the existing entry
   * from the L1 cache and delegates to the entry-level overload.
   *
   * <p>Used <em>outside</em> atomic {@code compute} blocks as a cheap first-pass
   * check (DCL pattern). A second guard inside the {@code compute} block is still
   * needed for correctness.
   *
   * @param cache               the local Caffeine L1 cache; must not be null
   * @param cacheKey            the cache key to look up; must not be null
   * @param incomingDataVersion the data version from the incoming sync message
   * @param incomingDegraded    {@code true} if the incoming sync message was sent in degraded mode
   * @return {@code true} if the incoming refresh should be skipped;
   *         {@code false} if the update may be needed
   */
  public static boolean shouldSkipForSync(
    Cache<String, Object> cache,
    String cacheKey,
    long incomingDataVersion,
    boolean incomingDegraded
  ) {
    Object existing = cache.getIfPresent(cacheKey);
    if (existing instanceof CacheEntry existingCacheEntry) {
      return shouldSkipForSync(existingCacheEntry, incomingDataVersion, incomingDegraded);
    }
    return false;
  }
}
