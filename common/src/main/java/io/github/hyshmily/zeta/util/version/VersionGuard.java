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
package io.github.hyshmily.zeta.util.version;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import java.util.Objects;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared version comparison logic for send message guards used by both
 * the Worker decision listener and the instance-to-instance cache sync listener.
 *
 * <p>This class provides two families of guard methods, each with a fast-path
 * variant (accepting a {@link Cache} reference) and an entry-level variant
 * (accepting an existing {@link CacheEntry}) for use inside atomic
 * {@code compute} blocks:
 *
 * <ul>
 *   <li><b>{@link #shouldSkipForWorker}</b> — Compares {@code decisionVersion}
 *       from Worker HOT/COOL broadcasts. Uses epoch (Worker incarnation,
 *       ADR-0010) and nodeId for cross-restart and cross-Worker ordering.
 *       Unlike {@code shouldSkipForSync}, the Worker-path guard does not
 *       consider the {@code isVersionDegraded} flag: the epoch mechanism
 *       already provides the safety net during Redis outages, and the
 *       degraded flag belongs to the {@code dataVersion} domain only.</li>
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
@Internal
@Slf4j
public final class VersionGuard {

  /**
   * Utility class — prevent instantiation.
   */
  private VersionGuard() {}

  /**
   * WorkerListener guard with epoch and node-id awareness.
   * <p>Decision logic:
   * <ol>
   * <li>No existing entry → accept (return false)</li>
   *   <li>Incoming epoch &gt; existing epoch → accept unconditionally
   *       (Worker restart detected — ADR-0010)</li>
   *   <li>Incoming epoch &lt; existing epoch → skip (stale incarnation message)</li>
   *   <li>Same epoch, same nodeId → compare {@code decisionVersion}: skip if
   *       existing dv &gt;= incoming dv (same counter, directly comparable)</li>
   *   <li>Same epoch, different nodeId → accept unconditionally (different
   *       counters are not comparable; last-writer-wins converges via next
   *       epoch or subsequent broadcasts)</li>
   * </ol>
   *
   * @param existing              the existing cache entry; may be null
   * @param incomingDecisionVersion the decision version from the incoming Worker message
   * @param incomingNodeId        the originating Worker's node ID
   * @param incomingEpoch         the originating Worker's epoch
   * @return true if the incoming message should be skipped, false if it should be applied
   */
  public static boolean shouldSkipForWorker(
    CacheEntry existing,
    long incomingDecisionVersion,
    String incomingNodeId,
    long incomingEpoch
  ) {
    if (existing == null) {
      return false;
    }

    if (incomingEpoch > existing.getDecisionEpoch()) {
      return false;
    }
    if (incomingEpoch < existing.getDecisionEpoch()) {
      // Stale message from an old Worker incarnation — the epoch comparison
      // already ensures correct ordering regardless of magnitude.
      log.debug(
        "Epoch rollback: existing={}, incoming={}, nodeId={} — skipping stale message",
        existing.getDecisionEpoch(),
        incomingEpoch,
        incomingNodeId
      );
      return true;
    }

    if (Objects.equals(incomingNodeId, existing.getDecisionNodeId())) {
      return existing.getDecisionVersion() >= incomingDecisionVersion;
    }

    // Different nodeId at the same epoch — cross-Worker ownership transfer;
    // accept unconditionally.  Counter values are not comparable across
    // Workers; convergence happens via the next epoch.
    return false;
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
   * @param incomingNodeId           the originating Worker's node ID, may be null
   * @param incomingEpoch            the originating Worker's epoch
   * @return {@code true} if the incoming message should be skipped (existing entry
   *         is already up-to-date); {@code false} if the decision may need to be applied
   */
  public static boolean shouldSkipForWorker(
    Cache<String, Object> cache,
    String cacheKey,
    long incomingDecisionVersion,
    String incomingNodeId,
    long incomingEpoch
  ) {
    return shouldSkip(cache, cacheKey, existing ->
      shouldSkipForWorker(existing, incomingDecisionVersion, incomingNodeId, incomingEpoch)
    );
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

    if (existingDegraded == incomingDegraded) {
      return existing.getDataVersion() >= incomingDataVersion;
    }
    return incomingDegraded;
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
    return shouldSkip(cache, cacheKey, existing -> shouldSkipForSync(existing, incomingDataVersion, incomingDegraded));
  }

  /**
   * Cache-level fast path shared by both guard families: resolves the existing
   * {@link CacheEntry} for the key and applies {@code guard} to it. Absent keys
   * and values that are not {@link CacheEntry} (raw user values) never skip.
   *
   * @param cache    the local Caffeine L1 cache; must not be null
   * @param cacheKey the cache key to look up; must not be null
   * @param guard    the entry-level guard to apply to the resolved entry
   * @return the guard's verdict, or {@code false} when no {@link CacheEntry} exists
   */
  private static boolean shouldSkip(Cache<String, Object> cache, String cacheKey, Predicate<CacheEntry> guard) {
    Object existing = cache.getIfPresent(cacheKey);
    return existing instanceof CacheEntry entry && guard.test(entry);
  }
}
