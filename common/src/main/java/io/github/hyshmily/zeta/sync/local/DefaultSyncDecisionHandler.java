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
package io.github.hyshmily.zeta.sync.local;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation of {@link SyncDecisionHandler} that performs Redis-backed
 * REFRESH, version-guarded INVALIDATE, batch INVALIDATE_ALL, and RULES_SYNC processing
 * with {@link SyncHook} dispatch.
 */
@Slf4j
@Internal
public class DefaultSyncDecisionHandler implements SyncDecisionHandler {

  /** Shared Jackson {@link ObjectMapper} for deserializing batch-invalidation key lists from JSON. */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Local Caffeine L1 cache — target of invalidation and refresh operations.
   * Accessed atomically via {@code asMap().compute()} for thread-safe updates. */
  private final Cache<String, Object> caffeineCache;

  /** Loads the current value from Redis given a cache key.
   * Used during REFRESH to fetch the authoritative value before writing to L1. */
  private final CacheLoader redisLoader;

  /** Computes hard and soft expiry timestamps for refreshed entries. */
  private final ExpireManager expireManager;

  /** Hot-key rule matcher whose rule set is updated when a RULES_SYNC message arrives. */
  private final RuleMatcher ruleMatcher;

  /** Optional lifecycle hooks for cache-sync events. Never null. */
  private final List<SyncHook> syncHooks;

  /**
   * Tracks the highest INVALIDATE version per key, preventing stale REFRESH
   * messages (from before the INVALIDATE) from recreating the entry.
   */
  private final Cache<String, Long> recentInvalidated = Caffeine.newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build();

  public DefaultSyncDecisionHandler(
    Cache<String, Object> caffeineCache,
    CacheLoader redisLoader,
    ExpireManager expireManager,
    RuleMatcher ruleMatcher,
    List<SyncHook> syncHooks
  ) {
    this.caffeineCache = caffeineCache;
    this.redisLoader = redisLoader;
    this.expireManager = expireManager;
    this.ruleMatcher = ruleMatcher;
    this.syncHooks = syncHooks != null ? syncHooks : Collections.emptyList();
  }

  /**
   * Atomically removes the specified key from the local cache in response to
   * an INVALIDATE sync message from a peer instance.
   *
   * <p><b>Version guard logic:</b>
   * <ul>
   *   <li><b>Unconditional path:</b> When {@code version == 0L && !isVersionDegraded}
   *       (clean invalidation from {@code invalidateAllLocal}), the guard is bypassed
   *       entirely — the entry is always removed.</li>
   *   <li><b>Guarded path:</b> Uses {@link VersionGuard#shouldSkipForSync} with the
   *       4-case degraded comparison. Case 2 (existing normal, incoming degraded)
   *       prevents a stale degraded INVALIDATE from wiping a healthy entry.</li>
   * </ul>
   *
   * <p>Double-checked locking (DCL): a fast version guard before the atomic
   * {@code compute} (first pass), and a second guard inside the {@code compute}
   * body (second pass) to prevent a concurrent REFRESH from being wiped by a
   * stale invalidate that arrived after the refresh.
   *
   * @param sm the sync message containing the key to invalidate; if the key
   *           is null or invalid, the invalidation is silently skipped
   */
  @Override
  public void handleLocalInvalidate(SyncMessage sm) {
    boolean unconditional = sm.version() == 0L && !sm.isVersionDegraded();

    if (
      !unconditional &&
      VersionGuard.shouldSkipForSync(caffeineCache, sm.cacheKey(), sm.version(), sm.isVersionDegraded())
    ) {
      log.debug("Stale invalidate ignored: key={}, incomingVersion={}", sm.cacheKey(), sm.version());
      return;
    }

    caffeineCache
      .asMap()
      .compute(sm.cacheKey(), (key, existing) -> {
        if (
          !unconditional &&
          existing instanceof CacheEntry ce &&
          VersionGuard.shouldSkipForSync(ce, sm.version(), sm.isVersionDegraded())
        ) {
          return existing;
        }
        return null;
      });
    log.debug("Invalidated by sync: {}", sm.cacheKey());
    recordInvalidation(sm.cacheKey(), sm.version());
    fireAfterInvalidate(sm.cacheKey(), sm);
  }

  /**
   * Batch-invalidates all keys contained in the JSON-array body of the sync message.
   *
   * <p>This method intentionally bypasses version guards. The publisher
   * ({@link CacheSyncPublisher#broadcastLocalInvalidateAll}) always sends clean
   * messages (version=0L, not degraded) and all keys are removed unconditionally.
   * This is more efficient than sending individual INVALIDATE messages for each key.
   *
   * <p>Deserialization failures (malformed JSON) are logged at ERROR level and
   * do not propagate.
   *
   * @param sm the sync message whose {@code cacheKey} field contains the JSON-array
   *           of keys to invalidate; must not be null
   */
  @Override
  public void handleLocalInvalidateAll(SyncMessage sm) {
    try {
      List<String> keys = OBJECT_MAPPER.readValue(sm.cacheKey(), new TypeReference<>() {});
      caffeineCache.invalidateAll(keys);
      log.debug("Batch invalidated {} keys", keys.size());
    } catch (Exception e) {
      log.error("Failed to deserialize batch invalidate keys", e);
    }
  }

  /**
   * Merges the incoming rule set from a RULES_SYNC message into the local
   * {@link RuleMatcher}, guarded by the message's {@code rulesVersion}.
   * <p>
   * Delegates to {@link RuleMatcher#syncRules}, which handles the actual
   * merge logic and version conflict resolution.
   *
   * @param sm the sync message whose {@code cacheKey} field contains the
   *           serialized rule-set JSON and whose {@code rulesVersion} field
   *           carries the version for conflict resolution; must not be null
   */
  @Override
  public void handleRulesSync(SyncMessage sm) {
    ruleMatcher.syncRules(sm.cacheKey(), sm.rulesVersion());
  }

  /**
   * Refreshes a cache entry with the latest value from Redis in response to a
   * REFRESH sync message from a peer instance.
   *
   * <p><b>Refresh flow:</b>
   * <ol>
   *   <li><b>DCL check 1:</b> Fast-path version guard ({@link VersionGuard#shouldSkipForSync})
   *       against the existing L1 entry. If a newer dataVersion is already present,
   *       the refresh is skipped.</li>
   *   <li><b>Redis fetch:</b> Loads the authoritative value from Redis.</li>
   *   <li><b>DCL check 2:</b> Second version guard inside the atomic {@code compute}
   *       to prevent overwriting a newer version that arrived during the Redis fetch.</li>
   *   <li><b>Write:</b> Replaces the value and dataVersion while preserving the existing
   *       entry's metadata (hard/soft TTLs, normal TTLs, key state, decision version,
   *       degradation flag). If no entry existed in L1 before the refresh, a fresh
   *       {@link CacheEntry} is created with default metadata and {@code KeyState.NORMAL}.</li>
   * </ol>
   *
   * <p>If the key is absent from L1 and Redis returns null (key does not exist),
   * the refresh is aborted — there is nothing to cache.
   *
   * @param sm the sync message containing the key and version to refresh;
   *           must not be null
   */
  @Override
  public void handleRefresh(SyncMessage sm) {
    String cacheKey = sm.cacheKey();
    // DCL first check – cheap, outside the compute lock
    if (VersionGuard.shouldSkipForSync(caffeineCache, cacheKey, sm.version(), sm.isVersionDegraded())) {
      log.debug("Stale refresh ignored: key={}, incomingVersion={}", cacheKey, sm.version());
      fireOnRefreshSkipped(cacheKey, sm);
      return;
    }

    Object value = loadFromRedis(sm);
    if (value == null) {
      log.warn("Refresh failed to load value from Redis for key={}", cacheKey);
      fireOnRefreshSkipped(cacheKey, sm);
      return;
    }

    caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) -> {
        // DCL second check – atomic with to write
        if (
          existing instanceof CacheEntry ce && VersionGuard.shouldSkipForSync(ce, sm.version(), sm.isVersionDegraded())
        ) {
          return existing;
        }
        // Atomically check invalidation record to prevent stale refresh after invalidate
        if (isInvalidation(key, sm.version())) {
          log.debug("Refresh skipped due to recent invalidation: key={}", key);
          return existing; // preserve whatever is currently in cache (may be null)
        }

        // Clear invalidation watermark before writing — the refresh is
        // proceeding, so the high-water mark is no longer meaningful.
        clearInvalidation(key);

        if (existing instanceof CacheEntry cacheEntry) {
          long hardExpireAt = expireManager.computeHardExpireAt(cacheEntry.getHardTtlMs());
          long softExpireAt = expireManager.computeSoftExpireAt(cacheEntry.getSoftTtlMs());
          return cacheEntry.withValueAndRefreshMeta(
            expireManager.wrapValue(value),
            sm.version(),
            sm.isVersionDegraded(),
            hardExpireAt,
            softExpireAt
          );
        }
        long defaultHardTtlMs = expireManager.getEffectiveHardTtlMs();
        long defaultSoftTtlMs = expireManager.getEffectiveSoftTtlMs();
        return expireManager.createBuilder(
          value,
          sm.version(),
          sm.isVersionDegraded(),
          0L,
          defaultHardTtlMs,
          defaultSoftTtlMs,
          defaultHardTtlMs,
          defaultSoftTtlMs,
          KeyState.NORMAL
        );
      });
    log.debug("Refreshed by sync: {}", cacheKey);
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent(cacheKey);
    if (entry != null) {
      fireAfterRefresh(cacheKey, sm, entry);
    }
  }

  /**
   * Loads the current value from Redis for the key carried in the sync message.
   * <p>
   * Any exception thrown by the {@code redisLoader} (connection timeout, Redis
   * outage, serialization error) is caught and logged at WARN level. The caller
   * should handle a {@code null} return by aborting the refresh.
   *
   * @param sm the sync message containing the cache key to load; must not be null
   * @return the value from Redis, or {@code null} if the key is absent in Redis
   *         or the load failed with an exception
   */
  private Object loadFromRedis(SyncMessage sm) {
    try {
      return redisLoader.load(sm.cacheKey());
    } catch (Exception e) {
      log.warn("handleRefresh: Redis load failed for key={}", sm.cacheKey(), e);
      return null;
    }
  }

  /**
   * Record the highest INVALIDATE version for a key.
   * Used to reject stale REFRESH messages that arrive after the INVALIDATE.
   */
  private void recordInvalidation(String key, long version) {
    long watermark = (version == 0L) ? Long.MAX_VALUE : version;
    recentInvalidated.asMap().merge(key, watermark, Math::max);
  }

  /**
   * Check whether a refresh should be skipped because the key was
   * invalidated at a version >= the refresh version.
   */
  private boolean isInvalidation(String key, long refreshVersion) {
    Long highWater = recentInvalidated.getIfPresent(key);
    return highWater != null && refreshVersion < highWater;
  }

  /**
   * Remove the invalidation reportToWorker after a successful refresh,
   * allowing future refreshes for this key to proceed normally.
   */
  private void clearInvalidation(String key) {
    recentInvalidated.invalidate(key);
  }

  /**
   * Called after the entry has been refreshed from the data store following a REFRESH
   * broadcast from a peer instance. The value and dataVersion have been updated while
   * the existing TTLs, key state, and decision metadata are preserved.
   */
  public void fireAfterRefresh(String cacheKey, SyncMessage sm, CacheEntry entry) {
    for (SyncHook hook : syncHooks) {
      try {
        hook.afterRefresh(cacheKey, sm, entry);
      } catch (Exception e) {
        log.warn("SyncHook.afterRefresh failed for key={}", cacheKey, e);
      }
    }
  }

  /**
   * Called after the entry has been removed from L1 in response to an INVALIDATE
   * broadcast from a peer instance. The cache no longer contains this key.
   */
  public void fireAfterInvalidate(String cacheKey, SyncMessage sm) {
    for (SyncHook hook : syncHooks) {
      try {
        hook.afterInvalidate(cacheKey, sm);
      } catch (Exception e) {
        log.warn("SyncHook.afterInvalidate failed for key={}", cacheKey, e);
      }
    }
  }

  /**
   * Called when the REFRESH message was not applied — either the local entry already
   * had a newer dataVersion, the value was not found in the data store, or the key
   * had been recently invalidated and the refresh was rejected by the invalidation
   * watermark.
   */
  public void fireOnRefreshSkipped(String cacheKey, SyncMessage sm) {
    for (SyncHook hook : syncHooks) {
      try {
        hook.onRefreshSkipped(cacheKey, sm);
      } catch (Exception e) {
        log.warn("SyncHook.onRefreshSkipped failed for key={}", cacheKey, e);
      }
    }
  }
}
