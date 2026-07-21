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
package io.github.hyshmily.zeta.cache.cachesupport.impl;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.DelayUtil;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Manages hard and soft TTL computation for {@link CacheEntry} instances.
 * <p>
 * Hard TTL controls Caffeine eviction; soft TTL controls stale-while-revalidate background refresh.
 * Each has a normal-key and hot-key variant, with an optional override taking precedence over the default.
 */
@Getter
@Slf4j
@Internal
public class ExpireManagerImpl implements ExpireManager {

  /** The underlying L1 Caffeine cache instance. */
  private final Cache<String, Object> caffeineCache;
  /** Async executor for background refresh tasks. */
  private final Executor executor;
  /** TTL configuration providing normal and hot-key TTL values. */
  private final ZetaProperties ttlConfig;
  /** Whether soft expire (stale-while-revalidate) is enabled (cached from config at construction).*/
  private final boolean softExpireEnabled;
  /** Semaphore limiting concurrent background refresh operations (null if soft expire disabled). */
  // null when soft expire is disabled; always guarded by softExpireEnabled check
  private final Semaphore refreshLimiter;
  /** Per-key dedup for background refreshes — prevents concurrent refresh for the same key. */
  private final ConcurrentHashMap<String, CompletableFuture<?>> pendingRefreshes = new ConcurrentHashMap<>();
  /** Compressor for L1 cache values. */
  private final CacheCompressor compressor;

  /** Jitter ratio applied to TTLs to prevent cache stampedes (from config, default 0.05 = ±5%). */
  private final double defaultTtlJitterRatio;

  private static final long refreshTimeoutSeconds = 30;

  private record snapshotEntry(
    long dataVersion,
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    KeyState keyState
  ) {
    static final snapshotEntry DEFAULT = new snapshotEntry(VERSION_DEFAULT, VERSION_DEFAULT, null, 0L, KeyState.NORMAL);
  }

  private static snapshotEntry snapshotEntry(Object raw) {
    if (raw instanceof CacheEntry entry) {
      return new snapshotEntry(
        entry.getDataVersion(),
        entry.getDecisionVersion(),
        entry.getDecisionNodeId(),
        entry.getDecisionEpoch(),
        entry.getKeyState()
      );
    }
    return snapshotEntry.DEFAULT;
  }

  /**
   * Creates a ExpireManagerImpl with the given Caffeine cache, executor, and TTL config.
   *
   * @param caffeineCache   the underlying L1 Caffeine cache
   * @param executor        async executor for background refresh
   * @param ttlConfig       TTL configuration (normal and hot-key variants)
   * @param refreshMaxPools maximum concurrent background refreshes (capped at 100)
   */
  public ExpireManagerImpl(
    Cache<String, Object> caffeineCache,
    Executor executor,
    ZetaProperties ttlConfig,
    int refreshMaxPools
  ) {
    this(caffeineCache, executor, ttlConfig, refreshMaxPools, CacheCompressor.NONE);
  }

  /**
   * Creates a ExpireManagerImpl with the given Caffeine cache, executor, TTL config,
   * and a {@link CacheCompressor} for L1 value compression.
   *
   * @param caffeineCache   the underlying L1 Caffeine cache
   * @param executor        async executor for background refresh
   * @param ttlConfig       TTL configuration (normal and hot-key variants)
   * @param refreshMaxPools maximum concurrent background refreshes (capped at 100)
   * @param compressor      compressor for L1 cache values
   */
  public ExpireManagerImpl(
    Cache<String, Object> caffeineCache,
    Executor executor,
    ZetaProperties ttlConfig,
    int refreshMaxPools,
    CacheCompressor compressor
  ) {
    this.caffeineCache = caffeineCache;
    this.executor = executor;
    this.ttlConfig = ttlConfig;
    this.compressor = compressor;
    this.softExpireEnabled = ttlConfig.isSoftExpireEnabled();
    this.refreshLimiter = initRefreshLimiter(refreshMaxPools);
    this.defaultTtlJitterRatio = ttlConfig.getTtlJitterRatio();
  }

  /**
   * Create a ExpireManagerImpl with explicit jitter ratio (for testing).
   */
  ExpireManagerImpl(
    Cache<String, Object> caffeineCache,
    Executor executor,
    ZetaProperties ttlConfig,
    int refreshMaxPools,
    double defaultTtlJitterRatio,
    CacheCompressor compressor
  ) {
    this.caffeineCache = caffeineCache;
    this.executor = executor;
    this.ttlConfig = ttlConfig;
    this.compressor = compressor;
    this.softExpireEnabled = ttlConfig.isSoftExpireEnabled();
    this.refreshLimiter = initRefreshLimiter(refreshMaxPools);
    this.defaultTtlJitterRatio = defaultTtlJitterRatio;
  }

  private Semaphore initRefreshLimiter(int refreshMaxPools) {
    int effectiveRefreshMaxPools = refreshMaxPools > 0 ? refreshMaxPools : 100;
    return softExpireEnabled ? new Semaphore(effectiveRefreshMaxPools) : null;
  }

  /**
   * Check whether a {@link CacheEntry} has logically expired based on its
   * {@code hardExpireAtMs}.  Entries with {@code hardExpireAtMs == Long.MAX_VALUE}
   * are treated as permanent (never logically expire).
   *
   * @param entry the cache entry to inspect
   * @return {@code true} if the entry has logically expired
   */
  @Override
  public boolean isLogicallyExpired(CacheEntry entry) {
    return entry.getHardExpireAtMs() != Long.MAX_VALUE && TimeSource.currentTimeMillis() >= entry.getHardExpireAtMs();
  }

  /**
   * Check whether the given raw cache value is a logically expired {@link CacheEntry}
   * and, if so, invalidate it and return {@code true}.
   * <p>Eliminates code duplication between {@link io.github.hyshmily.zeta.cache.HotKeyCache#get}
   * and {@link io.github.hyshmily.zeta.cache.HotKeyCache#getWithSoftExpire},
   * which both perform this check before and after side effects (TOCTOU guard).
   *
   * @param cacheKey the cache key to invalidate if expired
   * @param raw      the raw value from the Caffeine cache
   * @return {@code true} if the entry was expired and has been invalidated
   */
  @Override
  public boolean invalidateIfIsLogicallyExpired(String cacheKey, Object raw) {
    if (raw instanceof CacheEntry ce && isLogicallyExpired(ce)) {
      caffeineCache.invalidate(cacheKey);
      log.debug("Cache entry logically expired during processing, reloading: {}", cacheKey);
      return true;
    }
    return false;
  }

  @Override
  public long computeNullExpireAt(long nullTtlMs) {
    long effective = nullTtlMs > 0 ? nullTtlMs : ttlConfig.effectiveNullTtlMs();
    return toHardExpireTimestamp(effective);
  }

  /**
   * Hard expire timestamp from an explicit TTL duration.
   * Falls back to the normal-key default if {@code hardTtlMs <= 0}.
   *
   * @param hardTtlMs the hard TTL duration in milliseconds (&lt;= 0 uses configured default)
   * @return absolute epoch-ms timestamp for hard expiry
   */
  @Override
  public long computeHardExpireAt(long hardTtlMs) {
    return toHardExpireTimestamp(resolveEffectiveHardTtl(hardTtlMs));
  }

  /**
   * Hard expire timestamp for hot keys, using {@code default-hot-hard-ttl} / {@code hot-hard-ttl}.
   * Returns {@code Long.MAX_VALUE} if hot hard expire is disabled (TTL &lt;= 0).
   *
   * @return absolute epoch-ms timestamp for hot-key hard expiry
   */
  @Override
  public long computeHotHardExpireAt() {
    return toHardExpireTimestamp(ttlConfig.effectiveHotHardTtlMs());
  }

  /**
   * Soft expire timestamp for hot keys, using {@code default-hot-soft-ttl} / {@code hot-soft-ttl}.
   *
   * @return absolute epoch-ms timestamp for hot-key soft expiry, or 0 if disabled
   */
  @Override
  public long computeHotSoftExpireAt() {
    return toSoftExpireTimestamp(ttlConfig.effectiveHotSoftTtlMs());
  }

  /**
   * Soft expire timestamp from an explicit TTL duration.
   * Falls back to the normal-key default if {@code softTtlMs <= 0}. Returns 0 if soft expire is disabled.
   *
   * @param softTtlMs the soft TTL duration in milliseconds (&lt;= 0 uses configured default)
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if disabled
   */
  @Override
  public long computeSoftExpireAt(long softTtlMs) {
    if (!isSoftExpireEnabled()) {
      return 0L;
    }
    return toSoftExpireTimestamp(resolveEffectiveSoftTtl(softTtlMs));
  }

  /**
   * Effective hard TTL for normal keys (override > default).
   *
   * @return effective hard TTL duration in milliseconds
   */
  @Override
  public long getEffectiveHardTtlMs() {
    return ttlConfig.effectiveHardTtlMs();
  }

  /**
   * Resolve effective hard TTL for normal keys: use the override value if
   * positive, otherwise fall back to the configured default.
   *
   * @param hardTtlMs hard TTL override ({@code 0} or negative uses default)
   * @return effective hard TTL duration in milliseconds
   */
  @Override
  public long resolveEffectiveHardTtl(long hardTtlMs) {
    return hardTtlMs > 0 ? hardTtlMs : getEffectiveHardTtlMs();
  }

  /**
   * Effective hard TTL for hot keys (override > default).
   *
   * @return effective hot hard TTL duration in milliseconds
   */
  @Override
  public long getEffectiveHotHardTtlMs() {
    return ttlConfig.effectiveHotHardTtlMs();
  }

  /**
   * Resolve effective hard TTL for hot keys: use the override value if
   * positive, otherwise fall back to the configured hot-key hard TTL.
   *
   * @param hardTtlMs hard TTL override ({@code 0} or negative uses default)
   * @return effective hot-key hard TTL duration in milliseconds
   */
  @Override
  public long resolveEffectiveHotHard(long hardTtlMs) {
    return hardTtlMs > 0 ? hardTtlMs : getEffectiveHotHardTtlMs();
  }

  /**
   * Effective soft TTL for normal keys (override > default).
   *
   * @return effective soft TTL duration in milliseconds
   */
  @Override
  public long getEffectiveSoftTtlMs() {
    return ttlConfig.effectiveSoftTtlMs();
  }

  /**
   * Resolve effective soft TTL for normal keys: use the override value if
   * positive, otherwise fall back to the configured default.
   *
   * @param softTtlMs soft TTL override ({@code 0} or negative uses default)
   * @return effective soft TTL duration in milliseconds
   */
  @Override
  public long resolveEffectiveSoftTtl(long softTtlMs) {
    return softTtlMs > 0 ? softTtlMs : getEffectiveSoftTtlMs();
  }

  /**
   * Effective soft TTL for hot keys (override > default).
   *
   * @return effective hot soft TTL duration in milliseconds
   */
  @Override
  public long getEffectiveHotSoftTtlMs() {
    return ttlConfig.effectiveHotSoftTtlMs();
  }

  /**
   * Resolve effective soft TTL for hot keys: use the override value if
   * positive, otherwise fall back to the configured hot-key soft TTL.
   *
   * @param softTtlMs soft TTL override ({@code 0} or negative uses default)
   * @return effective hot-key soft TTL duration in milliseconds
   */
  @Override
  public long resolveEffectiveHotSoft(long softTtlMs) {
    return softTtlMs > 0 ? softTtlMs : getEffectiveHotSoftTtlMs();
  }

  /**
   * Build a {@link CacheEntry} from fully resolved fields, including
   * decision metadata (node, epoch), pre-computed expire timestamps,
   * and normal-TTL values. Normal TTL is applied via
   * {@link #applyNormalTtl} after construction.
   * <p>
   * This overload accepts pre-computed hard/soft expire timestamps,
   * which is useful when the caller already knows the exact expiry
   * baseline (e.g., when copying from an existing entry).
   *
   * @param value              the cached value
   * @param dataVersion        the data version for cross-instance sync
   * @param isVersionDegraded  whether the data version is degraded (local fallback)
   * @param decisionVersion    the Worker decision version
   * @param decisionNodeId     the Worker node ID that produced the decision
   * @param decisionEpoch      the epoch (restart counter) of the decision Worker
   * @param hardTtlMs          hard TTL duration in milliseconds
   * @param softTtlMs          soft TTL duration in milliseconds
   * @param hardExpireAtMs     pre-computed hard expiry absolute timestamp
   * @param softExpireAtMs     pre-computed soft expiry absolute timestamp
   * @param normalHardTtlMs    normal (non-hot) hard TTL for state reversion
   * @param normalSoftTtlMs    normal (non-hot) soft TTL for state reversion
   * @param keyState           the initial key state (NORMAL, HOT, COOL)
   * @return a new {@link CacheEntry} with all fields set
   */
  @Override
  public CacheEntry createBuilder(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    long hardTtlMs,
    long softTtlMs,
    long hardExpireAtMs,
    long softExpireAtMs,
    long normalHardTtlMs,
    long normalSoftTtlMs,
    KeyState keyState
  ) {
    return applyNormalTtl(
      CacheEntry.builder()
        .value(compressor.wrap(value))
        .dataVersion(dataVersion)
        .isVersionDegraded(isVersionDegraded)
        .decisionVersion(decisionVersion)
        .decisionNodeId(decisionNodeId)
        .decisionEpoch(decisionEpoch)
        .hardTtlMs(hardTtlMs)
        .softTtlMs(softTtlMs)
        .hardExpireAtMs(hardExpireAtMs)
        .softExpireAtMs(softExpireAtMs)
        .keyState(keyState)
        .build(),
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /**
   * Build a {@link CacheEntry} from fully resolved fields with decision
   * metadata but without pre-computed expire timestamps. Expire timestamps
   * are computed automatically via {@link #applyTtl}.
   * <p>
   * The {@code hardTtlMs} and {@code softTtlMs} passed here are used both
   * as field values <em>and</em> as inputs to {@code applyTtl}, which
   * overwrites the expire-at timestamps. This is the typical path for
   * entries sourced from a remote reader (Worker or Redis) where the
   * caller does not pre-compute timestamps.
   *
   * @param value              the cached value
   * @param dataVersion        the data version for cross-instance sync
   * @param isVersionDegraded  whether the data version is degraded
   * @param decisionVersion    the Worker decision version
   * @param decisionNodeId     the Worker node ID that produced the decision
   * @param decisionEpoch      the epoch (restart counter) of the decision Worker
   * @param hardTtlMs          hard TTL duration in milliseconds
   * @param softTtlMs          soft TTL duration in milliseconds
   * @param normalHardTtlMs    normal (non-hot) hard TTL for state reversion
   * @param normalSoftTtlMs    normal (non-hot) soft TTL for state reversion
   * @param keyState           the initial key state
   * @return a new {@link CacheEntry} with expire timestamps computed
   */
  @Override
  public CacheEntry createBuilder(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    long hardTtlMs,
    long softTtlMs,
    long normalHardTtlMs,
    long normalSoftTtlMs,
    KeyState keyState
  ) {
    return applyTtl(
      applyNormalTtl(
        CacheEntry.builder()
          .value(compressor.wrap(value))
          .dataVersion(dataVersion)
          .isVersionDegraded(isVersionDegraded)
          .decisionVersion(decisionVersion)
          .decisionNodeId(decisionNodeId)
          .decisionEpoch(decisionEpoch)
          .hardTtlMs(hardTtlMs)
          .softTtlMs(softTtlMs)
          .keyState(keyState)
          .build(),
        normalHardTtlMs,
        normalSoftTtlMs
      ),
      hardTtlMs,
      softTtlMs
    );
  }

  /**
   * Build a {@link CacheEntry} with pre-computed expire timestamps
   * but without decision node/epoch metadata. Normal TTL is applied
   * via {@link #applyNormalTtl} after construction.
   * <p>
   * This overload omits {@code decisionNodeId} and {@code decisionEpoch},
   * which is appropriate for entries created by local promotion (no
   * Worker origin). The expire timestamps are caller-supplied.
   *
   * @param value              the cached value
   * @param dataVersion        the data version for cross-instance sync
   * @param isVersionDegraded  whether the data version is degraded
   * @param decisionVersion    the Worker decision version (0 for local)
   * @param hardTtlMs          hard TTL duration in milliseconds
   * @param softTtlMs          soft TTL duration in milliseconds
   * @param hardExpireAtMs     pre-computed hard expiry absolute timestamp
   * @param softExpireAtMs     pre-computed soft expiry absolute timestamp
   * @param normalHardTtlMs    normal (non-hot) hard TTL for state reversion
   * @param normalSoftTtlMs    normal (non-hot) soft TTL for state reversion
   * @param keyState           the initial key state
   * @return a new {@link CacheEntry} with all fields set
   */
  @Override
  public CacheEntry createBuilder(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long decisionVersion,
    long hardTtlMs,
    long softTtlMs,
    long hardExpireAtMs,
    long softExpireAtMs,
    long normalHardTtlMs,
    long normalSoftTtlMs,
    KeyState keyState
  ) {
    return applyNormalTtl(
      CacheEntry.builder()
        .value(compressor.wrap(value))
        .dataVersion(dataVersion)
        .isVersionDegraded(isVersionDegraded)
        .decisionVersion(decisionVersion)
        .hardTtlMs(hardTtlMs)
        .softTtlMs(softTtlMs)
        .hardExpireAtMs(hardExpireAtMs)
        .softExpireAtMs(softExpireAtMs)
        .keyState(keyState)
        .build(),
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /**
   * Build a {@link CacheEntry} from raw fields without pre-computed
   * expire timestamps or decision metadata. Expire timestamps are
   * computed via {@link #applyTtl} after normal TTL is set.
   * <p>
   * This is the most compact overload, suitable for local promotions
   * where the caller has no Worker decision context and wants
   * timestamps computed automatically.
   *
   * @param value              the cached value
   * @param dataVersion        the data version for cross-instance sync
   * @param isVersionDegraded  whether the data version is degraded
   * @param decisionVersion    the Worker decision version (0 for local)
   * @param hardTtlMs          hard TTL duration in milliseconds
   * @param softTtlMs          soft TTL duration in milliseconds
   * @param normalHardTtlMs    normal (non-hot) hard TTL for state reversion
   * @param normalSoftTtlMs    normal (non-hot) soft TTL for state reversion
   * @param keyState           the initial key state
   * @return a new {@link CacheEntry} with expire timestamps computed
   */
  @Override
  public CacheEntry createBuilder(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long decisionVersion,
    long hardTtlMs,
    long softTtlMs,
    long normalHardTtlMs,
    long normalSoftTtlMs,
    KeyState keyState
  ) {
    return applyTtl(
      applyNormalTtl(
        CacheEntry.builder()
          .value(compressor.wrap(value))
          .dataVersion(dataVersion)
          .isVersionDegraded(isVersionDegraded)
          .decisionVersion(decisionVersion)
          .keyState(keyState)
          .build(),
        normalHardTtlMs,
        normalSoftTtlMs
      ),
      hardTtlMs,
      softTtlMs
    );
  }

  @Override
  public CacheEntry replaceEntryValue(CacheEntry entry, Object newValue) {
    return entry.withValue(compressor.wrap(newValue));
  }

  @Override
  public Object wrapValue(@Nullable Object rawValue) {
    return compressor.wrap(rawValue);
  }

  /**
   * Create a copy of the entry with the normal (non-hot) TTL values set,
   * leaving all other fields (hot TTLs, versions, state) untouched.
   * <p>
   * The normal TTLs ({@code normalHardTtlMs}, {@code normalSoftTtlMs})
   * are the baseline TTL values that the entry reverts to when its key
   * state transitions from HOT back to NORMAL. These are recorded at
   * entry creation and preserved across state transitions.
   *
   * @param original   the source {@link CacheEntry} to copy
   * @param hardTtlMs  normal hard TTL duration in milliseconds
   * @param softTtlMs  normal soft TTL duration in milliseconds
   * @return a new {@link CacheEntry} with the normal TTL fields updated
   */
  @Override
  public CacheEntry applyNormalTtl(CacheEntry original, long hardTtlMs, long softTtlMs) {
    return original.withNormalTtl(hardTtlMs, softTtlMs);
  }

  /**
   * Create a new {@link CacheEntry} with updated TTL fields, preserving all
   * other metadata from the supplied original entry.
   *
   * <p>Sets {@code hardTtlMs}, {@code softTtlMs},
   * {@code hardExpireAtMs} (via {@link #computeHardExpireAt}),
   * and {@code softExpireAtMs} (via {@link #computeSoftExpireAt}).
   *
   * @param original   an existing {@link CacheEntry} whose metadata should be preserved;
   *                   must not be null
   * @param hardTtlMs  hard TTL duration in milliseconds
   * @param softTtlMs  soft TTL duration in milliseconds
   * @return a new {@link CacheEntry} with the updated TTL timestamps,
   *         while keeping all version, state, and normal TTL fields unchanged
   */
  @Override
  public CacheEntry applyTtl(CacheEntry original, long hardTtlMs, long softTtlMs) {
    return original.withTtl(hardTtlMs, softTtlMs, computeHardExpireAt(hardTtlMs), computeSoftExpireAt(softTtlMs));
  }

  /**
   * Create a copy of the entry with only the hard TTL updated, leaving the
   * existing soft TTL and all version/state fields untouched.
   *
   * @param original   the source {@link CacheEntry} to copy
   * @param hardTtlMs  hard TTL duration in milliseconds
   * @return a new {@link CacheEntry} with the updated hard TTL and expiration
   */
  @Override
  public CacheEntry applyHardTtl(CacheEntry original, long hardTtlMs) {
    return original.withHardTtl(hardTtlMs, computeHardExpireAt(hardTtlMs));
  }

  /**
   * Create a copy of the entry with only the soft TTL updated, leaving the
   * existing hard TTL and all version/state fields untouched.
   *
   * @param original   the source {@link CacheEntry} to copy
   * @param softTtlMs  soft TTL duration in milliseconds
   * @return a new {@link CacheEntry} with the updated soft TTL and expiration
   */
  @Override
  public CacheEntry applySoftTtl(CacheEntry original, long softTtlMs) {
    return original.withSoftTtl(softTtlMs, computeSoftExpireAt(softTtlMs));
  }

  /**
   * Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp
   * using the configured default jitter ratio.
   * Propagates {@link Long#MAX_VALUE} unchanged — used to signal permanent entries
   * (pure logical expiry with no hard TTL eviction).
   */
  @Override
  public long toHardExpireTimestamp(long hardTtlMs) {
    return toHardExpireTimestamp(hardTtlMs, defaultTtlJitterRatio);
  }

  /**
   * Convert a TTL duration (ms) to an absolute epoch-ms expiration timestamp
   * using the given jitter ratio instead of the configured default.
   * Propagates {@link Long#MAX_VALUE} unchanged.
   *
   * @param hardTtlMs      the hard TTL duration in milliseconds
   * @param ttlJitterRatio the jitter ratio to apply (0.0–1.0)
   * @return absolute epoch-ms timestamp for hard expiry
   */
  @Override
  public long toHardExpireTimestamp(long hardTtlMs, double ttlJitterRatio) {
    if (hardTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long jitter = DelayUtil.computeTtlJitter(hardTtlMs, ttlJitterRatio);

    return hardTtlMs > 0 ? TimeSource.currentTimeMillis() + Math.max(1, hardTtlMs + jitter) : Long.MAX_VALUE;
  }

  /**
   * Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp.
   * Applies configurable jitter (default ±5%) to prevent cache stampedes.
   * Returns 0 if soft expire is disabled or the TTL is non-positive.
   *
   * @param softTtlMs the soft TTL duration in milliseconds
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if disabled
   */
  @Override
  public long toSoftExpireTimestamp(long softTtlMs) {
    return toSoftExpireTimestamp(softTtlMs, defaultTtlJitterRatio);
  }

  /**
   * Convert a soft TTL duration (ms) to an absolute epoch-ms expiration timestamp
   * using the given jitter ratio instead of the configured default.
   * Returns 0 if soft expire is disabled. Propagates {@link Long#MAX_VALUE} unchanged.
   *
   * @param softTtlMs      the soft TTL duration in milliseconds
   * @param ttlJitterRatio the jitter ratio to apply (0.0–1.0)
   * @return absolute epoch-ms timestamp for soft expiry, or 0 if disabled
   */
  @Override
  public long toSoftExpireTimestamp(long softTtlMs, double ttlJitterRatio) {
    if (!isSoftExpireEnabled() || softTtlMs <= 0) {
      return 0L;
    }
    if (softTtlMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long jitter = DelayUtil.computeTtlJitter(softTtlMs, ttlJitterRatio);

    return TimeSource.currentTimeMillis() + Math.max(1, softTtlMs + jitter);
  }

  /**
   * Check whether the given key's soft TTL has expired.
   *
   * @return {@code true} if the entry's soft TTL has expired or the entry is absent
   * @throws IllegalStateException if soft expire is disabled
   */
  @Override
  public boolean isSoftExpired(Object cacheEntry) {
    if (!isSoftExpireEnabled()) {
      throw new IllegalStateException("ExpireManager soft expire is disabled, isSoftExpired() should not be called");
    }
    if (cacheEntry instanceof CacheEntry ce) {
      long expireAt = ce.getSoftExpireAtMs();
      return expireAt <= 0 || expireAt < TimeSource.currentTimeMillis();
    }
    return true;
  }

  /**
   * Triggers an asynchronous background refresh for the given cache key if the
   * current entry has reached its soft expiry threshold. The caller (typically
   * {@link io.github.hyshmily.zeta.cache.HotKeyCache#getWithSoftExpire HotKeyCache.getWithSoftExpire}) has already returned the stale value to the
   * client, so this method executes entirely in the background without blocking
   * the caller.
   *
   * <p><b>Concurrency:</b> uses {@link ConcurrentHashMap#compute} on
   * {@code pendingRefreshes} to atomically decide whether a new refresh task
   * should be launched. This eliminates the earlier DCL (double-checked locking)
   * pattern that required manual clean-up of a placeholder
   * {@link CompletableFuture} when the limiter rejected the task or the executor
   * was saturated.
   *
   * @param cacheKey  the key whose value should be refreshed
   * @param reader    the data-source supplier
   * @param softTtlMs the soft TTL to set on the refreshed entry (milliseconds)
   */
  @Override
  @SuppressWarnings("java:S1181")
  public void triggerBackgroundRefresh(String cacheKey, Supplier<?> reader, long softTtlMs) {
    if (!isSoftExpireEnabled()) {
      return;
    }
    pendingRefreshes.compute(cacheKey, (k, existing) -> {
      // If there is already an in-flight refresh for this key, keep the
      // existing future and do nothing.
      if (existing != null && !existing.isDone()) {
        return existing;
      }

      // Try to acquire a permit from the global refresh limiter.
      if (!refreshLimiter.tryAcquire()) {
        log.debug("Refresh limiter blocked, skip background refresh: {}", cacheKey);
        // Returning null removes any previous entry, leaving no stale marker.
        return null;
      }

      try {
        // When the refresh completes (success, failure or timeout), update the
        // cache entry if the value is still applicable, and always release the
        // resources.
        return createRefreshTask(cacheKey, reader, softTtlMs);
      } catch (Throwable t) {
        // Unexpected failure before task creation (getIfPresent NPE, supplyAsync Error,
        // etc.) — release the semaphore so the refresh limiter does not permanently
        // lose a slot. The compute() will not store any entry, so the next read can retry.
        log.warn("Unexpected error during background refresh scheduling: {}", cacheKey, t);
        refreshLimiter.release();
        throw t;
      }
    });
  }

  /**
   * Creates a background async refresh task for the given cache key, with
   * soft-TTL semantics, version-guarded merge, and rate-limited concurrency.
   *
   * <p><b>Flow:</b>
   * <ol>
   *   <li>Snapshot the current entry's metadata (dataVersion, decisionVersion,
   *       decisionNodeId, decisionEpoch, keyState) at call time, so that
   *       {@link #applyRefreshTask} can detect superseding writes and
   *       preserve Worker decision state across the refresh boundary.</li>
   *   <li>Submit the supplier to a bounded executor; if the executor rejects,
   *       release the limiter permit immediately and return null so the next
   *       read can retry without waiting.</li>
   *   <li>On completion (success or failure): release the limiter permit,
   *       remove the in-flight marker from {@code pendingRefreshes}, and
   *       call {@link #applyRefreshTask} only when the value is non-null and
   *       no error occurred.</li>
   * </ol>
   *
   * <p><b>Version guard:</b> Inside {@code applyRefreshTask}, the stale
   * refresh result is discarded (keeping the existing entry) whenever the
   * current {@code dataVersion} is strictly newer than the snapshot taken at
   * creation time.
   *
   * <p><b>Limiter:</b> Controlled by the {@code refreshLimiter} semaphore
   * acquired in the calling method. One permit is held until the future
   * completes (success, error, timeout) and is released inside the
   * {@code whenComplete} callback.
   *
   * @param cacheKey  the key being refreshed
   * @param reader    the async value supplier (executed on the bounded pool)
   * @param softTtlMs soft-TTL in milliseconds applied to the resulting entry
   * @return the {@link CompletableFuture} representing the refresh, or
   *         {@code null} if the executor rejected the task
   */
  private CompletableFuture<?> createRefreshTask(String cacheKey, Supplier<?> reader, long softTtlMs) {
    // Snapshot the current entry metadata so we can detect superseding
    // writes and preserve Worker decision state across the refresh.
    snapshotEntry snap = snapshotEntry(caffeineCache.getIfPresent(cacheKey));

    // Build the async refresh task with timeout protection.
    CompletableFuture<?> task;
    try {
      task = CompletableFuture.supplyAsync(reader, executor).orTimeout(refreshTimeoutSeconds, TimeUnit.SECONDS);
    } catch (RejectedExecutionException e) {
      // Executor saturated – release the limiter permit and leave no
      // pending marker so the next read can retry immediately.
      refreshLimiter.release();
      log.warn("Background refresh rejected by executor (saturated), key={}", cacheKey);
      return null;
    }

    task.whenComplete((value, error) -> {
      try {
        if (error != null) {
          if (error instanceof TimeoutException) {
            log.warn("Background soft refresh timed out after {}s: {}", refreshTimeoutSeconds, cacheKey);
          } else {
            log.warn("Background soft refresh failed: {}", cacheKey, error);
          }
          return;
        }
        if (value != null) {
          applyRefreshTask(cacheKey, value, softTtlMs, snap);
        }
      } finally {
        // Always release the limiter permit and remove the in-flight
        // marker so that a future refresh can be scheduled.
        refreshLimiter.release();
        pendingRefreshes.remove(cacheKey);
      }
    });

    return task;
  }

  /**
   * Atomically applies a soft-refresh result to the cache, guarded by
   * a data-version check to prevent stale overwrites.
   *
   * <p><b>Logic:</b>
   * <ol>
   *   <li>Extract version metadata and keyState from the snapshot taken at
   *       refresh-creation time.</li>
   *   <li>Call {@link Caffeine compute} on the Caffeine map:
   *       <ul>
   *         <li><b>Entry exists:</b> if the current {@code dataVersion}
   *             exceeds the snapshot value, a newer write has arrived while
   *             the refresh was in-flight — discard the refresh result and
   *             return the existing entry unchanged.</li>
   *         <li><b>Entry exists, version not superseded:</b> replace the
   *             entry value and apply the soft TTL via
   *             {@link #applySoftTtl}({@link #replaceEntryValue}(entry, value), softTtlMs).</li>
   *         <li><b>Entry absent:</b> create a fresh entry preserving the
   *             snapshot's decision state (decisionVersion, decisionNodeId,
   *             decisionEpoch, keyState), with a default {@code dataVersion}
   *             of {@link ExpireManagerImpl} and the given
   *             soft TTL.</li>
   *       </ul>
   *   </li>
   * </ol>
   *
   * <p>This method is only called from
   * {@link #createRefreshTask}'s {@code whenComplete} callback on the
   * async-thread pool, so {@code compute} ensures safe atomic visibility
   * under concurrent reads and writes.
   *
   * @param cacheKey  the key whose value to update
   * @param value     the freshly-loaded value from the reader
   * @param softTtlMs soft-TTL in milliseconds applied to the resulting entry
   * @param snap      the entry metadata snapshot taken at refresh-creation time
   */
  private void applyRefreshTask(String cacheKey, Object value, long softTtlMs, snapshotEntry snap) {
    final long refreshStartDataVersion = snap.dataVersion();
    final long refreshStartDecisionVersion = snap.decisionVersion();
    final String refreshStartDecisionNodeId = snap.decisionNodeId();
    final long refreshStartDecisionEpoch = snap.decisionEpoch();
    final KeyState refreshStartKeyState = snap.keyState();

    caffeineCache
      .asMap()
      .compute(cacheKey, (key, existingEntry) ->
        Optional.ofNullable(existingEntry)
          .filter(CacheEntry.class::isInstance)
          .map(CacheEntry.class::cast)
          .map(entry -> {
            // Version guard: if a newer write has
            // arrived while we were refreshing,
            // discard the stale refresh result.
            if (entry.getDataVersion() > refreshStartDataVersion) {
              log.debug("Async refresh discarded: newer version exists: {}", cacheKey);
              return entry;
            }
            return entry.withValueAndSoftTtl(compressor.wrap(value), softTtlMs, computeSoftExpireAt(softTtlMs));
          })
          .orElseGet(() -> {
            long effectiveHardTtl = getEffectiveHardTtlMs();
            return createBuilder(
              value,
              VERSION_DEFAULT,
              false,
              refreshStartDecisionVersion,
              refreshStartDecisionNodeId,
              refreshStartDecisionEpoch,
              effectiveHardTtl,
              softTtlMs,
              toHardExpireTimestamp(effectiveHardTtl),
              computeSoftExpireAt(softTtlMs),
              getEffectiveHardTtlMs(),
              getEffectiveSoftTtlMs(),
              refreshStartKeyState
            );
          })
      );
  }

  /**
   * Extend both the hard and soft expiry for a cache entry.
   *
   * <p>If the caller passes {@code 0} for either TTL, the configured default
   * hot TTL ({@link #getEffectiveHotHardTtlMs()} / {@link #getEffectiveHotSoftTtlMs()})
   * is used.
   *
   * @param cacheKey  the key whose expiry should be extended
   * @param hardTtlMs new hard TTL in milliseconds; {@code 0} to use the
   *                  configured hot hard TTL
   * @param softTtlMs new soft TTL in milliseconds; {@code 0} to use the
   *                  configured hot soft TTL
   */
  @Override
  public void extendExpiry(String cacheKey, long hardTtlMs, long softTtlMs) {
    long hard = resolveEffectiveHotHard(hardTtlMs);
    long soft = resolveEffectiveHotSoft(softTtlMs);
    extendExpiry(cacheKey, hard, soft, true, true);
  }

  /**
   * Extend only the hard expiry for a cache entry, leaving the soft expiry
   * unchanged. Useful when promoting a NORMAL or COOL entry to HOT — the
   * hard TTL must be lengthened to the hot‑key value, but the existing soft
   * expiry (if any) should be preserved because it reflects a more recent
   * refresh cycle.
   *
   * <p>If the caller passes {@code 0} the configured default hot hard TTL
   * ({@link #getEffectiveHotHardTtlMs()}) is used.
   *
   * @param cacheKey  the key whose hard expiry should be extended
   * @param hardTtlMs new hard TTL in milliseconds; {@code 0} to use the
   *                  configured hot hard TTL
   */
  @Override
  public void extendHardExpiry(String cacheKey, long hardTtlMs) {
    long hard = resolveEffectiveHotHard(hardTtlMs);
    extendExpiry(cacheKey, hard, 0, true, false);
  }

  /**
   * Extend only the soft expiry for a cache entry, leaving the hard expiry
   * unchanged. Useful when a background refresh has completed and the caller
   * wants to reset the soft TTL without affecting the hard TTL.
   *
   * <p>If the caller passes {@code 0} the configured default hot soft TTL
   * ({@link #getEffectiveHotSoftTtlMs()}) is used.
   *
   * @param cacheKey  the key whose soft expiry should be extended
   * @param softTtlMs new soft TTL in milliseconds; {@code 0} to use the
   *                  configured hot soft TTL
   */
  @Override
  public void extendSoftExpiry(String cacheKey, long softTtlMs) {
    long soft = resolveEffectiveHotSoft(softTtlMs);
    extendExpiry(cacheKey, 0, soft, false, true);
  }

  /**
   * Atomically update the expiry timestamps of an existing cache entry.
   *
   * @param cacheKey    the key whose expiry should be extended
   * @param hardTtlMs   new hard TTL in milliseconds (ignored if {@code updateHard} is false)
   * @param softTtlMs   new soft TTL in milliseconds (ignored if {@code updateSoft} is false)
   * @param updateHard  whether to update the hard expiry timestamp
   * @param updateSoft  whether to update the soft expiry timestamp
   */
  private void extendExpiry(String cacheKey, long hardTtlMs, long softTtlMs, boolean updateHard, boolean updateSoft) {
    caffeineCache
      .asMap()
      .computeIfPresent(cacheKey, (k, existing) -> {
        if (existing instanceof CacheEntry entry) {
          if (updateHard) {
            entry = applyHardTtl(entry, resolveEffectiveHardTtl(hardTtlMs));
          }
          if (updateSoft) {
            entry = applySoftTtl(entry, resolveEffectiveSoftTtl(softTtlMs));
          }
          return entry;
        }
        return existing;
      });
  }
}
