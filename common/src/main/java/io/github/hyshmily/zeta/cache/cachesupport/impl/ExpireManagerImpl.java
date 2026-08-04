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
import io.github.hyshmily.zeta.cache.cachesupport.TtlPolicy;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.VersionedValue;
import io.github.hyshmily.zeta.util.version.VersionGuard;
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
  /** Pure TTL/expiry policy — all stateless lifecycle arithmetic lives here. */
  private final TtlPolicy ttlPolicy;
  /** Semaphore limiting concurrent background refresh operations. */
  private Semaphore refreshLimiter;
  /** Per-key dedup for background refreshes — prevents concurrent refresh for the same key. */
  private final ConcurrentHashMap<String, CompletableFuture<?>> pendingRefreshes = new ConcurrentHashMap<>();
  /** Compressor for L1 cache values. */
  private final CacheCompressor compressor;

  /** Jitter ratio applied to TTLs to prevent cache stampedes (from config, default 0.05 = ±5%). */
  private final double defaultTtlJitterRatio;

  @SuppressWarnings("all")
  private static final long refreshTimeoutSeconds = 30;

  @SuppressWarnings("all")
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
    initRefreshLimiter(refreshMaxPools);
    this.defaultTtlJitterRatio = ttlConfig.getTtlJitterRatio();
    this.ttlPolicy = new TtlPolicy(ttlConfig, this.defaultTtlJitterRatio);
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
    initRefreshLimiter(refreshMaxPools);
    this.defaultTtlJitterRatio = defaultTtlJitterRatio;
    this.ttlPolicy = new TtlPolicy(ttlConfig, defaultTtlJitterRatio);
  }

  private void initRefreshLimiter(int refreshMaxPools) {
    int effectiveRefreshMaxPools = refreshMaxPools > 0 ? refreshMaxPools : 100;
    this.refreshLimiter = new Semaphore(effectiveRefreshMaxPools);
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
    if (raw instanceof CacheEntry ce && ttlPolicy.isLogicallyExpired(ce)) {
      caffeineCache.invalidate(cacheKey);
      log.debug("Cache entry logically expired during processing, reloading: {}", cacheKey);
      return true;
    }
    return false;
  }

  /**
   * Build a {@link CacheEntry} from resolved fields.
   *
   * <p>The {@code decision} stamp is present only for Worker-sourced entries
   * (HOT/COOL broadcasts); {@code expiryAt} is supplied only when the caller
   * has pre-computed timestamps — otherwise they are computed via
   * {@link TtlPolicy#applyTtl} from the TTL spec. Normal TTL is applied via
   * {@link TtlPolicy#applyNormalTtl} after construction.
   *
   * @param value     the cached value
   * @param version   the data version stamp (sync ordering + degraded flag)
   * @param decision  the Worker decision stamp, or {@code null} for local entries
   * @param ttl       the current and normal TTL durations
   * @param expiryAt  pre-computed expire timestamps, or {@code null} to compute
   * @param keyState  the initial key state (NORMAL, HOT, COOL)
   * @return a new {@link CacheEntry} with all fields set
   */
  @Override
  public CacheEntry createBuilder(
    Object value,
    ExpireManager.VersionStamp version,
    ExpireManager.DecisionStamp decision,
    ExpireManager.TtlSpec ttl,
    ExpireManager.ExpiryAt expiryAt,
    KeyState keyState
  ) {
    CacheEntry built = CacheEntry.builder()
      .value(compressor.wrap(value))
      .dataVersion(version.dataVersion())
      .isVersionDegraded(version.isVersionDegraded())
      .decisionVersion(decision != null ? decision.decisionVersion() : VERSION_DEFAULT)
      .decisionNodeId(decision != null ? decision.decisionNodeId() : null)
      .decisionEpoch(decision != null ? decision.decisionEpoch() : 0L)
      .hardTtlMs(ttl.hardTtlMs())
      .softTtlMs(ttl.softTtlMs())
      .keyState(keyState)
      .build();
    CacheEntry withNormal = ttlPolicy.applyNormalTtl(built, ttl.normalHardTtlMs(), ttl.normalSoftTtlMs());
    return expiryAt != null
      ? withNormal.withTtl(ttl.hardTtlMs(), ttl.softTtlMs(), expiryAt.hardExpireAtMs(), expiryAt.softExpireAtMs())
      : ttlPolicy.applyTtl(withNormal, ttl.hardTtlMs(), ttl.softTtlMs());
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
   * The pure TTL and expiry policy backing this manager.
   *
   * @return the TTL policy; never null
   */
  @Override
  public TtlPolicy ttlPolicy() {
    return ttlPolicy;
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
        if (value instanceof VersionedValue vv && vv.value() != null) {
          applyRefreshTask(cacheKey, vv, softTtlMs, snap);
        }
      } finally {
        // Always release the limiter permit and remove the in-flight
        // marker so that a future refresh can be scheduled. Conditional
        // removal by object identity: a concurrent triggerBackgroundRefresh
        // may have replaced the map entry with a newer task while this
        // callback ran — unconditional removal would delete that newer
        // marker and allow a third concurrent refresh (ABA).
        refreshLimiter.release();
        pendingRefreshes.remove(cacheKey, task);
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
   *         <li><b>Stamped refresh (ADR-0033):</b> the result carries the
   *             {@code dataVersion} probed from Redis after the value read.
   *             Acceptance is decided by the shared 4-case comparison
   *             ({@link VersionGuard#shouldSkipForSync}): a degraded entry is
   *             never skipped (a normal probe overwrites degraded — case 4, so
   *             a recovered Redis heals degraded entries), and both-normal
   *             applies only when the entry is older than the probe. A numeric
   *             comparison would be wrong here — a degraded (negative) entry
   *             must not discard a normal probe just because the numbers say
   *             so. Discarding keeps the entry unchanged, so the stamped
   *             version can never regress L1.</li>
   *         <li><b>Unstamped refresh (fail-open):</b> the probe was withheld.
   *             Legacy L1-internal guards apply: discard if the entry is
   *             degraded, or if its current {@code dataVersion} exceeds the
   *             snapshot value (a newer write arrived while the refresh was
   *             in-flight).</li>
   *         <li><b>Entry exists, version not superseded:</b> replace the
   *             entry value, stamp the probed version (when present), and
   *             apply the soft TTL.</li>
   *         <li><b>Entry absent:</b> create a fresh entry preserving the
   *             snapshot's decision state (decisionVersion, decisionNodeId,
   *             decisionEpoch, keyState), stamped with the probed version when
   *             present, and the given soft TTL.</li>
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
   * @param vv        the freshly-loaded value paired with the probed
   *                  {@code dataVersion} (ADR-0033); unstamped when the probe
   *                  failed
   * @param softTtlMs soft-TTL in milliseconds applied to the resulting entry
   * @param snap      the entry metadata snapshot taken at refresh-creation time
   */
  @SuppressWarnings("all")
  private void applyRefreshTask(String cacheKey, VersionedValue vv, long softTtlMs, snapshotEntry snap) {
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
            if (vv.stamped()) {
              // ADR-0033 4-case guard against the probed version: applying the
              // refresh would stamp a version L1 already surpassed (or regress
              // a degraded entry's semantics), so discard — the existing entry
              // wins. See the method Javadoc for the case-by-case rationale.
              if (VersionGuard.shouldSkipForSync(entry, vv.dataVersion(), false)) {
                log.debug(
                  "Async refresh discarded: entry version {} not below probe {} for key={}",
                  entry.getDataVersion(),
                  vv.dataVersion(),
                  cacheKey
                );
                return entry;
              }
              CacheEntry refreshed = entry
                .withValueAndSoftTtl(compressor.wrap(vv.value()), softTtlMs, ttlPolicy.computeSoftExpireAt(softTtlMs))
                .withDataVersion(vv.dataVersion())
                .withIsVersionDegraded(false);
              return entry.getKeyState() == KeyState.COOL ? refreshed.withKeyState(KeyState.NORMAL) : refreshed;
            }
            // Degraded version: a Redis-outage write occurred during refresh.
            // Discard the refresh result unconditionally — it is older than
            // the degraded write and must not supersede it.
            if (entry.isVersionDegraded()) {
              log.debug("Async refresh discarded: degraded version write superseded, key={}", cacheKey);
              return entry;
            }
            // Version guard: if a newer write has
            // arrived while we were refreshing,
            // discard the stale refresh result.
            if (entry.getDataVersion() > refreshStartDataVersion) {
              log.debug("Async refresh discarded: newer version exists: {}", cacheKey);
              return entry;
            }
            // A successful refresh of a COOL entry downgrades it to NORMAL:
            // the local read that triggered the refresh shows the key is
            // still active locally, so it returns to the ordinary local
            // lifecycle (promotion-eligible, hard-TTL reload). Decision
            // metadata is preserved, so a later Worker broadcast still
            // overrides via decisionVersion. NORMAL/HOT entries keep state.
            CacheEntry refreshed = entry.withValueAndSoftTtl(
              compressor.wrap(vv.value()),
              softTtlMs,
              ttlPolicy.computeSoftExpireAt(softTtlMs)
            );
            return entry.getKeyState() == KeyState.COOL ? refreshed.withKeyState(KeyState.NORMAL) : refreshed;
          })
          .orElseGet(() -> {
            long effectiveHardTtl = ttlPolicy.getEffectiveHardTtlMs();
            // ADR-0033: stamp the probed version when present; fail-open
            // rebuilds with VERSION_DEFAULT.
            long stampedVersion = vv.stamped() ? vv.dataVersion() : VERSION_DEFAULT;
            return createBuilder(
              vv.value(),
              new ExpireManager.VersionStamp(stampedVersion, false),
              refreshStartDecisionNodeId != null
                ? new ExpireManager.DecisionStamp(
                    refreshStartDecisionVersion,
                    refreshStartDecisionNodeId,
                    refreshStartDecisionEpoch
                  )
                : null,
              new ExpireManager.TtlSpec(
                effectiveHardTtl,
                softTtlMs,
                ttlPolicy.getEffectiveHardTtlMs(),
                ttlPolicy.getEffectiveSoftTtlMs()
              ),
              new ExpireManager.ExpiryAt(
                ttlPolicy.toHardExpireTimestamp(effectiveHardTtl),
                ttlPolicy.computeSoftExpireAt(softTtlMs)
              ),
              // Same COOL → NORMAL downgrade as the existing-entry branch: a
              // successful refresh rebuilds a COOL entry as NORMAL.
              refreshStartKeyState == KeyState.COOL ? KeyState.NORMAL : refreshStartKeyState
            );
          })
      );
  }

  /**
   * Extend both the hard and soft expiry for a cache entry.
   *
   * <p>If the caller passes {@code 0} for either TTL, the configured default
   * hot TTL ({@link TtlPolicy#getEffectiveHotHardTtlMs()} / {@link TtlPolicy#getEffectiveHotSoftTtlMs()})
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
    long hard = ttlPolicy.resolveEffectiveHotHard(hardTtlMs);
    long soft = ttlPolicy.resolveEffectiveHotSoft(softTtlMs);
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
   * ({@link TtlPolicy#getEffectiveHotHardTtlMs()}) is used.
   *
   * @param cacheKey  the key whose hard expiry should be extended
   * @param hardTtlMs new hard TTL in milliseconds; {@code 0} to use the
   *                  configured hot hard TTL
   */
  @Override
  public void extendHardExpiry(String cacheKey, long hardTtlMs) {
    long hard = ttlPolicy.resolveEffectiveHotHard(hardTtlMs);
    extendExpiry(cacheKey, hard, 0, true, false);
  }

  /**
   * Extend only the soft expiry for a cache entry, leaving the hard expiry
   * unchanged. Useful when a background refresh has completed and the caller
   * wants to reset the soft TTL without affecting the hard TTL.
   *
   * <p>If the caller passes {@code 0} the configured default hot soft TTL
   * ({@link TtlPolicy#getEffectiveHotSoftTtlMs()}) is used.
   *
   * @param cacheKey  the key whose soft expiry should be extended
   * @param softTtlMs new soft TTL in milliseconds; {@code 0} to use the
   *                  configured hot soft TTL
   */
  @Override
  public void extendSoftExpiry(String cacheKey, long softTtlMs) {
    long soft = ttlPolicy.resolveEffectiveHotSoft(softTtlMs);
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
            entry = ttlPolicy.applyHardTtl(entry, ttlPolicy.resolveEffectiveHardTtl(hardTtlMs));
          }
          if (updateSoft) {
            entry = ttlPolicy.applySoftTtl(entry, ttlPolicy.resolveEffectiveSoftTtl(softTtlMs));
          }
          return entry;
        }
        return existing;
      });
  }
}
