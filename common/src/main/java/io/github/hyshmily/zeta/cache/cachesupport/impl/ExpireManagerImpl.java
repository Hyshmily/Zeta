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
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.TimeSource;
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

  /**
   * Cluster health view used for the Decision-Validity check
   * (ADR-0035). {@code null} disables demotion (test doubles, consumers
   * without a health view).
   */
  private final HealthView healthView;

  @SuppressWarnings("all")
  private static final long refreshTimeoutSeconds = 30;

  /** Lease-on-failure (ADR-0036): TTL halving divisor applied to the remaining budget. */
  @SuppressWarnings("all")
  private static final long LEASE_DIVISOR = 1;

  /** Lease-on-failure (ADR-0036): minimum lease duration in milliseconds — the decay floor. */
  @SuppressWarnings("all")
  private static final long LEASE_MIN_TTL_MS = 120_000;

  @SuppressWarnings("all")
  private record snapshotEntry(
    long dataVersion,
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    KeyState keyState,
    long hardExpireAtMs
  ) {
    static final snapshotEntry DEFAULT = new snapshotEntry(
      VERSION_DEFAULT,
      VERSION_DEFAULT,
      null,
      0L,
      KeyState.NORMAL,
      Long.MAX_VALUE
    );
  }

  private static snapshotEntry snapshotEntry(Object raw) {
    if (raw instanceof CacheEntry entry) {
      return new snapshotEntry(
        entry.getDataVersion(),
        entry.getDecisionVersion(),
        entry.getDecisionNodeId(),
        entry.getDecisionEpoch(),
        entry.getKeyState(),
        entry.getHardExpireAtMs()
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
    this(caffeineCache, executor, ttlConfig, refreshMaxPools, compressor, null);
  }

  /**
   * Creates a ExpireManagerImpl with the given Caffeine cache, executor, TTL config,
   * compressor, and cluster health view.
   *
   * <p>The {@link HealthView} powers the Decision-Validity demotion (ADR-0035):
   * Worker-sourced HOT entries whose issuing Worker incarnation is dead or
   * restarted are reverted to the NORMAL lifecycle on the read path. A
   * {@code null} health view disables demotion.
   *
   * @param caffeineCache   the underlying L1 Caffeine cache
   * @param executor        async executor for background refresh
   * @param ttlConfig       TTL configuration (normal and hot-key variants)
   * @param refreshMaxPools maximum concurrent background refreshes (capped at 100)
   * @param compressor      compressor for L1 cache values
   * @param healthView      cluster health view for the Decision-Validity check, or {@code null}
   */
  public ExpireManagerImpl(
    Cache<String, Object> caffeineCache,
    Executor executor,
    ZetaProperties ttlConfig,
    int refreshMaxPools,
    CacheCompressor compressor,
    HealthView healthView
  ) {
    this.caffeineCache = caffeineCache;
    this.executor = executor;
    this.ttlConfig = ttlConfig;
    this.compressor = compressor;
    this.healthView = healthView;
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
    this(caffeineCache, executor, ttlConfig, refreshMaxPools, defaultTtlJitterRatio, compressor, null);
  }

  /**
   * Create a ExpireManagerImpl with explicit jitter ratio and health view (for testing).
   */
  ExpireManagerImpl(
    Cache<String, Object> caffeineCache,
    Executor executor,
    ZetaProperties ttlConfig,
    int refreshMaxPools,
    double defaultTtlJitterRatio,
    CacheCompressor compressor,
    HealthView healthView
  ) {
    this.caffeineCache = caffeineCache;
    this.executor = executor;
    this.ttlConfig = ttlConfig;
    this.compressor = compressor;
    this.healthView = healthView;
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
   * Decision-Validity demotion (ADR-0035): revert a Worker-sourced HOT entry
   * to the NORMAL lifecycle when its issuing Worker incarnation is no longer
   * authoritative.
   *
   * <p>An entry's Worker decision is valid only while the issuing incarnation
   * is alive and its epoch is unchanged: the Worker is the sole authority for
   * cooling the key down, so a dead or restarted Worker leaves the entry with
   * HOT TTLs but no one to revoke them. On the first read after invalidity is
   * detected, the entry is rewritten in place — value preserved and still
   * served, TTLs reverted to the normal baseline, decision stamp cleared —
   * so it converges at the normal hard TTL and the local TopK re-decides on
   * the next reload.
   *
   * <p>Runs on the read path; the predicate is a cheap O(1) health-view lookup
   * and only fires for entries carrying a decision stamp. The predicate is
   * re-verified inside the atomic {@code compute}, so a concurrent fresh
   * broadcast (recovered Worker) is never clobbered. With no health view
   * configured, demotion is disabled (no-op, {@code false}).
   *
   * @param cacheKey the cache key
   * @param raw      the raw value from the Caffeine cache
   * @return {@code true} if the entry was demoted
   */
  @Override
  public boolean demoteIfDecisionInvalid(String cacheKey, Object raw) {
    HealthView view = healthView;
    if (view == null || !(raw instanceof CacheEntry entry)) {
      return false;
    }

    String decisionNodeId = entry.getDecisionNodeId();
    if (
      decisionNodeId == null ||
      entry.getKeyState() != KeyState.HOT ||
      decisionStillValid(view, decisionNodeId, entry.getDecisionEpoch())
    ) {
      return false;
    }

    boolean[] demoted = new boolean[1];
    caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) -> {
        if (!(existing instanceof CacheEntry current)) {
          return existing;
        }
        // Re-verify inside the atomic write: the entry may have been
        // re-stamped by a fresh broadcast (recovered Worker) or locally
        // re-promoted since the outer check. The pure function judges the
        // current entry independently — any now-invalid stamp is demoted.
        CacheEntry corrected = demoteIfDecisionInvalidInPlace(current);
        if (corrected != null) {
          demoted[0] = true;
          return corrected;
        }
        return existing;
      });
    if (demoted[0]) {
      log.debug(
        "Decision-Validity demotion: key={} nodeId={} epoch={} reverted to NORMAL lifecycle",
        cacheKey,
        decisionNodeId,
        entry.getDecisionEpoch()
      );
    }
    return demoted[0];
  }

  /**
   * Pure Decision-Validity demotion: judge the entry and, when its issuing
   * Worker incarnation is dead or restarted, return it rewritten to NORMAL
   * (value preserved, normal TTLs, decision stamp cleared). {@code null} when
   * no demotion applies. Side-effect-free — the caller provides atomicity.
   *
   * @param entry the Worker-sourced cache entry to inspect
   * @return the demoted entry, or {@code null} if no demotion applies
   */
  @Override
  @Nullable
  public CacheEntry demoteIfDecisionInvalidInPlace(CacheEntry entry) {
    HealthView view = healthView;
    if (view == null) {
      return null;
    }

    String decisionNodeId = entry.getDecisionNodeId();
    if (
      decisionNodeId == null ||
      entry.getKeyState() != KeyState.HOT ||
      decisionStillValid(view, decisionNodeId, entry.getDecisionEpoch())
    ) {
      return null;
    }

    long normalHardTtlMs = ttlPolicy.resolveEffectiveHardTtl(entry.getNormalHardTtlMs());
    long normalSoftTtlMs = ttlPolicy.resolveEffectiveSoftTtl(entry.getNormalSoftTtlMs());
    return entry
      .withTtlAndKeyState(
        normalHardTtlMs,
        normalSoftTtlMs,
        ttlPolicy.toHardExpireTimestamp(normalHardTtlMs),
        ttlPolicy.toSoftExpireTimestamp(normalSoftTtlMs),
        KeyState.NORMAL
      )
      .withDecisionVersion(VERSION_DEFAULT)
      .withDecisionNodeId(null)
      .withDecisionEpoch(0L);
  }

  /**
   * Whether a Worker decision stamped with the given nodeId/epoch is still
   * authoritative: the Worker has a health record, is alive (same freshness
   * judgment as the ring/report path), and its current epoch matches.
   *
   * @param view      the cluster health view (never {@code null} here)
   * @param nodeId    the decision's issuing Worker node ID
   * @param epoch     the decision's issuing epoch
   * @return {@code true} if the decision is still valid
   */
  private boolean decisionStillValid(HealthView view, String nodeId, long epoch) {
    return view.isAlive(nodeId) && view.epochOf(nodeId) == epoch;
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
   *       no error occurred. On failure, {@link #leaseOnFailure} extends the
   *       existing entry's expire timestamps instead (ADR-0036) — the stale
   *       value stays servable and the next soft-expiry read re-arms the
   *       refresh.</li>
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
          // ADR-0036: keep the stale entry servable instead of letting it run
          // into the hard TTL and stampede the failing source on every read.
          leaseOnFailure(cacheKey, snap);
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
   * Lease-on-Failure (ADR-0036): keep a stale entry servable after its
   * background refresh failed, by extending its expire timestamps.
   *
   * <p><b>Semantics:</b> the lease is a <i>provisional keep-alive</i>, not an
   * authoritative state transition — the value, all metadata (dataVersion,
   * decision stamps, keyState) and the {@code hardTtlMs}/{@code softTtlMs}
   * duration fields are preserved verbatim; only the expire timestamps move.
   * The hard timestamp extends to {@code now + max(remaining/2, 120s)} — the
   * halved remaining budget gives soft exponential decay to the 120s floor,
   * so repeated failure is graceful degradation rather than an entry clearing.
   * The soft timestamp extends to the lease midpoint ({@code now + lease/2}):
   * the entry is soft-expired during the second half of every lease while
   * still hard-valid, which is the read-triggered retry window — the next
   * read re-arms the refresh. {@code soft == hard} would close the window:
   * the read path checks hard expiry before soft expiry, so the entry would
   * die at lease end without ever retrying.
   *
   * <p><b>Identity guard:</b> mirrors {@link #applyRefreshTask}'s version
   * guard — the lease applies only to the same logical entry that failed the
   * refresh ({@code dataVersion} and {@code hardExpireAtMs} both equal to the
   * creation-time snapshot). A write, broadcast, or promotion that landed
   * in-flight already granted fresh TTLs; leasing it would shorten them. An
   * absent (evicted) entry is never recreated — the failure path carries no
   * new value. Permanent entries ({@code hardExpireAtMs == Long.MAX_VALUE})
   * are never leased: there is nothing to extend and the halving arithmetic
   * would overflow.
   *
   * @param cacheKey the key whose entry to lease
   * @param snap     the entry metadata snapshot taken at refresh-creation time
   */
  private void leaseOnFailure(String cacheKey, snapshotEntry snap) {
    caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) -> {
        if (
          !(existing instanceof CacheEntry entry) ||
          entry.getDataVersion() != snap.dataVersion() ||
          entry.getHardExpireAtMs() != snap.hardExpireAtMs() ||
          entry.getHardExpireAtMs() == Long.MAX_VALUE
        ) {
          return existing;
        }

        long now = TimeSource.currentTimeMillis();
        long remainingMs = entry.getHardExpireAtMs() - now;
        long leaseTtlMs = Math.max(LEASE_MIN_TTL_MS, Math.max(1, remainingMs) >> LEASE_DIVISOR);
        long leaseExpireAtMs = now + leaseTtlMs;
        // The soft timestamp sits at the midpoint of the lease: the entry spends
        // the second half of every lease soft-expired while still hard-valid, so
        // the next read re-arms the refresh (retry window). soft == hard would
        // close the window entirely — the read path checks hard expiry before
        // soft expiry, so the entry would die at lease end without ever retrying.
        // NB: the halving applies to the lease DURATION only — shifting the full
        // epoch timestamp (now + lease) would land it decades in the past.
        long leaseSoftExpireAtMs = now + (leaseTtlMs >> LEASE_DIVISOR);
        log.debug(
          "Lease-on-failure: extending stale entry for key={} by {}ms (floor {}ms)",
          cacheKey,
          leaseTtlMs,
          LEASE_MIN_TTL_MS
        );
        return entry.toBuilder().hardExpireAtMs(leaseExpireAtMs).softExpireAtMs(leaseSoftExpireAtMs).build();
      });
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
}
