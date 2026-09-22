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
package io.github.hyshmily.zeta.sync.worker;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.LogThrottle;
import io.github.hyshmily.zeta.util.ratelimit.impl.SreRateLimiterImpl;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation of {@link WorkerDecisionHandler} that performs loader-backed
 * HOT promotion and COOL downgrade with SRE rate limiting, version guarding, and
 * {@link WorkerDecisionHook} dispatch.
 */
@Slf4j
@Internal
public class DefaultWorkerDecisionHandler implements WorkerDecisionHandler {

  /** Local Caffeine L1 cache — target for HOT promotion and COOL downgrade operations.
   * Accessed atomically via {@code asMap().compute()} for thread-safe updates. */
  private final Cache<String, Object> caffeineCache;

  /** Loads the authoritative value for a key: a registered prefix routes to the
   * application's {@code CacheLoader}, an unregistered key falls back to the Redis
   * value channel. Used during HOT promotion to fetch the authoritative value
   * before writing to L1. */
  private final CacheLoader<Object> clusterLoader;

  /** Computes hard and soft expiry timestamps for HOT-promoted and default-TTL entries. */
  private final ExpireManager expireManager;

  /** Optional SRE adaptive rate limiter for HOT decision processing.
   * When non-null, HOT promotions are probabilistically dropped during overload.
   * {@code null} disables rate limiting. */
  private final SreRateLimiterImpl sreRateLimiter;

  /**
   * Optional version controller for reading the current {@code dataVersion}
   * from Redis when creating new L1 entries. When null (or when the version
   * cannot be read), the entry is created with {@code dataVersion=0}, which
   * may allow stale invalidation messages to evict the entry (see issue 4.11).
   */
  @Nullable
  private final VersionController versionController;

  /** Optional lifecycle hooks for Worker decision events. Never null. */
  private final List<WorkerDecisionHook> workerHooks;

  /** Fallback hard TTL (ms) for COOL entries when no normal TTL is configured on the existing entry. */
  private static final long COOL_DEFAULT_PROTECTION_HARDTTL_TIME = 120_000;
  /** Fallback soft TTL (ms) for COOL entries when no normal TTL is configured on the existing entry. */
  private static final long COOL_DEFAULT_PROTECTION_SOFTTTL_TIME = 60_000;
  /** Jitter ratio for COOL fallback hard TTL (±20%). */
  private static final double COOL_DEFAULT_PROTECTION_HARDTTL_TIME_RATIO = 0.2;
  /** Jitter ratio for COOL fallback soft TTL (±20%). */
  private static final double COOL_DEFAULT_PROTECTION_SOFTTTL_TIME_RATIO = 0.2;

  /**
   * Counts load failures and admits the full WARN once per
   * {@value LogThrottle#DEFAULT_WINDOW_MS}ms window (ADR-0037): the
   * window-opening WARN reports how many similar failures were suppressed
   * since the previous WARN; callers inside the window log at DEBUG with the
   * running tally. The admission decision, the tally and the monotonic clock
   * live in {@link LogThrottle.Counting} — the shared log-throttling utility.
   */
  private final LogThrottle.Counting loadFailureThrottle = new LogThrottle.Counting();

  public DefaultWorkerDecisionHandler(
    Cache<String, Object> caffeineCache,
    CacheLoader<Object> clusterLoader,
    ExpireManager expireManager,
    @Nullable SreRateLimiterImpl sreRateLimiter,
    @Nullable VersionController versionController,
    List<WorkerDecisionHook> workerHooks
  ) {
    this.caffeineCache = caffeineCache;
    this.clusterLoader = clusterLoader;
    this.expireManager = expireManager;
    this.sreRateLimiter = sreRateLimiter;
    this.versionController = versionController;
    this.workerHooks = workerHooks != null ? workerHooks : Collections.emptyList();
  }

  /**
   * Promotes a cache key to {@link KeyState#HOT} with extended TTL and active soft
   * expiration, following a Worker HOT decision.
   *
   * <p><b>Promotion flow:</b>
   * <ol>
   *   <li><b>SRE gate:</b> If the rate limiter drops this request, the promotion is
   *       skipped entirely — backpressure from downstream saturation.</li>
   *   <li><b>DCL check 1:</b> Fast-path version guard ({@link VersionGuard#shouldSkipForWorker})
   *       against the existing L1 entry. If a newer decision is already present, this is a no-op.</li>
   *   <li><b>Redis fetch:</b> Loads the authoritative value from Redis. If Redis is
   *       unavailable, falls back to the existing L1 entry value (any entry —
   *       degraded or not; {@code NullValue} sentinels excluded).</li>
   *   <li><b>DCL check 2:</b> Second version guard inside the atomic {@code compute} to
   *       prevent overwriting a newer decision that arrived during the Redis fetch.</li>
   *   <li><b>Write:</b> Replaces the entry with a new {@link CacheEntry} in
   *       {@code KeyState.HOT}, preserving data-version fields, setting HOT-specific
   *       TTLs, and recording the Worker's {@code decisionVersion}.</li>
   * </ol>
   *
   * <p>If no value is available from Redis <em>and</em> no usable entry exists in L1,
   * the promotion is aborted — there is nothing to cache.
   *
   * @param wm the Worker message containing the HOT decision; must not be null
   */
  @Override
  @SuppressWarnings("all")
  public void handleHot(WorkerMessage wm) {
    String cacheKey = wm.cacheKey();
    // SRE rate limiter gate — skip HOT promotion when the system is overloaded
    if (sreRateLimiter != null && !sreRateLimiter.tryAcquire()) {
      log.debug("SRE throttled HOT promotion for key={}", cacheKey);
      fireOnHotSkipped(cacheKey, wm, HotSkipReason.SRE_THROTTLED);
      return;
    }

    // DCL first check – cheap, outside the compute lock. The snapshot doubles as the
    // "does an entry already exist" input for the dataVersion probe below, saving the
    // extra getIfPresent the old code paid after the Redis load; the probe decision may
    // then be one Redis-load stale in a rare concurrent create/evict race, which the
    // version-floor cache (ADR-0022) already bounds.
    Object existingSnapshot = caffeineCache.getIfPresent(cacheKey);
    if (
      existingSnapshot instanceof CacheEntry snapshotEntry &&
      VersionGuard.shouldSkipForWorker(snapshotEntry, wm.decisionVersion(), wm.nodeId(), wm.epoch())
    ) {
      log.debug("handleHot: HotKey already up-to-date in L1: {}", cacheKey);
      fireOnHotSkipped(cacheKey, wm, HotSkipReason.VERSION_STALE);
      return;
    }

    // A clean Redis GET miss (key does not exist) is NOT a failure: recording
    // onFailed() for it would deflate the success ratio and make the SRE
    // limiter probabilistically drop legitimate HOT promotions while Redis is
    // perfectly healthy. Only a loader exception (outage, timeout) counts —
    // the VERSION_STALE skip path records nothing either, so this matches it.
    AtomicBoolean loadFailedFlag = new AtomicBoolean(false);
    Object value = loadAuthoritative(wm, loadFailedFlag);
    boolean fallbackValuePreWrapped = false;
    if (value == null) {
      // Redis outage fallback: any existing L1 entry value is promotable —
      // get() serves these values regardless of the degraded flag, so the
      // HOT promotion must not be stricter than the read path (ADR-0008).
      // NullValue sentinels are excluded: a 10s null marker does not deserve
      // the 1h HOT TTL. Read fresh: a stale snapshot could resurrect a value
      // a concurrent INVALIDATE just removed. The stored value is ALREADY in
      // wrapped form (the zeta envelope — flag byte + LZ4 payload for
      // compressible values): re-wrapping it would double-compress and
      // corrupt the format, so it bypasses the wrap below.
      Object fallback = caffeineCache.getIfPresent(cacheKey);
      if (fallback instanceof CacheEntry c && c.getValue() != null && !(c.getValue() instanceof NullValue)) {
        value = c.getValue();
        fallbackValuePreWrapped = true;
      }
    }

    if (value == null) {
      if (loadFailedFlag.get() && sreRateLimiter != null) {
        sreRateLimiter.onFailed();
      }
      log.debug("handleHot: HotKey value not found and no degraded entry: {}", cacheKey);
      fireOnHotSkipped(cacheKey, wm, HotSkipReason.VALUE_NOT_FOUND);
      return;
    }

    // Pre-read the real dataVersion from Redis BEFORE acquiring the compute
    // lock: this is network I/O and must not run under the Caffeine bin lock
    // (ADR-0030). Only needed when no entry existed at the DCL pre-check — the
    // pre-check mirrors the compute's own guard, so the common existing-entry path
    // pays nothing.  Falls back to 0/not-degraded when versionController is
    // absent or Redis is unreachable.  A version read slightly earlier than
    // the create is harmless: it avoids creating an entry with version=0
    // that could be evicted by a stale invalidation (see issue 4.11), and
    // any race window is bounded by the version-floor cache (ADR-0022).
    long actualDataVersion =
      !(existingSnapshot instanceof CacheEntry) && versionController != null
        ? versionController.currentVersion(cacheKey).orElse(0L)
        : 0L;

    // Compress BEFORE acquiring the Caffeine bin lock (write-side ADR-0030
    // discipline, mirroring ExpireManagerImpl.applyRefreshTask: compression
    // cost is linear in the value size and must not stall same-bin
    // reads/writes). The price is one wasted compression when the version
    // guard inside the compute rejects the store. An L1-fallback value is
    // already wrapped and passes through untouched.
    boolean[] applied = new boolean[1];
    // Effectively-final capture for the compute lambda below: value is
    // reassigned by the L1 fallback path above, so the lambda cannot capture
    // it directly.
    Object resolvedValue = fallbackValuePreWrapped ? value : expireManager.wrapValue(value);
    Object computed = caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) -> {
        long defaultHotHardTtl = expireManager.ttlPolicy().getEffectiveHotHardTtlMs();
        long defaultHotSoftTtl = expireManager.ttlPolicy().getEffectiveHotSoftTtlMs();

        // DCL second check – atomic with to write
        if (existing instanceof CacheEntry ce) {
          if (VersionGuard.shouldSkipForWorker(ce, wm.decisionVersion(), wm.nodeId(), wm.epoch())) {
            return existing;
          }

          applied[0] = true;
          // Single-draft rewrite: value + decision re-stamp + HOT state +
          // hot TTLs with computed expire timestamps (the hot TTL is the
          // floor per TtlPolicy.resolveEffectiveHot*, so a configured 0
          // means disabled → the draft's to* semantics stamp it permanent,
          // matching computeHotHardExpireAt's documented contract).
          return expireManager
            .editEntry(ce)
            .value(resolvedValue)
            .decision(new DecisionStamp(wm.decisionVersion(), wm.nodeId(), wm.epoch()))
            .keyState(KeyState.HOT)
            .ttl(defaultHotHardTtl, defaultHotSoftTtl)
            .build();
        }

        applied[0] = true;
        // Preserve the decision identity on the create path: without
        // nodeId/epoch, VersionGuard.shouldSkipForWorker treats every
        // subsequent decision as cross-Worker (unconditional accept), so
        // out-of-order replayed messages could overwrite this entry.
        return expireManager
          .newEntry()
          .value(resolvedValue)
          .version(actualDataVersion)
          .decision(new DecisionStamp(wm.decisionVersion(), wm.nodeId(), wm.epoch()))
          .ttl(
            defaultHotHardTtl,
            defaultHotSoftTtl,
            expireManager.ttlPolicy().getEffectiveHardTtlMs(),
            expireManager.ttlPolicy().getEffectiveSoftTtlMs()
          )
          .keyState(KeyState.HOT)
          .build();
      });
    // The inner DCL guard may have rejected the promotion as stale — neither
    // the limiter success nor the promotion hook may fire for a decision that
    // was not applied (a recorded success would inflate the success rate and
    // weaken the adaptive throttle exactly when decisions are churning).
    // Matches DefaultSyncDecisionHandler.handleRefresh: use the compute's own
    // return value instead of re-reading via getIfPresent — a concurrent
    // INVALIDATE/eviction between compute and the re-read could otherwise
    // deliver a stale or foreign entry to the hooks.
    if (!applied[0]) {
      return;
    }
    log.debug("HotKey promoted by Worker: {}", cacheKey);
    if (sreRateLimiter != null) {
      sreRateLimiter.onSuccess();
    }
    if (computed instanceof CacheEntry entry) {
      fireAfterHotPromotion(cacheKey, wm, entry);
    }
  }

  /**
   * Downgrades a cache key from {@link KeyState#HOT} to {@link KeyState#COOL},
   * following a Worker COOL decision.
   *
   * <p>This effectively reverts the HOT promotion:
   * <ul>
   *   <li>The cached value, {@code dataVersion}, degradation flag, and
   *       {@code decisionVersion} are all preserved — the data remains available.</li>
   *   <li>The hard TTL is reset to the normal value (from
   *       {@code normalHardTtlMs}, or a 120s protection default).</li>
   *   <li>The soft TTL is reset to the normal value (from
   *       {@code normalSoftTtlMs}, or a 60s protection default), so the entry
   *       eventually soft-expires.</li>
   *   <li>Read paths still refresh COOL entries on soft expiry
   *       (SOFT_REFRESH, when a reader is present); a successful refresh
   *       downgrades the entry to NORMAL — the local read traffic shows the
   *       key is still active locally, so it returns to the ordinary local
   *       lifecycle. After that, the entry follows the NORMAL lifecycle:
   *       soft-expired accesses re-arm the same background refresh (the
   *       stale-while-revalidate contract of the soft TTL), the hard TTL
   *       stays the absolute bound (ADR-0034), and a Worker broadcast can
   *       still override at any time via {@code decisionVersion}.</li>
   * </ul>
   *
   * <p>If no existing entry is present in L1, the COOL decision is a no-op —
   * there is nothing to downgrade. The entry will be evicted naturally by
   * Caffeine's capacity policy or a subsequent invalidation.
   *
   * @param wm the Worker message containing the COOL decision; must not be null
   */
  @Override
  @SuppressWarnings("all")
  public void handleCool(WorkerMessage wm) {
    String cacheKey = wm.cacheKey();
    boolean[] cooled = new boolean[1];

    Object computed = caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) -> {
        if (
          existing instanceof CacheEntry ce &&
          VersionGuard.shouldSkipForWorker(ce, wm.decisionVersion(), wm.nodeId(), wm.epoch())
        ) {
          return existing;
        }

        if (existing instanceof CacheEntry cacheEntry) {
          // A hard-expired entry is dead: the next read invalidates it and reloads
          // (ADR-0034 — the hard TTL is the absolute bound for every state). Cooling
          // it here would rewrite the stale value with fresh normal TTLs, resurrecting
          // data the hard TTL already condemned for a full normal-TTL lifetime — and
          // extend a NullValue sentinel far past its short penetration-protection
          // TTL. Leave it untouched so it dies on schedule; the reload re-decides.
          if (expireManager.ttlPolicy().isLogicallyExpired(cacheEntry)) {
            return existing;
          }
          long normalHardTtlMs = cacheEntry.getNormalHardTtlMs();
          long normalSoftTtlMs = cacheEntry.getNormalSoftTtlMs();

          long hardTtlMsIfZero = normalHardTtlMs > 0 ? normalHardTtlMs : COOL_DEFAULT_PROTECTION_HARDTTL_TIME;
          long softTtlMsIfZero = normalSoftTtlMs > 0 ? normalSoftTtlMs : COOL_DEFAULT_PROTECTION_SOFTTTL_TIME;

          long hardTtlExpireAtMs = expireManager
            .ttlPolicy()
            .toHardExpireTimestamp(hardTtlMsIfZero, COOL_DEFAULT_PROTECTION_HARDTTL_TIME_RATIO);
          long softTtlExpireAtMs = expireManager
            .ttlPolicy()
            .toSoftExpireTimestamp(softTtlMsIfZero, COOL_DEFAULT_PROTECTION_SOFTTTL_TIME_RATIO);

          cooled[0] = true;
          // Decision re-stamp + normal-lifecycle TTLs + COOL state in one
          // draft: the expire timestamps carry the COOL protection jitter
          // ratio, so they are set explicitly after the durations.
          return expireManager
            .editEntry(cacheEntry)
            .decision(new DecisionStamp(wm.decisionVersion(), wm.nodeId(), wm.epoch()))
            .ttl(hardTtlMsIfZero, softTtlMsIfZero)
            .hardExpiryAt(hardTtlExpireAtMs)
            .softExpiryAt(softTtlExpireAtMs)
            .keyState(KeyState.COOL)
            .build();
        }

        // No existing entry – nothing to cool
        return existing;
      });
    // Use the compute's own result instead of re-reading the cache via getIfPresent: a
    // concurrent INVALIDATE/eviction between the compute and a re-read could otherwise
    // deliver a stale or null entry to the hooks (same pattern as handleHot /
    // DefaultSyncDecisionHandler.handleRefresh). cooled[0] is only set on the rewrite
    // path, which returns the rewritten CacheEntry.
    if (cooled[0]) {
      log.debug("HotKey cooled by Worker: {}", cacheKey);
      if (computed instanceof CacheEntry entry) {
        // only CacheEntry we hook.
        fireAfterCoolDowngrade(cacheKey, wm, entry);
      }
    } else {
      fireOnCoolSkipped(cacheKey, wm);
    }
  }

  /**
   * Loads the authoritative value for the key carried in the Worker message via
   * the cluster loader: a registered prefix routes to the application's
   * {@code CacheLoader}, an unregistered key falls back to the Redis value channel.
   * <p>
   * Any exception thrown by the {@code clusterLoader} (connection timeout, Redis
   * outage, data-source error) is caught, reported via the rate-limited
   * {@link #logLoadFailure}, and surfaced to the caller through the
   * {@code loadFailed} flag (a clean miss with no exception is
   * indistinguishable from "no value" — only exceptions set the flag). The caller
   * is responsible for falling back to the existing L1 entry value when this
   * method returns {@code null}.
   *
   * @param wm          the Worker message containing the cache key to load; must not be null
   * @param loadFailed  single-element flag set to {@code true} when the load threw
   * @return the authoritative value, or {@code null} if the key is absent or the
   *         load failed with an exception
   */
  private Object loadAuthoritative(WorkerMessage wm, AtomicBoolean loadFailed) {
    try {
      return clusterLoader.load(wm.cacheKey());
    } catch (Exception e) {
      loadFailed.set(true);
      logLoadFailure(wm.cacheKey(), e);
      return null;
    }
  }

  /**
   * Reports a load failure, rate-limited to one full-stack WARN per
   * {@value LogThrottle#DEFAULT_WINDOW_MS}ms window. With the data source down and
   * consumers draining a broadcast backlog, a per-message WARN floods the log
   * at message rate — within the window failures are counted and surfaced as a
   * one-line DEBUG with the cumulative count instead.
   *
   * @param cacheKey the key whose load failed
   * @param e        the load failure (stack shown on the window-opening WARN)
   */
  private void logLoadFailure(String cacheKey, Exception e) {
    LogThrottle.Counting.Attempt attempt = loadFailureThrottle.record();
    if (!attempt.admitted()) {
      log.debug(
        "handleHot: load failed for key={}, trying L1 entry fallback ({} failures in current window)",
        cacheKey,
        attempt.count()
      );
      return;
    }
    if (attempt.count() > 0) {
      log.warn(
        "handleHot: load failed for key={}, trying L1 entry fallback ({} similar failures suppressed in the last {}ms)",
        cacheKey,
        attempt.count(),
        LogThrottle.DEFAULT_WINDOW_MS,
        e
      );
    } else {
      log.warn("handleHot: load failed for key={}, trying L1 entry fallback", cacheKey, e);
    }
  }

  /**
   * Called after the entry has been promoted to {@link KeyState#HOT} with extended
   * hard/soft TTLs. The {@code entry} reflects the newly written {@link CacheEntry}
   * in L1.
   */
  public void fireAfterHotPromotion(String cacheKey, WorkerMessage wm, CacheEntry entry) {
    for (WorkerDecisionHook hook : workerHooks) {
      try {
        hook.afterHotPromotion(cacheKey, wm, entry);
      } catch (Exception e) {
        log.warn("WorkerDecisionHook.afterHotPromotion failed for key={}", cacheKey, e);
      }
    }
  }

  /**
   * Called after the entry has been downgraded to {@link KeyState#COOL} with normal
   * TTLs and soft expiration disabled. The {@code entry} reflects the updated
   * {@link CacheEntry} in L1.
   */
  public void fireAfterCoolDowngrade(String cacheKey, WorkerMessage wm, CacheEntry entry) {
    for (WorkerDecisionHook hook : workerHooks) {
      try {
        hook.afterCoolDowngrade(cacheKey, wm, entry);
      } catch (Exception e) {
        log.warn("WorkerDecisionHook.afterCoolDowngrade failed for key={}", cacheKey, e);
      }
    }
  }

  /**
   * Called when the HOT decision was not applied. The {@code reason} indicates whether
   * the SRE rate limiter throttled the request, a newer decision version is already
   * present in L1, or the value could not be found in Redis or any degraded entry.
   */
  public void fireOnHotSkipped(String cacheKey, WorkerMessage wm, HotSkipReason reason) {
    for (WorkerDecisionHook hook : workerHooks) {
      try {
        hook.onHotSkipped(cacheKey, wm, reason);
      } catch (Exception e) {
        log.warn("WorkerDecisionHook.onHotSkipped failed for key={}", cacheKey, e);
      }
    }
  }

  /**
   * Called when the COOL decision was skipped because no entry exists in L1 for the
   * given cache key.
   */
  public void fireOnCoolSkipped(String cacheKey, WorkerMessage wm) {
    for (WorkerDecisionHook hook : workerHooks) {
      try {
        hook.onCoolSkipped(cacheKey, wm);
      } catch (Exception e) {
        log.warn("WorkerDecisionHook.onCoolSkipped failed for key={}", cacheKey, e);
      }
    }
  }
}
