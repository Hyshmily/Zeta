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

import static io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy.isWorkerManaged;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.util.LogThrottle;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation of {@link SyncDecisionHandler} that performs loader-backed
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

  /** Loads the authoritative value for a key: a registered prefix routes to the
   * application's {@code CacheLoader}, an unregistered key falls back to the Redis
   * value channel. Used during REFRESH to fetch the authoritative value before
   * writing to L1. */
  private final CacheLoader<Object> clusterLoader;

  /** Computes hard and soft expiry timestamps for refreshed entries. */
  private final ExpireManager expireManager;

  /** Hot-key rule matcher whose rule set is updated when a RULES_SYNC message arrives. */
  private final RuleMatcher ruleMatcher;

  /** Optional lifecycle hooks for cache-sync events. Never null. */
  private final List<SyncHook> syncHooks;

  /**
   * Optional SingleFlight dedup collaborator (ADR-0067). When present, every
   * applied entry removal (versioned INVALIDATE, value-less REFRESH fallback,
   * legacy batch INVALIDATE_ALL) also drops the key's dedup entry, so a
   * post-removal miss re-invokes the reader instead of replaying a completed
   * pre-removal load result onto L1. {@code null} (legacy constructor) keeps
   * the historical no-invalidation behavior.
   */
  @Nullable
  private final SingleFlight singleFlight;

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
    CacheLoader<Object> clusterLoader,
    ExpireManager expireManager,
    RuleMatcher ruleMatcher,
    List<SyncHook> syncHooks
  ) {
    this(caffeineCache, clusterLoader, expireManager, ruleMatcher, syncHooks, null);
  }

  public DefaultSyncDecisionHandler(
    Cache<String, Object> caffeineCache,
    CacheLoader<Object> clusterLoader,
    ExpireManager expireManager,
    RuleMatcher ruleMatcher,
    List<SyncHook> syncHooks,
    @Nullable SingleFlight singleFlight
  ) {
    this.caffeineCache = caffeineCache;
    this.clusterLoader = clusterLoader;
    this.expireManager = expireManager;
    this.ruleMatcher = ruleMatcher;
    this.syncHooks = syncHooks != null ? syncHooks : Collections.emptyList();
    this.singleFlight = singleFlight;
  }

  /**
   * Counts load failures and admits the full WARN once per
   * {@value LogThrottle#DEFAULT_WINDOW_MS}ms window (ADR-0037): the
   * window-opening WARN reports how many similar failures were suppressed
   * since the previous WARN; callers inside the window log at DEBUG with the
   * running tally. The admission decision, the tally and the monotonic clock
   * live in {@link LogThrottle.Counting} — the shared log-throttling utility.
   */
  private final LogThrottle.Counting loadFailureThrottle = new LogThrottle.Counting();

  /**
   * Atomically removes the specified key from the local cache in response to
   * an INVALIDATE sync message from a peer instance.
   *
   * <p><b>Version guard logic:</b>
   * <ul>
   *   <li><b>Unconditional path:</b> When {@code version == 0L && !isVersionDegraded}
   *       (clean invalidation from {@code invalidateAllLocal}), the guard is bypassed
   *       for ordinary entries. <em>Worker-managed entries</em> ({@link KeyState#HOT}
   *       or {@link KeyState#COOL}) are preserved: a version-less INVALIDATE carries
   *       no decision-version information, and clearing such an entry would discard
   *       its decision metadata and extended TTL (ADR-0021 / ADR-0024 depend on them
   *       surviving); it expires naturally or awaits the next Worker decision.</li>
   *   <li><b>Guarded path:</b> Uses {@link VersionGuard#shouldSkipForSync} with the
   *       4-case degraded comparison. Case 2 (existing normal, incoming degraded)
   *       prevents a stale degraded INVALIDATE from wiping a healthy entry.</li>
   * </ul>
   *
   * <p>Double-checked locking (DCL): a fast version guard before the atomic
   * {@code compute} (first pass), and a second guard inside the {@code compute}
   * body (second pass) to prevent a concurrent REFRESH from being wiped by a
   * stale invalidate that arrived after the refresh. Invalidation watermark
   * recording and {@link SyncHook} dispatch happen only when the entry was
   * actually removed — a rejected (preserved) invalidate must not block later
   * REFRESH messages via a {@code Long.MAX_VALUE} watermark.
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

    AtomicBoolean removed = new AtomicBoolean(false);
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
        if (unconditional && isWorkerManaged(existing)) {
          // Version-less INVALIDATE cannot be compared against the Worker
          // decision version; preserving HOT/COOL keeps decision metadata and
          // extended TTLs alive (see Javadoc above).
          return existing;
        }
        // Record the invalidation watermark atomically with the removal:
        // recording it outside the compute left a window in which a concurrent
        // REFRESH could pass the {@code isInvalidation} guard and re-warm a key
        // this message just invalidated. The merge is idempotent (Math::max),
        // so a compute re-invocation is harmless.
        recordInvalidation(key, sm.version());
        removed.set(true);
        return null;
      });
    if (removed.get()) {
      log.debug("Invalidated by sync: {}", sm.cacheKey());
      invalidateDedupEntry(sm.cacheKey());
      fireAfterInvalidate(sm.cacheKey(), sm);
    }
  }

  /**
   * Removes the keys carried in a batch INVALIDATE_ALL message.
   *
   * <p><b>Legacy wire format (ADR-0066):</b> the in-tree producer is gone —
   * {@code HotKeyCache.invalidate(Iterable, true)} now sends the same per-key
   * versioned INVALIDATE messages as the single-key path, so a delayed batch can no
   * longer bypass version ordering and blindly wipe entries a newer REFRESH just
   * applied. This handler is kept for rolling upgrades against older peers that
   * still broadcast the unversioned batch message.
   *
   * <p>Worker-managed entries ({@link KeyState#HOT} / {@link KeyState#COOL}) are
   * preserved, mirroring the version-less INVALIDATE path: a version-less batch
   * carries no ordering information, and removing such an entry would discard its
   * decision metadata and extended TTLs (ADR-0021 / ADR-0024 depend on them
   * surviving); it expires naturally or awaits the next Worker decision.
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
      for (String key : keys) {
        caffeineCache.asMap().compute(key, (k, existing) -> isWorkerManaged(existing) ? existing : null);
        invalidateDedupEntry(key);
      }
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
   * @param sm the sync message containing the key and version to refresh;
   *           must not be null
   */
  @Override
  public void handleRefresh(SyncMessage sm) {
    handleRefresh(sm, true);
  }

  /**
   * Refreshes a cache entry with the latest value from Redis in response to a
   * REFRESH sync message from a peer instance, with the dispatcher's batch-end
   * signal (ADR-0071, Disruptor {@code BatchEventProcessor} semantics).
   *
   * <p><b>Batch-tail amortization:</b> a burst of N same-key REFRESH messages queues N
   * tasks on the ordered dispatcher's per-key worker; each would otherwise pay its own
   * Redis load even though only the final applied state matters. When {@code endOfBatch}
   * is {@code false} (further tasks of the same key's granted batch follow), this message
   * is skipped without the Redis load, the L1 write, or the compression: a later REFRESH
   * in the same batch reloads the authoritative value and produces the same final state
   * with one load instead of N. {@code onRefreshSkipped} fires for the skipped message so
   * the hook contract ("a REFRESH that was not applied") stays symmetric; the delta
   * versus non-batched processing is that intermediate versions never become visible in
   * L1 and their per-message {@code afterRefresh} hooks do not fire.
   *
   * <p>Skips only ever apply within one granted batch: a REFRESH granted alone (the common
   * single-message case) always runs the full flow, and a REFRESH that is the last task of
   * its batch always loads — the signal is best-effort and never strands a key on a stale
   * value.
   *
   * <p><b>Refresh flow (batch-final message):</b>
   * <ol>
   *   <li><b>DCL check 1:</b> Fast-path version guard ({@link VersionGuard#shouldSkipForRefresh})
   *       against the existing L1 entry. If a strictly newer dataVersion is already present,
   *       the refresh is skipped (an <b>equal</b> version applies — ADR-0066).</li>
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
   * @param sm         the sync message containing the key and version to refresh;
   *                   must not be null
   * @param endOfBatch {@code true} if this is the last task of its key's currently granted
   *                   dispatch batch; {@code false} while further tasks of the same batch
   *                   follow
   */
  @Override
  @SuppressWarnings("all")
  public void handleRefresh(SyncMessage sm, boolean endOfBatch) {
    String cacheKey = sm.cacheKey();
    if (!endOfBatch) {
      // Batch-tail amortization (ADR-0071): a later REFRESH in this same key-batch reloads
      // the newest value from Redis anyway — applying this one would only add a redundant
      // Redis load, an L1 write and a compression for an intermediate state nobody can
      // observe across the batch (all tasks run back-to-back in one dispatch cycle).
      log.debug("Refresh skipped (non-final of dispatch batch): key={}, incomingVersion={}", cacheKey, sm.version());
      fireOnRefreshSkipped(cacheKey, sm);
      return;
    }
    // DCL first check – cheap, outside the compute lock. The REFRESH receiver uses
    // the strict-newer guard (ADR-0066): an equal version applies, so a REFRESH is
    // never swallowed by an entry that ADR-0033's probe-after-read over-stamped one
    // write ahead of its data.
    if (VersionGuard.shouldSkipForRefresh(caffeineCache, cacheKey, sm.version(), sm.isVersionDegraded())) {
      log.debug("Stale refresh ignored: key={}, incomingVersion={}", cacheKey, sm.version());
      fireOnRefreshSkipped(cacheKey, sm);
      return;
    }

    Object value = loadAuthoritative(sm);
    if (value == null) {
      // No value channel exists by default: nothing in Zeta writes the
      // cache-key namespace in Redis, so a REFRESH broadcast can never carry
      // the payload. Fall back to a local invalidation so the next read
      // reloads through the type-safe application reader instead of silently
      // keeping a stale entry (ADR-0031). Local-only: no re-broadcast, so this
      // cannot loop.
      log.debug("Refresh loaded no value for key={}, falling back to local invalidation", cacheKey);
      invalidateForValuelessRefresh(cacheKey, sm);
      fireOnRefreshSkipped(cacheKey, sm);
      return;
    }

    // Compress BEFORE acquiring the Caffeine bin lock (write-side ADR-0030
    // discipline, mirroring ExpireManagerImpl.applyRefreshTask: compression
    // cost is linear in the value size and must not stall same-bin
    // reads/writes). The price is one wasted compression when the version
    // guard or the invalidation watermark inside the compute discards the
    // refresh.
    Object wrappedValue = expireManager.wrapValue(value);

    boolean[] applied = new boolean[1];
    Object computed = caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) -> {
        // DCL second check – atomic with to write. Same strict-newer guard as the
        // fast path (ADR-0066): an equal version applies and heals an over-stamped
        // entry (ADR-0033 probe-after-read).
        if (
          existing instanceof CacheEntry ce &&
          VersionGuard.shouldSkipForRefresh(ce, sm.version(), sm.isVersionDegraded())
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

        applied[0] = true;
        if (existing instanceof CacheEntry cacheEntry) {
          // Refresh-in-place: fresh value + probed version, same TTL
          // durations, expiry re-armed from those durations. The draft's
          // to* semantics keep a disabled soft TTL (0) disabled — matching
          // the guard below it (ADR-0067).
          return expireManager.editEntry(cacheEntry).value(wrappedValue).version(sm.version()).rearmExpiry().build();
        }
        long defaultHardTtlMs = expireManager.ttlPolicy().getEffectiveHardTtlMs();
        long defaultSoftTtlMs = expireManager.ttlPolicy().getEffectiveSoftTtlMs();
        return expireManager
          .newEntry()
          .value(wrappedValue)
          .version(sm.version())
          .ttl(defaultHardTtlMs, defaultSoftTtlMs, defaultHardTtlMs, defaultSoftTtlMs)
          .keyState(KeyState.NORMAL)
          .build();
      });
    // The atomic second guard may have rejected the refresh (a newer dataVersion arrived
    // during the Redis fetch, or the invalidation watermark blocked it) — neither the
    // success hook nor the "refreshed" log may fire for a refresh that was not applied
    // (matches DefaultWorkerDecisionHandler.handleHot; the old code read the compute's
    // return value, which on a rejection IS the existing entry, and fired afterRefresh
    // for it). The skip hook fires instead so consumers see the outcome symmetrically
    // with the pre-check rejection above.
    if (!applied[0]) {
      fireOnRefreshSkipped(cacheKey, sm);
      return;
    }
    log.debug("Refreshed by sync: {}", cacheKey);
    // Use the compute's own result instead of re-reading the cache: a
    // concurrent INVALIDATE/eviction between the compute and a getIfPresent
    // could otherwise deliver a stale or null entry to the hooks.
    if (computed instanceof CacheEntry entry) {
      fireAfterRefresh(cacheKey, sm, entry);
    }
  }

  /**
   * Local invalidation fallback for a REFRESH whose value channel returned nothing
   * (ADR-0031). Unlike a plain {@link Cache#invalidate(Object)}, the removal is
   * guarded:
   * <ul>
   *   <li><b>Worker-managed entries preserved</b> — a peer's refresh fallback must
   *       not discard a decision stamp and extended TTLs (parity with the
   *       version-less INVALIDATE path); the entry expires at its hard TTL or the
   *       next Worker decision.</li>
   *   <li><b>Strictly newer local writes preserved</b> — a local {@code putThrough}
   *       that landed while the REFRESH was in flight is not wiped by an older
   *       peer's message. An <b>equal</b> version is removed: the ADR-0033
   *       probe-after-read can over-stamp an entry one write ahead of its data, and
   *       the write's own REFRESH must heal it via drop-and-reload (ADR-0066).</li>
   * </ul>
   *
   * <p>Removal records the invalidation watermark (so an older in-flight REFRESH
   * cannot re-apply past this point) and fires {@link SyncHook#afterInvalidate} —
   * the same observable contract as {@link #handleLocalInvalidate}.
   *
   * @param cacheKey the key to invalidate
   * @param sm       the REFRESH message that triggered the fallback
   */
  private void invalidateForValuelessRefresh(String cacheKey, SyncMessage sm) {
    boolean[] removed = new boolean[1];
    caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) -> {
        if (existing instanceof CacheEntry ce) {
          if (isWorkerManaged(ce)) {
            return existing;
          }
          if (VersionGuard.shouldSkipForRefresh(ce, sm.version(), sm.isVersionDegraded())) {
            return existing;
          }
        }
        // Watermark recorded atomically with the removal (same race as
        // {@link #handleLocalInvalidate}): a concurrent REFRESH must not slip
        // past the {@code isInvalidation} guard in the gap between removal and
        // recording. The merge is idempotent (Math::max), so a compute
        // re-invocation is harmless.
        recordInvalidation(key, sm.version());
        removed[0] = true;
        return null;
      });
    if (removed[0]) {
      invalidateDedupEntry(cacheKey);
      fireAfterInvalidate(cacheKey, sm);
    }
  }

  /**
   * Loads the authoritative value for the key carried in the sync message via the
   * cluster loader: a registered prefix routes to the application's
   * {@code CacheLoader}, an unregistered key falls back to the Redis value channel.
   * <p>
   * Any exception thrown by the {@code clusterLoader} (connection timeout, Redis
   * outage, data-source error) is caught and reported via the rate-limited
   * {@link #logLoadFailure}; the caller should handle a {@code null}
   * return by aborting the refresh.
   *
   * @param sm the sync message containing the cache key to load; must not be null
   * @return the authoritative value, or {@code null} if the key is absent in the
   *         data source or the load failed with an exception
   */
  private Object loadAuthoritative(SyncMessage sm) {
    try {
      return clusterLoader.load(sm.cacheKey());
    } catch (Exception e) {
      logLoadFailure(sm.cacheKey(), e);
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
      log.debug("handleRefresh: load failed for key={} ({} failures in current window)", cacheKey, attempt.count());
      return;
    }
    if (attempt.count() > 0) {
      log.warn(
        "handleRefresh: load failed for key={} ({} similar failures suppressed in the last {}ms)",
        cacheKey,
        attempt.count(),
        LogThrottle.DEFAULT_WINDOW_MS,
        e
      );
    } else {
      log.warn("handleRefresh: load failed for key={}", cacheKey, e);
    }
  }

  /**
   * Record the highest INVALIDATE version for a key.
   * Used to reject stale REFRESH messages that arrive after the INVALIDATE.
   *
   * <p>A version-less (0) invalidation carries no ordering information, so it
   * must NOT be recorded: the old {@code Long.MAX_VALUE} watermark made
   * {@link #isInvalidation} reject every finite-version REFRESH for the full
   * 10-minute {@code recentInvalidated} TTL, and because the watermark is
   * only cleared when a refresh proceeds, the key could never be re-warmed by
   * peers. A post-invalidation REFRESH loads the current value from Redis at
   * execution time anyway, so blocking it buys nothing.
   *
   * @param key the invalidated cache key
   * @param version the data version of the invalidation, or {@code 0} for a
   *                version-less (clean) invalidation
   */
  private void recordInvalidation(String key, long version) {
    if (version <= 0L) {
      return;
    }
    recentInvalidated.asMap().merge(key, version, Math::max);
  }

  /**
   * Drop the key's SingleFlight dedup entry after an applied removal (ADR-0067):
   * a completed load result from before the removal must not be replayed onto a
   * post-removal miss. No-op when no dedup collaborator is wired (legacy
   * constructor).
   *
   * @param key the removed cache key
   */
  private void invalidateDedupEntry(String key) {
    if (singleFlight != null) {
      singleFlight.invalidate(key);
    }
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
   * Remove the invalidation watermark after a successful refresh,
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
