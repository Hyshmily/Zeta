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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations.Applied;
import io.github.hyshmily.zeta.sync.DecisionPlaneSupport;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Default {@link SyncDecisionHandler}: loader-backed REFRESH, version-guarded INVALIDATE,
 * value-carrying PUT and the clear family.
 *
 * <p><b>Batch reduction.</b> A granted batch holds every queued message of one cache key.
 * Only its highest-version message can be observable, so the batch is reduced to that
 * winner before anything is applied: a superseded PUT never pays its payload
 * deserialization, a superseded REFRESH never runs the loader, and a superseded
 * INVALIDATE never takes the bin lock. The winner's load-failure fallback still honours
 * an explicit INVALIDATE from the same batch: it hard-removes instead of soft-expiring a
 * Worker-managed entry, because the covered removal already condemned that value. A clear
 * batch is reduced to one watermark record pass and one table sweep at each key's highest
 * targeting stamp.
 *
 * <p>Every apply re-checks the version guard and the invalidation tombstones inside its
 * atomic {@code compute}, so reduction is an optimization, never the only defence.
 */
@Slf4j
public class DefaultSyncDecisionHandler implements SyncDecisionHandler {

  @Nullable
  private final CacheLoader<String, Object> clusterLoader;

  private final Cache<String, CacheEntry> caffeineCache;

  @Nullable
  private final CacheSyncPublisher putPayloadParser;

  private final EntryLifecycle entryLifecycle;

  @Nullable
  private final SingleFlight singleFlight;

  private final List<SyncHook> syncHooks;

  private final TtlPolicy ttlPolicy;

  public DefaultSyncDecisionHandler(
    Cache<String, CacheEntry> caffeineCache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    @Nullable List<SyncHook> syncHooks,
    @Nullable SingleFlight singleFlight,
    @Nullable CacheSyncPublisher putPayloadParser
  ) {
    this.singleFlight = singleFlight;
    this.caffeineCache = caffeineCache;
    this.clusterLoader = clusterLoader;
    this.entryLifecycle = entryLifecycle;
    this.putPayloadParser = putPayloadParser;
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.syncHooks = syncHooks != null ? syncHooks : List.of();
  }

  @Override
  public void handleBatch(List<SyncMessage> batch) {
    SyncMessage first = batch.get(0);
    if (first.type().isClear()) {
      applyClearBatch(batch);
      return;
    }

    SyncMessage winner = null;
    SyncMessage coveredInvalidate = null;
    boolean allowSoftExpire = true;
    for (SyncMessage msg : batch) {
      // A newer REFRESH/PUT supersedes a covered INVALIDATE for the final value, but
      // its load-failure fallback must not undo the covered hard removal: keeping a
      // Worker-managed entry soft-expired there would serve data an explicit
      // invalidation in the same batch already condemned.
      allowSoftExpire &= msg.type() != SyncType.INVALIDATE;
      if (winner == null) {
        winner = msg;
      } else if (outranks(msg, winner)) {
        reportCovered(winner);
        coveredInvalidate = rememberCovered(winner, coveredInvalidate);
        winner = msg;
      } else {
        reportCovered(msg);
        coveredInvalidate = rememberCovered(msg, coveredInvalidate);
      }
    }
    switch (winner.type()) {
      case INVALIDATE -> invalidate(winner);
      case REFRESH, PUT -> applyValueMessage(winner, allowSoftExpire, coveredInvalidate);
      default -> {
      }
    }
  }

  /**
   * Whether {@code candidate} replaces {@code current} as the batch winner. A higher version
   * always wins; at the same version an INVALIDATE outranks a REFRESH/PUT, matching the
   * publisher's {@code BroadcastClaim} dedup rule.
   */
  private static boolean outranks(SyncMessage candidate, SyncMessage current) {
    if (candidate.version() != current.version()) {
      return candidate.version() > current.version();
    }
    return candidate.type() == SyncType.INVALIDATE && current.type() != SyncType.INVALIDATE;
  }

  /**
   * Tracks the highest-version INVALIDATE the batch covered, so a value fallback that has to
   * evict still reports the invalidation hook the covered message owned.
   */
  private static @Nullable SyncMessage rememberCovered(SyncMessage covered, @Nullable SyncMessage previous) {
    if (covered.type() != SyncType.INVALIDATE) {
      return previous;
    }
    return previous == null || covered.version() > previous.version() ? covered : previous;
  }

  /**
   * Reports a PUT/REFRESH of the batch that a newer message covered: it produced no
   * effect, and the hook contract is one terminal callback per message. A covered
   * INVALIDATE is dropped silently — a removal that never happened has no
   * applied/skipped outcome to report.
   */
  private void reportCovered(SyncMessage covered) {
    if (covered.type() == SyncType.REFRESH || covered.type() == SyncType.PUT) {
      log.debug(
        "Message covered by a newer message of the same batch: type={}, key={}, version={}",
        covered.type(),
        covered.cacheKey(),
        covered.version()
      );
      fireOnRefreshSkipped(covered);
    }
  }

  /**
   * Applies one REFRESH or value-carrying PUT. Both share the same pipeline: an optional
   * lock-free staleness pre-check, one value resolution (authoritative load or payload
   * deserialization), a fallback to the versioned local invalidation when no cachable value
   * exists, and the guarded write.
   *
   * @param message         the winning message of the batch
   * @param allowSoftExpire {@code false} when the batch also carried an explicit INVALIDATE
   * @param coveredInvalidate the highest covered INVALIDATE, or {@code null}
   */
  private void applyValueMessage(
    SyncMessage message,
    boolean allowSoftExpire,
    @Nullable SyncMessage coveredInvalidate
  ) {
    String key = message.cacheKey();
    long version = message.version();
    boolean put = message.type() == SyncType.PUT;
    // Without a cluster loader a REFRESH has no I/O to avoid: the fallback's compute applies
    // the same version guard inside the bin lock, so the lock-free pre-check would only
    // repeat it. A PUT always guards here: it must not pay deserialization for a pre-image
    // that already lost to the resident entry or an invalidation tombstone.
    if ((put || clusterLoader != null) && staleAgainstLocalState(key, version)) {
      log.debug("Stale {} ignored: key={}, incomingVersion={}", message.type(), key, version);
      fireOnRefreshSkipped(message);
      return;
    }

    Object value = put ? resolvePutValue(key, message) : loadAuthoritative(key);
    if (value == null) {
      log.debug("{} yielded no cachable value for key={}, falling back to local invalidation", message.type(), key);
      boolean removed = evict(key, version, allowSoftExpire);
      if (removed && coveredInvalidate != null) {
        // The covered INVALIDATE's removal was carried out by this fallback; its hook
        // contract must still observe the removal, not just the refresh skip.
        fireAfterInvalidate(coveredInvalidate);
      }
      fireOnRefreshSkipped(message);
      return;
    }

    Applied applied = writePeerValue(key, version, value);
    if (applied == null) {
      fireOnRefreshSkipped(message);
      return;
    }

    log.debug("Applied peer sync value: {}", key);
    if (applied.current() != null) {
      fireAfterRefresh(message, applied.current());
    }
  }

  /**
   * Resolves one PUT payload and applies the loader's admission rule exactly once.
   * {@link #loadAuthoritative} already admits a REFRESH load, so this path checks only the
   * deserialized payload.
   *
   * @return the admitted value, or {@code null} when the payload is unreadable or declined
   */
  @Nullable
  private Object resolvePutValue(String key, SyncMessage message) {
    Object value = putPayloadParser == null ? null : putPayloadParser.parsePutPayload(message.payload());
    if (value != null && clusterLoader != null && !clusterLoader.isCacheable(value)) {
      log.debug("Peer value rejected by loader admission, falling back to local invalidation: key={}", key);
      return null;
    }
    return value;
  }

  /**
   * The guarded peer-value write. Inside the atomic compute: the refresh guard (only a
   * strictly newer resident skips; an equal version applies), the invalidation tombstones,
   * then a value+version rewrite that keeps the resident TTLs and Worker decision metadata
   * — or a fresh NORMAL entry, because only a Worker decision may create a HOT one.
   *
   * @return the applied pre/post-images, or {@code null} when the guards refused the value
   */
  @Nullable
  private Applied writePeerValue(String key, long version, Object value) {
    Applied applied = EntryMutations.apply(caffeineCache, key, existing -> {
      CacheEntry resident = existing != null && !ttlPolicy.isLogicallyExpired(existing) ? existing : null;

      if (resident != null && VersionGuard.shouldSkipForRefresh(resident, version)) {
        return existing;
      }

      if (entryLifecycle.staleAgainstInvalidation(key, version)) {
        log.debug("Apply skipped due to recent invalidation: key={}", key);
        return existing;
      }

      long hardTtl = resident != null ? resident.getHardTtlMs() : ttlPolicy.effectiveHardTtlMs();
      long softTtl = resident != null ? resident.getSoftTtlMs() : ttlPolicy.effectiveSoftTtlMs();
      return entryLifecycle.writeValue(resident, value, version, hardTtl, softTtl, false);
    });
    if (applied != null) {
      invalidateDedupEntry(key);
    }
    return applied;
  }

  /**
   * Applies one explicit peer INVALIDATE: record the tombstone and remove the resident
   * in the same compute, then report {@code afterInvalidate} only when an entry was
   * actually removed.
   */
  private void invalidate(SyncMessage message) {
    String key = message.cacheKey();
    long version = message.version();
    if (evict(key, version, false)) {
      log.debug("Invalidated by sync: {}", key);
      fireAfterInvalidate(message);
    }
  }

  /** What one versioned eviction did to the resident entry. */
  private enum EvictOutcome {
    /** A newer resident outranked the eviction; nothing changed. */
    SKIPPED,
    /** The tombstone was recorded but no entry was removed (absent or soft-expired). */
    TOMBSTONED,
    /** The entry was removed from L1. */
    REMOVED,
  }

  /**
   * The shared versioned eviction. The tombstone is recorded in the same compute as the
   * removal, so a concurrent refresh cannot slip between them and re-warm the key. The
   * key's dedup flight is dropped whenever the tombstone is recorded — including an absent
   * key — so a read that starts after the invalidation cannot join a pre-invalidation load;
   * a version guard skip leaves the newer flight alone.
   *
   * <p>With {@code softExpireWorkerManaged}, a Worker-managed (HOT/COOL) entry survives:
   * this node has no loader, so the entry keeps its decision stamp and TTL regime and only
   * its soft window is closed — the next read reloads through the application reader.
   *
   * @return {@code true} only when a non-null entry was removed from L1
   */
  private boolean evict(String key, long version, boolean softExpireWorkerManaged) {
    EvictOutcome[] result = { EvictOutcome.SKIPPED };
    caffeineCache
      .asMap()
      .compute(key, (k, existing) -> {
        if (existing != null && VersionGuard.shouldSkipForRefresh(existing, version)) {
          return existing;
        }
        // Recorded even when absent: an in-flight load must not re-warm the key.
        entryLifecycle.recordInvalidation(key, version);
        if (existing == null) {
          result[0] = EvictOutcome.TOMBSTONED;
          return null;
        }
        if (softExpireWorkerManaged && existing.isWorkerManaged()) {
          // One millisecond in the past, so a read in this same millisecond already sees
          // the closed soft window.
          result[0] = EvictOutcome.TOMBSTONED;
          return entryLifecycle.editEntry(existing).softExpiryAt(TimeSource.currentTimeMillis() - 1L).build();
        }
        result[0] = EvictOutcome.REMOVED;
        return null;
      });
    if (result[0] != EvictOutcome.SKIPPED) {
      invalidateDedupEntry(key);
    }
    return result[0] == EvictOutcome.REMOVED;
  }

  /**
   * Applies a batch of clears: record each watermark first (a concurrent apply that passes
   * its per-key guard on an absent entry must already see it), then run <b>one</b> key-set
   * pass removing each key at the highest stamp that targets it. A prefix clear at or below
   * the batch's table-wide stamp is dropped: the table-wide stamp already removes every key
   * that prefix clear could, so keeping it would only add a redundant watermark and a walk.
   */
  private void applyClearBatch(List<SyncMessage> batch) {
    long fullStamp = 0L;
    Map<String, Long> prefixStamps = new HashMap<>(2);
    for (SyncMessage msg : batch) {
      switch (msg.type()) {
        case INVALIDATE_ALL -> fullStamp = Math.max(fullStamp, msg.version());
        case INVALIDATE_PREFIX -> prefixStamps.merge(msg.cacheKey(), msg.version(), Math::max);
        default -> {
        }
      }
    }

    if (fullStamp > 0L) {
      // A table-wide clear at fullStamp already covers every prefix clear at or
      // below it: stampFor returns fullStamp for those keys and the flight kill
      // below is table-wide.
      long fullClearStamp = fullStamp;
      prefixStamps.values().removeIf(prefixStamp -> prefixStamp <= fullClearStamp);
    }

    if (fullStamp == 0L && prefixStamps.isEmpty()) {
      return;
    }

    recordWatermarks(fullStamp, prefixStamps);
    killClearedFlights(fullStamp, prefixStamps);
    int targeted = sweep(fullStamp, prefixStamps);
    if (targeted > 0) {
      log.debug("Clear applied: fullStamp={}, prefixes={}, targeted={}", fullStamp, prefixStamps.keySet(), targeted);
    }
  }

  /** Records the batch's clear watermarks before the sweep touches a single entry. */
  private void recordWatermarks(long fullStamp, Map<String, Long> prefixStamps) {
    if (fullStamp > 0L) {
      entryLifecycle.recordClear(fullStamp);
    }
    if (!prefixStamps.isEmpty()) {
      entryLifecycle.recordPrefixClears(prefixStamps);
    }
  }

  /**
   * Kills pre-clear flights too: the watermark refuses their fills, but a read that starts
   * after the clear must not join one and receive the pre-clear value. A table-wide kill
   * already covers every prefix, so the per-prefix walk only runs without one.
   */
  private void killClearedFlights(long fullStamp, Map<String, Long> prefixStamps) {
    if (singleFlight == null) {
      return;
    }
    if (fullStamp > 0L) {
      singleFlight.invalidateAll();
    } else {
      for (String prefix : prefixStamps.keySet()) {
        singleFlight.invalidatePrefix(prefix);
      }
    }
  }

  /**
   * Removes every resident a clear of the batch targets, each at the highest stamp that reaches
   * it.
   *
   * @return the number of keys the batch targeted
   */
  private int sweep(long fullStamp, Map<String, Long> prefixStamps) {
    int targeted = 0;
    for (Map.Entry<String, CacheEntry> entry : caffeineCache.asMap().entrySet()) {
      String key = entry.getKey();
      long stamp = stampFor(key, fullStamp, prefixStamps);
      if (stamp <= 0L) {
        continue;
      }
      // Lock-free pre-filter on the value the entry set already carries: a resident
      // stamped above the clear is preserved by the compute, so its bin lock is skipped.
      CacheEntry resident = entry.getValue();
      if (VersionGuard.shouldSkipForRefresh(resident, stamp)) {
        continue;
      }
      targeted++;
      // No per-key dedup kill here: the clear family already marked every pre-clear
      // flight above (invalidateAll / invalidatePrefix), and the watermark recorded
      // before the sweep refuses any pre-clear fill. A flight created after that kill
      // belongs to a post-clear read and must survive; removing it here would only
      // force a duplicate load for every key the sweep touches.
      EntryMutations.mutate(caffeineCache, key, existing ->
        existing == null || VersionGuard.shouldSkipForRefresh(existing, stamp) ? existing : null
      );
    }
    return targeted;
  }

  /** The stamp a key must be removed at, or {@code 0} when no clear of the batch targets it. */
  private static long stampFor(String key, long fullStamp, Map<String, Long> prefixStamps) {
    long stamp = fullStamp;
    for (Map.Entry<String, Long> entry : prefixStamps.entrySet()) {
      if (entry.getValue() > stamp && key.startsWith(entry.getKey())) {
        stamp = entry.getValue();
      }
    }
    return stamp;
  }

  /** Whether the local entry or an applied tombstone already makes {@code version} obsolete. */
  private boolean staleAgainstLocalState(String key, long version) {
    CacheEntry resident = caffeineCache.asMap().get(key);
    boolean residentOutranks =
      resident != null &&
      !ttlPolicy.isLogicallyExpired(resident) &&
      VersionGuard.shouldSkipForRefresh(resident, version);
    return residentOutranks || entryLifecycle.staleAgainstInvalidation(key, version);
  }

  /**
   * Loads the authoritative value through the optional cluster loader; a clean miss, a
   * declined admission and a loader failure all answer {@code null} (design decisions:
   * sync messages).
   */
  @Nullable
  private Object loadAuthoritative(String key) {
    return DecisionPlaneSupport.loadAuthoritative(clusterLoader, key, log, "handleRefresh: load failed for key={}");
  }

  /** Drops the applied key's dedup entry: a pre-apply load result must not be replayed. */
  private void invalidateDedupEntry(String key) {
    if (singleFlight != null) {
      singleFlight.invalidate(key);
    }
  }

  /**
   * One owner of the hook-dispatch contract: a broken hook never breaks the decision
   * apply. The empty check runs before the capturing callback is built.
   *
   * @param hookName the callback name (for the WARN line)
   * @param key      the key the decision touched
   * @param callback the hook callback to invoke for each registered hook
   */
  private void fireHooks(String hookName, String key, Consumer<SyncHook> callback) {
    DecisionPlaneSupport.fireHooks(syncHooks, "SyncHook", hookName, key, callback, log);
  }

  /** A peer value was applied to L1 (REFRESH reload or value-carrying PUT). */
  private void fireAfterRefresh(SyncMessage msg, CacheEntry entry) {
    if (syncHooks.isEmpty()) {
      return;
    }
    fireHooks("afterRefresh", msg.cacheKey(), hook -> hook.afterRefresh(msg.cacheKey(), msg, entry));
  }

  /** The entry was removed from L1 in response to a peer INVALIDATE. */
  private void fireAfterInvalidate(SyncMessage msg) {
    if (syncHooks.isEmpty()) {
      return;
    }
    fireHooks("afterInvalidate", msg.cacheKey(), hook -> hook.afterInvalidate(msg.cacheKey(), msg));
  }

  /** A message produced no effect (stale, covered by a newer message, or unloadable). */
  private void fireOnRefreshSkipped(SyncMessage msg) {
    if (syncHooks.isEmpty()) {
      return;
    }
    fireHooks("onRefreshSkipped", msg.cacheKey(), hook -> hook.onRefreshSkipped(msg.cacheKey(), msg));
  }
}
