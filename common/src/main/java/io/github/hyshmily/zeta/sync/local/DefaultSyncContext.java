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
 * Default {@link SyncContext}: loader-backed REFRESH, version-guarded INVALIDATE,
 * value-carrying PUT and the clear family.
 *
 * <p>Every apply re-checks the version guard and the invalidation tombstones inside its
 * atomic {@code compute}, so batch reduction is an optimization, never the only defence.
 */
@Slf4j
public class DefaultSyncContext implements SyncContext {

  @Nullable
  private final CacheLoader<String, Object> clusterLoader;

  private final Cache<String, CacheEntry> cache;

  @Nullable
  private final PutPayloadCodec payloadCodec;

  private final EntryLifecycle entryLifecycle;

  @Nullable
  private final SingleFlight singleFlight;

  private final List<SyncHook> hooks;

  private final TtlPolicy ttlPolicy;

  public DefaultSyncContext(
    Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    @Nullable List<SyncHook> hooks,
    @Nullable SingleFlight singleFlight,
    @Nullable PutPayloadCodec payloadCodec
  ) {
    this.singleFlight = singleFlight;
    this.cache = cache;
    this.clusterLoader = clusterLoader;
    this.entryLifecycle = entryLifecycle;
    this.payloadCodec = payloadCodec;
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.hooks = hooks != null ? hooks : List.of();
  }

  @Override
  public void value(Batch.Value batch) {
    applyValue(batch);
  }

  @Override
  public void invalidate(SyncMessage message) {
    String key = message.cacheKey();
    long version = message.version();
    if (evict(key, version, false)) {
      log.debug("Invalidated by sync: {}", key);
      fireAfterInvalidate(message);
    }
  }

  @Override
  public void clear(Batch.Clear batch) {
    applyClear(batch.messages());
  }

  @Override
  public void skip(SyncMessage message) {
    // A covered INVALIDATE is dropped silently: a removal that never happened has no
    // applied/skipped outcome to report.
    if (message.type() == SyncType.REFRESH || message.type() == SyncType.PUT) {
      log.debug(
        "Message skipped by sync handler: type={}, key={}, version={}",
        message.type(),
        message.cacheKey(),
        message.version()
      );
      fireOnRefreshSkipped(message);
      return;
    }
    log.debug("Sync removal skipped by handler: type={}, key={}", message.type(), message.cacheKey());
  }

  /**
   * Applies one REFRESH or value-carrying PUT. Both share the same pipeline: an optional
   * lock-free staleness pre-check, one value resolution (authoritative load or payload
   * deserialization), a fallback to the versioned local invalidation when no cachable value
   * exists, and the guarded write.
   *
   * @param batch the reduced batch whose winner is a PUT or REFRESH
   */
  private void applyValue(Batch.Value batch) {
    SyncMessage message = batch.winner();
    String key = message.cacheKey();
    long version = message.version();
    boolean isPut = message.type() == SyncType.PUT;
    // Without a cluster loader a REFRESH has no I/O to avoid: the fallback's compute applies
    // the same version guard inside the bin lock, so the lock-free pre-check would only
    // repeat it. A PUT always guards here: it must not pay deserialization for a pre-image
    // that already lost to the resident entry or an invalidation tombstone.
    if ((isPut || clusterLoader != null) && isStale(key, liveResident(cache.asMap().get(key)), version)) {
      log.debug("Stale {} ignored: key={}, incomingVersion={}", message.type(), key, version);
      fireOnRefreshSkipped(message);
      return;
    }

    Object value = isPut ? resolvePutValue(key, message) : loadAuthoritative(key);
    if (value == null) {
      log.debug("{} yielded no cachable value for key={}, falling back to local invalidation", message.type(), key);
      if (evict(key, version, batch.allowSoftExpire()) && batch.coveredInvalidate() != null) {
        // The covered INVALIDATE's removal was carried out by this fallback; its hook
        // contract must still observe the removal, not just the refresh skip.
        fireAfterInvalidate(batch.coveredInvalidate());
      }
      fireOnRefreshSkipped(message);
      return;
    }

    Applied applied = writePeerValue(key, version, value);
    if (!applied.changed()) {
      fireOnRefreshSkipped(message);
      return;
    }

    log.debug("Applied peer sync value: {}", key);
    fireAfterRefresh(message, applied.current());
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
    Object value = payloadCodec == null ? null : payloadCodec.decode(message.payload());
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
   * @return the images the bin lock observed; {@link Applied#changed()} is
   *         {@code false} when the guards refused the value
   */
  private Applied writePeerValue(String key, long version, Object value) {
    Applied applied = EntryMutations.apply(cache, key, existing -> {
      CacheEntry resident = liveResident(existing);
      if (isStale(key, resident, version)) {
        return existing;
      }
      long hardTtl = resident != null ? resident.getHardTtlMs() : ttlPolicy.effectiveHardTtlMs();
      long softTtl = resident != null ? resident.getSoftTtlMs() : ttlPolicy.effectiveSoftTtlMs();
      return entryLifecycle.writeValue(resident, value, version, hardTtl, softTtl, false);
    });
    if (applied.changed()) {
      invalidateDedupEntry(key);
    }
    return applied;
  }

  /**
   * The shared versioned eviction. The tombstone is recorded in the same compute as the
   * removal, so a concurrent refresh cannot slip between them and re-warm the key. The
   * key's dedup flight is dropped immediately after the tombstone — including an absent
   * key — so a read that starts after the invalidation cannot join a pre-invalidation load;
   * a version guard skip leaves the newer flight alone. Dropping the flight under the bin
   * lock is safe: {@code SingleFlight.invalidate} is a non-blocking map removal and never
   * calls back into the cache.
   *
   * <p>With {@code softExpireWorkerManaged}, a Worker-managed (HOT/COOL) entry survives:
   * this node has no loader, so the entry keeps its decision stamp and TTL regime and only
   * its soft window is closed — the next read reloads through the application reader.
   *
   * @return {@code true} only when a non-null entry was removed from L1
   */
  private boolean evict(String key, long version, boolean softExpireWorkerManaged) {
    boolean[] removed = { false };
    cache
      .asMap()
      .compute(key, (k, existing) -> {
        if (existing != null && VersionGuard.shouldSkipForRefresh(existing, version)) {
          return existing;
        }
        // Recorded even when absent: an in-flight load must not re-warm the key.
        entryLifecycle.recordInvalidation(key, version);
        invalidateDedupEntry(key);
        if (existing == null) {
          return null;
        }
        if (softExpireWorkerManaged && existing.isWorkerManaged()) {
          // One millisecond in the past, so a read in this same millisecond already sees
          // the closed soft window.
          return entryLifecycle.editEntry(existing).softExpiryAt(TimeSource.currentTimeMillis() - 1L).build();
        }
        removed[0] = true;
        return null;
      });
    return removed[0];
  }

  /**
   * Applies a batch of clears: record each watermark first (a concurrent apply that passes
   * its per-key guard on an absent entry must already see it), then run <b>one</b> key-set
   * pass removing each key at the highest stamp that targets it. A prefix clear at or below
   * the batch's table-wide stamp is dropped: the table-wide stamp already removes every key
   * that prefix clear could, so keeping it would only add a redundant watermark and a walk.
   */
  private void applyClear(List<SyncMessage> batch) {
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
    for (Map.Entry<String, CacheEntry> entry : cache.asMap().entrySet()) {
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
      EntryMutations.mutate(cache, key, existing ->
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

  /** The entry the read path would serve, or {@code null} when it is absent or logically expired. */
  @Nullable
  private CacheEntry liveResident(@Nullable CacheEntry entry) {
    return entry != null && !ttlPolicy.isLogicallyExpired(entry) ? entry : null;
  }

  /** Whether the live resident or an applied tombstone already makes {@code version} obsolete. */
  private boolean isStale(String key, @Nullable CacheEntry resident, long version) {
    return (
      (resident != null && VersionGuard.shouldSkipForRefresh(resident, version)) ||
      entryLifecycle.staleAgainstInvalidation(key, version)
    );
  }

  /**
   * Loads the authoritative value through the optional cluster loader; a clean miss, a
   * declined admission and a loader failure all answer {@code null} (design decisions:
   * sync messages).
   */
  @Nullable
  private Object loadAuthoritative(String key) {
    return DecisionPlaneSupport.loadAuthoritative(clusterLoader, key, log, "refresh: load failed for key={}");
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
    DecisionPlaneSupport.fireHooks(hooks, "SyncHook", hookName, key, callback, log);
  }

  /** A peer value was applied to L1 (REFRESH reload or value-carrying PUT). */
  private void fireAfterRefresh(SyncMessage msg, CacheEntry entry) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("afterRefresh", msg.cacheKey(), hook -> hook.afterRefresh(msg, entry));
  }

  /** The entry was removed from L1 in response to a peer INVALIDATE. */
  private void fireAfterInvalidate(SyncMessage msg) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("afterInvalidate", msg.cacheKey(), hook -> hook.afterInvalidate(msg));
  }

  /** A message produced no effect (stale, covered by a newer message, or unloadable). */
  private void fireOnRefreshSkipped(SyncMessage msg) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("onRefreshSkipped", msg.cacheKey(), hook -> hook.onRefreshSkipped(msg));
  }
}
