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
package io.github.hyshmily.zeta.cache.cachesupport.lifecycle;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Default {@link EntryLifecycle} implementation managing the stateful side of {@link CacheEntry}
 * lifecycles: logical-expiry eviction, TOCTOU invalidation guards (per-key tombstones, bounded
 * prefix clear watermarks, and table-wide clear watermarks), and {@link EntryDraft} creation.
 */
@Slf4j
@Internal
public class EntryLifecycleImpl implements EntryLifecycle {

  /** Upper bound on live prefix watermarks before expired pruning or table-wide folding. */
  private static final int MAX_PREFIX_WATERMARKS = 64;

  private final Cache<String, CacheEntry> caffeineCache;
  private final ZetaProperties.CacheConfig cacheConfig;
  private final TtlPolicy ttlPolicy;
  private final Object watermarkLock = new Object();
  private final Cache<String, Long> invalidatedAt;
  private volatile ClearWatermarks watermarks = ClearWatermarks.EMPTY;

  /**
   * Creates an entry lifecycle manager bound to the given L1 cache and configuration properties.
   *
   * @param caffeineCache the L1 Caffeine cache
   * @param ttlConfig     root Zeta properties supplying TTL and tombstone configuration
   */
  public EntryLifecycleImpl(Cache<String, CacheEntry> caffeineCache, ZetaProperties ttlConfig) {
    this.caffeineCache = Objects.requireNonNull(caffeineCache, "caffeineCache must not be null");
    Objects.requireNonNull(ttlConfig, "ttlConfig must not be null");
    this.cacheConfig = Objects.requireNonNull(ttlConfig.getCache(), "cacheConfig must not be null");
    this.ttlPolicy = new TtlPolicy(ttlConfig);
    this.invalidatedAt = Caffeine.newBuilder()
      .maximumSize(cacheConfig.getTombstoneMaximumSize())
      .expireAfterWrite(cacheConfig.getTombstoneTtlSeconds(), TimeUnit.SECONDS)
      .ticker(() -> TimeUnit.MILLISECONDS.toNanos(TimeSource.monotonicMillis()))
      .build();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Eviction executes under the key's map bin lock and re-checks the live resident so a
   * concurrent valid replacement is returned rather than evicted along with the stale snapshot.
   *
   * @param cacheKey the physical cache key
   * @param raw      the observed cache entry snapshot, or {@code null}
   * @param now      the current monotonic timestamp in milliseconds
   * @return the live non-expired entry, or {@code null} if absent or evicted
   */
  @Override
  @SuppressWarnings("all")
  public @Nullable CacheEntry evictIfExpired(String cacheKey, @Nullable CacheEntry raw, long now) {
    if (raw == null || !ttlPolicy.isLogicallyExpired(raw, now)) {
      return raw;
    }
    CacheEntry live = caffeineCache
      .asMap()
      .computeIfPresent(cacheKey, (key, existing) ->
        existing == raw || ttlPolicy.isLogicallyExpired(existing, now) ? null : existing
      );
    if (live == null) {
      log.debug("Cache entry logically expired during processing, reloading: {}", cacheKey);
    }
    return live;
  }

  /**
   * Builds a value entry using the caller's resolved hard and soft TTLs.
   *
   * @param value     the value to store
   * @param version   the write version stamp
   * @param hardTtlMs the resolved hard TTL in milliseconds
   * @param softTtlMs the resolved soft TTL in milliseconds
   * @return the built {@link CacheEntry}
   */
  @Override
  public CacheEntry writeValue(Object value, long version, long hardTtlMs, long softTtlMs) {
    return newEntry().value(value).version(version).ttl(hardTtlMs, softTtlMs).build();
  }

  /**
   * Creates a blank {@link EntryDraft} wired to this lifecycle's jitter-aware {@link TtlPolicy}.
   *
   * @return a blank entry draft
   */
  @Override
  public EntryDraft newEntry() {
    return EntryDraft.blank(ttlPolicy);
  }

  /**
   * Creates an {@link EntryDraft} seeded from an existing entry and wired to this lifecycle's
   * {@link TtlPolicy}.
   *
   * @param source the entry to copy
   * @return a seeded entry draft
   */
  @Override
  public EntryDraft editEntry(CacheEntry source) {
    return EntryDraft.of(source, ttlPolicy);
  }

  /**
   * Returns the stateless TTL and logical-expiry policy owned by this lifecycle.
   *
   * @return the {@link TtlPolicy}
   */
  @Override
  public TtlPolicy ttlPolicy() {
    return ttlPolicy;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Checks the table-wide clear watermark first, then the bounded prefix snapshot, and finally
   * the per-key tombstone. A non-positive version consults only the per-key tombstone.
   *
   * @param key     the physical cache key
   * @param version the candidate write's version stamp
   * @return {@code true} if an invalidation or clear supersedes {@code version}
   */
  @Override
  public boolean staleAgainstInvalidation(String key, long version) {
    if (version <= 0L) {
      return invalidatedAt.getIfPresent(key) != null;
    }
    ClearWatermarks snapshot = watermarks;
    if (version <= snapshot.clearStamp()) {
      return true;
    }
    if (version <= snapshot.maxPrefixStamp()) {
      for (Map.Entry<String, Long> mark : snapshot.prefixes().entrySet()) {
        if (version <= mark.getValue() && key.startsWith(mark.getKey())) {
          return true;
        }
      }
    }
    Long tombstone = invalidatedAt.getIfPresent(key);
    return tombstone != null && version <= tombstone;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Non-positive stamps are ignored because recording them would block subsequent fills for the
   * full tombstone TTL window.
   *
   * @param key     the invalidated physical key
   * @param version the invalidation stamp
   */
  @Override
  public void recordInvalidation(String key, long version) {
    if (version <= 0L) {
      return;
    }
    Long prev = invalidatedAt.getIfPresent(key);
    if (prev != null && prev >= version) {
      return;
    }
    invalidatedAt.asMap().merge(key, version, Math::max);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Replayed or already-covered prefix clears return immediately without allocating a map or
   * acquiring {@link #watermarkLock}.
   *
   * @param prefix     the physical namespace prefix
   * @param clearStamp the clear stamp
   */
  @Override
  public void recordPrefixClear(String prefix, long clearStamp) {
    ClearWatermarks snapshot = watermarks;
    if (prefix == null || prefix.isEmpty() || clearStamp <= snapshot.clearStamp()) {
      return;
    }
    Long prev = snapshot.prefixes().get(prefix);
    if (prev != null && prev >= clearStamp) {
      return;
    }
    recordPrefixClears(Map.of(prefix, clearStamp));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Applies all prefix clears in a single immutable snapshot swap so readers never observe a
   * partially applied batch and {@link #MAX_PREFIX_WATERMARKS} is enforced at most once per batch.
   *
   * @param clears map from physical prefix to clear stamp
   */
  @Override
  public void recordPrefixClears(Map<String, Long> clears) {
    if (clears.isEmpty()) {
      return;
    }
    synchronized (watermarkLock) {
      ClearWatermarks snapshot = watermarks;
      long tableWatermark = snapshot.clearStamp();
      Map<String, Long> previous = snapshot.prefixes();
      Map<String, Long> updated = new HashMap<>(previous);
      boolean changed = updated.values().removeIf(stamp -> stamp <= tableWatermark);
      changed |= mergePrefixClears(updated, clears, tableWatermark);
      if (!changed) {
        return;
      }

      if (updated.size() > MAX_PREFIX_WATERMARKS) {
        pruneExpiredPrefixes(updated, previous, clears, tableWatermark);
        if (updated.size() > MAX_PREFIX_WATERMARKS) {
          long foldStamp = maxStamp(updated.values(), tableWatermark);
          watermarks = ClearWatermarks.tableOnly(Math.max(tableWatermark, foldStamp));
          log.debug(
            "Prefix clear watermarks saturated at {} with {} still live; folding into the table-wide watermark at stamp {}",
            MAX_PREFIX_WATERMARKS,
            updated.size(),
            foldStamp
          );
          return;
        }
      }

      watermarks = ClearWatermarks.of(tableWatermark, updated);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Publishes the updated table-wide watermark and pruned prefix map atomically under
   * {@link #watermarkLock} so readers never observe a mismatched pair.
   *
   * @param clearStamp the table-wide clear stamp
   */
  @Override
  public void recordClear(long clearStamp) {
    if (clearStamp <= 0L || clearStamp <= watermarks.clearStamp()) {
      return;
    }
    synchronized (watermarkLock) {
      ClearWatermarks snapshot = watermarks;
      if (clearStamp <= snapshot.clearStamp()) {
        return;
      }
      Map<String, Long> current = snapshot.prefixes();
      if (current.isEmpty() || clearStamp >= snapshot.maxPrefixStamp()) {
        watermarks = ClearWatermarks.tableOnly(clearStamp);
        return;
      }
      Map<String, Long> pruned = new HashMap<>(current);
      if (!pruned.values().removeIf(stamp -> stamp <= clearStamp)) {
        watermarks = new ClearWatermarks(clearStamp, current, snapshot.maxPrefixStamp());
        return;
      }
      watermarks = ClearWatermarks.of(clearStamp, pruned);
    }
  }

  /**
   * Merges incoming prefix clear stamps into {@code target}, ignoring blank prefixes, stamps
   * already covered by {@code tableWatermark}, and non-advancing stamps.
   *
   * @param target         the mutable prefix map being updated
   * @param clears         the incoming batch of prefix clears
   * @param tableWatermark the current table-wide clear watermark
   * @return {@code true} if at least one entry in {@code target} was inserted or advanced
   */
  private static boolean mergePrefixClears(Map<String, Long> target, Map<String, Long> clears, long tableWatermark) {
    boolean changed = false;
    for (Map.Entry<String, Long> clear : clears.entrySet()) {
      String prefix = clear.getKey();
      Long clearStamp = clear.getValue();
      if (prefix == null || prefix.isEmpty() || clearStamp == null || clearStamp <= tableWatermark) {
        continue;
      }
      Long existing = target.get(prefix);
      if (existing != null && existing >= clearStamp) {
        continue;
      }
      target.put(prefix, clearStamp);
      changed = true;
    }
    return changed;
  }

  /**
   * Removes prefix watermarks older than the tombstone TTL window, preserving any entries that
   * were freshly inserted or advanced by the current {@code clears} batch.
   *
   * @param updated        the candidate prefix map exceeding {@link #MAX_PREFIX_WATERMARKS}
   * @param previous       the prefix map prior to this batch
   * @param clears         the current batch of prefix clears
   * @param tableWatermark the current table-wide clear watermark
   */
  private void pruneExpiredPrefixes(
    Map<String, Long> updated,
    Map<String, Long> previous,
    Map<String, Long> clears,
    long tableWatermark
  ) {
    long expireAtMs = TimeSource.currentTimeMillis() - TimeUnit.SECONDS.toMillis(cacheConfig.getTombstoneTtlSeconds());
    updated
      .entrySet()
      .removeIf(mark -> {
        Long recorded = clears.get(mark.getKey());
        boolean recordedHere =
          recorded != null && recorded > tableWatermark && recorded > previous.getOrDefault(mark.getKey(), 0L);
        return !recordedHere && StampClock.timestampOf(mark.getValue()) <= expireAtMs;
      });
  }

  /**
   * Returns the maximum stamp in {@code stamps}, or {@code defaultValue} if the collection is empty.
   *
   * @param stamps       the stamps to inspect
   * @param defaultValue the fallback value when {@code stamps} is empty
   * @return the maximum stamp value
   */
  private static long maxStamp(Collection<Long> stamps, long defaultValue) {
    if (stamps.isEmpty()) {
      return defaultValue;
    }
    long max = Long.MIN_VALUE;
    for (Long stamp : stamps) {
      if (stamp > max) {
        max = stamp;
      }
    }
    return max;
  }

  /**
   * Immutable snapshot of table-wide and prefix-scoped clear watermarks.
   *
   * @param clearStamp     table-wide clear stamp ({@code 0} when none recorded)
   * @param prefixes       immutable map of live prefix watermarks strictly greater than {@code clearStamp}
   * @param maxPrefixStamp highest stamp present in {@code prefixes}, or {@code 0} when empty
   */
  private record ClearWatermarks(long clearStamp, Map<String, Long> prefixes, long maxPrefixStamp) {
    static final ClearWatermarks EMPTY = new ClearWatermarks(0L, Map.of(), 0L);

    /**
     * Creates a watermark snapshot carrying only a table-wide clear stamp and no prefix entries.
     *
     * @param clearStamp the table-wide clear stamp
     * @return {@link #EMPTY} when {@code clearStamp == 0L}, otherwise a table-only snapshot
     */
    private static ClearWatermarks tableOnly(long clearStamp) {
      return clearStamp == 0L ? EMPTY : new ClearWatermarks(clearStamp, Map.of(), 0L);
    }

    /**
     * Creates an immutable watermark snapshot from a table-wide stamp and a map of live prefixes.
     *
     * @param clearStamp the table-wide clear stamp
     * @param prefixes   the live prefix watermarks
     * @return the immutable {@link ClearWatermarks} snapshot
     */
    private static ClearWatermarks of(long clearStamp, Map<String, Long> prefixes) {
      if (prefixes.isEmpty()) {
        return tableOnly(clearStamp);
      }
      return new ClearWatermarks(clearStamp, Map.copyOf(prefixes), maxStamp(prefixes.values(), 0L));
    }
  }
}
