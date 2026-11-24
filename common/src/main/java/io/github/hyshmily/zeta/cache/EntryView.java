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
package io.github.hyshmily.zeta.cache;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.QuietCache;
import io.github.hyshmily.zeta.util.TimeSource;
import org.jspecify.annotations.Nullable;

/**
 * A read-only snapshot of one L1 entry, handed to {@code SyncPolicy}, {@code SyncHook} and
 * {@code HotKeyContext}. The snapshot is advisory: the resident may change before the framework
 * applies a decision, so the guarded write path re-checks every version and tombstone.
 *
 * <p>{@link #value()} is the entry's own reference, not a copy. A policy must treat it as
 * read-only; mutating a value in place bypasses the cache's copy-on-write discipline.
 *
 * @param value             the cached value, or {@code null} when the entry carries none
 * @param dataVersion       the data stamp ({@code 0} = unknown)
 * @param softExpireAtMs    the absolute soft-expiry timestamp
 * @param hardExpireAtMs    the absolute hard-expiry timestamp
 * @param softExpired       whether the soft window was closed at snapshot time
 * @param logicallyExpired  whether the hard expiry had passed at snapshot time
 */
public record EntryView(
  @Nullable Object value,
  long dataVersion,
  long softExpireAtMs,
  long hardExpireAtMs,
  boolean softExpired,
  boolean logicallyExpired
) {
  /**
   * Snapshots one entry against a single clock sample, or returns {@code null} when the
   * entry is absent.
   *
   * @param entry     the resident entry, or {@code null}
   * @param ttlPolicy the TTL policy used for the expiry judgments
   * @param now       the epoch-millis sample shared by both judgments
   * @return the snapshot, or {@code null} when {@code entry} is {@code null}
   */
  public static @Nullable EntryView of(@Nullable CacheEntry entry, TtlPolicy ttlPolicy, long now) {
    if (entry == null) {
      return null;
    }
    return new EntryView(
      entry.getValue(),
      entry.getDataVersion(),
      entry.getSoftExpireAtMs(),
      entry.getHardExpireAtMs(),
      ttlPolicy.isSoftExpired(entry, now),
      ttlPolicy.isLogicallyExpired(entry, now)
    );
  }

  /**
   * Snapshots a resident key against one clock sample: the cache-side form of {@link #of}, shared
   * by the planes that read an entry without owning the table.
   *
   * @param cache     the L1 table
   * @param key       the key to snapshot
   * @param ttlPolicy the expiry arithmetic
   * @return the snapshot, or {@code null} when the key is absent
   */
  public static @Nullable EntryView snapshot(Cache<String, CacheEntry> cache, String key, TtlPolicy ttlPolicy) {
    return of(QuietCache.getIfPresent(cache, key), ttlPolicy, TimeSource.currentTimeMillis());
  }

  /**
   * Whether a snapshot represents a servable entry: present and not past its hard deadline. The
   * null-safe form of {@code view != null && !view.logicallyExpired()}, so a policy does not
   * repeat the pair — and cannot get it wrong by testing only one half.
   *
   * @param view the snapshot, or {@code null} for an absent key
   * @return whether the entry is servable
   */
  public static boolean isLive(@Nullable EntryView view) {
    return view != null && !view.logicallyExpired();
  }
}
