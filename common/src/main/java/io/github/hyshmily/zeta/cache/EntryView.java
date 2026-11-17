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

import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import org.jspecify.annotations.Nullable;

/**
 * A read-only snapshot of one L1 entry, handed to {@code SyncPolicy}, {@code WorkerPolicy}
 * and {@code SyncHook}. The snapshot is advisory: the resident may change before the framework
 * applies a decision, so the guarded write path re-checks every version and tombstone.
 *
 * <p>{@link #value()} is the entry's own reference, not a copy. A policy must treat it as
 * read-only; mutating a value in place bypasses the cache's copy-on-write discipline.
 *
 * @param value             the cached value, or {@code null} when the entry carries none
 * @param dataVersion       the data stamp ({@code 0} = unknown)
 * @param keyState          the current hot/cool state, or {@code null} when unset
 * @param softExpireAtMs    the absolute soft-expiry timestamp
 * @param hardExpireAtMs    the absolute hard-expiry timestamp
 * @param softExpired       whether the soft window was closed at snapshot time
 * @param logicallyExpired  whether the hard expiry had passed at snapshot time
 */
public record EntryView(
  @Nullable Object value,
  long dataVersion,
  @Nullable KeyState keyState,
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
      entry.getKeyState(),
      entry.getSoftExpireAtMs(),
      entry.getHardExpireAtMs(),
      ttlPolicy.isSoftExpired(entry, now),
      ttlPolicy.isLogicallyExpired(entry, now)
    );
  }
}
