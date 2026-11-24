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

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.util.Internal;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * The stateful side of the {@link CacheEntry} lifecycle: TOCTOU invalidation guards and the entry
 * factory.
 *
 * <p>Every creation and modification flows through the single {@link EntryDraft} API obtained
 * here. All stateless TTL arithmetic lives in {@link TtlPolicy}, exposed via
 * {@link #ttlPolicy()}.
 */
@Internal
public interface EntryLifecycle {
  /**
   * The pure TTL and expiry policy backing this lifecycle: every stateless computation
   * (override-vs-default resolution, expire timestamps, jitter, expiry predicates) is performed
   * through this module.
   *
   * @return the TTL policy; never null
   */
  TtlPolicy ttlPolicy();

  /**
   * Judges hard expiry and removes the supplied snapshot only when it is still the resident.
   *
   * <p>A concurrent replacement is returned live: its expiry is not this caller's judgment, so
   * the read path must classify the replacement rather than treating the key as a miss. The
   * removal and the re-judgment happen in one bin-lock compute, so a caller never has to re-check
   * the replacement.
   *
   * @param cacheKey the cache key
   * @param raw      the entry the caller read, or {@code null}
   * @param now      the per-call epoch-millis sample
   * @return the surviving resident, or {@code null} when the key is absent
   */
  @Nullable
  CacheEntry evictIfExpired(String cacheKey, @Nullable CacheEntry raw, long now);

  /**
   * Whether a write at {@code version} loses to an invalidation this node already applied — the
   * single anti-resurrection guard shared by every L1 write path and by the sync receiver.
   *
   * <p>The tombstones are core L1 state, not a sync-plane detail: an in-flight read that started
   * before an {@code INVALIDATE}/{@code clearAll} holds an older stamp, and on an absent entry
   * the resident guard accepts any stamp — this predicate is what refuses it. Every removal path
   * must record here, or a slow read pins the pre-invalidation value back into L1 for its whole
   * TTL.
   *
   * <p>A version-less write ({@code <= 0}) is outside the stamp order: only the per-key
   * tombstone is consulted, because the clear and prefix watermarks never expire and ordering an
   * unstamped write against them would refuse every such fill for the rest of the process.
   *
   * @param cacheKey the key being written
   * @param version  the stamp the write would store
   * @return {@code true} when the write predates an applied invalidation
   */
  boolean staleAgainstInvalidation(String cacheKey, long version);

  /**
   * Records a per-key invalidation at {@code version}: writes at or below it are refused from
   * now on.
   *
   * <p>The tombstone is bounded in time and size — it only has to outlive in-flight reads, not
   * the entry's hard TTL. A non-positive stamp records nothing: treating it as "invalidated
   * forever" would block every later fill for the tombstone's whole lifetime.
   *
   * @param cacheKey the invalidated key
   * @param version  the invalidation stamp ({@code <= 0} records nothing)
   */
  void recordInvalidation(String cacheKey, long version);

  /**
   * Records a table-wide clear: every write stamped at or below {@code clearStamp} is refused
   * from now on, whichever key it targets.
   *
   * <p>A version-less write is outside that order and can only be refused by a per-key
   * tombstone. The table stamp and the bounded prefix snapshot must publish together, or a
   * reader could observe a new table stamp with an old prefix map.
   *
   * @param clearStamp the clear stamp ({@code <= 0} records nothing)
   */
  void recordClear(long clearStamp);

  /**
   * Records a namespace clear: every write at or below {@code clearStamp} whose key starts with
   * {@code prefix} is refused from now on.
   *
   * <p>The prefix must be namespace-complete (it ends with the delimiter): a bare name would
   * also refuse writes to sibling namespaces. Prefix watermarks are bounded and self-pruning;
   * past the cap the live set folds into the table-wide watermark at the highest stamp involved,
   * a safe over-approximation.
   *
   * @param prefix     the namespace-complete prefix
   * @param clearStamp the clear stamp ({@code <= 0} records nothing)
   */
  void recordPrefixClear(String prefix, long clearStamp);

  /**
   * Batch form of {@link #recordPrefixClear(String, long)}: applies every prefix as one snapshot
   * swap, so a reader never observes a half-applied clear and the cap is enforced once per batch.
   *
   * @param clears merged prefix to clear stamp
   */
  default void recordPrefixClears(Map<String, Long> clears) {
    clears.forEach(this::recordPrefixClear);
  }

  /**
   * Creates a blank {@link EntryDraft} wired to this lifecycle's jitter-aware arithmetic — the
   * single creation path.
   *
   * @return a blank draft with TTL arithmetic attached
   */
  EntryDraft newEntry();

  /**
   * Creates an {@link EntryDraft} seeded from an existing entry — the single copy-on-write
   * modification path. Untouched fields carry over.
   *
   * @param source the entry to modify (not changed itself)
   * @return a seeded draft with TTL arithmetic attached
   */
  EntryDraft editEntry(CacheEntry source);

  /**
   * Builds a value entry under the caller's resolved TTLs.
   *
   * <p>A new value starts its own lifetime, re-arming both windows from this call; no heat
   * decision participates. An expired resident is treated as absent and never hands its dead
   * deadline to the new value.
   *
   * @param value     the value to store
   * @param version   the data stamp
   * @param hardTtlMs the resolved hard TTL
   * @param softTtlMs the resolved soft TTL
   * @return the entry to store
   */
  CacheEntry writeValue(Object value, long version, long hardTtlMs, long softTtlMs);
}
