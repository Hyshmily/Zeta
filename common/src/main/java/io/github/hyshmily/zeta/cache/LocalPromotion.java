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
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.cache.cachesupport.EntryLifecycle;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.sharding.HealthView;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * Local-TopK promotion: the policy that lifts a NORMAL (or degraded COOL)
 * entry to HOT when the local TopK considers its key hot.
 *
 * <p>Extracted from {@link HotKeyCache} so the promotion rules evolve in one
 * place, independently of the cache mechanics around them. Both promotion
 * sites share it:
 * <ul>
 *   <li>the lock-free read path ({@link #promoteIfNeeded}), which runs its
 *       own Caffeine {@code compute}; and</li>
 *   <li>the in-lock fast path ({@link #promoteInLock}), called from inside an
 *       already-held bin {@code compute} — it performs no locking of its own
 *       and must never be wrapped in another {@code compute}.</li>
 * </ul>
 *
 * <p>Worker-managed entries (HOT/COOL stamped by a live Worker) are never
 * touched: cooling them down is the Worker's job via HOT rebroadcast
 * (ADR-0024) and reload-after-expiry. A {@link NullValue} sentinel is never
 * promoted either — it is a NORMAL entry, and promoting one would extend the
 * null block to the hot hard TTL.
 *
 * <p>Thread safety: stateless apart from its collaborators (all thread-safe);
 * safe for concurrent reads/writes with no locking of its own.
 */
@Internal
final class LocalPromotion {

  /** Underlying L1 Caffeine cache storing {@link CacheEntry} slots. */
  private final Cache<String, CacheEntry> caffeineCache;
  /** Local TopK detector (HotKeyDetector) for identifying hot keys. */
  private final HotKeyDetector hotKeyDetector;
  /** Manages entry lifecycle: guards, demotion, the draft factory, value wrapping. */
  private final EntryLifecycle entryLifecycle;
  /** Cached view of Worker cluster health, used for COOL promotion decisions. */
  private final HealthView healthView;

  LocalPromotion(
    Cache<String, CacheEntry> caffeineCache,
    HotKeyDetector hotKeyDetector,
    EntryLifecycle entryLifecycle,
    HealthView healthView
  ) {
    this.caffeineCache = caffeineCache;
    this.hotKeyDetector = hotKeyDetector;
    this.entryLifecycle = entryLifecycle;
    this.healthView = healthView;
  }

  /**
   * Promote a NORMAL or COOL entry to HOT if the local TopK now considers it
   * hot (lock-free read path: runs its own Caffeine {@code compute}).
   *
   * <p>
   * The TTL overrides stay lazy here — the short-circuit below means a plain
   * NORMAL hit never triggers a SpEL evaluation (the {@link ReadPolicy}
   * contract).
   *
   * <p>
   * Guard order is load-bearing for the hot path: the entry-state check
   * ({@code isPromotableState}, one field read) runs <i>before</i> the TopK
   * sketch probe ({@code contains}, fingerprint + hash + table walk), so a
   * steady-state HOT hit — the highest-QPS shape — pays no probe at all, and
   * a COOL entry under a healthy cluster short-circuits on the health bit.
   * The order is behavior-preserving ({@code contains} is a side-effect-free
   * read; promotion needs both predicates) and matches
   * {@link #promoteInLock}'s order.
   *
   * @param cacheKey  the normalized cache key
   * @param raw       the L1 entry just read (may be {@code null})
   * @param hardTtlMs lazy hard TTL override supplier (evaluated only when promoting)
   * @param softTtlMs lazy soft TTL override supplier (evaluated only when promoting)
   * @return {@code true} if the entry was promoted
   */
  boolean promoteIfNeeded(String cacheKey, CacheEntry raw, LongSupplier hardTtlMs, LongSupplier softTtlMs) {
    if (
      raw == null ||
      raw.getValue() == NullValue.INSTANCE ||
      !isPromotableState(raw) ||
      !hotKeyDetector.contains(cacheKey)
    ) {
      return false;
    }
    long hotHard = entryLifecycle.ttlPolicy().resolveEffectiveHotHard(hardTtlMs.getAsLong());
    long hotSoft = entryLifecycle.ttlPolicy().resolveEffectiveHotSoft(softTtlMs.getAsLong());
    return promote(cacheKey, hotHard, hotSoft);
  }

  /**
   * Promotion half of an already-held bin {@code compute} (in-lock fast
   * path): returns the HOT rewrite when the current entry is promotable,
   * otherwise the entry unchanged. Performs no locking of its own.
   *
   * <p>
   * The TTL overrides stay lazy (same contract as {@link #promoteIfNeeded}):
   * the suppliers evaluate only when all three pure guards already passed, so
   * a plain NORMAL hit never triggers a SpEL evaluation.
   *
   * @param current    the entry observed inside the caller's {@code compute}
   * @param cacheKey   the normalized cache key (for the TopK re-check)
   * @param hardTtlMs  lazy hard TTL override supplier (evaluated only when promoting)
   * @param softTtlMs  lazy soft TTL override supplier (evaluated only when promoting)
   * @return the promoted entry, or {@code current} when not promotable
   */
  CacheEntry promoteInLock(CacheEntry current, String cacheKey, LongSupplier hardTtlMs, LongSupplier softTtlMs) {
    if (!isPromotableState(current) || current.getValue() == NullValue.INSTANCE || !hotKeyDetector.contains(cacheKey)) {
      return current;
    }
    long hotHardTtl = entryLifecycle.ttlPolicy().resolveEffectiveHotHard(hardTtlMs.getAsLong());
    long hotSoftTtl = entryLifecycle.ttlPolicy().resolveEffectiveHotSoft(softTtlMs.getAsLong());
    return promotedEntry(current, hotHardTtl, hotSoftTtl);
  }

  /**
   * Atomically promote a cache entry to HOT state in L1, respecting the
   * Worker-managed guard (entries in HOT/COOL state from the Worker are
   * left untouched).
   *
   * @param cacheKey the key to promote
   * @param hotHard  the hot-entry hard TTL
   * @param hotSoft  the hot-entry soft TTL
   * @return {@code true} when this call transitioned the entry to HOT
   */
  private boolean promote(String cacheKey, long hotHard, long hotSoft) {
    AtomicBoolean promoted = new AtomicBoolean();
    caffeineCache
      .asMap()
      .compute(cacheKey, (k, existing) -> {
        if (existing != null) {
          if (!isPromotableState(existing) || !hotKeyDetector.contains(k)) {
            return existing;
          }
          promoted.set(true);
          return promotedEntry(existing, hotHard, hotSoft);
        }
        return null;
      });
    // Trimmed to the transition itself: promoted is set only when this call
    // rewrites the entry — an entry already HOT under a concurrent promotion
    // is not reported as promoted (the old state-based detection returned
    // true for it, spuriously flagging a "promotion" that never happened).
    return promoted.get();
  }

  /**
   * The local-promotion rewrite: hot TTLs with computed expire timestamps,
   * state → HOT; the normal baseline carries over from the source entry for
   * later demotion. Shared by the lock-free read path ({@link #promote}) and
   * the in-lock fast path ({@link #promoteInLock}).
   *
   * @param current    the entry to promote (not modified)
   * @param hotHardTtl the resolved hot-entry hard TTL
   * @param hotSoftTtl the resolved hot-entry soft TTL
   * @return the promoted entry to store back
   */
  private CacheEntry promotedEntry(CacheEntry current, long hotHardTtl, long hotSoftTtl) {
    return entryLifecycle.editEntry(current).ttl(hotHardTtl, hotSoftTtl).keyState(KeyState.HOT).build();
  }

  /**
   * Whether the given {@link KeyState} is eligible for local promotion to HOT.
   * <p>
   * {@link KeyState#NORMAL} entries are always eligible: when the local TopK
   * detects them as hot, they get promoted to HOT with longer TTLs.
   * <p>
   * {@link KeyState#COOL} entries are only eligible when no Worker shard is
   * alive. This provides graceful degradation — when the Worker cluster is
   * unavailable, the local TopK drives TTL decisions instead of preserving
   * stale Worker verdicts. Once a Worker comes back online and broadcasts a
   * new decision, it overrides the local promotion via
   * {@code decisionVersion} comparison.
   * <p>
   * {@link KeyState#HOT} entries are never eligible — they already have the
   * longest TTLs.
   *
   * @param cacheEntry the cache entry to inspect
   * @return {@code true} if the entry may be promoted by local TopK
   */
  private boolean isPromotableState(CacheEntry cacheEntry) {
    KeyState state = cacheEntry.getKeyState();
    return state == KeyState.NORMAL || (state == KeyState.COOL && !healthView.isClusterHealthy());
  }
}
