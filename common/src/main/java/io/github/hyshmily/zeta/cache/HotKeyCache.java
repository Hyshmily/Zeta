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
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.cachesupport.TransactionSupport;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations;
import io.github.hyshmily.zeta.cache.cachesupport.raw.LocalPromotion;
import io.github.hyshmily.zeta.cache.cachesupport.raw.QuietCache;
import io.github.hyshmily.zeta.cache.cachesupport.raw.ReadContext;
import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.exception.ZetaSuppressedException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Core orchestration class for hot-key caching.
 * <p>
 * Manages L1 (Caffeine) operations with hot-key awareness, single
 * data-stamp space ({@code dataVersion} for data mutation ordering, orthogonal
 * to {@code decisionVersion} for Worker HOT/COOL decision ordering),
 * SingleFlight deduplication, soft-expire (stale-while-revalidate), and
 * cross-instance synchronization via {@link CacheSyncPublisher}.
 * All write operations are transaction-aware via {@link TransactionSupport}.
 *
 * <p>
 * <b>Key validation lives at the boundary, not here.</b> Every caller of this
 * engine delivers validated keys: the facade asserts its single-key arguments
 * and filters its collections ({@link CacheKeysPolicy#validKeys}), and the
 * annotation interceptor validates the namespace and SpEL-resolved keys it
 * supplies. Every method therefore assumes non-null, non-blank keys —
 * single-key
 * and collection alike. The facade likewise resolves TTL overrides and never
 * combines a permanent hard TTL with a broadcast, so the write paths here take
 * both as given.
 */
@Slf4j
@Internal
public class HotKeyCache {

  /** Underlying L1 Caffeine cache storing {@link CacheEntry} slots. */
  private final Cache<String, CacheEntry> caffeineCache;

  /** Background soft-expire refresh executor (scheduler seam). */
  private final BackgroundRefresher backgroundRefresher;

  /** Local TopK detector (HotKeyDetector) for identifying hot keys. */
  private final HotKeyDetector hotKeyDetector;

  /** Manages entry lifecycle: guards, demotion and the draft factory. */
  private final EntryLifecycle entryLifecycle;

  /** Central dispatcher: access counting and peer broadcasts. */
  private final CentralDispatcher dispatcher;

  /** Deduplicator preventing concurrent in-flight loads for the same key. */
  private final SingleFlight singleFlight;

  /**
   * Allocates {@code dataVersion} stamps for this node (design decisions: version
   * stamps).
   */
  private final StampClock stampClock;

  /**
   * The stateless TTL/expiry arithmetic, resolved once from the lifecycle it
   * belongs to.
   */
  private final TtlPolicy ttlPolicy;

  /**
   * Internal probes: stats-quiet and policy-quiet when Caffeine's policy is
   * available.
   */
  private final QuietCache<String, CacheEntry> quietCache;

  /**
   * Request-level hit/miss counters behind {@link #stats()}: incremented once
   * per served key on the load-capable read paths ({@code readThrough},
   * {@code batchGet}). A key served from L1 without invoking the reader counts
   * as a hit; a key that reaches the loader counts as a miss — unlike the raw
   * Caffeine counters, internal {@code asMap().compute()} maintenance writes
   * never touch these. {@code peek()} probes are excluded.
   */
  private final LongAdder requestMisses = new LongAdder();
  private final LongAdder requestHits = new LongAdder();

  /**
   * Local-TopK promotion collaborator (see {@link LocalPromotion}): owns
   * event-driven NORMAL promotion and the degraded-mode COOL promotion that
   * remains on the read path. Composed here from the same collaborators, not
   * taken as a constructor parameter.
   */
  private final LocalPromotion promotion;

  /**
   * Creates the cache orchestrator; {@link LocalPromotion} is composed
   * internally from the same collaborators.
   *
   * @param hotKeyDetector      local TopK detector
   * @param caffeineCache       underlying L1 Caffeine cache
   * @param singleFlight        load deduplication layer
   * @param entryLifecycle      entry lifecycle manager
   * @param backgroundRefresher background soft-expire refresh executor
   * @param dispatcher          central send/report dispatch
   * @param stampClock          data-version stamp allocation
   * @param healthView          cluster health view for Decision-Validity demotion
   */
  public HotKeyCache(
      HotKeyDetector hotKeyDetector,
      Cache<String, CacheEntry> caffeineCache,
      SingleFlight singleFlight,
      EntryLifecycle entryLifecycle,
      BackgroundRefresher backgroundRefresher,
      CentralDispatcher dispatcher,
      StampClock stampClock,
      HealthView healthView) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.stampClock = Objects.requireNonNull(stampClock, "stampClock");
    this.singleFlight = Objects.requireNonNull(singleFlight, "singleFlight");
    this.caffeineCache = Objects.requireNonNull(caffeineCache, "caffeineCache");
    this.hotKeyDetector = Objects.requireNonNull(hotKeyDetector, "hotKeyDetector");
    this.entryLifecycle = Objects.requireNonNull(entryLifecycle, "entryLifecycle");
    this.backgroundRefresher = Objects.requireNonNull(backgroundRefresher, "backgroundRefresher");
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.quietCache = new QuietCache<>(this.caffeineCache);
    this.promotion = new LocalPromotion(
        this.caffeineCache,
        this.hotKeyDetector,
        this.entryLifecycle,
        Objects.requireNonNull(healthView, "healthView"));
    this.hotKeyDetector.setAdmittedSink(this.promotion::onAdmitted);
    // The lifecycle owns demotion; the promoter owns re-promotion. One sink
    // wiring here covers the read paths AND the background sweeper — a
    // sweeper-demoted key that is still locally hot re-queues exactly like a
    // read-path demotion (the sweeper's own call site has no promoter handle).
    this.entryLifecycle.setDemotedSink(this.promotion::onDemoted);
  }

  /**
   * Drain TopK admission events and promote resident NORMAL entries. The
   * scheduling layer calls this off the read path; {@code onAdmitted} only
   * enqueues, so detection never performs a cache write under its lock.
   *
   * @return the number of entries transitioned to HOT
   */
  public int promoteAdmitted() {
    return promotion.promoteAdmitted();
  }

  /**
   * Check whether a key is currently local-hot: resident in L1 with
   * {@link KeyState#HOT} and not logically expired, or tracked by the local
   * TopK detector (promotable but not yet promoted).
   *
   * @param cacheKey the key to inspect
   * @return {@code true} if the key is L1-HOT or detector-tracked
   */
  public boolean isHot(String cacheKey) {
    CacheEntry entry = quietCache.getIfPresent(cacheKey);

    return ((entry != null && !ttlPolicy.isLogicallyExpired(entry) && entry.getKeyState() == KeyState.HOT) ||
        hotKeyDetector.contains(cacheKey));
  }

  /**
   * Record a cache access as potentially hot, reporting it to the Worker
   * without performing a cache read.
   *
   * <p>
   * This is essentially a fire-and-forget hint: it updates the local
   * HeavyKeeper sketch and enqueues the key for batch reporting to the
   * Worker cluster, exactly as {@code get()} would, but without touching
   * the L1 cache or invoking any reader/loader. Useful when the caller
   * already has the value in hand (e.g. from a write path or an external
   * source) and wants the hot-key detector and Worker to know about the
   * access without a redundant cache lookup.
   *
   * @param cacheKey the key to record
   */
  public void recordAccess(String cacheKey) {
    recordAccess(cacheKey, Zeta.TagMode.RECORD);
  }

  /**
   * Record a cache access with an explicit recording mode.
   *
   * @param cacheKey the key to record
   * @param mode     how much of the pipeline to run (never {@code null})
   */
  public void recordAccess(String cacheKey, Zeta.TagMode mode) {
    dispatcher.recordAccess(cacheKey, mode);
  }

  /**
   * Record {@code add} accesses in one call, on the normal pipeline (local
   * detection <b>and</b> Worker reporting). The counts travel through the
   * shared access counter's tide, so a batch replay behaves exactly like the
   * equivalent number of {@link #recordAccess(String)} calls.
   *
   * @param cacheKey the key to record
   * @param count    the number of accesses to add (must be positive; the
   *                 facade rejects non-positive counts)
   */
  public void recordAccess(String cacheKey, long count) {
    dispatcher.recordAccess(cacheKey, count);
  }

  /**
   * Look up a cached value without loading or triggering hot-key detection.
   * Unlike {@link #get}, this method never invokes the reader or SingleFlight.
   * It still honours the hard-TTL bound (a logically expired entry resolves
   * to {@code null}) — so it is free of load/detection/report side effects.
   *
   * @param cacheKey the key to inspect
   * @return the raw cached value, or {@code null} on a miss (null = miss
   *         contract, same as {@link #get})
   */
  public <T> T peek(String cacheKey) {
    return unwrap(peekEntry(cacheKey));
  }

  /**
   * The peek's L1 probe: the live entry, or {@code null} on a miss or a logically
   * expired hit
   * (which is evicted as a side effect).
   *
   * @param key the key to probe
   * @return the live entry, or {@code null}
   */
  private CacheEntry peekEntry(String key) {
    CacheEntry entry = caffeineCache.getIfPresent(key);
    if (entry == null) {
      return null;
    }
    // evictIfExpired judges a concurrent replacement against the same clock
    // sample, so a non-null return is live and needs no second check. Expiry is
    // not a mutation: the dedup flight is left alone because a concurrent load
    // is fresh data and the version guards already reject anything older.
    return entryLifecycle.evictIfExpired(key, entry, TimeSource.currentTimeMillis());
  }

  /**
   * Get with a plain reader — the convenience form with {@code isCacheable}
   * always true (everything non-null is cached). To serve-but-not-store
   * specific values, use {@link #get(String, CacheLoader, GetPolicy)} with a
   * loader whose {@code isCacheable} declines them.
   *
   * <p>
   * The TTL suppliers are evaluated lazily — at most once per call, and only
   * when an entry is created, promoted, or a soft-expire refresh is scheduled;
   * never on a plain NORMAL-entry hit (the {@link GetPolicy} contract), so
   * SpEL-based TTL expressions cost nothing on the hit path.
   * <p>
   * A {@code null} reader result is never cached: no entry is written and the
   * next call re-invokes the reader.
   * <p>
   * {@link StalePolicy#REVALIDATE} is honored on the hit path: a soft-expired
   * entry is reloaded and replaced; a worker-managed entry keeps its TTL
   * regime while the reloaded value is written (see {@link #loadCacheEntry}).
   * {@link StalePolicy#RETURN} serves the cached value without refresh
   * handling; {@link StalePolicy#SOFT_REFRESH} (the default) serves it and
   * arms a background refresh via {@link #refreshSoftExpire} (no-op when the
   * reader is {@code null}).
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses (may be {@code null}
   *                 when no load is desired — a miss then resolves to
   *                 {@code null} with no entry write)
   * @param options  the resolved per-invocation read options (TTL overrides,
   *                 stale policy, report-allow and failure semantics)
   * @param <T>      the value type
   * @return the cached or loaded value; {@code null} = miss (reader returned
   *         {@code null}) — null is never cached
   */
  public <T> T get(String cacheKey, Supplier<T> reader, GetPolicy options) {
    return readThrough(cacheKey, reader, null, options);
  }

  /**
   * Get with a typed loader: the loader (including its {@code isCacheable}
   * admission decision) travels with the call. A rejected value is still
   * served to the caller but leaves no entry, while detection and Worker
   * reporting continue unchanged (the rejected key still counts toward
   * hot-key statistics).
   *
   * <p>
   * The engine calls {@link CacheLoader#load} for a foreground miss on this
   * thread and {@link CacheLoader#reload} for a background refresh, so a loader
   * carrying caller-bound state never resumes it from a refresh.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value loader for cache misses / refreshes
   * @param options  the resolved per-invocation read options
   * @param <T>      the value type
   * @return the cached or loaded value; {@code null} = miss
   */
  public <T> T get(String cacheKey, CacheLoader<String, T> loader, GetPolicy options) {
    return readThrough(cacheKey, null, loader, options);
  }

  /**
   * Shared body of the single-key read path. The options'
   * {@link StalePolicy} governs a soft-expired hit directly —
   * {@code SOFT_REFRESH} arms a background refresh, {@code REVALIDATE} blocks
   * on a reload, {@code RETURN} serves as-is.
   *
   * <p>
   * The TTL suppliers are evaluated lazily — at most once per call, and only
   * when an entry is created, promoted, or a soft-expire refresh is scheduled;
   * never on a plain NORMAL-entry hit (the {@link GetPolicy} contract), so
   * SpEL-based TTL expressions cost nothing on the hit path.
   *
   * @param key     the key to retrieve
   * @param reader  the value supplier for cache misses (may be
   *                {@code null} — a miss then resolves to {@code null}
   *                with no entry write)
   * @param loader  the typed loader when the call carries one; mutually
   *                exclusive with {@code reader} and built into a
   *                {@link ReadContext} only when the read needs one
   * @param options the resolved per-invocation read options (TTLs,
   *                stale policy and report-allow flag)
   * @param <T>     the value type
   * @return the cached (possibly stale) or loaded value; {@code null} = miss
   */
  private <T> T readThrough(
      String key,
      @Nullable Supplier<T> reader,
      @Nullable CacheLoader<String, T> loader,
      GetPolicy options) {
    boolean report = options.report();
    CacheEntry entry = caffeineCache.getIfPresent(key);
    if (entry == null) {
      requestMisses.increment();
      return loadAndCache(key, readContext(key, reader, loader, options));
    }

    // One clock sample serves the classification and the reconciliation: a plain
    // hit pays one TimeSource read.
    long now = TimeSource.currentTimeMillis();
    HitClassification hit = classifyHit(key, entry, StalePolicy.orDefault(options.stalePolicy()), now);
    if (hit.isPlainServe()) {
      T value = servePlain(key, hit.entry(), report);
      if (value != null) {
        requestHits.increment();
        return value;
      }
    } else if (hit.needsReconcile()) {
      // A reconciliation may consume this call's memoized TTLs and still end in a
      // reload; the fallback load reuses the same context, so one call never
      // evaluates the suppliers twice.
      ReadContext ctx = readContext(key, reader, loader, options);
      if (hit.reload()) {
        // Apply the state correction (notably a Decision-Validity demotion) before the
        // reload
        // decides the write regime; the reload itself records the single access and
        // hit/miss.
        reconcileStateOnly(key, hit, ctx);
        requestMisses.increment();
        return loadAndCache(key, ctx);
      }
      T value = reconcileHit(key, hit, ctx, now);
      if (value != null) {
        requestHits.increment();
        return value;
      }
      requestMisses.increment();
      return loadAndCache(key, ctx);
    }
    requestMisses.increment();
    return loadAndCache(key, readContext(key, reader, loader, options));
  }

  /**
   * Builds the per-call context only when a read actually needs it: a plain hit
   * returns
   * before this method is called, so the loader-backed path allocates neither
   * lambda on
   * the hit path.
   */
  private <T> ReadContext readContext(
      String key,
      @Nullable Supplier<T> reader,
      @Nullable CacheLoader<String, T> loader,
      GetPolicy options) {
    return loader == null ? ReadContext.of(reader, options) : ReadContext.ofLoader(loader, key, options);
  }

  /**
   * Serves a {@link HitAction#NONE} entry that needs no refresh: the value is
   * read once and the access is recorded only when a value is actually served —
   * a null payload is not a read.
   *
   * @param key    the key being read
   * @param entry  the resident entry (never {@code null})
   * @param report whether this access feeds the Worker reporting plane
   * @param <T>    the value type
   * @return the served value, or {@code null} when the entry carries no value
   */
  @Nullable
  private <T> T servePlain(String key, CacheEntry entry, boolean report) {
    T value = unwrap(entry);
    if (value != null) {
      dispatcher.recordAccess(key, report);
    }
    return value;
  }

  /**
   * What one resident entry means for the current read, decided once by
   * {@link #classifyHit}.
   */
  private enum HitAction {
    /** Hard-expired (or carries a null value): evicted, caller reloads. */
    EXPIRED,
    /** No state transition required; may still arm a background soft refresh. */
    NONE,
    /** Worker-HOT decision is no longer authoritative: demote to NORMAL. */
    DEMOTE,
    /** Local override HOT with a COOL lease and healthy cluster: reassert COOL. */
    REASSERT,
    /**
     * Degraded-mode COOL (or NORMAL with COOL lease) that is locally hot: promote
     * to HOT.
     */
    PROMOTE,
  }

  /**
   * The classification plus the resident it applies to (possibly a concurrent
   * replacement),
   * whether a {@code SOFT_REFRESH} hit must arm a refresh, and whether the caller
   * must reload
   * after applying any state correction.
   */
  private record HitClassification(@Nullable CacheEntry entry, HitAction action, boolean refresh, boolean reload) {
    static final HitClassification EXPIRED_HIT = new HitClassification(null, HitAction.EXPIRED, false, false);

    boolean isPlainServe() {
      return action == HitAction.NONE && !refresh && !reload;
    }

    boolean needsReconcile() {
      return action != HitAction.EXPIRED && !isPlainServe();
    }
  }

  /**
   * The single classification both read paths share: an entry is judged once
   * against hard expiry, the per-call stale policy, its key state and the two
   * read-path corrections, so the single-key and batch probes cannot drift.
   *
   * <p>
   * A hard-expired entry is evicted here, under the identity guard (a
   * concurrent replacement is left alone). Expiry is not a mutation: the dedup
   * flight is untouched, because a load already in flight is fresh. A concurrent
   * replacement is re-judged in the loop rather than recursively.
   *
   * <p>
   * The returned {@link HitAction} names the single rewrite a
   * {@link #reconcileHit} call must perform (when any), so the reconciliation
   * never re-runs the classification's predicates or probe the TopK sketch a
   * second time.
   *
   * @param key    the key being read
   * @param entry  the resident entry (never {@code null})
   * @param policy the resolved stale policy for this read (never {@code null})
   * @param now    the per-call clock sample
   * @return what this read does with the entry
   */
  private HitClassification classifyHit(String key, CacheEntry entry, StalePolicy policy, long now) {
    CacheEntry current = entry;
    for (;;) {
      CacheEntry live = entryLifecycle.evictIfExpired(key, current, now);
      if (live == null) {
        return HitClassification.EXPIRED_HIT;
      }
      if (live == current) {
        break;
      }
      // A concurrent write replaced an expired snapshot: judge the live
      // replacement against the same clock sample.
      current = live;
    }

    boolean softExpired = ttlPolicy.isSoftExpired(current, now);
    if (softExpired && policy == StalePolicy.REVALIDATE) {
      // The caller blocks on a reload, but a state correction must still run first: a
      // soft-expired
      // Worker-HOT entry whose Worker is gone has to be demoted before writeValue
      // decides whether
      // to preserve the elevated regime. The reload otherwise re-stores the stale HOT
      // lease.
      return new HitClassification(current, resolveStateAction(key, current), false, true);
    }
    boolean refresh = softExpired && policy == StalePolicy.SOFT_REFRESH;
    return new HitClassification(current, resolveStateAction(key, current), refresh, false);
  }

  /**
   * The one state rewrite (if any) a hit requires, decided without mutating the
   * entry.
   */
  private HitAction resolveStateAction(String key, CacheEntry entry) {
    if (entry.getKeyState() != KeyState.HOT) {
      return promotion.isPromotable(key, entry) ? HitAction.PROMOTE : HitAction.NONE;
    }
    if (entryLifecycle.needsDemotion(entry)) {
      return HitAction.DEMOTE;
    }
    return promotion.needsReassert(entry) ? HitAction.REASSERT : HitAction.NONE;
  }

  /**
   * Batch variant of {@link #get(String, Supplier, GetPolicy)}. All keys share
   * the same explicit options (TTL overrides are evaluated once for the whole
   * batch — the batch pipeline has no per-key lazy-TTL path); the positional
   * {@code reader} governs loading. The batch pipeline carries no admission
   * filter — every non-{@code null} value is stored.
   *
   * <p>
   * Keys are used as passed by the caller for L1 access, matching the
   * single-key path, and are assumed valid: the facade filters its collection
   * arguments before delegating ({@link CacheKeysPolicy#validKeys}).
   * <b>Missing keys are omitted from the result map</b> (missing key = miss):
   * a key whose loader returned {@code null} does not appear in the map; a load
   * failure is rethrown after the whole batch has been processed. Detection and
   * Worker reporting continue unchanged.
   *
   * @param cacheKeys the keys to retrieve, already validated
   * @param reader    the value function for cache misses
   * @param options   the shared per-invocation read options
   * @param <T>       the value type
   * @return a map of key → loaded or cached value (never {@code null});
   *         keys with no value are omitted
   */
  public <T> Map<String, T> get(
      List<String> cacheKeys,
      Function<? super String, ? extends T> reader,
      GetPolicy options) {
    // The batch pipeline has no per-key lazy-TTL path: evaluate the override
    // suppliers once for the whole batch (the documented batch contract).
    long hard = options.hardTtlMs().getAsLong();
    long soft = options.softTtlMs().getAsLong();
    return batchGet(cacheKeys, reader, options, hard, soft);
  }

  /**
   * Initial capacity that holds {@code expectedEntries} without an intermediate
   * rehash at the default 0.75 load factor.
   *
   * @param expectedEntries the number of entries the collection must hold
   * @return the constructor capacity
   */
  private static int mapCapacity(int expectedEntries) {
    return (int) (expectedEntries / 0.75f) + 1;
  }

  /**
   * Shared body of the batch read path: per-key guard, hit check, and miss
   * reload. The options' {@link StalePolicy} governs a soft-expired hit
   * exactly as on the single-key path (SOFT_REFRESH arms a background
   * refresh, REVALIDATE blocks on a reload, RETURN serves as-is).
   *
   * @param cacheKeys the keys to retrieve, already validated
   * @param reader    the value function for cache misses (and hit-phase
   *                  refreshes)
   * @param options   the shared per-invocation read options
   * @param hardTtlMs the batch-resolved hard TTL override (0 = default)
   * @param softTtlMs the batch-resolved soft TTL override (0 = default)
   * @param <T>       the value type
   * @return a map of key → loaded or cached value (never {@code null});
   *         keys with no value are omitted
   */
  private <T> Map<String, T> batchGet(
      List<String> cacheKeys,
      Function<? super String, ? extends T> reader,
      GetPolicy options,
      long hardTtlMs,
      long softTtlMs) {
    List<String> misses = new ArrayList<>(cacheKeys.size());
    Map<String, T> results = new LinkedHashMap<>(mapCapacity(cacheKeys.size()));
    // GetPolicy is an immutable record: the report flag is batch-wide, not per key.
    boolean report = options.report();
    // One clock sample per batch: every key's expiry decisions below judge
    // against the same `now` instead of re-reading the system clock up to three
    // times per hit (logical expiry, soft expiry, refresh arming). A batch is
    // a single logical read, so its keys resolving against a single instant is
    // both cheaper and more consistent.
    long now = TimeSource.currentTimeMillis();
    // Duplicate keys are probed, counted and loaded once. Without this a
    // repeated key ("k1", "k1") double-counts the read and, worse, misfires
    // the batched load's in-flight guard: the first iteration releases the
    // key's dedup ticket, so the second sees `!isCurrent` and treats its own
    // release as a concurrent invalidation — skipping the store and adding a
    // second access record for one logical read.
    Set<String> seen = new HashSet<>(mapCapacity(cacheKeys.size()));
    // The batch resolves the tri-state policy once, the same way ReadContext does
    // for
    // the single-key path: a null option means the engine default, never "RETURN".
    StalePolicy stalePolicy = StalePolicy.orDefault(options.stalePolicy());

    for (String key : cacheKeys) {
      if (!seen.add(key)) {
        continue;
      }

      T hit = probeBatchHit(key, reader, stalePolicy, hardTtlMs, softTtlMs, report, now);
      if (hit != null) {
        requestHits.increment();
        results.put(key, hit);
      } else {
        requestMisses.increment();
        // The misses keep the caller's key form (keys are never normalized,
        // design decisions: api surface); only a miss is consulted by the batched load
        // below.
        misses.add(key);
      }
    }
    if (misses.isEmpty())
      return results;

    loadAndCache(misses, reader, hardTtlMs, softTtlMs, report, results);
    return results;
  }

  /**
   * The batch loop's single-key hit probe: classifies the entry through
   * {@link #classifyHit}, then serves, defers to the batched load, or
   * reconciles. A plain serve needs neither a state rewrite nor a refresh, so it
   * pays no context allocation.
   *
   * @param key         the key to probe
   * @param reader      the batch reader (only bound under {@code SOFT_REFRESH})
   * @param stalePolicy the batch-resolved soft-expiry policy (never {@code null})
   * @param hardTtlMs   the batch-resolved hard TTL override (0 = default)
   * @param softTtlMs   the batch-resolved soft TTL override (0 = default)
   * @param report      whether the batch feeds Worker reporting
   * @param now         the batch's single clock sample
   * @param <T>         the value type
   * @return the served value, or {@code null} on a miss, an expired entry, or a
   *         reload
   */
  private <T> T probeBatchHit(
      String key,
      Function<? super String, ? extends T> reader,
      StalePolicy stalePolicy,
      long hardTtlMs,
      long softTtlMs,
      boolean report,
      long now) {
    CacheEntry entry = caffeineCache.getIfPresent(key);
    if (entry == null) {
      return null;
    }

    HitClassification hit = classifyHit(key, entry, stalePolicy, now);
    if (hit.isPlainServe()) {
      return hit.entry() == null ? null : servePlain(key, hit.entry(), report);
    }
    if (hit.reload()) {
      // The batched load is the reload; apply the state correction first so its write
      // does not
      // preserve a Worker-HOT regime whose Worker is no longer authoritative.
      reconcileStateOnly(
          key,
          hit,
          ReadContext.ofBatch(null, hardTtlMs, softTtlMs, report, stalePolicy));
      return null;
    }
    // EXPIRED defers to the batched load: the key joins the one loadAndCache
    // instead
    // of reloading inline.
    if (!hit.needsReconcile() || hit.entry() == null) {
      return null;
    }
    // A stale Worker decision, a degraded-mode promotion or a soft-expired entry
    // needs the context. The reader is armed for every SOFT_REFRESH hit: a
    // concurrent peer invalidation can close a Worker-managed entry's soft window
    // between this probe and reconcileHit(), and the demotion that follows
    // preserves that past deadline.
    Supplier<?> refreshReader = stalePolicy == StalePolicy.SOFT_REFRESH ? () -> reader.apply(key) : null;
    return reconcileHit(key, hit, ReadContext.ofBatch(refreshReader, hardTtlMs, softTtlMs, report, stalePolicy), now);
  }

  /**
   * Trigger a background refresh if the raw value is a soft-expired
   * {@link CacheEntry}. HOT and COOL entries are refreshed — their value must
   * stay fresh while the Worker has not re-decided; a successful refresh
   * downgrades COOL to NORMAL (see {@code BackgroundRefresher#applyRefreshTask}).
   * NORMAL entries are refreshed too, mirroring the read path's
   * SOFT_REFRESH branch: the refresh re-arms whenever the entry is soft-expired
   * (stale-while-revalidate semantics of {@code zeta.local.soft-ttl-ms})
   * and keeps the entry inside its hard-TTL bound (design decisions: expiry and
   * refresh). This is what makes
   * {@code Zeta.registerRefresh} hold its documented "every cycle triggers an
   * async refresh" contract for normal keys.
   *
   * <p>
   * Timestamp-threading form: judges soft expiry against the caller's single
   * per-call {@code now} sample instead of reading the clock again.
   *
   * <p>
   * <b>Zero lookups on a fresh hit.</b> The soft-expiry test runs against the
   * entry the caller already holds and returns before the table is touched; only
   * a
   * genuinely soft-expired entry confirms that it is still the resident one
   * before
   * arming — the refresh re-warms the key, so it must not fire for an entry a
   * concurrent invalidate removed.
   *
   * @param key   the cache key
   * @param entry the L1 entry in force for this read (never {@code null})
   * @param ctx   the per-call read context (refresh reader {@code null}
   *              skips the refresh — convenience policies carry no separate
   *              refresh reader; the soft TTL override is consumed lazily
   *              through the memoized supplier)
   * @param now   the current epoch-millis (a single per-call sample)
   */
  private void refreshSoftExpire(String key, CacheEntry entry, ReadContext ctx, long now) {
    Supplier<?> refreshReader = ctx.refreshReader();
    if (refreshReader == null || !ttlPolicy.isSoftExpired(entry, now)) {
      return;
    }

    CacheEntry current = quietCache.getIfPresent(key);
    if (current == null) {
      return;
    }
    // Only a concurrent replacement needs the soft-expiry test re-applied; the
    // snapshot that reached this point was already judged soft-expired.
    if (current != entry && !ttlPolicy.isSoftExpired(current, now)) {
      return;
    }

    long softTtl = ttlPolicy.resolveRefreshSoftTtl(current, ctx.softTtlMs());
    // design decisions: version stamps — the refresh task takes its stamp BEFORE
    // reading the value, so the result is a lower bound and any write allocated
    // after the load began wins. The admission filter gates L1 storage here too: a
    // rejected value resolves to null, which the refresher treats as "skip the
    // apply".
    backgroundRefresher.triggerBackgroundRefresh(
        key,
        () -> {
          long stamp = nextStamp();
          Object value = refreshReader.get();
          return ctx.admits(value) ? new VersionedValue(value, stamp) : null;
        },
        softTtl);
  }

  /**
   * Applies the one state rewrite a classification named without serving or
   * counting the value —
   * the reload path's pre-step. A reload discards the returned image anyway, so
   * this method only
   * performs the correction; the following load records the single access.
   *
   * @param key the cache key
   * @param hit the classification whose action to apply
   * @param ctx the per-call read context for a degraded promotion's TTLs
   */
  private void reconcileStateOnly(String key, HitClassification hit, ReadContext ctx) {
    CacheEntry entry = hit.entry();
    if (entry == null) {
      return;
    }
    switch (hit.action()) {
      case DEMOTE -> entryLifecycle.demoteIfRemoteNodeIdInvalid(key, entry);
      case REASSERT -> promotion.reassertIfNeeded(key, entry);
      case PROMOTE -> promotion.promoteDegraded(key, ctx);
      default -> {
      }
    }
  }

  /**
   * Reconcile a classified hit: perform the one state rewrite the
   * classification named (demote an invalid Worker decision, reassert a
   * recovered COOL lease, or promote a degraded-mode COOL entry), then re-check
   * logical expiry on the post-image the mutation reported (TOCTOU) — no second
   * table lookup. Both the single-key and batch hit paths converge here (design
   * decisions: hot state).
   *
   * <p>
   * The {@code SOFT_REFRESH} arming lives here rather than in the caller so the
   * refresh is judged against the entry <em>in force</em> — a demotion above
   * replaces the snapshot in place, and arming on the stale one would refresh a
   * demoted key at the hot cadence on every hit — while leaving the caller with
   * no second table lookup of its own. A plain serve never reaches this method;
   * a rewrite or a genuine soft-expire does.
   *
   * <p>
   * <b>The access is recorded only when the read serves.</b> The expired branch
   * records nothing: the caller's reload (or the batch loader) records exactly
   * once, so one logical read cannot be counted twice
   * (design decisions: access counting).
   *
   * @param key the cache key
   * @param hit the classification and the resident it applies to
   * @param ctx the per-call read context (TTL overrides stay lazy; carries
   *            the report flag)
   * @param now the per-call clock sample
   * @param <T> the value type
   * @return the cached value, or {@code null} if logically expired
   */
  @SuppressWarnings("unchecked")
  private <T> T reconcileHit(String key, HitClassification hit, ReadContext ctx, long now) {
    CacheEntry entry = hit.entry();
    if (entry == null) {
      return null;
    }
    T value = (T) entry.getValue();
    if (value == null) {
      return null;
    }
    EntryMutations.Applied applied = switch (hit.action()) {
      case DEMOTE -> entryLifecycle.demoteIfRemoteNodeIdInvalid(key, entry);
      case REASSERT -> promotion.reassertIfNeeded(key, entry);
      case PROMOTE -> promotion.promoteDegraded(key, ctx);
      default -> null;
    };
    CacheEntry effective = applied != null ? applied.current() : entry;
    if (effective == null) {
      return null;
    }
    // TOCTOU: a rewrite may have crossed its hard deadline, or a concurrent
    // replacement may have landed. No access is recorded when the key is gone: the
    // caller's reload records it once.
    if (effective != entry) {
      effective = entryLifecycle.evictIfExpired(key, effective, now);
      if (effective == null) {
        return null;
      }
    }

    dispatcher.recordAccess(key, ctx.report());
    // Arm only when the classification asked for a refresh or a rewrite changed
    // the entry's expiry image; a rewrite alone can leave a fresh entry.
    if (ctx.stalePolicy() == StalePolicy.SOFT_REFRESH && (hit.refresh() || effective != entry)) {
      refreshSoftExpire(key, effective, ctx, now);
    }
    // Return the post-image's value: a concurrent replacement observed under the
    // bin lock may
    // differ from the snapshot this call started with.
    return unwrap(effective);
  }

  /**
   * Load via SingleFlight, detect hot key, cache with HOT or NORMAL TTL, and
   * return value.
   *
   * <p>
   * The option's {@code callerRunsLoader} flag selects the dedup election:
   * {@code true} runs the leader's reader on this thread (annotation reads use
   * this, so the business body keeps its transaction, security context and
   * MDC), while same-key followers still join its future. The default keeps the
   * reader on the load executor.
   *
   * <p>
   * A caller-affine load inside an active transaction is isolated: the reader
   * still runs on this
   * thread, but no flight is published and no L1 store is registered. The value
   * may include the
   * transaction's own uncommitted writes, so a follower must not observe it and a
   * rollback must not
   * leave it cached; the pre-read stamp also cannot be ordered against the
   * transaction's own
   * deferred invalidations without letting a stale fill outrank a concurrent
   * write.
   *
   * @param key the key to load
   * @param ctx the per-call read context (reader, cacheFilter and
   *            TTL overrides; the TTL suppliers are consumed exactly once
   *            per call through the memoized context)
   */
  @SuppressWarnings("unchecked")
  private <T> T loadAndCache(String key, ReadContext ctx) {
    Supplier<T> reader = (Supplier<T>) ctx.reader();
    if (reader == null) {
      // No reader on a miss resolves as the documented null answer instead of an
      // NPE inside the SingleFlight execution.
      return resolveNullAnswer(key, ctx.report());
    }

    long hardTtl = ttlPolicy.resolveEffectiveHardTtl(ctx.hardTtlMs());
    long softTtl = ttlPolicy.resolveEffectiveSoftTtl(ctx.softTtlMs());
    // design decisions: version stamps — the stamp is taken BEFORE reader.get(),
    // so it is a lower bound and any write allocated after the load began outranks
    // it. Outside a transaction the store runs inside the leader's commit, before
    // the followers wake; inside one it waits for afterCommit.
    Supplier<VersionedValue> stampedReader = () -> {
      long stamp = nextStamp();
      T loaded = reader.get();
      return loaded == null ? null : new VersionedValue(loaded, stamp);
    };
    boolean transactionalCaller = ctx.callerRunsLoader() && TransactionSupport.isDeferred(false);
    LoaderOutcome<VersionedValue> outcome = transactionalCaller
        ? singleFlight.loadIsolated(key, stampedReader, value -> {
        })
        : singleFlight.load(
            key,
            stampedReader,
            ctx.callerRunsLoader(),
            value -> loadCacheEntry(key, hardTtl, softTtl, value, ctx.cacheFilter()));
    return resolveLoaded(key, outcome, ctx.report());
  }

  /**
   * Batch variant of the single-key {@link #loadAndCache(String, ReadContext)}.
   * Submits all keys to SingleFlight in a single pass, then processes each loaded
   * value through {@link #loadCacheEntry}; keys whose source answered
   * {@code null} resolve to a missing entry (omitted by the caller).
   *
   * @param cacheKeys the keys to load (the misses from a preceding Caffeine
   *                  check); caller's original key form, kept for reader
   *                  parity and result-map identity
   * @param reader    the value function for cache misses (called on executor
   *                  threads)
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param report    whether the batch feeds Worker reporting
   * @param into      the result map to fill; the caller owns it, so a batch
   *                  with misses
   *                  allocates one map instead of one here plus a copy
   * @param <T>       the value type
   */
  private <T> void loadAndCache(
      List<String> cacheKeys,
      Function<? super String, ? extends T> reader,
      long hardTtlMs,
      long softTtlMs,
      boolean report,
      Map<String, T> into) {
    final long effectiveHardTtlMs = ttlPolicy.resolveEffectiveHardTtl(hardTtlMs);
    final long effectiveSoftTtlMs = ttlPolicy.resolveEffectiveSoftTtl(softTtlMs);
    // design decisions: version stamps: every key's stamp is allocated BEFORE the
    // batch load
    // starts and as one contiguous block per chunk, so a concurrent single write is
    // ordered against the whole batch instead of interleaving between two keys.
    Map<String, Long> stamps = new HashMap<>(mapCapacity(cacheKeys.size()));
    allocateStampBlocks(cacheKeys, stamps);

    Map<String, LoaderOutcome<VersionedValue>> loaded = singleFlight.load(
        cacheKeys,
        k -> {
          T value = reader.apply(k);
          return value == null ? null : new VersionedValue(value, stamps.get(k));
        },
        (k, value) -> loadCacheEntry(k, effectiveHardTtlMs, effectiveSoftTtlMs, value, null));

    Throwable failure = null;
    for (String key : cacheKeys) {
      try {
        T value = resolveLoaded(key, loaded.get(key), report);
        if (value != null) {
          into.put(key, value);
        }
      } catch (RuntimeException | Error e) {
        // The batch closes before the throw: the remaining keys are processed first, so
        // a
        // leader that abandoned them here would leave every follower with a value this
        // node
        // never wrote. The first failure is rethrown with the rest suppressed.
        failure = collectFailure(failure, e);
      }
    }
    if (failure != null) {
      if (failure instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw (Error) failure;
    }
  }

  /**
   * Accumulates a batch failure without losing the earlier one or duplicating the
   * same throwable.
   */
  private static Throwable collectFailure(Throwable failure, Throwable error) {
    if (failure == null) {
      return error;
    }
    if (failure != error) {
      failure.addSuppressed(error);
    }
    return failure;
  }

  /**
   * The per-key tail of the batch load: dispatch one outcome to its terminal
   * policy. The switch
   * is exhaustive over {@link LoadStatus}, so a future status fails at compile
   * time.
   */
  @SuppressWarnings("unchecked")
  private <T> T resolveLoaded(String key, @Nullable LoaderOutcome<VersionedValue> outcome, boolean report) {
    if (outcome == null) {
      return resolveNullAnswer(key, report);
    }
    return switch (outcome.status()) {
      case LOADED -> {
        VersionedValue value = outcome.value();
        if (value == null || value.value() == null) {
          yield resolveNullAnswer(key, report);
        }
        dispatcher.recordAccess(key, report);
        yield (T) value.value();
      }
      // An empty source is not a failure: no throw, no suppression log.
      case SOURCE_EMPTY -> resolveNullAnswer(key, report);
      case SUPPRESSED_BREAKER, SUPPRESSED_TIMEOUT, SUPPRESSED_EXECUTOR -> resolveSuppressed(
          key,
          outcome.status(),
          report);
      case FAILED -> resolveFailed(key, outcome.cause(), report);
    };
  }

  /**
   * A suppressed load: an open breaker degrades to the stale fallback, any other
   * suppression still propagates its {@link ZetaSuppressedException}. The breaker
   * is consulted once — the stale fallback trusts this verdict.
   *
   * @param key    the key whose load was suppressed
   * @param status the suppression reason
   * @param report whether this read feeds Worker reporting
   * @param <T>    the value type
   * @return the stale value when the breaker is open, otherwise this method
   *         throws
   */
  private <T> T resolveSuppressed(String key, LoadStatus status, boolean report) {
    if (!singleFlight.isBreakerOpen(key)) {
      throw new ZetaSuppressedException(key, status);
    }
    logSuppressedLoad(key, status);
    return serveStaleEntry(key, report);
  }

  /**
   * A real load failure: {@code Error} always propagates, an
   * {@code InterruptedException} restores the interrupt bit and degrades, and an
   * open breaker degrades to the stale fallback. Otherwise the cause is rethrown
   * (unchecked) or wrapped.
   *
   * @param key    the key whose load failed
   * @param cause  the reader failure
   * @param report whether this read feeds Worker reporting
   * @param <T>    the value type
   * @return the degraded answer, when the read degrades
   */
  private <T> T resolveFailed(String key, @Nullable Throwable cause, boolean report) {
    if (cause instanceof Error err) {
      throw err;
    }
    if (cause instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      return resolveNullAnswer(key, report);
    }
    if (singleFlight.isBreakerOpen(key)) {
      return serveStaleEntry(key, report);
    }
    if (cause instanceof RuntimeException re) {
      throw re;
    }
    throw new CompletionException(cause);
  }

  /**
   * Receives one contiguous stamp block from {@link #forEachStampBlock}: the
   * block's keys plus its base stamp. The i-th key's stamp is {@code base + i}.
   */
  @FunctionalInterface
  private interface BlockSink {
    void accept(List<String> blockKeys, long base);
  }

  /**
   * Reserve one stamp per key in contiguous blocks — the load-path form of
   * {@link #forEachStampBlock} that only fills a map. Keys beyond one block
   * ({@link StampClock#MAX_ATOMIC_RANGE}) loop over further blocks.
   *
   * @param keys the keys to stamp, in batch order
   * @param into receives key → stamp in ascending block order
   */
  private void allocateStampBlocks(List<String> keys, Map<String, Long> into) {
    forEachStampBlock(keys, StampClock.MAX_ATOMIC_RANGE, (blockKeys, base) -> {
      for (int i = 0; i < blockKeys.size(); i++) {
        into.put(blockKeys.get(i), base + i);
      }
    });
  }

  /**
   * Reserve one contiguous stamp block per chunk and hand each block to
   * {@code sink} — the single owner of the split-and-CAS loop shared by every
   * bulk path (load-path stamping, batch invalidation, remote invalidation).
   *
   * <p>
   * Two independent upper bounds apply:
   * <ul>
   * <li>{@link StampClock#MAX_ATOMIC_RANGE} — the most one {@code nextStamps} CAS
   * can issue;</li>
   * <li>{@code chunkSize} — the caller's batch/wire limit.</li>
   * </ul>
   * The effective block size is the smaller of the two; keys beyond it loop over
   * further blocks. Inside a block the versions form one uninterleaved range in
   * the node's stamp space, so a concurrent single write is ordered against the
   * whole block at once (design decisions: version stamps).
   *
   * @param keys      the keys to stamp, in batch order
   * @param chunkSize the caller's block limit (clamped to
   *                  {@link StampClock#MAX_ATOMIC_RANGE})
   * @param sink      receives each block's keys and base stamp
   */
  private void forEachStampBlock(List<String> keys, int chunkSize, BlockSink sink) {
    int blockSize = Math.min(chunkSize, StampClock.MAX_ATOMIC_RANGE);
    for (int start = 0; start < keys.size(); start += blockSize) {
      int end = Math.min(keys.size(), start + blockSize);
      List<String> blockKeys = keys.subList(start, end);
      long base = stampClock.nextStamps(blockKeys.size());
      sink.accept(blockKeys, base);
    }
  }

  /**
   * Handle a load that produced a null answer or no answer: serve a hard-valid
   * stale entry when the circuit breaker is open, otherwise resolve {@code null}.
   *
   * <p>
   * A null answer arrives in two shapes: the built-in paths wrap the reader
   * result in a {@link VersionedValue}, so a source {@code null} surfaces as
   * {@code LOADED} with a null payload; a custom {@code SingleFlight} may report
   * it as {@link LoadStatus#SOURCE_EMPTY} instead. Both converge here, and so do
   * the no-answer statuses (reader failure, suppression) — the degraded answer is
   * the same for all of them (design decisions: read path).
   *
   * <p>
   * <b>Null is never cached:</b> a null loader result writes no entry and
   * returns {@code null} — the next read re-invokes the loader. Callers needing
   * penetration protection apply it at their loader (a placeholder value +
   * {@code isCacheable}) instead.
   *
   * <p>
   * Only entries whose hard TTL has <b>not</b> expired may serve as the
   * stale fallback — the hard TTL contract (see CONTEXT.md "Expire") is never
   * bypassed, even during a source outage. A hard-expired entry resolves to
   * {@code null} and is evicted right here — this is the one path that inspects
   * the entry with no reader behind it, so waiting for the next read would
   * leave a dead entry resident for that whole window.
   *
   * <p>
   * A stale serve is a served read and is recorded as one
   * (design decisions: access counting): without it a breaker-open window
   * silently erases the
   * key's access history, and a key that is hot enough to be read during the
   * outage stops looking hot exactly when the Worker needs to know.
   *
   * @param key    the key whose load produced a null or no answer
   * @param report whether this batch/call feeds Worker reporting
   * @param <T>    the expected value type
   * @return a stale value if the circuit breaker is open and a cached entry
   *         exists within its hard TTL; {@code null} otherwise
   */
  private <T> T resolveNullAnswer(String key, boolean report) {
    return singleFlight.isBreakerOpen(key) ? serveStaleEntry(key, report) : null;
  }

  /**
   * The degraded stale answer: the resident entry when it is still inside its
   * hard TTL, {@code null} otherwise. Hard expiry is never bypassed, even during
   * an outage (CONTEXT.md "Expire"); the eviction happens here because this is
   * the one path that inspects the entry with no reader behind it.
   *
   * @param key    the key whose load produced no value
   * @param report whether this read feeds Worker reporting
   * @param <T>    the value type
   * @return the stale value, or {@code null}
   */
  private <T> T serveStaleEntry(String key, boolean report) {
    CacheEntry stale = quietCache.getIfPresent(key);
    if (stale == null) {
      return null;
    }
    CacheEntry resident = entryLifecycle.evictIfExpired(key, stale, TimeSource.currentTimeMillis());
    if (resident == null) {
      log.debug("CB open, discarding hard-expired entry for key={}", key);
      return null;
    }
    dispatcher.recordAccess(key, report);
    T value = unwrap(resident);
    log.debug("CB open, returning stale entry for key={}", key);
    return value;
  }

  /**
   * Suppression log for the {@code loadAndCache} paths: a swallowed
   * {@code SUPPRESSED_*} outcome produces no exception, so without this line
   * the self-protection would be invisible (the FAILED path is logged by the
   * caller's failure policy instead). DEBUG — a suppression is the guard
   * working as designed, not a fault.
   *
   * @param key    the key whose load was suppressed
   * @param status the suppression cause (breaker, timeout, or executor)
   */
  private void logSuppressedLoad(String key, LoadStatus status) {
    log.debug("HotKeyCache load suppressed for key={} (status={}; returning empty/stale)", key, status);
  }

  /**
   * The single write-admission rule every L1 write path shares: a stamp at or
   * below an applied invalidation or clear is refused, and so is a write the
   * live resident already outranks. An expired resident never outranks a write;
   * an equal stamp is refused except on the REFRESH receiver, where it heals.
   *
   * @param key          the key being written
   * @param entry        the resident entry, or {@code null}
   * @param stamp        the incoming data stamp
   * @param equalApplies {@code true} for the REFRESH receiver (an equal version
   *                     applies)
   * @return {@code true} when the write must leave the resident untouched
   */
  private boolean writeRefused(String key, @Nullable CacheEntry entry, long stamp, boolean equalApplies) {
    if (entryLifecycle.staleAgainstInvalidation(key, stamp)) {
      return true;
    }
    return (entry != null &&
        !ttlPolicy.isLogicallyExpired(entry) &&
        (equalApplies ? VersionGuard.shouldSkipForRefresh(entry, stamp)
            : VersionGuard.shouldSkipForSync(entry, stamp)));
  }

  /**
   * Atomically insert or update the entry in L1 via the shared write builder:
   * a load whose stamp predates an applied invalidation is dropped, and one
   * that a newer resident outranks yields to it. An elevated (HOT/COOL) entry
   * keeps its TTL regime and decision lease — only the freshly loaded value,
   * its stamp and the soft window move.
   *
   * @param key           the key to store
   * @param normalHardTtl the normal (non-hot) hard TTL baseline
   * @param normalSoftTtl the normal (non-hot) soft TTL baseline
   * @param loaded        the load carrier; every entry is built with its
   *                      positive pre-read stamp (design decisions: version
   *                      stamps)
   * @param cacheFilter   admission predicate ({@code null} = admit all);
   *                      {@code false} skips the store — the value is served
   *                      but leaves no L1 entry
   */
  private void loadCacheEntry(
      String key,
      long normalHardTtl,
      long normalSoftTtl,
      VersionedValue loaded,
      @Nullable Predicate<Object> cacheFilter) {
    // cacheFilter admission: a rejected value never takes L1 residency
    // (detection/reporting already ran for the value). Evaluate it here, on the
    // load thread, so a deferred store does not pin the loader or its scope.
    if (cacheFilter != null && !cacheFilter.test(loaded.value())) {
      log.debug("cacheFilter rejected value, skipping L1 store: key={}", key);
      return;
    }

    Object value = loaded.value();
    long stamp = loaded.dataVersion();
    boolean hot = hotKeyDetector.contains(key);
    // Inside an active transaction the loader may have read that transaction's
    // own uncommitted writes. The store waits for afterCommit, so a rollback
    // never leaves those writes in L1. Outside a transaction it runs inline.
    TransactionSupport.run(false, () -> EntryMutations.mutate(caffeineCache, key,
        entry -> buildEntryOnWrite(key, entry, value, stamp, normalHardTtl, normalSoftTtl, false, hot)));
  }

  /**
   * Apply one invalidation policy to a single key: a {@code REMOTE} scope
   * broadcasts without touching the local entry, while {@code LOCAL}/{@code BOTH}
   * record the tombstone and drop L1 (and notify peers when the scope includes
   * them).
   *
   * <p>
   * A stamp is allocated for every invalidation, including a {@code REMOTE}
   * broadcast. For a local removal the same stamp is the tombstone: a load that
   * started before this invalidation must not resurrect the removed value,
   * whether or not peers were told (design decisions: invalidation).
   *
   * @param cacheKey the key to invalidate
   * @param policy   where the invalidation applies and whether it bypasses the
   *                 active-transaction deferral
   */
  public void invalidate(String cacheKey, InvalidatePolicy policy) {
    TransactionSupport.run(policy.immediate(), () -> {
      long stamp = nextStamp();
      if (policy.scope() == InvalidatePolicy.Scope.REMOTE) {
        dispatcher.broadcastInvalidate(cacheKey, stamp);
        return;
      }
      dropAndBroadcastInvalidate(cacheKey, stamp, policy.sync());
    });
  }

  /**
   * Allocate the next data stamp. The {@code StampClock} is total —
   * it never throws — so there is no fallback space to degrade into
   * (design decisions: version stamps).
   *
   * @return the allocated stamp, always positive
   */
  private long nextStamp() {
    return stampClock.nextStamp();
  }

  /**
   * Drop L1 (and the dedup entry) for one key, record the invalidation
   * tombstone and — when {@code broadcast} is set — send a versioned
   * INVALIDATE.
   *
   * <p>
   * <b>The version must already be allocated</b> — call {@link #nextStamp()}
   * before
   * the L1 removal and pass the result here, so the batch path cannot drift from
   * the single-key
   * order. Allocating after the drop reopens the stale-reload window: a reader
   * that misses after
   * the drop reloads at the old version {@code v}, the INVALIDATE then carries
   * {@code v+1}, and
   * the guard treats the reloaded entry as newer and skips it — pinning the stale
   * value for its
   * whole TTL. Bumping first makes every racing reload stamp {@code v+1}, so the
   * mutation
   * supersedes it (design decisions: invalidation).
   *
   * <p>
   * The tombstone is recorded for local-only invalidations too: {@code broadcast}
   * gates the peer send, never the local guard (design decisions: invalidation).
   *
   * @param key       the cache key
   * @param stamp     the version allocated before the drop (positive)
   * @param broadcast whether peers are notified
   */
  private void dropAndBroadcastInvalidate(String key, long stamp, boolean broadcast) {
    dropLocal(key, stamp);
    if (broadcast) {
      dispatcher.broadcastInvalidate(key, stamp);
    }
  }

  /**
   * The local half of an invalidation, shared by the single-key and batch
   * paths: tombstone first, then L1, then the dedup slot. A load that commits
   * between the tombstone and the removal already sees the invalidation, and
   * one that committed before it is reclaimed by the removal itself; dropping
   * the dedup slot keeps a pre-invalidation result from being replayed onto the
   * next miss.
   *
   * @param key   the key to drop locally
   * @param stamp the version allocated before the drop (positive)
   */
  private void dropLocal(String key, long stamp) {
    entryLifecycle.recordInvalidation(key, stamp);
    caffeineCache.invalidate(key);
    singleFlight.invalidate(key);
  }

  /**
   * Batch form of {@link #invalidate(String, InvalidatePolicy)}: the same
   * policies, one contiguous version block per wire chunk. Keys are assumed
   * valid — the facade drops the invalid ones before delegating
   * ({@link CacheKeysPolicy#validKeys}), an all-invalid collection never arriving
   * here.
   *
   * @param cacheKeys the keys to invalidate, already validated
   * @param policy    where the invalidations apply and whether they bypass the
   *                  active-transaction deferral
   */
  public void invalidate(List<String> cacheKeys, InvalidatePolicy policy) {
    // Transactional timing: defer every bump into afterCommit so registration order
    // becomes version order (same rule as putThrough) and a rollback leaks nothing.
    // Only a deferred run outlives this call, so only it needs the list frozen.
    List<String> keysSnapshot = TransactionSupport.isDeferred(policy.immediate()) ? List.copyOf(cacheKeys) : cacheKeys;
    TransactionSupport.run(policy.immediate(), () -> executeBatchInvalidate(keysSnapshot, policy));
  }

  /**
   * Shared batch body: reserve one contiguous version block per wire chunk
   * ({@link SyncMessage#MAX_BATCH_ENTRIES} entries per broadcast), apply the
   * policy per key (a {@code REMOTE} scope skips the local drop), then broadcast
   * the chunk's key/version pairs when the policy includes peers.
   *
   * <p>
   * The versions inside a chunk are reserved as <b>one contiguous block
   * before any drop</b> (design decisions: invalidation), so a concurrent single
   * write cannot interleave inside the chunk's version block — the per-key guard
   * orders it against the chunk all-or-nothing: older than every chunk entry
   * (the batch supersedes it) or newer than every chunk entry (its value
   * survives every batch apply), never partially applied between two keys.
   *
   * @param keys   the validated keys (never empty)
   * @param policy where the invalidations apply
   */
  private void executeBatchInvalidate(List<String> keys, InvalidatePolicy policy) {
    boolean remoteOnly = policy.scope() == InvalidatePolicy.Scope.REMOTE;
    boolean broadcast = policy.sync();
    forEachStampBlock(keys, SyncMessage.MAX_BATCH_ENTRIES, (blockKeys, base) -> {
      List<SyncMessage.Entry> entries = broadcast ? new ArrayList<>(blockKeys.size()) : null;
      for (int i = 0; i < blockKeys.size(); i++) {
        String key = blockKeys.get(i);
        long stamp = base + i;
        if (!remoteOnly) {
          dropLocal(key, stamp);
        }
        if (broadcast) {
          entries.add(new SyncMessage.Entry(key, stamp));
        }
      }
      if (broadcast) {
        dispatcher.broadcastInvalidateBatch(entries);
      }
    });
  }

  /**
   * Refresh the given key: allocate the version <b>before</b> the load, then
   * apply the loaded value through the write-through guard.
   *
   * <p>
   * <b>Ordering:</b> the version is allocated before the load, so a
   * concurrent sync write landing during the load holds the newer version and
   * this refresh's stale apply is skipped instead of regressing it (design
   * decisions: version stamps). A burned version on loader failure is a
   * harmless watermark hole.
   *
   * <p>
   * A {@code null} loader result skips the refresh entirely (DEBUG): L1 keeps
   * its current entry — a refresh that learned nothing
   * must not evict the value it was asked to renew. Outside a transaction the
   * whole bump-load-apply runs inline on the caller thread; inside a
   * transaction it waits for {@code afterCommit}, so the loader observes
   * committed data and registration order becomes version order.
   *
   * @param cacheKey  the key to refresh
   * @param loader    the value supplier (caller thread outside a transaction,
   *                  commit thread inside one)
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param sync      whether to broadcast the refresh to peers
   * @param <V>       the value type
   */
  public <V> void refresh(String cacheKey, Supplier<V> loader, long hardTtlMs, long softTtlMs, boolean sync) {
    TransactionSupport.run(false, () -> executeRefreshNow(cacheKey, loader, hardTtlMs, softTtlMs, sync));
  }

  /**
   * Bump-then-load-then-apply refresh body: invoked inline outside a
   * transaction and from {@code afterCommit} inside one, so bump-before-load
   * holds either way, the loader observes committed data, and the allocated
   * version follows registration order.
   */
  private <V> void executeRefreshNow(String key, Supplier<V> loader, long hardTtlMs, long softTtlMs, boolean sync) {
    long stamp = nextStamp();
    V value = loader.get();
    if (value == null) {
      // Null is never cached: a null loader result must neither overwrite the
      // resident entry with a null value nor broadcast a refresh for a value
      // that was never loaded.
      log.debug("refresh loader returned null for key={}, refresh skipped (L1 keeps current entry)", key);
      return;
    }
    writeThroughApply(
        key,
        value,
        stamp,
        ttlPolicy.resolveEffectiveHardTtl(hardTtlMs),
        ttlPolicy.resolveEffectiveSoftTtl(softTtlMs),
        sync);
  }

  /**
   * Cluster-sync write: update L1 with a version stamp and broadcast a
   * value-carrying PUT to peers (they apply the value version-guarded, no
   * reload). There is no business writer: a put is pure cache maintenance,
   * so "mutate the source, then update the cache" orchestration belongs to the
   * caller.
   *
   * <p>
   * Pass 0 to use the configured default for that TTL type.
   *
   * @param cacheKey  the key to write
   * @param value     the value to cache (never {@code null} — the facade
   *                  rejects null puts)
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  public void putThrough(String cacheKey, Object value, long hardTtlMs, long softTtlMs) {
    put(cacheKey, value, hardTtlMs, softTtlMs, true);
  }

  /**
   * Apply one write to L1, retire the key's dedup entry, and — when
   * {@code broadcast} is set and the store was actually applied — broadcast the
   * value-carrying PUT to peers (they apply version-guarded, no reload; degrades
   * to a value-less REFRESH inside the dispatcher when the value is not
   * serializable or over the size bound). This is the single apply tail of
   * {@code putThrough}, {@code refresh} and {@code putLocal}.
   *
   * <p>
   * <b>L1 first, then the dedup removal, then the broadcast</b> (design
   * decisions: dedup lifecycle): the store obsoletes every load result recorded
   * before it, so the dedup entry is dropped immediately after the apply; a
   * failing broadcast must not leave that pre-write future replayable.
   *
   * <p>
   * <b>A refused store broadcasts nothing.</b> The guard inside
   * {@link #buildEntryOnWrite} can refuse the apply — a strictly newer
   * resident, or a tombstone from an INVALIDATE/CLEAR that landed between the
   * stamp allocation and this compute. Broadcasting a refused write would ship
   * a value this node has already superseded (or invalidated) to peers that
   * may not hold the newer entry, re-warming data the local tombstone just
   * removed. The same applied/refused distinction the load path makes before
   * touching the dedup entry.
   */
  private void writeThroughApply(
      String key,
      Object value,
      long stamp,
      long normalHardTtl,
      long normalSoftTtl,
      boolean broadcast) {
    boolean hot = hotKeyDetector.contains(key);
    EntryMutations.Applied applied = EntryMutations.apply(caffeineCache, key,
        entry -> buildEntryOnWrite(key, entry, value, stamp, normalHardTtl, normalSoftTtl, true, hot));
    singleFlight.invalidate(key);
    // Only an applied store reaches peers: a refused write returns the untouched
    // resident.
    if (broadcast && applied != null) {
      // The predicate runs when the record enters the buffer, after serialization:
      // an invalidation that lands while the payload is being built suppresses it.
      dispatcher.broadcastPut(key, value, stamp, version -> !entryLifecycle.staleAgainstInvalidation(key, version));
    }
  }

  /**
   * The single L1 write builder shared by the load, write-through and local
   * write paths: the write-admission guard ({@link #writeRefused}), the
   * Worker-managed regime preservation and the hot/normal TTL selection live
   * here once.
   *
   * <p>
   * The last two parameters are always the <b>normal</b> TTL baseline. For
   * a hot key the active TTL is resolved from them; the baseline itself is
   * kept unchanged so a later demotion can return the entry to its normal
   * lifetime.
   *
   * <p>
   * {@code hot} is resolved by the caller, before the map's {@code compute}:
   * the detector probe is an external call and holding the bin lock across it
   * would lengthen the critical section for every key in the bin. The answer is
   * advisory — it only selects the TTL regime for this write, and a promotion or
   * demotion corrects a stale choice on the next read.
   *
   * @param key           the key being written
   * @param entry         the resident entry, or {@code null}
   * @param value         the value to store
   * @param stamp         the positive data stamp allocated for this write
   * @param normalHardTtl the normal (non-hot) hard TTL baseline
   * @param normalSoftTtl the normal (non-hot) soft TTL baseline
   * @param equalApplies  {@code true} for the REFRESH receiver (an equal
   *                      version applies), {@code false} for the load path
   * @param hot           whether the local TopK currently holds the key
   * @return the entry to store; the resident unchanged when the write is refused
   */
  private CacheEntry buildEntryOnWrite(
      String key,
      @Nullable CacheEntry entry,
      Object value,
      long stamp,
      long normalHardTtl,
      long normalSoftTtl,
      boolean equalApplies,
      boolean hot) {
    if (writeRefused(key, entry, stamp, equalApplies)) {
      return entry;
    }

    boolean promoteHot = hot
        && (entry == null || ttlPolicy.isLogicallyExpired(entry) || promotion.mayPromoteLocally(entry));

    return entryLifecycle.writeValue(entry, value, stamp, normalHardTtl, normalSoftTtl, promoteHot);
  }

  /**
   * Write a value directly into the local L1 cache with a fresh data stamp,
   * without a cluster broadcast, without feeding access counts to detection,
   * and without reporting.
   * <p>
   * Existing entry metadata is preserved. On an elevated (HOT/COOL) existing
   * entry the active TTL regime is kept as well — only the value, its stamp
   * and the soft window are updated. If no entry
   * exists, a fresh {@link CacheEntry} is created with {@link KeyState#HOT}
   * when the local TopK already contains the key, otherwise
   * {@link KeyState#NORMAL}.
   * <p>
   * The local write obsoletes every load result recorded before it (design
   * decisions: dedup lifecycle,
   * same contract as {@link #putThrough}): a completed SingleFlight result from
   * before this write must not outlive it and replay the pre-write value onto
   * the next miss.
   * <p>
   * Every branch takes a real stamp from the {@code StampClock} (design
   * decisions: version stamps I4):
   * a {@code putLocal} must never push an entry back into the unknown state.
   * <p>
   * Pass {@code 0} for either TTL to use the configured default.
   *
   * @param cacheKey  the key to store
   * @param value     the value to cache (never {@code null})
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   */
  public void putLocal(String cacheKey, Object value, long hardTtlMs, long softTtlMs) {
    put(cacheKey, value, hardTtlMs, softTtlMs, false);
  }

  /**
   * The single write-through/local write body: one transaction boundary, both TTL
   * overrides
   * resolved once, then the shared apply tail.
   *
   * @param key       the key to store
   * @param value     the value to cache (never {@code null})
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param sync      whether the applied value is broadcast to peers
   */
  private void put(String key, Object value, long hardTtlMs, long softTtlMs, boolean sync) {
    // Inline outside a transaction (read-your-write), afterCommit inside one. The
    // registration-order sequence keeps a later invalidate from the same
    // transaction
    // behind this write.
    TransactionSupport.run(false, () -> {
      long effectiveHardTtl = ttlPolicy.resolveEffectiveHardTtl(hardTtlMs);
      long effectiveSoftTtl = ttlPolicy.resolveEffectiveSoftTtl(softTtlMs);
      writeThroughApply(key, value, nextStamp(), effectiveHardTtl, effectiveSoftTtl, sync);
    });
  }

  /**
   * Batch form of the write-through/local write pair: one transaction boundary,
   * one contiguous stamp block per {@link StampClock#MAX_ATOMIC_RANGE} keys, and
   * the same per-key write guard and dedup removal as a single write.
   *
   * <p>
   * A concurrent single write is ordered against an entire block, not
   * against one key inside it: its stamp is either below every stamp in the
   * block or above every one, so the batch cannot be partially superseded. The
   * batch itself is not atomic: a failure after some applies leaves those keys
   * written.
   *
   * <p>
   * The map is frozen before a deferred run, so the caller may reuse or
   * mutate it after this method returns; an inline run iterates it once and the
   * caller must not mutate it concurrently. Keys and values are assumed
   * validated at the facade boundary.
   *
   * @param entries   the keys and values to write
   * @param hardTtlMs hard TTL override (0 = use configured default)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param sync      whether each applied value is broadcast to peers
   */
  public void putAll(Map<String, ?> entries, long hardTtlMs, long softTtlMs, boolean sync) {
    if (entries.isEmpty()) {
      return;
    }
    Map<String, ?> snapshot = TransactionSupport.isDeferred(false) ? Map.copyOf(entries) : entries;
    TransactionSupport.run(false, () -> executePutAll(snapshot, hardTtlMs, softTtlMs, sync));
  }

  /**
   * The batch body: resolve TTLs once, reserve one contiguous block per chunk,
   * then apply each row through the shared write tail.
   */
  private void executePutAll(Map<String, ?> entries, long hardTtlMs, long softTtlMs, boolean sync) {
    long effectiveHard = ttlPolicy.resolveEffectiveHardTtl(hardTtlMs);
    long effectiveSoft = ttlPolicy.resolveEffectiveSoftTtl(softTtlMs);
    // Each block's stamps are one uninterrupted range, so a concurrent single
    // write is ordered against the whole block rather than against one key
    // inside it. Iterating the entry set directly avoids the key-list copy and
    // the second hash lookup per row.
    var iterator = entries.entrySet().iterator();
    int remaining = entries.size();
    while (iterator.hasNext()) {
      int blockSize = Math.min(remaining, StampClock.MAX_ATOMIC_RANGE);
      long base = stampClock.nextStamps(blockSize);
      for (int i = 0; i < blockSize; i++) {
        Map.Entry<String, ?> entry = iterator.next();
        writeThroughApply(entry.getKey(), entry.getValue(), base + i, effectiveHard, effectiveSoft, sync);
      }
      remaining -= blockSize;
    }
  }

  /**
   * Return a snapshot of request-level L1 cache statistics.
   * <p>
   * {@code hitCount}/{@code missCount}/{@code hitRate} add caller requests,
   * not Caffeine operations: one increment per served key on the load-capable
   * read paths ({@code get}, batch {@code get}).
   * A key served from L1 without invoking the reader is a hit; a key that reaches
   * the loader — including a REVALIDATE
   * soft-expiry that blocks on a reload — is a miss. Internal
   * {@code asMap().compute()} maintenance and {@code peek()} probes never
   * touch these counters (use the Actuator cache view for storage internals).
   * {@code evictionCount} still comes from Caffeine (and is {@code 0} when a
   * custom {@code Cache} bean is supplied without stats recording), while the
   * size comes from {@code estimatedSize()}, which needs no stats recording.
   *
   * @return a {@link ZetaCacheStats} report with request-level hit/miss
   */
  public ZetaCacheStats stats() {
    long hits = requestHits.sum();
    long misses = requestMisses.sum();
    CacheStats counters = caffeineCache.stats();
    return new ZetaCacheStats(hits, misses, counters.evictionCount(), caffeineCache.estimatedSize());
  }

  /**
   * Clear the L1 table: a {@code LOCAL}/{@code BOTH} scope records the clear
   * watermark, drops every dedup slot and sweeps every local entry at or below
   * the clear stamp; a {@code REMOTE} scope only broadcasts. The sweep is
   * version-guarded: a write allocated after the clear stamp survives locally,
   * exactly as a synced clear's peers keep it, so a concurrent write is never
   * erased on this node while the cluster keeps it. The watermark is recorded
   * for every local clear, so a fill that starts after the clear cannot
   * resurrect what the sweep removed.
   *
   * <p>
   * Removal is best-effort termination, not a linearizable barrier: a fill
   * that already passed its write guard and commits while the sweep walks the
   * table can survive it, bounded by its own hard TTL — the same convergence
   * bound a peer that missed the broadcast has.
   *
   * @param policy where the clear applies and whether it bypasses the
   *               active-transaction deferral
   * @return the allocated {@code clearStamp}, or {@code -1} when
   *         local-only/remote-only/deferred
   */
  public long clear(InvalidatePolicy policy) {
    return doClear(null, policy);
  }

  /**
   * Clear one key namespace: record a scoped clear watermark, drop the prefix's
   * dedup slots, sweep every local entry whose key starts with {@code prefix} at
   * or below the clear stamp, and (when {@code sync}) broadcast one
   * {@code INVALIDATE_PREFIX}.
   *
   * <p>
   * This is the namespace-scoped clear, deliberately not a table flush: the
   * engine owns a single table shared by every namespace, so a full clear would
   * evict unrelated namespaces and, worse, advance the table-wide watermark
   * that refuses every pre-clear fill regardless of namespace.
   *
   * <p>
   * Dedup slots of every in-flight load under the same prefix are dropped before
   * the sweep: a read that starts after the clear must not join a load that began
   * before it. A bulk {@code invalidateAll} would additionally discard unrelated
   * namespaces' completed loads, forcing needless reader re-invocations.
   *
   * <p>
   * Removal is best-effort termination, not a linearizable barrier — see
   * {@link #clear(InvalidatePolicy)}.
   *
   * @param prefix the physical key prefix (validated at the facade boundary:
   *               non-null and non-blank)
   * @param policy where the clear applies and whether it bypasses the
   *               active-transaction deferral
   * @return the allocated {@code clearStamp}, or {@code -1} when
   *         local-only/remote-only/deferred
   */
  public long clearPrefix(String prefix, InvalidatePolicy policy) {
    return doClear(prefix, policy);
  }

  /**
   * Shared clear body of {@link #clear} and {@link #clearPrefix}: a
   * {@code REMOTE} scope only broadcasts, while {@code LOCAL}/{@code BOTH}
   * record the clear watermark (table-wide for {@code prefix == null}, scoped
   * otherwise), drop the dedup slots, sweep the matching entries at or below
   * the stamp, and broadcast when the scope includes peers.
   *
   * <p>
   * The kill precedes the sweep: a table-wide clear drops every dedup slot with
   * one {@code invalidateAll()}; a scoped clear drops every in-flight load under
   * the same prefix, so a read that starts after the clear cannot join a
   * pre-clear load and keeps its own flight. Unrelated namespaces keep their
   * slots — discarding their completed loads would only force needless reader
   * re-invocations.
   *
   * @param prefix {@code null} for a table-wide clear, otherwise the key
   *               namespace prefix
   * @param policy where the clear applies and whether it bypasses the
   *               active-transaction deferral
   * @return the allocated {@code clearStamp}, or {@code -1} when
   *         local-only/remote-only/deferred
   */
  private long doClear(@Nullable String prefix, InvalidatePolicy policy) {
    if (TransactionSupport.isDeferred(policy.immediate())) {
      TransactionSupport.run(false, () -> clearNow(prefix, policy));
      return -1L;
    }
    return clearNow(prefix, policy);
  }

  private long clearNow(@Nullable String prefix, InvalidatePolicy policy) {
    // The watermark and the dedup kill form one prefix decision: the watermark
    // refuses pre-clear fills, and killing first lets every flight created after
    // this point belong to a post-clear read, so the sweep leaves it alone.
    long clearStamp = nextStamp();
    if (policy.scope() == InvalidatePolicy.Scope.REMOTE) {
      broadcastClear(prefix, clearStamp);
      return -1L;
    }

    if (prefix == null) {
      entryLifecycle.recordClear(clearStamp);
      singleFlight.invalidateAll();
    } else {
      entryLifecycle.recordPrefixClear(prefix, clearStamp);
      singleFlight.invalidatePrefix(prefix);
    }

    sweep(prefix, clearStamp);

    if (policy.sync()) {
      broadcastClear(prefix, clearStamp);
    }
    return policy.sync() ? clearStamp : -1L;
  }

  private void broadcastClear(@Nullable String prefix, long clearStamp) {
    if (prefix == null) {
      dispatcher.broadcastInvalidateAll(clearStamp);
    } else {
      dispatcher.broadcastInvalidatePrefix(prefix, clearStamp);
    }
  }

  /**
   * Guarded removal sweep shared by {@link #clear} and {@link #clearPrefix}:
   * remove every key under {@code prefix} whose data version is at or below
   * {@code versionCeiling}. Dedup slots are dropped once by the caller —
   * {@code invalidateAll} or {@code invalidatePrefix} — never per swept key.
   *
   * <p>
   * Weakly consistent key-set iteration: a key added mid-sweep either makes
   * it (fine — the guard judges it) or is skipped because the iterator already
   * passed its bin. A skipped key whose write guard predates the clear stamp
   * therefore survives, bounded by its own hard TTL: the sweep is best-effort
   * termination, not a linearizable barrier.
   *
   * <p>
   * Zero per-key allocation: {@code compute}'s return value is the post-image,
   * so {@code null} already means "removed (or absent)" — no out-param holder is
   * needed.
   *
   * @param prefix         which physical keys the sweep may touch ({@code null}
   *                       = every key; the engine owns the only L1 table, so the
   *                       full clear has no prefix to filter on)
   * @param versionCeiling entries at or below this stamp are removed; a write
   *                       allocated after it survives the sweep
   */
  private void sweep(@Nullable String prefix, long versionCeiling) {
    for (String key : caffeineCache.asMap().keySet()) {
      if (prefix != null && !key.startsWith(prefix)) {
        continue;
      }
      caffeineCache
          .asMap()
          .compute(key, (k, entry) -> (entry != null && entry.getDataVersion() > versionCeiling) ? entry : null);
    }
  }

  /**
   * The live L1 table this engine drives — the raw handle behind
   * {@code Zeta.unsafe().raw()}. Keys are physical keys; values are
   * {@link CacheEntry} wrappers.
   *
   * @return the table (never {@code null})
   */
  public Cache<String, CacheEntry> table() {
    return caffeineCache;
  }

  /**
   * Forget the given keys' SingleFlight dedup entries — the contract-preserving
   * companion to a {@link #table() raw} removal or overwrite. Every removal
   * site inside the engine wires this itself (design decisions: dedup lifecycle);
   * a raw mutation is the one path that cannot, so the unsafe view hands the
   * caller the same tool. Touches nothing else: no L1 effect, no broadcast,
   * in-flight loads still complete.
   *
   * @param cacheKeys the keys whose dedup entries to drop, already validated
   *                  by the facade's unsafe view
   */
  public void forgetDedup(String... cacheKeys) {
    for (String key : cacheKeys) {
      singleFlight.invalidate(key);
    }
  }

  /**
   * Resolve the effective soft TTL in milliseconds.
   * <p>
   * A non-positive value (meaning "use configured default") resolves to the
   * configured soft TTL; a positive value is returned as-is.
   *
   * @param softTtlMs the soft TTL value to resolve (0 = use configured default)
   * @return the effective soft TTL in milliseconds ({@code 0} = soft expiry
   *         disabled)
   */
  public long resolveEffectiveSoftTtl(long softTtlMs) {
    return ttlPolicy.resolveEffectiveSoftTtl(softTtlMs);
  }

  /**
   * Resolve a hard TTL override to the value the write path will actually store,
   * including the configured default and the permanent-entry sentinel.
   *
   * <p>
   * The facade calls this before a synced write so a configuration whose
   * {@code hard-ttl-ms} is {@code 0} (permanent) cannot broadcast an entry that
   * peers can never expire through a hard deadline.
   *
   * @param hardTtlMs the hard TTL override ({@code 0} = use configured default)
   * @return the effective hard TTL, with {@link Long#MAX_VALUE} for a permanent
   *         entry
   */
  public long resolveSyncableHardTtl(long hardTtlMs) {
    return ttlPolicy.normalHardBaseline(ttlPolicy.resolveEffectiveHardTtl(hardTtlMs));
  }

  /**
   * Unwrap an L1 entry to its user-facing form: the entry's inner value,
   * as stored. The shared shape of every read-side extraction
   * ({@code peek}, the hit paths, the stale fallback).
   * Values are stored as passed (no codec), so this is a plain passthrough.
   *
   * @param raw the L1 entry (may be {@code null})
   * @param <T> the expected value type
   * @return the user-facing value, or {@code null} when the entry is absent
   */
  @SuppressWarnings("unchecked")
  public static <T> T unwrap(@Nullable CacheEntry raw) {
    return raw != null ? (T) raw.getValue() : null;
  }
}
