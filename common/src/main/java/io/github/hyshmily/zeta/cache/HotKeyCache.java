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
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.*;
import io.github.hyshmily.zeta.exception.ZetaSuppressedException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
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
 * cross-instance synchronization via {@link BroadcastBuffer}.
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

  /** Central access counter: the read path's single detection/reporting entry. */
  private final AccessCounter accessCounter;

  /** Sync plane: deferred PUT/REFRESH plus the immediate INVALIDATE family. */
  private final BroadcastBuffer broadcastBuffer;

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
   * @param accessCounter       shared read-path counter for detection and reporting
   * @param broadcastBuffer     sync-plane send outlet for PUT/REFRESH and INVALIDATE
   * @param stampClock          data-version stamp allocation
   * @param healthView          cluster health view for Decision-Validity demotion
   */
  public HotKeyCache(
    HotKeyDetector hotKeyDetector,
    Cache<String, CacheEntry> caffeineCache,
    SingleFlight singleFlight,
    EntryLifecycle entryLifecycle,
    BackgroundRefresher backgroundRefresher,
    AccessCounter accessCounter,
    BroadcastBuffer broadcastBuffer,
    StampClock stampClock,
    HealthView healthView
  ) {
    this.accessCounter = Objects.requireNonNull(accessCounter, "accessCounter");
    this.broadcastBuffer = Objects.requireNonNull(broadcastBuffer, "broadcastBuffer");
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
      Objects.requireNonNull(healthView, "healthView")
    );
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

    return (
      (entry != null && !ttlPolicy.isLogicallyExpired(entry) && entry.getKeyState() == KeyState.HOT) ||
      hotKeyDetector.contains(cacheKey)
    );
  }

  /**
   * Record {@code count} cache accesses under an explicit recording mode without performing a
   * cache lookup.
   *
   * @param cacheKey the validated key to record
   * @param count    the positive number of accesses to add
   * @param mode     how much of the pipeline to run (never {@code null})
   */
  public void recordAccess(String cacheKey, long count, Zeta.TagMode mode) {
    accessCounter.record(cacheKey, count, mode);
  }

  /**
   * Batch form of {@link #recordAccess(String, long, Zeta.TagMode)}: record weighted accesses
   * for multiple validated keys under a shared mode.
   *
   * @param keyCounts validated map of cacheKey to a positive access count
   * @param mode      how much of the pipeline to run (never {@code null})
   */
  public void recordAccess(Map<String, Long> keyCounts, Zeta.TagMode mode) {
    accessCounter.recordAll(keyCounts, mode);
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
    GetPolicy options
  ) {
    boolean report = options.report();
    CacheEntry entry = caffeineCache.getIfPresent(key);
    if (entry == null) {
      requestMisses.increment();
      return loadAndCache(key, readContext(reader, loader, options));
    }

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
      ReadContext ctx = readContext(reader, loader, options);
      if (hit.reload()) {
        applyHit(key, hit, ctx, now);
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
    return loadAndCache(key, readContext(reader, loader, options));
  }

  /**
   * Builds the per-call context only when a read actually needs it: a plain hit
   * returns before this method is called, so the adapter for a plain supplier
   * is allocated only on the miss/reconcile path.
   */
  @SuppressWarnings("unchecked")
  private <T> ReadContext readContext(
    @Nullable Supplier<T> reader,
    @Nullable CacheLoader<String, T> loader,
    GetPolicy options
  ) {
    CacheLoader<String, Object> source = null;
    if (loader != null) {
      source = (CacheLoader<String, Object>) loader;
    } else if (reader != null) {
      source = key -> reader.get();
    }
    return ReadContext.of(source, options);
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
      accessCounter.record(key, report);
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
   * The single classification both read paths share, so the single-key and
   * batch probes cannot drift. A hard-expired entry is evicted here under the
   * identity guard; expiry is not a mutation, so the dedup flight is untouched.
   * The returned {@link HitAction} names the single rewrite
   * {@link #reconcileHit} must perform, so no predicate runs twice.
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
    // REVALIDATE is the only policy that blocks the caller on a reload; SOFT_REFRESH serves
    // the stale value and arms the refresh below. The state correction travels with both.
    boolean revalidate = softExpired && policy == StalePolicy.REVALIDATE;
    boolean refresh = softExpired && policy == StalePolicy.SOFT_REFRESH;
    return new HitClassification(current, resolveStateAction(key, current), refresh, revalidate);
  }

  /**
   * The one state rewrite (if any) a hit requires, decided without mutating the
   * entry.
   */
  private HitAction resolveStateAction(String key, CacheEntry entry) {
    if (entry.getKeyState() != KeyState.HOT) {
      return promotion.isDegradedPromotable(key, entry) ? HitAction.PROMOTE : HitAction.NONE;
    }
    if (entryLifecycle.needsDemotion(entry)) {
      return HitAction.DEMOTE;
    }
    return promotion.needsReassert(entry) ? HitAction.REASSERT : HitAction.NONE;
  }

  /**
   * Batch variant of {@link #get(String, Supplier, GetPolicy)}. All keys share
   * the same explicit options (TTL overrides are evaluated once for the whole
   * batch — the batch pipeline has no per-key lazy-TTL path — and validated
   * before any load or breaker settlement); the positional {@code reader}
   * governs loading. The batch pipeline carries no admission
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
   * @param reader    the value function for cache misses, or {@code null} when
   *                  no load is desired (a miss then resolves to the stale
   *                  fallback only)
   * @param options   the shared per-invocation read options
   * @param <T>       the value type
   * @return a map of key → loaded or cached value (never {@code null});
   *         keys with no value are omitted
   */
  public <T> Map<String, T> get(
    List<String> cacheKeys,
    @Nullable Function<? super String, ? extends T> reader,
    GetPolicy options
  ) {
    // The batch pipeline has no per-key lazy-TTL path: evaluate the override
    // suppliers once for the whole batch (the documented batch contract), and
    // validate the result at this boundary exactly like the single-key lazy path.
    long hard = options.hardTtlMs().getAsLong();
    long soft = options.softTtlMs().getAsLong();
    CacheEntry.requireEncodableTtl(hard, "hardTtlMs");
    CacheEntry.requireEncodableTtl(soft, "softTtlMs");
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
   * @param reader    the value function for cache misses and hit-phase
   *                  refreshes, or {@code null} for a cache-only read
   * @param options   the shared per-invocation read options
   * @param hardTtlMs the batch-resolved hard TTL override (0 = default)
   * @param softTtlMs the batch-resolved soft TTL override (0 = default)
   * @param <T>       the value type
   * @return a map of key → loaded or cached value (never {@code null});
   *         keys with no value are omitted
   */
  private <T> Map<String, T> batchGet(
    List<String> cacheKeys,
    @Nullable Function<? super String, ? extends T> reader,
    GetPolicy options,
    long hardTtlMs,
    long softTtlMs
  ) {
    List<String> misses = new ArrayList<>(cacheKeys.size());
    Map<String, T> results = new LinkedHashMap<>(mapCapacity(cacheKeys.size()));
    boolean report = options.report();
    long now = TimeSource.currentTimeMillis();
    Set<String> seen = new HashSet<>(mapCapacity(cacheKeys.size()));
    StalePolicy stalePolicy = StalePolicy.orDefault(options.stalePolicy());

    for (String key : cacheKeys) {
      if (!seen.add(key)) {
        continue;
      }

      T hit = batchHit(key, reader, stalePolicy, hardTtlMs, softTtlMs, report, now);
      if (hit != null) {
        requestHits.increment();
        results.put(key, hit);
      } else {
        requestMisses.increment();
        misses.add(key);
      }
    }
    if (misses.isEmpty()) {
      return results;
    }

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
  private <T> T batchHit(
    String key,
    @Nullable Function<? super String, ? extends T> reader,
    StalePolicy stalePolicy,
    long hardTtlMs,
    long softTtlMs,
    boolean report,
    long now
  ) {
    CacheEntry entry = caffeineCache.getIfPresent(key);
    if (entry == null) {
      return null;
    }

    HitClassification hit = classifyHit(key, entry, stalePolicy, now);
    if (hit.isPlainServe()) {
      return servePlain(key, hit.entry(), report);
    }
    if (hit.reload()) {
      applyHit(key, hit, hardTtlMs, softTtlMs, now);
      return null;
    }
    if (!hit.needsReconcile()) {
      return null;
    }
    // A stale Worker decision, a degraded-mode promotion or a soft-expired entry
    // needs the context; arm the reader for every SOFT_REFRESH hit, because a
    // concurrent invalidation can close the soft window between this probe and
    // reconcileHit() (design decisions: read path).
    CacheLoader<String, Object> refreshLoader =
      stalePolicy == StalePolicy.SOFT_REFRESH && reader != null ? reader::apply : null;
    return reconcileHit(key, hit, ReadContext.ofBatch(refreshLoader, hardTtlMs, softTtlMs, report, stalePolicy), now);
  }

  /**
   * Arm a background refresh for a soft-expired entry — HOT, COOL and NORMAL
   * alike, so every stale read re-arms its refresh regardless of the TTL regime
   * (design decisions: expiry and refresh).
   *
   * <p>
   * Judged against the caller's single {@code now} sample, and with zero lookups
   * on a fresh hit: only a genuinely soft-expired entry confirms it is still
   * resident before arming, because the refresh re-warms the key and must not
   * fire for an entry a concurrent invalidate removed. The scheduled task holds
   * only the reload source and the key — never the read context — so a queued
   * refresh cannot pin the call's TTL suppliers or their SpEL scope.
   *
   * @param key   the cache key
   * @param entry the L1 entry in force for this read (never {@code null})
   * @param ctx   the per-call read context (no source skips the refresh — a
   *              call without a reader or loader has nothing to reload)
   * @param now   the current epoch-millis (a single per-call sample)
   */
  private void refreshSoftExpire(String key, CacheEntry entry, ReadContext ctx, long now) {
    CacheLoader<String, Object> source = ctx.loader();
    if (source == null || !ttlPolicy.isSoftExpired(entry, now)) {
      return;
    }

    CacheEntry current = quietCache.getIfPresent(key);
    if (current == null) {
      return;
    }
    if (current != entry && !ttlPolicy.isSoftExpired(current, now)) {
      return;
    }

    long softTtl = ttlPolicy.resolveRefreshSoftTtl(current, ctx.softTtlMs());
    // Capture the reload source, never the context: a queued refresh must not pin
    // the TTL suppliers or the SpEL scope that produced them.
    backgroundRefresher.triggerBackgroundRefresh(
      key,
      () -> {
        long stamp = nextStamp();
        Object value = source.reload(key);
        return value != null && source.isCacheable(value) ? new VersionedValue(value, stamp) : null;
      },
      softTtl
    );
  }

  /**
   * Resolve the promotion TTLs only when the classification actually promotes. A demotion,
   * reassertion or plain rewrite must not evaluate a SpEL-backed TTL supplier: the batch path
   * uses the long-taking form below with values it already resolved once for the whole batch.
   */
  private CacheEntry applyHit(String key, HitClassification hit, ReadContext ctx, long now) {
    if (hit.action() == HitAction.PROMOTE) {
      return applyHit(key, hit, ctx.hardTtlMs(), ctx.softTtlMs(), now);
    }
    return applyHit(key, hit, 0L, 0L, now);
  }

  /**
   * Applies the one state rewrite a hit classification named and returns the
   * post-image the bin lock observed. A {@code null} answer means the key is now
   * absent (a removal, or a hard-expired resident cleared by the compute), so
   * the caller reloads instead of serving the pre-lock snapshot. A reload
   * discards the returned image; it calls this only for the correction.
   *
   * @param key                the cache key
   * @param hit                the classification whose action to apply
   * @param hardTtlOverrideMs the per-call hard TTL override (0 = none)
   * @param softTtlOverrideMs the per-call soft TTL override (0 = none)
   * @param now                the per-call clock sample
   * @return the entry to serve, or {@code null} when the key is gone
   */
  @Nullable
  private CacheEntry applyHit(
    String key,
    HitClassification hit,
    long hardTtlOverrideMs,
    long softTtlOverrideMs,
    long now
  ) {
    CacheEntry entry = hit.entry();
    if (entry == null) {
      return null;
    }
    return switch (hit.action()) {
      case DEMOTE -> entryLifecycle.demoteIfDecisionInvalid(key, entry, now).current();
      case REASSERT -> promotion.reassertIfNeeded(key, entry, now).current();
      case PROMOTE -> promotion.promoteDegraded(key, hardTtlOverrideMs, softTtlOverrideMs, now).current();
      case NONE -> entry;
      case EXPIRED -> null;
    };
  }

  /**
   * Reconcile a classified hit: perform the one state rewrite the
   * classification named, then serve the post-image its bin lock observed — no
   * second table lookup and no fallback to the pre-lock snapshot, so a
   * concurrent invalidation or eviction resolves as a miss. Both read paths
   * converge here (design decisions: hot state).
   *
   * <p>
   * A {@code SOFT_REFRESH} is armed here, against the entry <em>in force</em>: a
   * demotion above replaces the snapshot in place, and arming on the stale one
   * would refresh a demoted key at the hot cadence. The access is recorded only
   * when the read serves; the expired branch records nothing and the caller's
   * reload records once (design decisions: access counting).
   *
   * @param key the cache key
   * @param hit the classification and the resident it applies to
   * @param ctx the per-call read context (TTL overrides stay lazy; carries
   *            the report flag)
   * @param now the per-call clock sample
   * @param <T> the value type
   * @return the cached value, or {@code null} if logically expired
   */
  private <T> T reconcileHit(String key, HitClassification hit, ReadContext ctx, long now) {
    CacheEntry entry = hit.entry();
    if (entry == null || entry.getValue() == null) {
      return null;
    }

    // The post-image comes from the mutation's own compute, i.e. from what the
    // bin lock observed: an entry a concurrent invalidate/eviction removed
    // yields null and becomes a miss, never the snapshot read before the lock.
    // Every rewrite removes a resident that was hard-expired at `now`, so the
    // post-image is live and needs no second expiry check.
    CacheEntry effective = applyHit(key, hit, ctx, now);
    if (effective == null) {
      return null;
    }

    accessCounter.record(key, ctx.report());
    // Arm only when the classification asked for a refresh or a rewrite changed
    // the entry's expiry image; a rewrite alone can leave a fresh entry.
    if (ctx.stalePolicy() == StalePolicy.SOFT_REFRESH && (hit.refresh() || effective != entry)) {
      refreshSoftExpire(key, effective, ctx, now);
    }
    // Return the post-image's value: a concurrent replacement observed under the
    // bin lock may differ from the snapshot this call started with.
    return unwrap(effective);
  }

  /**
   * Load via SingleFlight, stamp the value, and store it with the HOT or NORMAL
   * TTL regime. The option's {@code callerRunsLoader} flag selects the dedup
   * election: {@code true} runs the leader's reader on this thread (annotation
   * reads keep their transaction, security context and MDC), while same-key
   * followers still join its future; the default runs it on the load executor.
   *
   * <p>
   * A caller-affine load inside an active transaction is isolated: the reader
   * runs on this thread, but no flight is published and no L1 store is
   * registered, because the value may include the transaction's own uncommitted
   * writes (design decisions: read path).
   *
   * @param key the key to load
   * @param ctx the per-call read context (loader/reader source and the loader's
   *            admission rule; the TTL suppliers are consumed exactly once per
   *            call through the memoized context)
   */
  @SuppressWarnings("unchecked")
  private <T> T loadAndCache(String key, ReadContext ctx) {
    if (!ctx.hasSource()) {
      // No reader on a miss resolves as the documented null answer instead of an
      // NPE inside the SingleFlight execution.
      return resolveNullAnswer(key, ctx.report());
    }

    long hardTtl = ttlPolicy.resolveEffectiveHardTtl(ctx.hardTtlMs());
    long softTtl = ttlPolicy.resolveEffectiveSoftTtl(ctx.softTtlMs());
    Supplier<VersionedValue> stampedReader = () -> {
      long stamp = nextStamp();
      T loaded = (T) ctx.loadValue(key);
      return loaded == null ? null : new VersionedValue(loaded, stamp);
    };
    boolean transactionalCaller = ctx.callerRunsLoader() && TransactionSupport.isDeferred(false);
    LoaderOutcome<VersionedValue> outcome = transactionalCaller
      ? singleFlight.loadIsolated(key, stampedReader, value -> {})
      : singleFlight.load(key, stampedReader, ctx.callerRunsLoader(), value ->
          loadCacheEntry(key, hardTtl, softTtl, value, ctx)
        );
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
   *                  threads), or {@code null} for a cache-only read
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
    @Nullable Function<? super String, ? extends T> reader,
    long hardTtlMs,
    long softTtlMs,
    boolean report,
    Map<String, T> into
  ) {
    if (reader == null) {
      // No load is desired: a miss resolves like the single-key path's null reader, including the
      // breaker-open stale fallback, instead of dereferencing null on the load executor.
      for (String key : cacheKeys) {
        T value = resolveNullAnswer(key, report);
        if (value != null) {
          into.put(key, value);
        }
      }
      return;
    }
    final long effectiveHardTtlMs = ttlPolicy.resolveEffectiveHardTtl(hardTtlMs);
    final long effectiveSoftTtlMs = ttlPolicy.resolveEffectiveSoftTtl(softTtlMs);
    Map<String, Long> stamps = new HashMap<>(mapCapacity(cacheKeys.size()));
    allocateStampBlocks(cacheKeys, stamps);

    Map<String, LoaderOutcome<VersionedValue>> loaded = singleFlight.load(
      cacheKeys,
      k -> {
        T value = reader.apply(k);
        return value == null ? null : new VersionedValue(value, stamps.get(k));
      },
      (k, value) -> loadCacheEntry(k, effectiveHardTtlMs, effectiveSoftTtlMs, value, null)
    );

    Throwable failure = null;
    for (String key : cacheKeys) {
      try {
        T value = resolveLoaded(key, loaded.get(key), report);
        if (value != null) {
          into.put(key, value);
        }
      } catch (RuntimeException | Error e) {
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
        accessCounter.record(key, report);
        yield (T) value.value();
      }
      // An empty source is not a failure: no throw, no suppression log.
      case SOURCE_EMPTY -> resolveNullAnswer(key, report);
      case SUPPRESSED_BREAKER, SUPPRESSED_TIMEOUT, SUPPRESSED_EXECUTOR -> resolveSuppressed(
        key,
        outcome.status(),
        report
      );
      case FAILED -> resolveFailed(key, outcome.cause(), report);
    };
  }

  /**
   * A suppressed load: the reader never ran, so the caller must be able to tell
   * this from "no data" ({@code SOURCE_EMPTY}). Try the hard-valid stale entry
   * once — without a second breaker probe, so a recovery that landed between
   * admission and resolution cannot turn a designed degradation into an
   * exception — and throw when nothing is servable. A <em>failed</em> load with
   * an open breaker follows the same stale-first rule and rethrows the original
   * cause when no stale entry is servable (see {@link #resolveFailed}).
   *
   * @param key    the key whose load was suppressed
   * @param status the suppression reason
   * @param report whether this read feeds Worker reporting
   * @param <T>    the value type
   * @return the stale value
   * @throws ZetaSuppressedException when no hard-valid stale value is servable
   */
  private <T> T resolveSuppressed(String key, LoadStatus status, boolean report) {
    T stale = serveStaleEntry(key, report);
    if (stale != null) {
      logSuppressedLoad(key, status);
      return stale;
    }
    throw new ZetaSuppressedException(key, status);
  }

  /**
   * A real load failure: {@code Error} always propagates, an
   * {@code InterruptedException} degrades to a miss, and an open breaker serves a
   * hard-valid stale entry when one exists. Otherwise the cause is rethrown
   * (unchecked) or wrapped.
   *
   * <p>The thread that ran the reader owns the interrupt flag: {@code LoadFlights}
   * restores it on the caller-affine and isolated paths, and an executor thread's
   * interrupt belongs to the timeout that raised it. A follower that merely joins a
   * failed future must never be interrupted for an outcome it only observed.
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
      return resolveNullAnswer(key, report);
    }
    if (singleFlight.isBreakerOpen(key)) {
      T stale = serveStaleEntry(key, report);
      if (stale != null) {
        return stale;
      }
      // Nothing servable: the failure is the only truthful answer. Falling through
      // to null would make an outage indistinguishable from SOURCE_EMPTY.
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
   * A source {@code null} arrives as {@code LOADED} with a null payload from the
   * built-in paths, or as {@link LoadStatus#SOURCE_EMPTY} from a custom
   * {@code SingleFlight}; the no-answer statuses (failure, suppression) resolve
   * the same way (design decisions: read path). <b>Null is never cached:</b> the
   * next read re-invokes the loader; callers needing penetration protection
   * decline the value at their loader instead.
   *
   * @param key    the key whose load produced a null or no answer
   * @param report whether this batch/call feeds Worker reporting
   * @param <T>    the expected value type
   * @return a stale value when the circuit breaker is open and a cached entry
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
      log.debug("Degraded read, discarding hard-expired entry for key={}", key);
      return null;
    }
    T value = unwrap(resident);
    // Only a served value is a read: a value-less entry is not served and must
    // not feed the access history.
    if (value == null) {
      return null;
    }
    accessCounter.record(key, report);
    log.debug("Degraded read, returning stale entry for key={}", key);
    return value;
  }

  /**
   * Suppression log for the {@code loadAndCache} paths: a suppression that
   * serves a stale value produces no exception, so without this line the
   * degradation would be invisible (a suppression with nothing servable throws
   * and needs no log). DEBUG — a suppression is the guard working as designed,
   * not a fault.
   *
   * @param key    the key whose load was suppressed
   * @param status the suppression cause (breaker, timeout, or executor)
   */
  private void logSuppressedLoad(String key, LoadStatus status) {
    log.debug("HotKeyCache load suppressed for key={} (status={}; returning the stale entry)", key, status);
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
   * @param now          the caller's single epoch-millis sample
   * @return {@code true} when the write must leave the resident untouched
   */
  private boolean writeRefused(String key, @Nullable CacheEntry entry, long stamp, boolean equalApplies, long now) {
    if (entryLifecycle.staleAgainstInvalidation(key, stamp)) {
      return true;
    }
    return (
      entry != null &&
      !ttlPolicy.isLogicallyExpired(entry, now) &&
      (equalApplies ? VersionGuard.shouldSkipForRefresh(entry, stamp) : VersionGuard.shouldSkipForSync(entry, stamp))
    );
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
   * @param ctx           the per-call read context whose loader admission rule
   *                      gates the store, or {@code null} to admit every value
   */
  private void loadCacheEntry(
    String key,
    long normalHardTtl,
    long normalSoftTtl,
    VersionedValue loaded,
    @Nullable ReadContext ctx
  ) {
    // Loader admission: a rejected value never takes L1 residency
    // (detection/reporting already ran for the value). Evaluate it here, on the
    // load thread, so a deferred store does not pin the loader or its scope.
    if (ctx != null && !ctx.admits(loaded.value())) {
      log.debug("loader admission rejected value, skipping L1 store: key={}", key);
      return;
    }

    Object value = loaded.value();
    long stamp = loaded.dataVersion();
    boolean hot = hotKeyDetector.contains(key);
    // Inside an active transaction the loader may have read that transaction's
    // own uncommitted writes. The store waits for afterCommit, so a rollback
    // never leaves those writes in L1. Outside a transaction it runs inline.
    TransactionSupport.run(false, () ->
      EntryMutations.mutate(caffeineCache, key, entry ->
        entryForWrite(key, entry, value, stamp, normalHardTtl, normalSoftTtl, false, hot)
      )
    );
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
        broadcastBuffer.invalidate(cacheKey, stamp);
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
   * <b>The version must already be allocated</b> by {@link #nextStamp()} before
   * the drop, so a racing reload carries the same stamp and the mutation
   * supersedes it instead of pinning the stale value (design decisions:
   * invalidation).
   *
   * <p>
   * The tombstone is recorded for local-only invalidations too: {@code broadcast}
   * gates the peer send, never the local guard.
   *
   * @param key       the cache key
   * @param stamp     the version allocated before the drop (positive)
   * @param broadcast whether peers are notified
   */
  private void dropAndBroadcastInvalidate(String key, long stamp, boolean broadcast) {
    dropLocal(key, stamp);
    if (broadcast) {
      broadcastBuffer.invalidate(key, stamp);
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
    if (cacheKeys.isEmpty()) {
      return;
    }
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
   * The versions inside a chunk are reserved as <b>one contiguous block before
   * any drop</b>, so a concurrent single write is ordered against the chunk
   * all-or-nothing: older than every entry (the batch supersedes it) or newer
   * than every one (it survives), never partially applied (design decisions:
   * invalidation).
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
        broadcastBuffer.invalidateBatch(entries);
      }
    });
  }

  /**
   * Refresh the given key from a caller supplier: allocate the version before
   * the load, resolve the value-only TTL policy, then apply the loaded value
   * through the write-through guard.
   *
   * <p><b>Value-only renewal.</b> A resident entry keeps its lifecycle policy:
   * a {@code HOT}/{@code COOL} entry keeps its state, Worker decision lease and
   * hard deadline (only the value, stamp and soft window move), and a
   * {@code NORMAL} entry keeps its stored normal hard/soft TTL baselines. An
   * absent or hard-expired key is rebuilt from the configured defaults.
   * A refresh never changes a stored TTL; a {@code put} can change it only on a
   * {@code NORMAL} entry — an elevated entry's active regime and normal baseline
   * stay with the state machine until demotion.
   *
   * <p><b>Ordering:</b> the version is allocated before the resident probe and
   * the load, so a concurrent write landing after the allocation holds the
   * newer version and this refresh's stale apply is skipped instead of
   * regressing it (design decisions: version stamps). A burned version on
   * loader failure is a harmless watermark hole.
   *
   * <p>A {@code null} loader result <b>skips the refresh</b> entirely
   * (DEBUG): L1 keeps its current entry — a refresh that learned nothing must
   * not evict the value it was asked to renew. Outside a transaction the whole
   * bump-probe-load-apply runs inline on the caller thread; inside a
   * transaction it waits for {@code afterCommit}, so the loader observes
   * committed data and registration order becomes version order.
   *
   * @param cacheKey the key to refresh
   * @param reader   the value supplier (caller thread outside a transaction,
   *                 commit thread inside one)
   * @param sync     whether to broadcast the applied value to peers
   * @param <V>      the value type
   */
  public <V> void refresh(String cacheKey, Supplier<V> reader, boolean sync) {
    TransactionSupport.run(false, () -> executeRefreshNow(cacheKey, reader, null, sync));
  }

  /**
   * Refresh the given key through a typed loader: the value comes from
   * {@link CacheLoader#reload} and is applied under the same value-only contract
   * as {@link #refresh(String, Supplier, boolean)}. The loader's
   * {@link CacheLoader#isCacheable} admission is honored; a declined value
   * skips the apply exactly like a {@code null} reload.
   *
   * <p>{@code reload}, not {@code load}, is the refresh seam: a refresh can run
   * on an after-commit or caller-owned scheduler thread where the original
   * call's transaction, security context and MDC are gone.
   *
   * @param cacheKey the key to refresh
   * @param loader   the loader whose {@code reload} produces the value
   * @param sync     whether to broadcast the applied value to peers
   * @param <V>      the value type
   */
  public <V> void refresh(String cacheKey, CacheLoader<String, V> loader, boolean sync) {
    TransactionSupport.run(false, () -> executeRefreshNow(cacheKey, null, loader, sync));
  }

  /**
   * Bump-then-probe-then-load-then-apply refresh body: invoked inline outside a
   * transaction and from {@code afterCommit} inside one, so bump-before-load
   * holds either way, the loader observes committed data, and the allocated
   * version follows registration order.
   *
   * <p>The stamp is allocated before the resident probe on purpose: a write
   * landing after the allocation carries a newer stamp and refuses this apply,
   * so the probed TTL baseline cannot overwrite a baseline the refresh did not
   * observe. A write landing before the allocation is what the probe sees.
   *
   * @param key    the key to refresh
   * @param reader the supplier source, or {@code null} when a {@code loader} is
   *               supplied
   * @param loader the loader source, or {@code null} when a {@code reader} is
   *               supplied
   * @param sync   whether to broadcast the applied value to peers
   * @param <V>    the value type
   */
  private <V> void executeRefreshNow(
    String key,
    @Nullable Supplier<V> reader,
    @Nullable CacheLoader<String, V> loader,
    boolean sync
  ) {
    long stamp = nextStamp();
    CacheEntry resident = quietCache.getIfPresent(key);
    CacheEntry live = resident != null && !ttlPolicy.isLogicallyExpired(resident) ? resident : null;
    // Value-only renewal: the resident owns the lifecycle policy, so its stored
    // normal baselines (0 = unset) win; only an absent/hard-expired key falls
    // back to the configured defaults. A HOT/COOL entry ignores these values:
    // EntryLifecycle.writeValue keeps its active regime, hard deadline and normal
    // baseline.
    long hardTtlMs = live != null ? live.getNormalHardTtlMs() : ttlPolicy.effectiveHardTtlMs();
    if (hardTtlMs == 0L) {
      hardTtlMs = ttlPolicy.effectiveHardTtlMs();
    }
    long softTtlMs = live != null ? live.getNormalSoftTtlMs() : ttlPolicy.effectiveSoftTtlMs();
    // The inherited baseline is known only here; the facade validated the
    // configured default before entering the engine.
    PutPolicy.requireSyncable(ttlPolicy.normalHardBaseline(hardTtlMs), sync);

    V value = loader != null ? loader.reload(key) : reader.get();
    if (value == null) {
      log.debug("refresh loader returned null for key={}, refresh skipped (L1 keeps current entry)", key);
      return;
    }
    if (loader != null && !loader.isCacheable(value)) {
      log.debug("refresh loader declined cacheability for key={}, refresh skipped (L1 keeps current entry)", key);
      return;
    }
    writeThroughApply(key, value, stamp, hardTtlMs, softTtlMs, sync);
  }

  /**
   * Cluster-sync write: update L1 with a version stamp and broadcast a
   * value-carrying PUT to peers (they apply the value version-guarded, no
   * reload). There is no business writer: a put is pure cache maintenance,
   * so "mutate the source, then update the cache" orchestration belongs to the
   * caller.
   *
   * <p>
   * Pass 0 to use the configured default for that TTL type. On an elevated
   * (HOT/COOL) resident the call is a value-only update: the active TTL regime and
   * the stored normal baseline survive, exactly as in a refresh. The state machine
   * owns both until the entry returns to NORMAL.
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
   * Apply one write to L1, retire the key's dedup entry when the store applied,
   * and — when {@code broadcast} is set and the store was actually applied —
   * broadcast the value-carrying PUT to peers (they apply version-guarded, no
   * reload; degrades to a value-less REFRESH inside the broadcast buffer when the
   * value is not serializable or over the size bound). This is the single apply
   * tail of {@code putThrough}, {@code refresh} and {@code putLocal}.
   *
   * <p>
   * <b>L1 first, then the dedup removal, then the broadcast</b> (design
   * decisions: dedup lifecycle): the store obsoletes every load result recorded
   * before it, so the dedup entry is dropped immediately after the apply; a
   * failing broadcast must not leave that pre-write future replayable.
   *
   * <p>
   * <b>A refused store does nothing else.</b> The guard inside
   * {@link #entryForWrite} can refuse the apply — a strictly newer resident, or
   * a tombstone recorded between the stamp allocation and this compute. Such a
   * write landed no value, so it must not broadcast it (peers would re-warm a
   * value this node already superseded or invalidated) and must not retire the
   * dedup entry: the newer write or invalidation already retired the flight it
   * superseded, and a flight created afterwards may be the one filling L1 with
   * the newer value.
   */
  private void writeThroughApply(
    String key,
    Object value,
    long stamp,
    long normalHardTtl,
    long normalSoftTtl,
    boolean broadcast
  ) {
    boolean hot = hotKeyDetector.contains(key);
    CacheEntry applied = EntryMutations.mutate(caffeineCache, key, entry ->
      entryForWrite(key, entry, value, stamp, normalHardTtl, normalSoftTtl, true, hot)
    );
    // Only an applied store obsoletes a recorded load. A refused write returns the
    // untouched resident, whose older stamp cannot equal this call's freshly
    // allocated one.
    if (applied != null && applied.getDataVersion() == stamp) {
      singleFlight.invalidate(key);
      if (broadcast) {
        // The predicate runs when the record enters the buffer, after serialization:
        // an invalidation that lands while the payload is being built suppresses it.
        broadcastBuffer.put(key, value, stamp, version -> !entryLifecycle.staleAgainstInvalidation(key, version));
      }
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
  private CacheEntry entryForWrite(
    String key,
    @Nullable CacheEntry entry,
    Object value,
    long stamp,
    long normalHardTtl,
    long normalSoftTtl,
    boolean equalApplies,
    boolean hot
  ) {
    // One clock sample for the guard, the promotion decision and the rewrite: a
    // second sample can cross the hard deadline between them and turn a live HOT
    // rewrite into a fresh NORMAL one.
    long now = TimeSource.currentTimeMillis();
    if (writeRefused(key, entry, stamp, equalApplies, now)) {
      return entry;
    }

    boolean promoteHot =
      hot && (entry == null || ttlPolicy.isLogicallyExpired(entry, now) || promotion.mayPromote(entry));
    return entryLifecycle.writeValue(entry, value, stamp, normalHardTtl, normalSoftTtl, promoteHot, now);
  }

  /**
   * Write a value directly into the local L1 cache with a fresh data stamp,
   * without a cluster broadcast, without feeding access counts to detection,
   * and without reporting.
   * <p>
   * Existing entry metadata is preserved. On an elevated (HOT/COOL) existing
   * entry the active TTL regime and the stored normal baseline are kept as well —
   * only the value, its stamp and the soft window are updated. If no entry
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
   *
   * <p>The map is caller-supplied and may mutate under a direct (non-deferred)
   * run. {@code Math.max(1, remaining)} keeps a stale zero size from requesting
   * an empty stamp block (an infinite loop), and the inner {@code hasNext} guard
   * keeps a shrinking map from calling {@code next()} past its last entry. A
   * shrink can burn unused stamps in the reserved block — a harmless watermark
   * hole, never a correctness gap.
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
      int blockSize = Math.min(Math.max(1, remaining), StampClock.MAX_ATOMIC_RANGE);
      long base = stampClock.nextStamps(blockSize);
      int applied = 0;
      while (applied < blockSize && iterator.hasNext()) {
        Map.Entry<String, ?> entry = iterator.next();
        writeThroughApply(entry.getKey(), entry.getValue(), base + applied, effectiveHard, effectiveSoft, sync);
        applied++;
      }
      remaining -= applied;
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
      broadcastBuffer.invalidateAll(clearStamp);
    } else {
      broadcastBuffer.invalidatePrefix(prefix, clearStamp);
    }
  }

  /**
   * Guarded removal sweep shared by {@link #clear} and {@link #clearPrefix}:
   * remove every key under {@code prefix} whose data version is at or below
   * {@code versionCeiling}. Dedup slots are dropped once by the caller —
   * {@code invalidateAll} or {@code invalidatePrefix} — never per swept key.
   *
   * <p>
   * Weakly consistent entry-set iteration: a key added mid-sweep either makes
   * it (fine — the guard judges it) or is skipped because the iterator already
   * passed its bin. A skipped key whose write guard predates the clear stamp
   * therefore survives, bounded by its own hard TTL: the sweep is best-effort
   * termination, not a linearizable barrier.
   *
   * <p>
   * The version and prefix tests run on the snapshot the iterator already holds,
   * so an entry above the ceiling never takes the bin lock; only a candidate
   * reaches {@code computeIfPresent}, which re-judges the live value and removes
   * it. {@code null} is the post-image, so no out-param holder is needed.
   *
   * @param prefix         which physical keys the sweep may touch ({@code null}
   *                       = every key; the engine owns the only L1 table, so the
   *                       full clear has no prefix to filter on)
   * @param versionCeiling entries at or below this stamp are removed; a write
   *                       allocated after it survives the sweep
   */
  private void sweep(@Nullable String prefix, long versionCeiling) {
    for (Map.Entry<String, CacheEntry> candidate : caffeineCache.asMap().entrySet()) {
      // Integer compare first: startsWith is the expensive half and most entries
      // in a table-wide sweep fail this cheaper test.
      if (candidate.getValue().getDataVersion() > versionCeiling) {
        continue;
      }
      if (prefix != null && !candidate.getKey().startsWith(prefix)) {
        continue;
      }
      caffeineCache
        .asMap()
        .computeIfPresent(candidate.getKey(), (key, entry) -> entry.getDataVersion() > versionCeiling ? entry : null);
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
