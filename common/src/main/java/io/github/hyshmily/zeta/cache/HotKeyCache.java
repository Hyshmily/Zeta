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
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.WarmScheduler;
import io.github.hyshmily.zeta.scheduler.WarmSignal;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.CollectionSizing;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Core orchestrator for the L1 cache.
 *
 * <p>Coordinates Caffeine storage, node-local {@code dataVersion} stamp allocation, SingleFlight
 * load deduplication, soft-expiry (stale-while-revalidate), background warming, and peer
 * synchronization via {@link BroadcastBuffer}. All write and invalidation paths are transaction-
 * aware through {@link TransactionSupport}.
 *
 * <p><b>Heat isolation.</b> Hot/cool classification never alters data-plane TTLs or adds bin-lock
 * work to cache hits; the only bridge is offering newly stored entries to {@link WarmScheduler},
 * which reloads values strictly under the caller's declared lifetimes.
 *
 * <p><b>Boundary validation.</b> Callers ({@link Zeta} facade and annotation interceptor) validate
 * keys and resolve TTL overrides before entering this class, so all methods assume non-null,
 * non-blank keys.
 */
@Slf4j
@Internal
public class HotKeyCache {

  /** Underlying L1 Caffeine table storing {@link CacheEntry} values. */
  private final Cache<String, CacheEntry> caffeineCache;

  /** Executor managing soft-refresh and hard-renewal background tasks. */
  private final BackgroundRefresher backgroundRefresher;

  /** Entry draft factory and TOCTOU invalidation guards. */
  private final EntryLifecycle entryLifecycle;

  /** Shared read-path access counter feeding local detection and worker reporting. */
  private final AccessCounter accessCounter;

  /** Sync-plane send outlet for deferred PUT/REFRESH and immediate INVALIDATE messages. */
  private final BroadcastBuffer broadcastBuffer;

  /** Deduplicator preventing concurrent redundant loads for the same key. */
  private final SingleFlight singleFlight;

  /** Node-local stamp allocator producing monotonic {@code dataVersion} values. */
  private final StampClock stampClock;

  /** Stateless TTL resolution and expiry arithmetic owned by {@link #entryLifecycle}. */
  private final TtlPolicy ttlPolicy;

  /** Policy-quiet, stats-quiet wrapper over {@link #caffeineCache} for internal probes. */
  private final QuietCache<String, CacheEntry> quietCache;

  /** Request-level miss counter for load-capable read paths (excludes {@code peek()}). */
  private final LongAdder requestMisses = new LongAdder();

  /** Request-level hit counter for load-capable read paths (excludes {@code peek()}). */
  private final LongAdder requestHits = new LongAdder();

  /** Optional local renewal scheduler installed after construction; {@code null} disables warming. */
  private volatile @Nullable WarmScheduler warmScheduler;

  /**
   * Creates the L1 cache orchestrator.
   *
   * @param caffeineCache       underlying L1 Caffeine table
   * @param singleFlight        load deduplication coordinator
   * @param entryLifecycle      entry factory and invalidation watermark manager
   * @param backgroundRefresher soft-refresh and hard-renewal executor
   * @param accessCounter       shared read-path counter for detection and reporting
   * @param broadcastBuffer     sync-plane send outlet for PUT/REFRESH and INVALIDATE
   * @param stampClock          node-local {@code dataVersion} stamp allocator
   */
  public HotKeyCache(
    Cache<String, CacheEntry> caffeineCache,
    SingleFlight singleFlight,
    EntryLifecycle entryLifecycle,
    BackgroundRefresher backgroundRefresher,
    AccessCounter accessCounter,
    BroadcastBuffer broadcastBuffer,
    StampClock stampClock
  ) {
    this.caffeineCache = Objects.requireNonNull(caffeineCache, "caffeineCache must not be null");
    this.singleFlight = Objects.requireNonNull(singleFlight, "singleFlight must not be null");
    this.entryLifecycle = Objects.requireNonNull(entryLifecycle, "entryLifecycle must not be null");
    this.backgroundRefresher = Objects.requireNonNull(backgroundRefresher, "backgroundRefresher must not be null");
    this.accessCounter = Objects.requireNonNull(accessCounter, "accessCounter must not be null");
    this.broadcastBuffer = Objects.requireNonNull(broadcastBuffer, "broadcastBuffer must not be null");
    this.stampClock = Objects.requireNonNull(stampClock, "stampClock must not be null");
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.quietCache = new QuietCache<>(this.caffeineCache);
  }

  /**
   * Installs or clears the local background renewal scheduler after construction.
   *
   * @param warmScheduler the scheduler receiving newly stored entries, or {@code null} to disable
   */
  public void setWarmScheduler(@Nullable WarmScheduler warmScheduler) {
    this.warmScheduler = warmScheduler;
  }

  /**
   * Returns request-level hit/miss statistics together with Caffeine eviction count and estimated
   * entry count.
   *
   * @return snapshot of {@link ZetaCacheStats}
   */
  public ZetaCacheStats stats() {
    long hits = requestHits.sum();
    long misses = requestMisses.sum();
    CacheStats counters = caffeineCache.stats();
    return new ZetaCacheStats(hits, misses, counters.evictionCount(), caffeineCache.estimatedSize());
  }

  /**
   * Exposes the underlying raw L1 Caffeine table for internal infrastructure components.
   *
   * @return the raw L1 {@link Cache}
   */
  public Cache<String, CacheEntry> table() {
    return caffeineCache;
  }

  /**
   * Resolves a hard TTL override to the effective duration that a synced write will store.
   *
   * @param hardTtlMs the declared hard TTL override ({@code <= 0} inherits configured default)
   * @return the effective hard TTL in milliseconds, including {@link Long#MAX_VALUE} if permanent
   */
  public long resolveSyncableHardTtl(long hardTtlMs) {
    return ttlPolicy.resolveHardTtl(hardTtlMs);
  }

  /**
   * Records weighted accesses for a single key under an explicit {@link Zeta.TagMode} without
   * performing a cache lookup.
   *
   * @param cacheKey the validated physical key
   * @param count    the positive access count
   * @param mode     the detection/reporting pipeline selection
   */
  public void recordAccess(String cacheKey, long count, Zeta.TagMode mode) {
    accessCounter.record(cacheKey, count, mode);
  }

  /**
   * Records weighted accesses for a batch of keys under one shared {@link Zeta.TagMode} without
   * performing cache lookups.
   *
   * @param keyCounts validated map from physical key to positive access count
   * @param mode      the detection/reporting pipeline selection
   */
  public void recordAccess(Map<String, Long> keyCounts, Zeta.TagMode mode) {
    accessCounter.recordAll(keyCounts, mode);
  }

  /**
   * Looks up a cached value without invoking loaders, SingleFlight, request stats, or access
   * counting. Logically expired entries are evicted as a side effect.
   *
   * @param cacheKey the physical key to inspect
   * @param <T>      the expected value type
   * @return the resident value, or {@code null} on a miss or logical expiry
   */
  public <T> T peek(String cacheKey) {
    return unwrap(peekEntry(cacheKey));
  }

  /**
   * Retrieves a value using a plain {@link Supplier} on misses, admitting every non-null result.
   *
   * <p>TTL suppliers in {@code options} are evaluated lazily only on misses or soft refreshes,
   * never on plain hits. {@code null} results are never cached.
   *
   * @param cacheKey the physical key to retrieve
   * @param reader   the miss supplier, or {@code null} for a cache-only read
   * @param options  per-invocation read policy and lazy TTL suppliers
   * @param <T>      the expected value type
   * @return the cached or loaded value, or {@code null} on a miss
   */
  public <T> T get(String cacheKey, @Nullable Supplier<T> reader, GetPolicy options) {
    return readThrough(cacheKey, reader, null, options);
  }

  /**
   * Retrieves a value using a typed {@link CacheLoader}, honoring {@link CacheLoader#isCacheable}
   * on foreground misses and invoking {@link CacheLoader#reload} on background soft refreshes.
   *
   * @param cacheKey the physical key to retrieve
   * @param loader   the typed loader for misses and refreshes
   * @param options  per-invocation read policy and lazy TTL suppliers
   * @param <T>      the expected value type
   * @return the cached or loaded value, or {@code null} on a miss
   */
  public <T> T get(String cacheKey, CacheLoader<String, T> loader, GetPolicy options) {
    return readThrough(cacheKey, null, loader, options);
  }

  /**
   * Batch variant of {@link #get(String, Supplier, GetPolicy)}: serves hits without evaluating a
   * TTL supplier, and loads all distinct misses concurrently through SingleFlight.
   *
   * <p>One {@link ReadContext} is built on the first soft-expired hit or the first miss and shared
   * for the rest of the call, so a batch of pure hits evaluates no supplier and allocates no
   * context.
   *
   * @param cacheKeys the validated physical keys
   * @param reader    the per-key loader function, or {@code null} for a cache-only read
   * @param options   shared per-invocation options
   * @param <T>       the expected value type
   * @return map of found or loaded keys to values; missing keys are omitted
   */
  public <T> Map<String, T> get(
    List<String> cacheKeys,
    @Nullable Function<? super String, ? extends T> reader,
    GetPolicy options
  ) {
    if (cacheKeys.isEmpty()) {
      return Map.of();
    }
    List<String> misses = null;
    Map<String, T> results = new LinkedHashMap<>(CollectionSizing.capacity(cacheKeys.size()));
    Set<String> seen = new HashSet<>(CollectionSizing.capacity(cacheKeys.size()));
    long now = TimeSource.currentTimeMillis();
    StalePolicy stalePolicy = StalePolicy.orDefault(options.stalePolicy());
    ReadContext ctx = null;

    for (String key : cacheKeys) {
      if (!seen.add(key)) {
        continue;
      }

      CacheEntry entry = caffeineCache.getIfPresent(key);
      CacheEntry live = entry != null ? entryLifecycle.evictIfExpired(key, entry, now) : null;
      if (live != null) {
        boolean softExpired = ttlPolicy.isSoftExpired(live, now);
        if (!(softExpired && stalePolicy == StalePolicy.REVALIDATE)) {
          T value = unwrap(live);
          if (value != null) {
            requestHits.increment();
            accessCounter.record(key, options.report());
            if (softExpired && stalePolicy == StalePolicy.SOFT_REFRESH && reader != null) {
              if (ctx == null) {
                ctx = ReadContext.of(reader::apply, options);
              }
              refreshSoftExpire(key, live, ctx, now);
            }
            results.put(key, value);
            continue;
          }
        }
      }
      requestMisses.increment();
      if (misses == null) {
        misses = new ArrayList<>(cacheKeys.size());
      }
      misses.add(key);
    }

    if (misses == null) {
      return results;
    }
    if (reader != null && ctx == null) {
      ctx = ReadContext.of(reader::apply, options);
    }
    loadAndCache(misses, reader, ctx, options.report(), results);
    return results;
  }

  /**
   * Asynchronously warms a missing or expired entry through a policy loader without blocking the
   * calling worker and without recording synthetic read heat.
   *
   * @param key     the physical key to warm
   * @param loader  the background cache loader
   * @param hardTtl the namespace's resolved hard TTL in milliseconds
   * @param softTtl the namespace's resolved soft TTL in milliseconds
   * @return a future completing with {@code true} if a fresh non-expired value is now resident
   */
  public CompletableFuture<Boolean> warmAsync(
    String key,
    CacheLoader<String, Object> loader,
    long hardTtl,
    long softTtl
  ) {
    CacheEntry live = quietCache.getIfPresent(key);
    if (ttlPolicy.isLive(live)) {
      return CompletableFuture.completedFuture(false);
    }
    Supplier<VersionedValue> reader = VersionedValue.stampedReload(key, loader, stampClock);
    return singleFlight
      .loadAsync(key, reader, value -> loadCacheEntry(key, hardTtl, softTtl, value, null, WarmSignal.HEAT))
      .thenApply(outcome -> {
        if (outcome.status() != LoadStatus.LOADED || outcome.value() == null || outcome.value().value() == null) {
          return false;
        }
        CacheEntry resident = quietCache.getIfPresent(key);
        return ttlPolicy.isLive(resident);
      });
  }

  /**
   * Writes a value locally and broadcasts it to peers via {@link #broadcastBuffer} if the write
   * wins version admission.
   *
   * @param cacheKey  the physical key to write
   * @param value     the non-null value to store
   * @param hardTtlMs the hard TTL override ({@code <= 0} inherits configured default)
   * @param softTtlMs the soft TTL override ({@code 0} inherits default, {@code < 0} disables)
   */
  public void putThrough(String cacheKey, Object value, long hardTtlMs, long softTtlMs) {
    put(cacheKey, value, hardTtlMs, softTtlMs, true);
  }

  /**
   * Writes a value into the local L1 table without broadcasting to peers.
   *
   * @param cacheKey  the physical key to write
   * @param value     the non-null value to store
   * @param hardTtlMs the hard TTL override ({@code <= 0} inherits configured default)
   * @param softTtlMs the soft TTL override ({@code 0} inherits default, {@code < 0} disables)
   */
  public void putLocal(String cacheKey, Object value, long hardTtlMs, long softTtlMs) {
    put(cacheKey, value, hardTtlMs, softTtlMs, false);
  }

  /**
   * Writes a batch of key-value pairs under one transaction boundary using contiguous stamp blocks.
   *
   * @param entries   the validated key-to-value map
   * @param hardTtlMs the hard TTL override
   * @param softTtlMs the soft TTL override
   * @param sync      {@code true} to broadcast each applied entry to peers
   */
  public void putAll(Map<String, ?> entries, long hardTtlMs, long softTtlMs, boolean sync) {
    if (entries.isEmpty()) {
      return;
    }
    Map<String, ?> snapshot = TransactionSupport.isDeferred(false) ? Map.copyOf(entries) : entries;
    TransactionSupport.run(false, () -> executePutAll(snapshot, hardTtlMs, softTtlMs, sync));
  }

  /**
   * Refreshes an entry's value from a caller {@link Supplier} while preserving the resident's
   * declared hard and soft TTLs.
   *
   * @param cacheKey the physical key to refresh
   * @param reader   the value supplier
   * @param sync     {@code true} to broadcast the refreshed value to peers
   * @param <V>      the value type
   */
  public <V> void refresh(String cacheKey, Supplier<V> reader, boolean sync) {
    TransactionSupport.run(false, () -> executeRefreshNow(cacheKey, reader, null, sync));
  }

  /**
   * Refreshes an entry's value via {@link CacheLoader#reload} while preserving the resident's
   * declared hard and soft TTLs and honoring {@link CacheLoader#isCacheable}.
   *
   * @param cacheKey the physical key to refresh
   * @param loader   the typed loader supplying the refreshed value
   * @param sync     {@code true} to broadcast the refreshed value to peers
   * @param <V>      the value type
   */
  public <V> void refresh(String cacheKey, CacheLoader<String, V> loader, boolean sync) {
    TransactionSupport.run(false, () -> executeRefreshNow(cacheKey, null, loader, sync));
  }

  /**
   * Applies an {@link InvalidatePolicy} to a single key: {@code REMOTE} broadcasts only, whereas
   * {@code LOCAL} and {@code BOTH} record a tombstone, remove the L1 entry, drop in-flight dedup,
   * cancel scheduled warming, and optionally broadcast to peers.
   *
   * @param cacheKey the physical key to invalidate
   * @param policy   invalidation scope, transaction timing, and peer-sync policy
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
   * Batch variant of {@link #invalidate(String, InvalidatePolicy)} allocating contiguous stamp
   * blocks per wire chunk under a single transaction boundary.
   *
   * @param cacheKeys the validated physical keys to invalidate
   * @param policy    invalidation scope, transaction timing, and peer-sync policy
   */
  public void invalidate(List<String> cacheKeys, InvalidatePolicy policy) {
    if (cacheKeys.isEmpty()) {
      return;
    }
    List<String> keysSnapshot = TransactionSupport.isDeferred(policy.immediate()) ? List.copyOf(cacheKeys) : cacheKeys;
    TransactionSupport.run(policy.immediate(), () -> executeBatchInvalidate(keysSnapshot, policy));
  }

  /**
   * Clears the entire table according to {@code policy}, recording a table-wide clear watermark
   * and sweeping entries at or below the allocated stamp.
   *
   * @param policy invalidation scope, transaction timing, and peer-sync policy
   * @return the allocated clear stamp, or {@code -1} if remote-only or deferred
   */
  public long clear(InvalidatePolicy policy) {
    return doClear(null, policy);
  }

  /**
   * Clears all keys sharing one physical namespace prefix according to {@code policy} without
   * affecting unrelated namespaces.
   *
   * @param prefix the physical namespace prefix
   * @param policy invalidation scope, transaction timing, and peer-sync policy
   * @return the allocated clear stamp, or {@code -1} if remote-only or deferred
   */
  public long clearPrefix(String prefix, InvalidatePolicy policy) {
    return doClear(prefix, policy);
  }

  /**
   * Invalidates SingleFlight deduplication entries for keys mutated directly through
   * {@link #table()} without modifying L1 or broadcasting.
   *
   * @param cacheKeys the physical keys whose in-flight loads must be marked superseded
   */
  public void forgetDedup(String... cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    for (String key : cacheKeys) {
      singleFlight.invalidate(key);
    }
  }

  /**
   * Extracts the user-facing value from a {@link CacheEntry}.
   *
   * @param raw the cache entry, or {@code null}
   * @param <T> the expected value type
   * @return the stored value, or {@code null} when {@code raw} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public static <T> T unwrap(@Nullable CacheEntry raw) {
    return raw != null ? (T) raw.getValue() : null;
  }

  /**
   * Probes L1 for {@code key} and evicts it if logically expired, without touching request stats
   * or access counting.
   *
   * @param key the physical key to probe
   * @return the live {@link CacheEntry}, or {@code null}
   */
  private @Nullable CacheEntry peekEntry(String key) {
    CacheEntry entry = caffeineCache.getIfPresent(key);
    if (entry == null) {
      return null;
    }
    return entryLifecycle.evictIfExpired(key, entry, TimeSource.currentTimeMillis());
  }

  /**
   * Executes the single-key read-through flow: probes L1, enforces hard expiry, handles soft
   * expiry per {@link StalePolicy}, records stats/accesses, or falls through to SingleFlight.
   *
   * @param key     the physical key
   * @param reader  optional plain supplier
   * @param loader  optional typed loader
   * @param options per-call read options
   * @param <T>     the expected value type
   * @return the served or loaded value, or {@code null}
   */
  private <T> T readThrough(
    String key,
    @Nullable Supplier<T> reader,
    @Nullable CacheLoader<String, T> loader,
    GetPolicy options
  ) {
    CacheEntry entry = caffeineCache.getIfPresent(key);
    long now = entry != null ? TimeSource.currentTimeMillis() : 0L;
    CacheEntry live = entry != null ? entryLifecycle.evictIfExpired(key, entry, now) : null;
    StalePolicy policy = StalePolicy.orDefault(options.stalePolicy());
    boolean softExpired = live != null && ttlPolicy.isSoftExpired(live, now);
    T value = (live != null && !(softExpired && policy == StalePolicy.REVALIDATE)) ? unwrap(live) : null;

    if (value == null) {
      requestMisses.increment();
      return loadAndCache(key, readContext(reader, loader, options));
    }
    requestHits.increment();
    accessCounter.record(key, options.report());
    if (softExpired && policy == StalePolicy.SOFT_REFRESH && (reader != null || loader != null)) {
      refreshSoftExpire(key, live, readContext(reader, loader, options), now);
    }
    return value;
  }

  /**
   * Constructs a {@link ReadContext} lazily only when a miss or soft refresh requires one.
   *
   * @param reader  optional plain supplier
   * @param loader  optional typed loader
   * @param options per-call read options
   * @param <T>     the value type
   * @return the constructed {@link ReadContext}
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
      source = new SupplierLoader(reader);
    }
    return ReadContext.of(source, options);
  }

  /**
   * Re-verifies entry residency and soft expiry before submitting a background soft refresh to
   * {@link #backgroundRefresher}.
   *
   * @param key   the physical key
   * @param entry the stale entry observed on the read path
   * @param ctx   the per-call read context
   * @param now   the per-call clock sample
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
    backgroundRefresher.triggerBackgroundRefresh(key, VersionedValue.stampedReload(key, source, stampClock), softTtl);
  }

  /**
   * Loads a single cache miss through {@link #singleFlight} (or {@link SingleFlight#loadIsolated}
   * when running caller-affine inside a read-write transaction) and commits the loaded value under
   * the caller's resolved TTLs.
   *
   * @param key the physical key to load
   * @param ctx the per-call read context carrying source, admission rule, and memoized TTLs
   * @param <T> the expected value type
   * @return the loaded or degraded value, or {@code null}
   */
  private <T> T loadAndCache(String key, ReadContext ctx) {
    if (!ctx.hasSource()) {
      return resolveNullAnswer(key, ctx.report());
    }

    long hardTtl = ttlPolicy.resolveHardTtl(ctx.hardTtlMs());
    long softTtl = ttlPolicy.resolveSoftTtl(ctx.softTtlMs());
    Supplier<VersionedValue> stampedReader = () -> {
      long stamp = nextStamp();
      Object loaded = ctx.loadValue(key);
      return loaded == null ? null : new VersionedValue(loaded, stamp);
    };

    boolean uncommittedVisible =
      ctx.callerRunsLoader() &&
      TransactionSupport.isDeferred(false) &&
      !TransactionSupport.isCurrentTransactionReadOnly();
    // An isolated load observes the transaction's own uncommitted writes, so it must not store
    // anything: not inline, and not through an afterCommit hook either (the pre-read stamp could
    // outrank a concurrent invalidation registered later in the same transaction).
    Consumer<VersionedValue> leaderCommit = uncommittedVisible
      ? value -> {}
      : value -> loadCacheEntry(key, hardTtl, softTtl, value, ctx, WarmSignal.READ);

    LoaderOutcome<VersionedValue> outcome = uncommittedVisible
      ? singleFlight.loadIsolated(key, stampedReader, leaderCommit)
      : singleFlight.load(key, stampedReader, ctx.callerRunsLoader(), leaderCommit);
    return resolveLoaded(key, outcome, ctx.report());
  }

  /**
   * Loads a batch of cache misses concurrently through {@link #singleFlight}, stores loaded values
   * into L1, populates {@code into}, and rethrows the first failure with secondary failures
   * attached as suppressed.
   *
   * @param cacheKeys the missed physical keys
   * @param reader    the per-key loader function, or {@code null} for cache-only resolution
   * @param ctx       the call's read context carrying the lazy TTL supplies, or {@code null} when
   *                  there is no reader
   * @param report    whether reads feed worker reporting
   * @param into      destination map receiving resolved non-null values
   * @param <T>       the value type
   */
  private <T> void loadAndCache(
    List<String> cacheKeys,
    @Nullable Function<? super String, ? extends T> reader,
    @Nullable ReadContext ctx,
    boolean report,
    Map<String, T> into
  ) {
    if (reader == null || ctx == null) {
      for (String key : cacheKeys) {
        T value = resolveNullAnswer(key, report);
        if (value != null) {
          into.put(key, value);
        }
      }
      return;
    }

    long effectiveHardTtlMs = ttlPolicy.resolveHardTtl(ctx.hardTtlMs());
    long effectiveSoftTtlMs = ttlPolicy.resolveSoftTtl(ctx.softTtlMs());
    Map<String, Long> stamps = new HashMap<>(CollectionSizing.capacity(cacheKeys.size()));
    allocateStampBlocks(cacheKeys, stamps);

    Map<String, LoaderOutcome<VersionedValue>> loaded = singleFlight.load(
      cacheKeys,
      k -> {
        T value = reader.apply(k);
        if (value == null) {
          return null;
        }
        Long allocated = stamps.get(k);
        return new VersionedValue(value, allocated != null ? allocated : nextStamp());
      },
      (k, value) -> loadCacheEntry(k, effectiveHardTtlMs, effectiveSoftTtlMs, value, null, WarmSignal.READ)
    );

    Throwable failure = null;
    Set<Throwable> attached = null;
    for (String key : cacheKeys) {
      try {
        T value = resolveLoaded(key, loaded.get(key), report);
        if (value != null) {
          into.put(key, value);
        }
      } catch (RuntimeException | Error e) {
        if (failure == null) {
          failure = e;
        } else if (failure != e) {
          if (attached == null) {
            attached = Collections.newSetFromMap(new IdentityHashMap<>());
            attached.add(failure);
          }
          if (attached.add(e)) {
            failure.addSuppressed(e);
          }
        }
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
   * Resolves a SingleFlight {@link LoaderOutcome} into a return value, degraded stale fallback, or
   * propagated exception.
   *
   * @param key     the physical key whose load completed
   * @param outcome the load outcome, or {@code null}
   * @param report  whether reads feed worker reporting
   * @param <T>     the expected value type
   * @return the servable value, or {@code null}
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
   * Handles a suppressed load by serving a hard-valid stale entry if present, or throwing
   * {@link ZetaSuppressedException} so callers can distinguish suppression from empty data.
   *
   * @param key    the physical key
   * @param status the suppression reason
   * @param report whether reads feed worker reporting
   * @param <T>    the expected value type
   * @return the hard-valid stale value
   * @throws ZetaSuppressedException if no hard-valid stale entry exists
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
   * Handles a reader failure: rethrows {@link Error}, degrades {@link InterruptedException} to a
   * null answer, serves a hard-valid stale entry if the shard breaker is open, and otherwise
   * rethrows unchecked causes or wraps checked causes in {@link CompletionException}.
   *
   * @param key    the physical key
   * @param cause  the failure cause, or {@code null}
   * @param report whether reads feed worker reporting
   * @param <T>    the expected value type
   * @return the degraded answer when fallback applies
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
    }
    if (cause instanceof RuntimeException re) {
      throw re;
    }
    throw new CompletionException(cause);
  }

  /**
   * Resolves a load that yielded no value: returns a hard-valid stale entry when the key's circuit
   * breaker is open, or {@code null} otherwise (nulls are never cached).
   *
   * @param key    the physical key
   * @param report whether reads feed worker reporting
   * @param <T>    the expected value type
   * @return a hard-valid stale value when degraded, otherwise {@code null}
   */
  private <T> T resolveNullAnswer(String key, boolean report) {
    return singleFlight.isBreakerOpen(key) ? serveStaleEntry(key, report) : null;
  }

  /**
   * Returns a resident entry's value for degraded fallback only if it has not passed hard expiry,
   * evicting logically expired residents and recording a read only when a value is served.
   *
   * @param key    the physical key
   * @param report whether reads feed worker reporting
   * @param <T>    the expected value type
   * @return the stale value, or {@code null} if absent or hard-expired
   */
  private <T> T serveStaleEntry(String key, boolean report) {
    CacheEntry stale = quietCache.getIfPresent(key);
    if (stale == null) {
      return null;
    }
    CacheEntry resident = entryLifecycle.evictIfExpired(key, stale, TimeSource.currentTimeMillis());
    if (resident == null) {
      return null;
    }
    T value = unwrap(resident);
    if (value == null) {
      return null;
    }
    accessCounter.record(key, report);
    return value;
  }

  /**
   * Emits a debug log line when a suppressed load degrades cleanly to a stale entry.
   *
   * @param key    the physical key
   * @param status the suppression reason
   */
  private void logSuppressedLoad(String key, LoadStatus status) {
    log.debug("HotKeyCache load suppressed for key={} (status={}; returning the stale entry)", key, status);
  }

  /**
   * Executes a single local or synced write inside {@link TransactionSupport}.
   *
   * @param key       the physical key
   * @param value     the value to store
   * @param hardTtlMs hard TTL override
   * @param softTtlMs soft TTL override
   * @param sync      {@code true} to broadcast after applying locally
   */
  private void put(String key, Object value, long hardTtlMs, long softTtlMs, boolean sync) {
    TransactionSupport.run(false, () -> {
      long effectiveHardTtl = ttlPolicy.resolveHardTtl(hardTtlMs);
      long effectiveSoftTtl = ttlPolicy.resolveSoftTtl(softTtlMs);
      writeThroughApply(key, value, nextStamp(), effectiveHardTtl, effectiveSoftTtl, sync);
    });
  }

  /**
   * Applies a batch write using contiguous stamp blocks per {@link StampClock#MAX_ATOMIC_RANGE}.
   *
   * @param entries   the key-to-value map
   * @param hardTtlMs hard TTL override
   * @param softTtlMs soft TTL override
   * @param sync      {@code true} to broadcast each applied entry
   */
  private void executePutAll(Map<String, ?> entries, long hardTtlMs, long softTtlMs, boolean sync) {
    long effectiveHard = ttlPolicy.resolveHardTtl(hardTtlMs);
    long effectiveSoft = ttlPolicy.resolveSoftTtl(softTtlMs);
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
   * Executes an immediate refresh: allocates a pre-load stamp, probes existing TTL declarations,
   * reloads the value, checks cacheability, and applies through {@link #writeThroughApply}.
   *
   * @param key    the physical key to refresh
   * @param reader optional plain supplier
   * @param loader optional typed loader
   * @param sync   {@code true} to broadcast the refreshed value
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
    CacheEntry live = ttlPolicy.isLive(resident) ? resident : null;
    long hardTtlMs = live != null ? live.getHardTtlMs() : ttlPolicy.effectiveHardTtlMs();
    long softTtlMs = live != null ? live.getSoftTtlMs() : ttlPolicy.effectiveSoftTtlMs();
    PutPolicy.requireSyncable(hardTtlMs, sync);

    V value;
    if (loader != null) {
      value = loader.reload(key);
    } else if (reader != null) {
      value = reader.get();
    } else {
      return;
    }

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
   * Applies one version-guarded write to L1, retires in-flight SingleFlight state, offers the new
   * entry to {@link #warmScheduler}, and optionally broadcasts via {@link #broadcastBuffer}.
   *
   * @param key       the physical key
   * @param value     the value to store
   * @param stamp     the pre-allocated data version stamp
   * @param hardTtl   resolved hard TTL in milliseconds
   * @param softTtl   resolved soft TTL in milliseconds
   * @param broadcast {@code true} to publish a value-carrying PUT broadcast
   */
  private void writeThroughApply(String key, Object value, long stamp, long hardTtl, long softTtl, boolean broadcast) {
    CacheEntry applied = EntryMutations.mutate(caffeineCache, key, entry ->
      entryForWrite(key, entry, value, stamp, hardTtl, softTtl, true)
    );

    if (applied != null && applied.getDataVersion() == stamp) {
      singleFlight.invalidate(key);
      offerWarm(key, applied, WarmSignal.WRITE);
      if (broadcast) {
        broadcastBuffer.put(key, value, stamp, version -> !entryLifecycle.staleAgainstInvalidation(key, version));
      }
    }
  }

  /**
   * Commits a freshly loaded {@link VersionedValue} to L1 after checking loader admission, then
   * offers the applied entry to {@link #warmScheduler}.
   *
   * <p>The write happens on the thread that produced the load. A load whose value may include the
   * caller's own uncommitted writes never reaches this method with a value to store, so the stamp
   * carried here is always a committed-data stamp and must not wait for an {@code afterCommit} hook:
   * the leader may run on the dedup pool, where no transaction context exists, and a deferred
   * pre-read stamp could outrank an invalidation registered later in the same transaction.
   *
   * @param key     the physical key
   * @param hardTtl resolved hard TTL in milliseconds
   * @param softTtl resolved soft TTL in milliseconds
   * @param loaded  the loaded value paired with its pre-read stamp
   * @param ctx     optional read context enforcing loader admission
   * @param signal  reason for the store forwarded to {@link #warmScheduler}
   */
  private void loadCacheEntry(
    String key,
    long hardTtl,
    long softTtl,
    VersionedValue loaded,
    @Nullable ReadContext ctx,
    WarmSignal signal
  ) {
    if (ctx != null && !ctx.admits(loaded.value())) {
      return;
    }
    CacheEntry applied = EntryMutations.mutate(caffeineCache, key, entry ->
      entryForWrite(key, entry, loaded.value(), loaded.dataVersion(), hardTtl, softTtl, false)
    );
    if (applied != null && applied.getDataVersion() == loaded.dataVersion()) {
      offerWarm(key, applied, signal);
    }
  }

  /**
   * Applies the shared L1 write-admission guard and builds a new {@link CacheEntry} if admitted,
   * otherwise returns {@code entry} unchanged.
   *
   * @param key          the physical key
   * @param entry        the current resident entry, or {@code null}
   * @param value        the value to store
   * @param stamp        the incoming data version stamp
   * @param hardTtl      resolved hard TTL in milliseconds
   * @param softTtl      resolved soft TTL in milliseconds
   * @param equalApplies {@code true} when an equal stamp is allowed to apply (write/refresh path)
   * @return the newly built entry, or {@code entry} unchanged when refused
   */
  private @Nullable CacheEntry entryForWrite(
    String key,
    @Nullable CacheEntry entry,
    Object value,
    long stamp,
    long hardTtl,
    long softTtl,
    boolean equalApplies
  ) {
    long now = TimeSource.currentTimeMillis();
    if (writeRefused(key, entry, stamp, equalApplies, now)) {
      return entry;
    }
    return entryLifecycle.writeValue(value, stamp, hardTtl, softTtl);
  }

  /**
   * Determines whether an L1 write at {@code stamp} is superseded by an applied invalidation/clear
   * watermark or by a non-expired resident entry with a higher (or equal, depending on mode) stamp.
   *
   * @param key          the physical key
   * @param entry        the resident entry, or {@code null}
   * @param stamp        the incoming write stamp
   * @param equalApplies {@code true} when an equal stamp is permitted to overwrite
   * @param now          the current epoch-millisecond timestamp
   * @return {@code true} if the write must be skipped
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
   * Drops a key locally and optionally broadcasts a versioned INVALIDATE message to peers.
   *
   * @param key       the physical key
   * @param stamp     the pre-allocated invalidation stamp
   * @param broadcast {@code true} to broadcast to peers
   */
  private void dropAndBroadcastInvalidate(String key, long stamp, boolean broadcast) {
    dropLocal(key, stamp);
    if (broadcast) {
      broadcastBuffer.invalidate(key, stamp);
    }
  }

  /**
   * Performs the local invalidation sequence: records the tombstone watermark, invalidates
   * SingleFlight, then removes the entry at or below {@code stamp} from L1 and cancels any
   * scheduled renewal.
   *
   * <p>A resident entry with a newer data version survived a concurrent write and is kept: the
   * tombstone makes that write lose the next version comparison, not this one.
   *
   * @param key   the physical key
   * @param stamp the invalidation stamp
   */
  private void dropLocal(String key, long stamp) {
    entryLifecycle.recordInvalidation(key, stamp);
    singleFlight.invalidate(key);
    CacheEntry remaining = caffeineCache
      .asMap()
      .computeIfPresent(key, (k, entry) -> entry.getDataVersion() > stamp ? entry : null);
    if (remaining == null) {
      cancelWarm(key);
    }
  }

  /**
   * Executes batch invalidation in wire-sized contiguous stamp chunks.
   *
   * @param keys   the validated physical keys
   * @param policy the invalidation policy
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
        if (entries != null) {
          entries.add(new SyncMessage.Entry(key, stamp));
        }
      }
      if (entries != null) {
        broadcastBuffer.invalidateBatch(entries);
      }
    });
  }

  /**
   * Dispatches a table-wide or prefix-scoped clear either immediately or after transaction commit.
   *
   * @param prefix {@code null} for a table-wide clear, or the physical namespace prefix
   * @param policy the invalidation policy
   * @return the allocated clear stamp, or {@code -1} when deferred or remote-only
   */
  private long doClear(@Nullable String prefix, InvalidatePolicy policy) {
    if (TransactionSupport.isDeferred(policy.immediate())) {
      TransactionSupport.run(false, () -> clearNow(prefix, policy));
      return -1L;
    }
    return clearNow(prefix, policy);
  }

  /**
   * Executes an immediate table-wide or prefix-scoped clear: records the watermark, invalidates
   * matching in-flight loads, sweeps matching L1 entries at or below the stamp, and broadcasts.
   *
   * @param prefix {@code null} for a table-wide clear, or the physical namespace prefix
   * @param policy the invalidation policy
   * @return the allocated clear stamp, or {@code -1} for a remote-only clear
   */
  private long clearNow(@Nullable String prefix, InvalidatePolicy policy) {
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
    return clearStamp;
  }

  /**
   * Broadcasts either a table-wide or prefix-scoped clear message via {@link #broadcastBuffer}.
   *
   * @param prefix     {@code null} for a table-wide clear, or the physical namespace prefix
   * @param clearStamp the allocated clear stamp
   */
  private void broadcastClear(@Nullable String prefix, long clearStamp) {
    if (prefix == null) {
      broadcastBuffer.invalidateAll(clearStamp);
    } else {
      broadcastBuffer.invalidatePrefix(prefix, clearStamp);
    }
  }

  /**
   * Performs a best-effort, version-guarded sweep of L1 entries matching {@code prefix} (or all
   * entries when {@code prefix} is {@code null}) at or below {@code versionCeiling}, cancelling
   * scheduled warm tasks for removed keys.
   *
   * @param prefix         {@code null} for all keys, or the physical prefix to match
   * @param versionCeiling entries with {@code dataVersion <= versionCeiling} are removed
   */
  private void sweep(@Nullable String prefix, long versionCeiling) {
    for (Map.Entry<String, CacheEntry> candidate : caffeineCache.asMap().entrySet()) {
      if (candidate.getValue().getDataVersion() > versionCeiling) {
        continue;
      }
      if (prefix != null && !candidate.getKey().startsWith(prefix)) {
        continue;
      }

      CacheEntry remaining = caffeineCache
        .asMap()
        .computeIfPresent(candidate.getKey(), (key, entry) -> entry.getDataVersion() > versionCeiling ? entry : null);
      if (remaining == null) {
        cancelWarm(candidate.getKey());
      }
    }
  }

  /**
   * Allocates the next single node-local data version stamp.
   *
   * @return a strictly positive monotonic stamp
   */
  private long nextStamp() {
    return stampClock.nextStamp();
  }

  /**
   * Allocates contiguous stamp blocks for {@code keys} and stores each key's stamp into {@code into}.
   *
   * @param keys the batch keys to stamp
   * @param into map populated with key-to-stamp assignments
   */
  private void allocateStampBlocks(List<String> keys, Map<String, Long> into) {
    forEachStampBlock(keys, StampClock.MAX_ATOMIC_RANGE, (blockKeys, base) -> {
      for (int i = 0; i < blockKeys.size(); i++) {
        into.put(blockKeys.get(i), base + i);
      }
    });
  }

  /**
   * Splits {@code keys} into chunks bounded by both {@code chunkSize} and
   * {@link StampClock#MAX_ATOMIC_RANGE}, reserving one contiguous stamp range per chunk.
   *
   * @param keys      the keys to process in order
   * @param chunkSize the caller's maximum chunk size
   * @param sink      consumer receiving each chunk's keys and base stamp
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
   * Offers a newly stored entry to {@link #warmScheduler} when warming is enabled.
   *
   * @param key    the stored physical key
   * @param entry  the applied cache entry
   * @param signal the store origin signal
   */
  private void offerWarm(String key, CacheEntry entry, WarmSignal signal) {
    WarmScheduler scheduler = warmScheduler;
    if (scheduler != null) {
      scheduler.scheduleIfEligible(key, entry, signal);
    }
  }

  /**
   * Cancels any scheduled renewal for {@code key} when {@link #warmScheduler} is installed.
   *
   * @param key the removed physical key
   */
  private void cancelWarm(String key) {
    WarmScheduler scheduler = warmScheduler;
    if (scheduler != null) {
      scheduler.cancel(key);
    }
  }

  /**
   * Foreground-only {@link CacheLoader} adapter wrapping a caller's one-shot {@link Supplier}.
   */
  private record SupplierLoader(Supplier<?> reader) implements CacheLoader<String, Object> {
    /**
     * Invokes the wrapped supplier to produce a value for the foreground miss.
     *
     * @param key ignored because the supplier is already bound to its key
     * @return the supplied value, or {@code null}
     */
    @Override
    public @Nullable Object load(String key) {
      return reader.get();
    }
  }

  /**
   * Consumer receiving one contiguous chunk of keys paired with the chunk's base version stamp
   * (where index {@code i} owns stamp {@code base + i}).
   */
  @FunctionalInterface
  private interface BlockSink {
    /**
     * Processes one stamped chunk of keys.
     *
     * @param blockKeys the keys in this chunk
     * @param base      the base stamp for index {@code 0}
     */
    void accept(List<String> blockKeys, long base);
  }
}
