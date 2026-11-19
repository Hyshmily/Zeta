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
package io.github.hyshmily.zeta;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.util.Internal;
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.hyshmily.zeta.cache.CacheKeysPolicy.validKeys;

/**
 * Default implementation of the {@link Zeta} facade — the sole public API
 * entry point. Reads/writes/invalidations and value-only refreshes go to
 * {@link HotKeyCache}, and the administrative views
 * ({@link Zeta.DetectorAdmin detector()}, {@link Zeta.UnsafeAdmin unsafe()})
 * are projected from the same components. The facade adds only orchestration
 * that has no better home: argument validation, option normalization,
 * deployment-mode gating, the bulk-refresh fan-out, and the projections
 * themselves.
 *
 * <p>
 * A {@code null} options argument selects the same policy as the verb's
 * no-options overload; a loader's {@code isCacheable} decision travels as an
 * explicit admission predicate, so a caller can never override it per call.
 *
 * <p>
 * <b>Thread safety:</b> All public methods are thread-safe. The underlying
 * collaborators are thread-safe by contract; the facade keeps no per-call
 * mutable state, only the shared stateless views and the injected bulk-refresh
 * executor.
 */
public class DefaultZeta implements Zeta {

  /** Label used when the facade runs without an app-side cache. */
  private static final String WORKER_ONLY_MODE = "Worker-only mode (no app-side cache)";

  /** Core cache engine. {@code null} in Worker-only deployments. */
  private final @Nullable HotKeyCache hotKeyCache;
  /** App-side TopK sketch (read side). {@code null} when the detector is not wired. */
  private final @Nullable TopK appTopK;

  /** Shared administrative views; all are stateless projections. */
  private final Zeta.DetectorAdmin detectorAdmin = new DetectorView();
  private final Zeta.UnsafeAdmin unsafeAdmin = new UnsafeView();

  /**
   * Executor for fan-out bulk operations ({@link #refreshAll}). Wired from the
   * dedicated refresh pool ({@code hotKeyExecutor}) by the auto-configuration;
   * when absent (direct construction, mostly tests) the bulk path falls back to
   * the common pool. The pool is shared and never shut down here.
   */
  private final @Nullable Executor refreshExecutor;

  /**
   * Creates the facade without a bulk-refresh executor; a multi-key
   * {@link #refreshAll} falls back to the common pool.
   *
   * @param hotKeyCache the cache engine, or {@code null} in Worker-only mode
   * @param appTopK     the app-side detector, or {@code null} when not wired
   */
  @Internal
  public DefaultZeta(@Nullable HotKeyCache hotKeyCache, @Nullable TopK appTopK) {
    this(hotKeyCache, appTopK, null);
  }

  /**
   * Creates the facade. Container wiring, not an extension point: the
   * auto-configuration wires it ({@code ZetaFacadeAutoConfiguration});
   * subclassing is not supported.
   *
   * @param hotKeyCache     the cache engine, or {@code null} in Worker-only mode
   * @param appTopK         the app-side detector, or {@code null} when not wired
   * @param refreshExecutor the dedicated bulk-refresh pool, or {@code null} to
   *                        fall back to the common pool
   */
  @Internal
  public DefaultZeta(@Nullable HotKeyCache hotKeyCache, @Nullable TopK appTopK, @Nullable Executor refreshExecutor) {
    this.appTopK = appTopK;
    this.hotKeyCache = hotKeyCache;
    this.refreshExecutor = refreshExecutor;
  }

  @Override
  public void tag(String cacheKey, long count, TagMode mode) {
    requireKey(cacheKey);
    Assert.isTrue(count > 0, () -> "count must be positive, but got: " + count);
    Objects.requireNonNull(mode, "mode must not be null");
    HotKeyCache cache = requireAppCache("tag");
    if (mode == TagMode.SILENT) {
      return;
    }
    cache.recordAccess(cacheKey, count, mode);
  }

  @Override
  public void tagAll(Map<String, Long> keyCounts, TagMode mode) {
    Objects.requireNonNull(keyCounts, "keyCounts must not be null");
    Objects.requireNonNull(mode, "mode must not be null");
    HotKeyCache cache = requireAppCache("tagAll");
    if (keyCounts.isEmpty()) {
      return;
    }
    // Snapshot while validating: prevents concurrent caller mutation from
    // injecting unchecked keys or non-positive counts into the engine.
    Map<String, Long> snapshot = new LinkedHashMap<>((int) (keyCounts.size() / 0.75f) + 1);
    for (Map.Entry<String, Long> entry : keyCounts.entrySet()) {
      String key = entry.getKey();
      Long count = entry.getValue();
      requireKey(key);
      Objects.requireNonNull(count, "count must not be null");
      Assert.isTrue(count > 0, () -> "count must be positive for key '" + key + "', but got: " + count);
      snapshot.put(key, count);
    }
    if (mode == TagMode.SILENT) {
      return;
    }
    cache.recordAccess(snapshot, mode);
  }

  @Override
  public <V> @Nullable V get(String cacheKey, CacheLoader<String, V> loader, @Nullable GetPolicy options) {
    requireKey(cacheKey);
    Objects.requireNonNull(loader, "loader must not be null");
    GetPolicy opts = orDefault(options, GetPolicy.defaults());
    return requireAppCache("get").get(cacheKey, loader, opts);
  }

  @Override
  public <V> @Nullable V get(String cacheKey, Supplier<V> reader, @Nullable GetPolicy options) {
    requireKey(cacheKey);
    Objects.requireNonNull(reader, "reader must not be null");
    GetPolicy opts = orDefault(options, GetPolicy.defaults());
    return requireAppCache("get").get(cacheKey, reader, opts);
  }

  @Override
  public <T> @Nullable T peek(String cacheKey) {
    requireKey(cacheKey);
    return requireAppCache("peek").peek(cacheKey);
  }

  @Override
  public void put(String cacheKey, Object value, @Nullable PutPolicy options) {
    requireKey(cacheKey);
    // Null is never cached: a null put is a caller error.
    Objects.requireNonNull(value, "value must not be null: put(key, null) is rejected (null is never cached)");
    PutPolicy opts = orDefault(options, PutPolicy.defaults());
    ResolvedTtls ttls = ResolvedTtls.of(opts);
    HotKeyCache cache = requireWritableCache("put", opts.sync(), ttls.hardMs());
    if (opts.sync()) {
      cache.putThrough(cacheKey, value, ttls.hardMs(), ttls.softMs());
    } else {
      cache.putLocal(cacheKey, value, ttls.hardMs(), ttls.softMs());
    }
  }

  @Override
  public void putAll(Map<String, ?> entries, @Nullable PutPolicy options) {
    Objects.requireNonNull(entries, "entries must not be null");
    PutPolicy opts = orDefault(options, PutPolicy.defaults());
    ResolvedTtls ttls = ResolvedTtls.of(opts);
    // Mode, TTL and syncability validation precedes the empty short-circuit: invalid options are
    // caller errors even when the batch carries no entries, and Worker-only deployments throw for
    // every data-plane call.
    HotKeyCache cache = requireWritableCache("putAll", opts.sync(), ttls.hardMs());
    if (entries.isEmpty()) {
      return;
    }
    // Writes are caller intent: reject the whole batch before any key lands.
    // Unlike the read/invalidate collection methods, dropping an invalid element
    // would silently leave a batch half-applied. Copy while validating: the
    // engine must receive exactly the checked rows, even if the caller mutates
    // a concurrent map after this call starts.
    // Capacity for size entries at the default 0.75 load factor: `size` alone
    // would resize mid-put.
    Map<String, Object> batch = new LinkedHashMap<>((int) (entries.size() / 0.75f) + 1);
    for (Map.Entry<String, ?> entry : entries.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      requireKey(key);
      Objects.requireNonNull(value, "value must not be null: putAll rejects a null value (null is never cached)");
      batch.put(key, value);
    }
    cache.putAll(batch, ttls.hardMs(), ttls.softMs(), opts.sync());
  }

  @Override
  public void invalidate(String cacheKey, @Nullable InvalidatePolicy options) {
    requireKey(cacheKey);
    InvalidatePolicy opts = orDefault(options, InvalidatePolicy.defaults());
    requireAppCache("invalidate").invalidate(cacheKey, opts);
  }

  @Override
  public void invalidate(Collection<String> cacheKeys, @Nullable InvalidatePolicy options) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    InvalidatePolicy opts = orDefault(options, InvalidatePolicy.defaults());
    HotKeyCache cache = requireAppCache("invalidate");
    // An invalid key has no local or peer copy to invalidate. Dropped here —
    // the only gate — so the engine's batch paths assume valid keys.
    List<String> keys = validKeys(cacheKeys);
    if (!keys.isEmpty()) {
      cache.invalidate(keys, opts);
    }
  }

  @Override
  public void clearAll(@Nullable InvalidatePolicy options) {
    InvalidatePolicy opts = orDefault(options, InvalidatePolicy.defaults());
    requireAppCache("clearAll").clear(opts);
  }

  @Override
  public void clearPrefix(String prefix, @Nullable InvalidatePolicy options) {
    Assert.hasText(prefix, "prefix must not be blank");
    InvalidatePolicy opts = orDefault(options, InvalidatePolicy.defaults());
    requireAppCache("clearPrefix").clearPrefix(prefix, opts);
  }

  @Override
  public <V> Map<String, V> getAll(
    Collection<String> cacheKeys,
    Function<? super String, ? extends V> reader,
    @Nullable GetPolicy options
  ) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    GetPolicy opts = orDefault(options, GetPolicy.defaults());
    // The batch contract resolves the TTL overrides once for the whole batch;
    // doing it here validates them even for an empty collection and keeps the
    // engine from evaluating the caller's suppliers a second time.
    ResolvedTtls ttls = ResolvedTtls.of(opts);
    GetPolicy batch =
      opts == GetPolicy.defaults()
        ? opts
        : GetPolicy.of(ttls.hardMs(), ttls.softMs(), opts.report(), opts.stalePolicy());
    HotKeyCache cache = requireAppCache("getAll");
    // An invalid key cannot be a map key and is a miss by definition: dropped
    // here, so the engine's batch read assumes valid keys.
    return cache.get(validKeys(cacheKeys), reader, batch);
  }

  @Override
  public <V> void refresh(String cacheKey, Supplier<V> reader, boolean sync) {
    requireKey(cacheKey);
    Objects.requireNonNull(reader, "reader must not be null");
    HotKeyCache cache = requireWritableCache("refresh", sync, 0L);
    // Bump-before-load ordering lives in HotKeyCache#refresh: a concurrent
    // synced put during the load holds the newer version and the stale apply
    // is skipped instead of regressing it.
    cache.refresh(cacheKey, reader, sync);
  }

  @Override
  public <V> void refresh(String cacheKey, CacheLoader<String, V> loader, boolean sync) {
    requireKey(cacheKey);
    Objects.requireNonNull(loader, "loader must not be null");
    HotKeyCache cache = requireWritableCache("refresh", sync, 0L);
    cache.refresh(cacheKey, loader, sync);
  }

  @Override
  @SuppressWarnings("all")
  public void refreshAll(Map<String, Supplier<?>> loaders, boolean sync) {
    Objects.requireNonNull(loaders, "loaders must not be null");
    // Mode and sync-configuration validation precedes the empty short-circuit: an invalid
    // configuration is a caller error even when there is nothing to refresh, and Worker-only
    // deployments throw for every data-plane call.
    HotKeyCache cache = requireWritableCache("refreshAll", sync, 0L);
    if (loaders.isEmpty()) {
      return;
    }
    // Validate and snapshot every entry before submitting anything: argument errors are caller
    // errors, not per-key loader failures. The snapshot also closes the race where a concurrent
    // map insertion between validation and submission could bypass the checks.
    List<Map.Entry<String, Supplier<?>>> validated = new ArrayList<>(loaders.size());
    for (Map.Entry<String, Supplier<?>> entry : loaders.entrySet()) {
      String key = entry.getKey();
      Supplier<?> loader = entry.getValue();
      requireKey(key);
      Objects.requireNonNull(loader, "loader must not be null");
      validated.add(Map.entry(key, loader));
    }
    // One entry: run on the calling thread. A fan-out task would add a queue
    // allocation, a thread switch and a future to wait on for no parallelism.
    if (validated.size() == 1) {
      Map.Entry<String, Supplier<?>> single = validated.get(0);
      try {
        cache.refresh(single.getKey(), single.getValue(), sync);
      } catch (Throwable failure) {
        rethrow(failure);
      }
      return;
    }
    // Independent per key: fan out so one slow loader cannot head-block the rest, and rethrow
    // the first failure after the fan-out settles. The injected refresh pool keeps this off the
    // JVM-global common pool; its admission policy owns back-pressure.
    Executor executor = refreshExecutor != null ? refreshExecutor : ForkJoinPool.commonPool();
    Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
    List<CompletableFuture<Void>> futures = new ArrayList<>(validated.size());
    try {
      for (Map.Entry<String, Supplier<?>> entry : validated) {
        try {
          futures.add(
            CompletableFuture.runAsync(
              () -> {
                try {
                  cache.refresh(entry.getKey(), entry.getValue(), sync);
                } catch (Throwable ex) {
                  // Throwable, not RuntimeException: an Error escaping the task
                  // would complete the future exceptionally and every other key's
                  // failure would be lost. Restore an interrupt a sneaky-throwing
                  // supplier consumed so the pool thread keeps its signal.
                  if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                  }
                  failures.add(ex);
                }
              },
              executor
            )
          );
        } catch (RejectedExecutionException rejection) {
          // The pool is saturated or shut down: record the admission failure and stop
          // submitting, or one saturation event becomes a rejection storm.
          failures.add(rejection);
          break;
        } catch (RuntimeException submission) {
          // Any other submission failure is an admission failure for THIS key only.
          failures.add(submission);
        } catch (Throwable fatalSubmission) {
          // An Error can leave the executor unusable: record it, stop submitting, and let the
          // finally join the tasks this call already accepted.
          failures.add(fatalSubmission);
          break;
        }
      }
    } finally {
      // The finally keeps a submission failure from abandoning the tasks already handed to the
      // pool. Every task catches Throwable, so no future can complete exceptionally and a plain
      // join loop waits exactly as long as allOf without the array copy.
      for (CompletableFuture<Void> future : futures) {
        future.join();
      }
    }
    // All tasks have settled, so the queue is quiescent: poll the first and build
    // the identity set only when a second failure actually needs it.
    Throwable first = failures.poll();
    if (first != null) {
      if (!failures.isEmpty()) {
        Set<Throwable> attached = Collections.newSetFromMap(new IdentityHashMap<>());
        attached.add(first);
        for (Throwable next : failures) {
          if (attached.add(next)) {
            attachSuppressed(first, next);
          }
        }
      }
      rethrow(first);
    }
  }

  /**
   * Rethrows a collected failure: unchecked throwables keep their identity, a
   * checked throwable from a sneaky-throwing supplier is wrapped rather than
   * cast, or the cast itself would mask the real failure.
   *
   * @param failure the failure to rethrow
   */
  private static void rethrow(Throwable failure) {
    if (failure instanceof RuntimeException runtime) {
      throw runtime;
    }
    if (failure instanceof Error error) {
      throw error;
    }
    throw new CompletionException(failure);
  }

  /**
   * Attaches a secondary failure without letting the diagnostic replace the
   * primary one. {@link Throwable#addSuppressed} allocates, so an exhausted heap
   * can fail the attachment after the real failure has already been collected.
   */
  private static void attachSuppressed(Throwable primary, Throwable secondary) {
    try {
      primary.addSuppressed(secondary);
    } catch (RuntimeException | Error ignored) {
      // Best-effort diagnostics: the primary failure is what the caller must see.
    }
  }

  @Override
  public boolean isCacheAvailable() {
    // Cache presence is the whole availability contract: the cache owns a
    // detector by construction, while the facade's detector field feeds only
    // detector().
    return hotKeyCache != null;
  }

  @Override
  public Zeta.DetectorAdmin detector() {
    return detectorAdmin;
  }

  @Override
  public Zeta.UnsafeAdmin unsafe() {
    return unsafeAdmin;
  }

  @Override
  public ZetaCacheStats stats() {
    return requireAppCache("stats").stats();
  }

  private HotKeyCache requireAppCache(String operation) {
    if (hotKeyCache == null) {
      throw new ZetaModeException(operation, WORKER_ONLY_MODE, "App-mode cache");
    }
    return hotKeyCache;
  }

  /**
   * The write-side entry guard: resolve the cache and, for a synced write,
   * reject a hard TTL the sync plane cannot honour before any state changes.
   *
   * @param operation the facade verb, for the mode error
   * @param sync      whether the write is broadcast to peers
   * @param hardMs    the resolved hard TTL of the write
   * @return the app-side cache
   */
  private HotKeyCache requireWritableCache(String operation, boolean sync, long hardMs) {
    HotKeyCache cache = requireAppCache(operation);
    if (sync) {
      PutPolicy.requireSyncable(cache.resolveSyncableHardTtl(hardMs), true);
    }
    return cache;
  }

  /**
   * The facade's single-key contract: every public data-plane method rejects a
   * null/blank key here, and the engine below assumes validity instead of
   * re-checking it. The batch read and invalidate paths filter elements through
   * {@link io.github.hyshmily.zeta.cache.CacheKeysPolicy#validKeys validKeys}
   * instead, because a batch built from a query result legitimately contains
   * no-key rows; a raw dedup mutation validates every element with this method.
   *
   * @param key the key to validate
   */
  private static void requireKey(@Nullable String key) {
    Assert.hasText(key, "cacheKey must not be null or blank");
  }

  private static <T> T orDefault(@Nullable T value, T fallback) {
    return value == null ? fallback : value;
  }

  /**
   * Static TTL values resolved exactly once per call, with the shared
   * non-negative guard.
   */
  private record ResolvedTtls(long hardMs, long softMs) {
    /** The all-default resolution: no TTL override on either axis. */
    static final ResolvedTtls DEFAULT = new ResolvedTtls(0L, 0L);

    static ResolvedTtls of(PutPolicy options) {
      if (options == PutPolicy.defaults() || options == PutPolicy.synced()) {
        return DEFAULT;
      }
      return of(options.hardTtlMs().getAsLong(), options.softTtlMs().getAsLong());
    }

    static ResolvedTtls of(GetPolicy options) {
      return options == GetPolicy.defaults()
        ? DEFAULT
        : of(options.hardTtlMs().getAsLong(), options.softTtlMs().getAsLong());
    }

    private static ResolvedTtls of(long hardMs, long softMs) {
      Assert.isTrue(hardMs >= 0, "hardTtlMs must not be negative");
      Assert.isTrue(softMs >= 0, "softTtlMs must not be negative");
      return new ResolvedTtls(hardMs, softMs);
    }
  }

  /**
   * Raw-table escape-hatch view. A dedicated projection (not the facade) so
   * callers cannot cast through to the full facade — the same pattern as
   * {@link DetectorView}.
   */
  private final class UnsafeView implements Zeta.UnsafeAdmin {

    @Override
    public Cache<String, CacheEntry> raw() {
      return requireAppCache("raw").table();
    }

    @Override
    public void forgetDedup(String... cacheKeys) {
      Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
      // Snapshot the caller's varargs array so an element cannot be swapped
      // between validation and the engine's loop.
      String[] keys = cacheKeys.clone();
      // Fail-fast per element, as on every single-key verb: the engine's dedup
      // loop assumes valid keys.
      for (String cacheKey : keys) {
        requireKey(cacheKey);
      }
      HotKeyCache cache = requireAppCache("forgetDedup");
      if (keys.length > 0) {
        cache.forgetDedup(keys);
      }
    }
  }

  /**
   * Detection-observation view over the facade's detector and cache.
   * Non-static inner class for direct access to the facade's guards and
   * collaborators.
   */
  private final class DetectorView implements Zeta.DetectorAdmin {

    @Override
    public boolean isLocalHotKey(String cacheKey) {
      requireKey(cacheKey);
      return requireAppCache("isLocalHotKey").isHot(cacheKey);
    }

    @Override
    public List<Item> localTopKeys(int limit) {
      // Validate before the detector check: App and Worker-only modes must
      // reject the same arguments.
      Assert.isTrue(limit >= 0, () -> "limit must be non-negative, but got: " + limit);
      return limit == 0 || appTopK == null ? List.of() : appTopK.listTopN(limit);
    }
  }
}
