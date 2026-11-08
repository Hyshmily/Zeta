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

import static io.github.hyshmily.zeta.cache.CacheKeysPolicy.validKeys;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.scheduler.TimedRefreshCoordinator;
import io.github.hyshmily.zeta.util.Internal;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Setter;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

/**
 * Default implementation of the {@link Zeta} facade — the sole public API
 * entry point. Reads/writes/invalidations go to {@link HotKeyCache}, timed
 * refresh scheduling to {@code TimedRefreshCoordinator}, and the
 * administrative views ({@link Zeta.DetectorAdmin detector()},
 * {@link Zeta.RefreshAdmin refresh()}, {@link Zeta.UnsafeAdmin unsafe()}) are
 * projected from the same components. The facade adds only orchestration that
 * has no better home: argument validation, option normalization,
 * deployment-mode gating, the bulk-refresh fan-out, and the projections
 * themselves.
 *
 * <p>
 * All cache read/write/invalidation operations are dispatched through this
 * class, which delegates to {@link HotKeyCache} for L1 orchestration, version
 * stamping, SingleFlight dedup, and cluster sync. {@code null} options
 * arguments are normalized to their family's {@code defaults()}; a loader's
 * {@code isCacheable} decision travels as an explicit admission predicate, so
 * a caller can never override it per call.
 *
 * <p>
 * <b>Thread safety:</b> All public methods are thread-safe. The underlying
 * collaborators are thread-safe by contract; the facade keeps no per-call
 * mutable state, only the shared stateless views and the injected refresh
 * executor.
 */
public class DefaultZeta implements Zeta, DisposableBean {

  /**
   * Label used when the facade runs without an app-side cache. Kept as one
   * constant so the mode report cannot drift from the exception text.
   */
  private static final String WORKER_ONLY_MODE = "Worker-only mode (no app-side cache)";

  /** Timed background refresh scheduler, or {@code null} in Worker-only deployments. */
  private final @Nullable TimedRefreshCoordinator refreshCoordinator;
  /** Core cache engine. {@code null} in Worker-only deployments. */
  private final @Nullable HotKeyCache hotKeyCache;
  /** App-side TopK sketch (read side). {@code null} when the detector is not wired. */
  private final @Nullable TopK appTopK;

  /** Shared administrative views; all are stateless projections. */
  private final Zeta.DetectorAdmin detectorAdmin = new DetectorView();
  private final Zeta.RefreshAdmin refreshAdmin = new RefreshView();
  private final Zeta.UnsafeAdmin unsafeAdmin = new UnsafeView();

  /**
   * Executor for fan-out bulk operations ({@link #refreshAll}). Wired from the
   * dedicated refresh pool ({@code hotKeyExecutor}) by the auto-configuration;
   * when absent (direct construction, mostly tests) the bulk path falls back to
   * the common pool. The pool is shared and never shut down here.
   */
  @Setter
  private volatile Executor refreshExecutor;

  /**
   * Creates the facade. Container wiring, not an extension point: the
   * auto-configuration wires it ({@code ZetaFacadeAutoConfiguration});
   * subclassing is not supported.
   */
  @Internal
  public DefaultZeta(@Nullable HotKeyCache hotKeyCache, @Nullable TopK appTopK) {
    this.appTopK = appTopK;
    this.hotKeyCache = hotKeyCache;
    this.refreshCoordinator = hotKeyCache != null ? new TimedRefreshCoordinator() : null;
  }

  @Override
  public void tag(String cacheKey) {
    requireKey(cacheKey);
    requireAppCache("tag").recordAccess(cacheKey);
  }

  @Override
  public void tag(String cacheKey, TagMode mode) {
    requireKey(cacheKey);
    Objects.requireNonNull(mode, "mode must not be null");
    requireAppCache("tag").recordAccess(cacheKey, mode);
  }

  @Override
  public void tag(String cacheKey, long count) {
    requireKey(cacheKey);
    Assert.isTrue(count > 0, () -> "count must be positive, but got: " + count);
    requireAppCache("tag").recordAccess(cacheKey, count);
  }

  @Override
  public <V> V get(String cacheKey, CacheLoader<String, V> loader, GetPolicy options) {
    requireKey(cacheKey);
    Objects.requireNonNull(loader, "loader must not be null");
    GetPolicy opts = orDefault(options, GetPolicy.defaults());
    return requireAppCache("get").get(cacheKey, loader, opts);
  }

  @Override
  public <V> V get(String cacheKey, Supplier<V> reader, GetPolicy options) {
    requireKey(cacheKey);
    Objects.requireNonNull(reader, "reader must not be null");
    GetPolicy opts = orDefault(options, GetPolicy.defaults());
    return requireAppCache("get").get(cacheKey, reader, opts);
  }

  @Override
  public <T> T peek(String cacheKey) {
    requireKey(cacheKey);
    return requireAppCache("peek").peek(cacheKey);
  }

  @Override
  public void put(String cacheKey, Object value, PutPolicy options) {
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
  public void putAll(Map<String, ?> entries, PutPolicy options) {
    Objects.requireNonNull(entries, "entries must not be null");
    // The mode check precedes the empty short-circuit: Worker-only deployments
    // throw for every data-plane call, including one with nothing to write.
    HotKeyCache cache = requireAppCache("putAll");
    if (entries.isEmpty()) {
      return;
    }
    PutPolicy opts = orDefault(options, PutPolicy.defaults());
    ResolvedTtls ttls = ResolvedTtls.of(opts);
    // Writes are caller intent: reject the whole batch before any key lands.
    // Unlike the read/invalidate collection methods, dropping an invalid element
    // would silently leave a batch half-applied.
    for (Map.Entry<String, ?> entry : entries.entrySet()) {
      requireKey(entry.getKey());
      Objects.requireNonNull(
        entry.getValue(),
        "value must not be null: putAll rejects a null value (null is never cached)"
      );
    }
    if (opts.sync()) {
      PutPolicy.requireSyncable(cache.resolveSyncableHardTtl(ttls.hardMs()), true);
    }
    cache.putAll(entries, ttls.hardMs(), ttls.softMs(), opts.sync());
  }

  @Override
  public void invalidate(String cacheKey, InvalidatePolicy options) {
    requireKey(cacheKey);
    InvalidatePolicy opts = orDefault(options, InvalidatePolicy.defaults());
    requireAppCache("invalidate").invalidate(cacheKey, opts);
  }

  @Override
  public void invalidate(Collection<String> cacheKeys, InvalidatePolicy options) {
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
  public void clearAll(InvalidatePolicy options) {
    InvalidatePolicy opts = orDefault(options, InvalidatePolicy.defaults());
    requireAppCache("clearAll").clear(opts);
  }

  @Override
  public void clearPrefix(String prefix, InvalidatePolicy options) {
    Assert.hasText(prefix, "prefix must not be blank");
    InvalidatePolicy opts = orDefault(options, InvalidatePolicy.defaults());
    requireAppCache("clearPrefix").clearPrefix(prefix, opts);
  }

  @Override
  public <V> Map<String, V> getAll(
    Collection<String> cacheKeys,
    Function<? super String, ? extends V> reader,
    GetPolicy options
  ) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    GetPolicy opts = orDefault(options, GetPolicy.defaults());
    HotKeyCache cache = requireAppCache("getAll");
    // An invalid key cannot be a map key and is a miss by definition: dropped
    // here, so the engine's batch read assumes valid keys.
    return cache.get(validKeys(cacheKeys), reader, opts);
  }

  @Override
  public <V> void refresh(String cacheKey, Supplier<V> loader, PutPolicy options) {
    requireKey(cacheKey);
    Objects.requireNonNull(loader, "loader must not be null");
    PutPolicy opts = orDefault(options, PutPolicy.defaults());
    ResolvedTtls ttls = ResolvedTtls.of(opts);
    HotKeyCache cache = requireWritableCache("refresh", opts.sync(), ttls.hardMs());
    // Bump-before-load ordering lives in HotKeyCache#refresh: a concurrent
    // synced put during the load holds the newer version and the stale apply
    // is skipped instead of regressing it.
    cache.refresh(cacheKey, loader, ttls.hardMs(), ttls.softMs(), opts.sync());
  }

  @Override
  public void refreshAll(Map<String, Supplier<?>> loaders) {
    Objects.requireNonNull(loaders, "loaders must not be null");
    // The mode check precedes the empty short-circuit: Worker-only deployments
    // throw for every data-plane call, including one with nothing to refresh.
    HotKeyCache cache = requireAppCache("refreshAll");
    if (loaders.isEmpty()) {
      return;
    }
    // refreshAll is always a cluster write; validate the effective default hard
    // TTL once before any task is submitted.
    PutPolicy.requireSyncable(cache.resolveSyncableHardTtl(0L), true);
    // Validate every entry before submitting anything: argument errors are caller
    // errors, not per-key loader failures.
    for (Map.Entry<String, Supplier<?>> entry : loaders.entrySet()) {
      requireKey(entry.getKey());
      Objects.requireNonNull(entry.getValue(), "loader must not be null");
    }
    // Independent per key: fan out so one slow loader cannot head-block the
    // rest, and rethrow the first failure after the fan-out settles.
    // The injected refresh pool keeps this off the JVM-global common pool
    // (directly constructed facades, mostly tests, fall back to it).
    Executor executor = refreshExecutor != null ? refreshExecutor : ForkJoinPool.commonPool();
    // Lock-free collector: the fan-out tasks add concurrently, and the list is only
    // materialized when something failed — the normal path allocates no synchronized
    // wrapper and no backing array.
    Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
    List<CompletableFuture<Void>> futures = new ArrayList<>(loaders.size());
    for (Map.Entry<String, Supplier<?>> entry : loaders.entrySet()) {
      try {
        futures.add(
          CompletableFuture.runAsync(
            () -> {
              try {
                // Same defaults as refresh(key, loader): no TTL override, synced.
                cache.refresh(entry.getKey(), entry.getValue(), 0L, 0L, true);
              } catch (Throwable ex) {
                // Throwable, not RuntimeException: an Error escaping the task
                // would complete the future exceptionally and every other key's
                // failure would be lost.
                failures.add(ex);
              }
            },
            executor
          )
        );
      } catch (RejectedExecutionException rejection) {
        // The pool is saturated or shut down: every further submission would
        // allocate another full-stack rejection for a key that cannot run.
        // Record the admission failure and stop submitting — continuing the
        // loop only turns one saturation event into a rejection storm.
        failures.add(rejection);
        break;
      } catch (RuntimeException submission) {
        // Any other submission failure is an admission failure for THIS key
        // only; the remaining keys are still attempted.
        failures.add(submission);
      }
    }
    // Every task catches Throwable, so no future can complete exceptionally: a
    // plain join loop waits exactly as long as allOf (the slowest task) without
    // the array copy and the BiCompletion tree allOf builds.
    for (CompletableFuture<Void> future : futures) {
      future.join();
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
            first.addSuppressed(next);
          }
        }
      }
      if (first instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (first instanceof Error error) {
        throw error;
      }
      // A checked Throwable from a sneaky-throwing supplier: wrap instead of
      // casting, or the cast itself would mask the real failure.
      throw new CompletionException(first);
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
  public Zeta.RefreshAdmin refresh() {
    return refreshAdmin;
  }

  @Override
  public Zeta.UnsafeAdmin unsafe() {
    return unsafeAdmin;
  }

  @Override
  public ZetaCacheStats stats() {
    return requireAppCache("stats").stats();
  }

  /**
   * Cancel all timed refreshes and shut down the refresh scheduler. Called
   * automatically by Spring when this bean is destroyed.
   *
   * <p>The {@link io.github.hyshmily.zeta.util.TimeSource} ticker is NOT
   * stopped here: it is a JVM-global singleton, and this bean is not its owner.
   * The owning auto-configuration holds a reference-counted lifecycle claim
   * ({@code registerLifecycleUser}/{@code releaseLifecycleUser}), so tearing
   * down one context in a JVM that runs several (parallel integration tests,
   * modular multi-context deployments) does not degrade every other context to
   * a JNI clock read per call.
   */
  @Override
  public void destroy() {
    TimedRefreshCoordinator coordinator = refreshCoordinator;
    if (coordinator != null) {
      coordinator.destroy();
    }
  }

  /**
   * Returns a human-readable label of the current deployment mode.
   */
  private String currentModeLabel() {
    if (hotKeyCache != null && appTopK != null) {
      return "App-only mode";
    }
    if (hotKeyCache == null) {
      // The facade is only ever created with a null cache in Worker-only
      // deployments (ZetaFacadeAutoConfiguration wires it via ObjectProvider);
      // a cache-less facade in an app deployment would be a wiring error.
      return WORKER_ONLY_MODE;
    }
    return "App-mode with missing TopK detector (partial wiring)";
  }

  private HotKeyCache requireAppCache(String operation) {
    if (hotKeyCache == null) {
      throw new ZetaModeException(operation, currentModeLabel(), "App-mode cache");
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
   * re-checking it. Collection arguments take the sibling policy
   * ({@link io.github.hyshmily.zeta.cache.CacheKeysPolicy#validKeys validKeys}):
   * invalid elements are dropped rather than rejected, because a batch built
   * from a query result legitimately contains no-key rows.
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
   * Timed-refresh administration view. A dedicated projection (not the
   * facade) so callers cannot cast through to the full facade — the same
   * pattern as {@link DetectorView}. In Worker-only deployments the coordinator
   * is absent and registration throws the mode error.
   */
  private final class RefreshView implements Zeta.RefreshAdmin {

    @Override
    public <T> void registerRefresh(String key, Supplier<T> supplier, GetPolicy options) {
      requireKey(key);
      Objects.requireNonNull(supplier, "supplier must not be null");
      HotKeyCache cache = requireAppCache("registerRefresh");
      GetPolicy opts = orDefault(options, GetPolicy.defaults());
      ResolvedTtls ttls = ResolvedTtls.of(opts);
      long effectiveSoft = cache.resolveEffectiveSoftTtl(ttls.softMs());
      Assert.isTrue(
        effectiveSoft > 0,
        "registerRefresh requires a positive effective soft TTL (stale-while-revalidate cadence); " +
          "got " +
          effectiveSoft +
          " — enable zeta.local.soft-ttl-ms or pass a positive softTtlMs"
      );

      long intervalMs = TimedRefreshCoordinator.intervalFor(effectiveSoft);
      // The constructor creates the coordinator together with the cache; a null here is a
      // wiring bug, not a deployment mode.
      TimedRefreshCoordinator coordinator = Objects.requireNonNull(
        refreshCoordinator,
        "refresh coordinator must exist with an app cache"
      );
      // Freeze the per-tick options outside the coordinator's map lock. The tick calls the
      // cache directly: key, supplier and options were validated at registration, so the
      // facade's argument/normalization path does not run again on every firing.
      final GetPolicy tick = GetPolicy.of(ttls.hardMs(), ttls.softMs(), opts.report(), StalePolicy.SOFT_REFRESH);
      coordinator.register(key, intervalMs, () -> cache.get(key, supplier, tick));
    }

    @Override
    public void unregisterRefresh(String key) {
      TimedRefreshCoordinator coordinator = refreshCoordinator;
      if (coordinator == null) {
        // Worker-only mode has no scheduler to unregister from.
        return;
      }
      requireKey(key);
      coordinator.unregister(key);
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
      // Fail-fast per element, as on every single-key verb: the engine's dedup
      // loop assumes valid keys.
      for (String cacheKey : cacheKeys) {
        requireKey(cacheKey);
      }
      requireAppCache("forgetDedup").forgetDedup(cacheKeys);
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
      return appTopK == null ? List.of() : appTopK.listTopN(limit);
    }
  }
}
