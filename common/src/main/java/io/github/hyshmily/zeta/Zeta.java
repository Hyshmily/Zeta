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
import io.github.hyshmily.zeta.cache.fluentAPI.ZetaReadQuery;
import io.github.hyshmily.zeta.cache.fluentAPI.ZetaWriteCommand;
import io.github.hyshmily.zeta.cache.loader.ZetaLoaderRegistry;
import io.github.hyshmily.zeta.cache.loader.ZetaLoadingSpec;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.CachePolicy;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sync.distributedlock.AutoReleaseLock;
import io.github.hyshmily.zeta.sync.distributedlock.LockProvider;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

/**
 * Public facade for the HotKey library — the sole public API entry point.
 *
 * <p>
 * All cache read/write/invalidation operations are dispatched through this
 * class, which delegates to {@link HotKeyCache} for L1 orchestration, version
 * tracking, and cross-instance broadcast, and to {@link TopK} (HeavyKeeper)
 * for local and cluster-wide hot-key detection queries.
 *
 * <p>
 * Rule management (blacklist/whitelist) is also exposed here, with pattern
 * matching for exact, prefix, wildcard, and regex rules.
 *
 * <p>
 * <b>Thread safety:</b> All public methods are thread-safe. The underlying
 * Caffeine cache and HeavyKeeper sketch are designed for concurrent access.
 *
 * <p>
 * <b>API shape — two-arity convention:</b> every operation family exposes
 * exactly two positional forms — the common-case convenience form, and a
 * full-control form whose last parameter is a {@link CachePolicy}, the named
 * options object. Intermediate positional overloads (per-parameter TTLs,
 * boolean flags) intentionally do not exist: customize via
 * {@code CachePolicy.defaults().withHardTtl(...)}-style chains, or via the
 * fluent {@link #read(String)}/{@link #write(String)} builders for complex
 * chains. Each policy-form javadoc states which {@link CachePolicy} components
 * are honored by that family; unlisted components are ignored. The single
 * exception to the no-boolean rule is {@link #tag(String, boolean, boolean)},
 * the transport for the {@code @Intercept} annotation path.
 *
 * <p>
 * Depending on the runtime mode, some dependencies may be absent:
 * <ul>
 * <li><b>App-only mode</b> (default): all services available.</li>
 * <li><b>Worker-only mode</b> ({@code zeta.worker.enabled=true}):
 * cache and app‑side TopK are absent; only the Worker TopK is available.</li>
 * </ul>
 * Methods whose backing service is absent throw {@link ZetaModeException}
 * (cache read/write) or return empty / zero (TopK queries).
 *
 * <p>
 * <b>Version constraint for permanent entries:</b> Entries created with
 * {@link Long#MAX_VALUE} hard TTL ("permanent" entry) can outlive the Redis
 * version key ({@code zeta:version:{key}}), whose TTL defaults to 7 days
 * ({@code zeta.local.versionKeyTtlMinutes}). When the version key expires,
 * the next write produces a version of 1 (INCR restart), which is numerically
 * lower than the existing entry's version on peer instances — causing
 * {@link io.github.hyshmily.zeta.util.version.VersionGuard#shouldSkipForSync}
 * to reject the update silently. Avoid {@link Long#MAX_VALUE} hard TTL on keys
 * that participate in cross-instance cache sync, or ensure
 * {@code versionKeyTtlMinutes} exceeds the entry's actual lifetime.
 */
public class Zeta implements DisposableBean {

  /**
   * Cache orchestrator that manages L1 (Caffeine), version tracking, TTL
   * enforcement, cross-instance broadcast, and rule evaluation.
   * {@code null} in Worker-only mode.
   */
  private final HotKeyCache hotKeyCache;
  /**
   * App-side local hot-key detector (HeavyKeeper). Records every cache
   * access to maintain local frequency sketches. Never {@code null} in
   * app mode.
   */
  private final HotKeyDetector appHotKeyDetector;
  /**
   * Distributed lock provider. {@code null} when no Redis is available
   * (graceful degradation — {@link #tryLock} returns {@code null}).
   */
  private final LockProvider lockProvider;

  /**
   * Optional registry binding key prefixes to
   * {@link io.github.hyshmily.zeta.cache.loader.CacheLoader} specs
   * (ADR-0070). {@code null} when no registry bean exists — the no-reader
   * {@code get(cacheKey)} overloads then fail fast with an explanatory
   * {@link IllegalStateException}.
   */
  @Nullable
  private final ZetaLoaderRegistry loaderRegistry;

  /**
   * Scheduler for timed background refresh tasks (lazily initialized).
   */
  @SuppressWarnings("java:S3077") // ScheduledExecutorService is thread-safe; we manage its lifecycle
  private volatile ScheduledExecutorService refreshScheduler;

  /**
   * Per-key scheduled refresh futures, keyed by cache key.
   */
  private final ConcurrentHashMap<String, ScheduledFuture<?>> refreshFutures;

  /**
   * Create a HotKey facade with cache and detector only.
   *
   * <p>
   * Either parameter may be {@code null} depending on the deployment mode.
   *
   * @param hotKeyCache       the cache orchestrator (maybe {@code null} in
   *                          Worker-only mode)
   * @param appHotKeyDetector the app-side local TopK detector (maybe {@code null}
   *                          in Worker-only mode)
   */
  public Zeta(HotKeyCache hotKeyCache, HotKeyDetector appHotKeyDetector) {
    this(hotKeyCache, appHotKeyDetector, null, null);
  }

  /**
   * Create a HotKey facade with an optional distributed lock provider and
   * loader registry (ADR-0070).
   *
   * <p>
   * Each parameter may be {@code null} depending on the deployment mode.
   *
   * @param hotKeyCache       the cache orchestrator (maybe {@code null} in
   *                          Worker-only mode)
   * @param appHotKeyDetector the app-side local TopK detector (maybe {@code null}
   *                          in Worker-only mode)
   * @param lockProvider      the distributed lock provider (maybe {@code null}
   *                          when no Redis)
   * @param loaderRegistry    the prefix→loader registry backing the no-reader
   *                          {@code get(cacheKey)} overloads (maybe {@code null})
   */
  public Zeta(
      HotKeyCache hotKeyCache,
      HotKeyDetector appHotKeyDetector,
      LockProvider lockProvider,
      @Nullable ZetaLoaderRegistry loaderRegistry) {
    this.hotKeyCache = hotKeyCache;
    this.appHotKeyDetector = appHotKeyDetector;
    this.lockProvider = lockProvider;
    this.loaderRegistry = loaderRegistry;
    this.refreshFutures = new ConcurrentHashMap<>();
  }

  /**
   * Create a fluent read query for the given key.
   *
   * <p>
   * Returns a {@link ZetaReadQuery} builder that lets you configure
   * the primary reader, fallback chain, cache mode, TTL overrides, null
   * caching policy, and broadcast behaviour before executing.
   *
   * <p>
   * Useful for read-heavy call-sites that prefer a declarative style
   * over manual {@code get()}/{@code getWithSoftExpire()} orchestration.
   *
   * <p>
   * Example:
   * 
   * <pre>
   * Optional&lt;User&gt; user = hotKey.read("user:42")
   *     .withPrimary(userRepo::findById)
   *     .thenExecute(backupRepo::findById)
   *     .withHardTtl(30_000)
   *     .withSoftTtl(10_000)
   *     .allowBroadcast()
   *     .execute();
   * </pre>
   *
   * @param cacheKey the cache key to read
   * @param <T>      the value type
   * @return a new {@link ZetaReadQuery} instance (single-use)
   */
  public <T> ZetaReadQuery<T> read(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    return new ZetaReadQuery<>(this, cacheKey);
  }

  /**
   * Create a fluent write command for the given key.
   *
   * <p>
   * Returns a {@link ZetaWriteCommand} builder that lets you configure
   * TTL overrides before executing a write-through, invalidate-before-write,
   * or plain invalidation.
   *
   * <p>
   * Example:
   * 
   * <pre>
   * hotKey.write("user:42")
   *     .withHardTtl(30_000)
   *     .putThrough(newValue, dbWriter);
   * </pre>
   *
   * @param cacheKey the cache key to operate on
   * @param <T>      the value type
   * @return a new {@link ZetaWriteCommand} instance (single-use)
   */
  public <T> ZetaWriteCommand<T> write(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    return new ZetaWriteCommand<>(this, cacheKey);
  }

  /**
   * Tag a cache key as potentially hot, reporting it to the Worker without
   * performing a cache read.
   *
   * <p>
   * Updates the local HeavyKeeper sketch and enqueues the key for batch
   * reporting to the Worker, exactly as {@link #get} would, but without
   * touching the L1 cache or invoking any reader/loader.
   *
   * @param cacheKey the key to tag
   */
  public void tag(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("tag");
    hotKeyCache.tag(cacheKey);
  }

  /**
   * Tag a cache key with fine-grained control over detection and reporting.
   *
   * <p>
   * Unlike {@link #tag(String)}, this overload lets callers independently
   * skip the local HeavyKeeper increment and/or the Worker report. It is the
   * transport for the {@code @Intercept} annotation path
   * ({@code CacheExtensionAspect}); application code should prefer
   * {@link #tag(String)}.
   *
   * @param cacheKey      the key to tag
   * @param skipDetection if {@code true}, skip the local HeavyKeeper count
   * @param skipReport    if {@code true}, skip the Worker report
   */
  public void tag(String cacheKey, boolean skipDetection, boolean skipReport) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("tag");
    hotKeyCache.tag(cacheKey, skipDetection, skipReport);
  }

  /**
   * No-reader get: resolves the key against the {@link ZetaLoaderRegistry}
   * (ADR-0070) and loads through the registered
   * {@link io.github.hyshmily.zeta.cache.loader.CacheLoader} on a
   * miss — the Caffeine {@code LoadingCache} style, where the loader is
   * registered once instead of passed at every call site.
   *
   * <p>
   * <b>Precedence — no conflict with explicit readers:</b> the
   * {@code get(key, reader)}, {@code get(key, CachePolicy)} and
   * {@code read(key).withPrimary(...)} overloads never consult the registry;
   * an explicit reader at the call site always wins. The registered loader
   * applies only to the no-reader overloads, plus — via the composite
   * sync-plane loader — to Worker HOT warm-up and peer REFRESH. A plain L1
   * hit never invokes any loader.
   *
   * <p>
   * The spec registered for the winning prefix supplies the TTL overrides
   * (its {@code hardTtlMs} is the expireAfterWrite equivalent, {@code softTtlMs}
   * the refreshAfterWrite equivalent), the stale policy, null caching,
   * reporting, and failure semantics. The lookup itself is delegated to
   * {@link #get(String, CachePolicy)}, so SingleFlight deduplication, circuit
   * breaking, soft-expire background refresh, and Worker reporting all apply
   * unchanged. A plain L1 hit never invokes the loader.
   *
   * @param cacheKey the key to retrieve
   * @param <T>      the value type (erased — the loader's own type governs)
   * @return an {@link Optional} containing the cached or loaded value
   * @throws ZetaModeException     when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException  when the key matches a blacklist rule
   * @throws IllegalStateException when no registry is wired or no registered
   *                               prefix matches the key
   */
  public <T> Optional<T> get(String cacheKey) {
    ZetaLoadingSpec<?> spec = requireRegisteredSpec(cacheKey, "get");
    return get(cacheKey, spec.toPolicy(cacheKey));
  }

  /**
   * No-reader get with soft-expire: as {@link #get(String)}, but forces
   * {@link StalePolicy#SOFT_REFRESH} (serve the stale value, refresh in the
   * background) regardless of the winning spec's stale policy — mirroring the
   * {@code getWithSoftExpire(key, reader)} naming in the reader-based API.
   *
   * @param cacheKey the key to retrieve
   * @param <T>      the value type (erased — the loader's own type governs)
   * @return an {@link Optional} containing the cached (possibly stale) or loaded
   *         value
   * @throws ZetaModeException     when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException  when the key matches a blacklist rule
   * @throws IllegalStateException when no registry is wired or no registered
   *                               prefix matches the key
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey) {
    ZetaLoadingSpec<?> spec = requireRegisteredSpec(cacheKey, "getWithSoftExpire");
    return getWithSoftExpire(cacheKey, spec.toPolicy(cacheKey, StalePolicy.SOFT_REFRESH));
  }

  /**
   * Get a value from L1 or load it via the reader.
   * Hot keys are promoted to L1 with configured hot TTLs; normal keys use default
   * TTLs.
   *
   * <p>
   * <b>Registry precedence:</b> this overload never consults the
   * {@link ZetaLoaderRegistry} — a spec registered for the key's prefix (its
   * TTL overrides, null-caching, {@code failOnError}, reporting) is <i>not</i>
   * applied; global defaults govern instead. To inherit a spec's policy while
   * swapping only the data source, derive it explicitly —
   * {@code spec.withLoader(custom).toPolicy(key)} handed to
   * {@link #get(String, CachePolicy)}.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    return get(cacheKey, CachePolicy.of(reader, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));
  }

  /**
   * Get with an explicit {@link CachePolicy}. On this (non-atomic) path the
   * policy's TTL suppliers are evaluated lazily — at most once per call and
   * only when an entry is created, promoted, or a soft-expire refresh is
   * scheduled (the {@link CachePolicy} contract), so SpEL-based TTL
   * expressions cost nothing on a plain hit; a valid {@code NullValue}
   * sentinel is served as an empty hit without re-invoking the reader, and
   * the access is still counted for hot-key detection. When
   * {@link CachePolicy#nullCaching()} is {@code false}, a {@code null} reader
   * result leaves no cache entry. The policy carries the reader
   * ({@link CachePolicy#withReader(Supplier)} may inject one) and the
   * report-allow flag.
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation cache policy (carries reader,
   *                 TTLs, null-caching, stale policy, fail-fast (failOnError),
   *                 and report-allow flag)
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, CachePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("get");
    requireAppDetector("get");
    return hotKeyCache.get(cacheKey, policy);
  }

  /**
   * Get with soft-expire using all-default configuration.
   *
   * <p>
   * <b>Registry precedence:</b> this overload never consults the
   * {@link ZetaLoaderRegistry} — a spec registered for the key's prefix is
   * <i>not</i> applied; global defaults govern and the stale policy is forced
   * to {@link StalePolicy#SOFT_REFRESH}. To inherit a spec's policy while
   * swapping only the data source, derive it explicitly —
   * {@code spec.withLoader(custom).toPolicy(key, StalePolicy.SOFT_REFRESH)}
   * handed to {@link #getWithSoftExpire(String, CachePolicy)}.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses / refreshes
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded
   *         value
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    return getWithSoftExpire(cacheKey, CachePolicy.of(reader, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));
  }

  /**
   * Get with soft-expire and an explicit {@link CachePolicy}. Serves valid
   * {@code NullValue} sentinels as empty hits without reloading. The policy
   * carries the reader, TTLs, null-caching, stale-policy, and report-allow
   * flag — all per-invocation parameters consolidated in a single object.
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation cache policy (carries reader,
   *                 TTLs, null-caching, stale-policy, fail-fast (failOnError),
   *                 and report-allow flag)
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded
   *         value
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, CachePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("getWithSoftExpire");
    requireAppDetector("getWithSoftExpire");
    return hotKeyCache.getWithSoftExpire(cacheKey, policy);
  }

  /**
   * Batch no-reader get: each key resolves against the {@link ZetaLoaderRegistry}
   * (ADR-0070) and loads through its registered
   * {@link io.github.hyshmily.zeta.cache.loader.CacheLoader} on a miss — the
   * batch
   * counterpart of {@link #get(String)} (the Caffeine {@code getAll} style).
   *
   * <p>
   * Keys matching different prefixes may carry different specs, so keys are
   * grouped by winning spec and each group is executed with its own spec policy
   * (loader + hard/soft TTL overrides + report flag + failOnError); results are
   * merged into one map.
   *
   * <p>
   * Precedence matches the single-key overloads: the reader-based batch
   * overloads never consult the registry. Any key with no registered prefix
   * fails the whole batch fast with an actionable {@link IllegalStateException}
   * — the batch never partially resolves.
   *
   * @param cacheKeys the keys to retrieve
   * @param <T>       the value type (erased — each loader's own type governs)
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws ZetaModeException     when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException  when any key matches a blacklist rule
   * @throws IllegalStateException when no registry is wired or any key matches no
   *                               registered prefix
   */
  @SuppressWarnings("unchecked")
  public <T> Map<String, Optional<T>> getAll(Iterable<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<ZetaLoadingSpec<?>, List<String>> groups = groupKeysBySpec(cacheKeys, "getAll");
    Map<String, Optional<T>> merged = new LinkedHashMap<>();
    for (Map.Entry<ZetaLoadingSpec<?>, List<String>> group : groups.entrySet()) {
      ZetaLoadingSpec<?> spec = group.getKey();
      List<String> keys = group.getValue();
      Function<String, T> groupReader = key -> (T) spec.loader().load(key);
      merged.putAll(
          hotKeyCache.get(keys, groupReader, spec.hardTtlMs(), spec.softTtlMs(), spec.reportEnabled(),
              spec.failOnError()));
    }
    return merged;
  }

  /**
   * Batch no-reader get with soft-expire: the batch counterpart of
   * {@link #getWithSoftExpire(String)} — every group is executed with
   * {@link StalePolicy#SOFT_REFRESH} forced, exactly like the single-key
   * overload; grouping, routing, and fail-fast semantics match
   * {@link #getAll(Iterable)}.
   *
   * @param cacheKeys the keys to retrieve
   * @param <T>       the value type (erased — each loader's own type governs)
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws ZetaModeException     when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException  when any key matches a blacklist rule
   * @throws IllegalStateException when no registry is wired or any key matches no
   *                               registered prefix
   */
  @SuppressWarnings("unchecked")
  public <T> Map<String, Optional<T>> getAllWithSoftExpire(Iterable<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<ZetaLoadingSpec<?>, List<String>> groups = groupKeysBySpec(cacheKeys, "getAllWithSoftExpire");
    Map<String, Optional<T>> merged = new LinkedHashMap<>();
    for (Map.Entry<ZetaLoadingSpec<?>, List<String>> group : groups.entrySet()) {
      ZetaLoadingSpec<?> spec = group.getKey();
      List<String> keys = group.getValue();
      Function<String, T> groupReader = key -> (T) spec.loader().load(key);
      merged.putAll(
          hotKeyCache.getWithSoftExpire(
              keys,
              groupReader,
              spec.hardTtlMs(),
              spec.softTtlMs(),
              spec.reportEnabled(),
              spec.failOnError()));
    }
    return merged;
  }

  /**
   * Batch get: load each missing key via the reader with all-default
   * configuration (no TTL override, Worker reporting enabled, reader failures
   * swallowed as per-key misses).
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param <T>       the value type
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> getAll(Iterable<String> cacheKeys, Function<? super String, ? extends T> reader) {
    return getAll(cacheKeys, reader, CachePolicy.defaults());
  }

  /**
   * Batch get with an explicit {@link CachePolicy} — the full-control form.
   * All keys share the same policy.
   *
   * <p>
   * <b>Honored components:</b> {@code hardTtlMs}, {@code softTtlMs}
   * (evaluated once for the whole batch; the batch pipeline has no per-key
   * lazy-TTL path), {@code reportEnabled}, and {@code failOnError} (when
   * {@code true}, the first reader failure aborts the whole batch and
   * propagates to the caller instead of being swallowed as per-key misses).
   * <b>Ignored components:</b> {@code reader} (the explicit {@code reader}
   * argument governs), {@code nullCaching} (the batch pipeline always caches
   * {@code null} results as short-TTL sentinels), {@code stalePolicy}, and
   * {@code skipBroadcast} (reads never broadcast).
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param policy    the shared per-invocation cache policy
   * @param <T>       the value type
   * @return a map of key → loaded or cached value (never {@code null})
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   * @throws RuntimeException     the first reader failure cause when
   *                              {@code policy.failOnError()} is {@code true}
   */
  public <T> Map<String, Optional<T>> getAll(
      Iterable<String> cacheKeys,
      Function<? super String, ? extends T> reader,
      CachePolicy policy) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("getAll");
    requireAppDetector("getAll");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    return hotKeyCache.get(cacheKeys, reader, hardTtlMs, softTtlMs, policy.reportEnabled(), policy.failOnError());
  }

  /**
   * Batch get with soft-expire: load each missing key via the reader with
   * all-default configuration (no TTL override, Worker reporting enabled,
   * reader failures swallowed as per-key misses).
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses / refreshes
   * @param <T>       the value type
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, Optional<T>> getAllWithSoftExpire(
      Iterable<String> cacheKeys,
      Function<? super String, ? extends T> reader) {
    return getAllWithSoftExpire(cacheKeys, reader, CachePolicy.defaults());
  }

  /**
   * Batch get with soft-expire and an explicit {@link CachePolicy} — the
   * full-control form. Honored and ignored components match
   * {@link #getAll(Iterable, Function, CachePolicy)}.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses / refreshes
   * @param policy    the shared per-invocation cache policy
   * @param <T>       the value type
   * @return a map of key → cached (possibly stale) or loaded value
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   * @throws RuntimeException     the first reader failure cause when
   *                              {@code policy.failOnError()} is {@code true}
   */
  public <T> Map<String, Optional<T>> getAllWithSoftExpire(
      Iterable<String> cacheKeys,
      Function<? super String, ? extends T> reader,
      CachePolicy policy) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("getAllWithSoftExpire");
    requireAppDetector("getAllWithSoftExpire");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    return hotKeyCache.getWithSoftExpire(
        cacheKeys,
        reader,
        hardTtlMs,
        softTtlMs,
        policy.reportEnabled(),
        policy.failOnError());
  }

  /**
   * Routes each key to its winning {@link ZetaLoadingSpec} and groups the keys
   * by spec (insertion-ordered) for the batch no-reader operations. Fails fast —
   * with an actionable message — when no registry is wired or any key matches
   * no registered prefix: the batch never partially resolves.
   *
   * @param cacheKeys the keys to route; never {@code null}
   * @param operation the calling operation name (used in error messages)
   * @return keys grouped by winning spec
   */
  private Map<ZetaLoadingSpec<?>, List<String>> groupKeysBySpec(Iterable<String> cacheKeys, String operation) {
    requireAppCache(operation);
    requireAppDetector(operation);
    if (loaderRegistry == null) {
      throw new IllegalStateException(
          "No ZetaLoaderRegistry is wired — register one (ZetaLoaderRegistry bean) or use " +
              operation +
              "(keys, reader) for these keys");
    }
    Map<ZetaLoadingSpec<?>, List<String>> groups = new LinkedHashMap<>();
    for (String key : cacheKeys) {
      ZetaLoadingSpec<?> spec = loaderRegistry.match(key);
      if (spec == null) {
        throw new IllegalStateException(
            "No CacheLoader registered for key '" +
                key +
                "' — register a matching prefix via ZetaLoaderRegistry.register(prefix, spec), or use " +
                operation +
                "(keys, reader)");
      }
      groups.computeIfAbsent(spec, s -> new ArrayList<>()).add(key);
    }
    return groups;
  }

  /**
   * Resolves the winning {@link ZetaLoadingSpec} for a no-reader call, failing
   * fast with an actionable message when no registry is wired or no prefix
   * matches.
   */
  private ZetaLoadingSpec<?> requireRegisteredSpec(String cacheKey, String operation) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache(operation);
    requireAppDetector(operation);
    if (loaderRegistry == null) {
      throw new IllegalStateException(
          "No ZetaLoaderRegistry is wired — register one (ZetaLoaderRegistry bean) or use " +
              operation +
              "(key, reader) for this key: " +
              cacheKey);
    }
    ZetaLoadingSpec<?> spec = loaderRegistry.match(cacheKey);
    if (spec == null) {
      throw new IllegalStateException(
          "No CacheLoader registered for key '" +
              cacheKey +
              "' — register a matching prefix via ZetaLoaderRegistry.register(prefix, spec), or use " +
              operation +
              "(key, reader)");
    }
    return spec;
  }

  /**
   * No-reader compute-if-absent: resolves the key against the
   * {@link ZetaLoaderRegistry} (ADR-0070) and loads through the registered
   * loader on a miss, with the spec's TTL/semantics — the V-returning
   * counterpart of {@link #get(String)} ({@code null} when the loader returned
   * {@code null}).
   *
   * <p>
   * Precedence matches the single-key overloads: the loader-based
   * {@code computeIfAbsent(key, loader)} and
   * {@code computeIfAbsent(key, CachePolicy)}
   * overloads never consult the registry.
   *
   * @param cacheKey the key to retrieve
   * @param <V>      the value type (erased — the loader's own type governs)
   * @return the cached or loaded value, or {@code null} if the loader returned
   *         {@code null}
   * @throws ZetaModeException     when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException  when the key matches a blacklist rule
   * @throws IllegalStateException when no registry is wired or no registered
   *                               prefix matches
   */
  public <V> V computeIfAbsent(String cacheKey) {
    ZetaLoadingSpec<?> spec = requireRegisteredSpec(cacheKey, "computeIfAbsent");
    return (V) hotKeyCache.computeIfAbsent(cacheKey, spec.toPolicy(cacheKey)).orElse(null);
  }

  /**
   * Per-key compute-if-absent. The hit side (state checks, promotion,
   * stale-policy handling) runs atomically inside one Caffeine
   * {@code asMap().compute()}; on a miss the loader runs <i>outside</i> the
   * bin lock via SingleFlight — deduped, timeout-bounded, and
   * circuit-breaker protected (ADR-0030) — and the loaded value is stored by
   * a short second compute. A plain cache hit never invokes the loader.
   *
   * <p>
   * <b>Registry precedence:</b> this overload never consults the
   * {@link ZetaLoaderRegistry} — a spec registered for the key's prefix (its
   * TTL overrides, null-caching, {@code failOnError}, reporting) is <i>not</i>
   * applied; global defaults govern instead. To inherit a spec's policy while
   * swapping only the data source, derive it explicitly —
   * {@code spec.withLoader(custom).toPolicy(key)} handed to
   * {@link #computeIfAbsent(String, CachePolicy)}.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value supplier for cache misses
   * @param <V>      the value type
   * @return the cached or loaded value, or {@code null} if the loader returned
   *         {@code null}
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsent(String cacheKey, Supplier<V> loader) {
    Objects.requireNonNull(loader, "loader must not be null");
    return (V) hotKeyCache
        .computeIfAbsent(cacheKey, CachePolicy.of(loader, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
        .orElse(null);
  }

  /**
   * Compute-if-absent with an explicit {@link CachePolicy} — the
   * full-control form. The policy carries the reader (inject it via
   * {@link CachePolicy#withReader(Supplier)}), TTLs, null-caching decision,
   * stale policy, and report-allow flag — all parameters in a single named
   * object. The policy's TTL suppliers are evaluated lazily — at most once
   * per call, and only when an entry is created, promoted, renewed, or a
   * background refresh is scheduled — so SpEL-based TTL expressions cost
   * nothing on a plain cache hit. When {@link CachePolicy#nullCaching()} is
   * {@code false}, a {@code null} loader result leaves no cache entry, so the
   * next call re-invokes the loader.
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation cache policy (carries reader,
   *                 TTLs, null-caching, stale-policy, fail-fast (failOnError),
   *                 and report-allow flag)
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> computeIfAbsent(String cacheKey, CachePolicy policy) {
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("computeIfAbsent");
    requireAppDetector("computeIfAbsent");
    return hotKeyCache.computeIfAbsent(cacheKey, policy);
  }

  /**
   * No-reader compute-if-absent with soft-expire: the V-returning counterpart
   * of {@link #getWithSoftExpire(String)} — {@link StalePolicy#SOFT_REFRESH}
   * forced, TTL/semantics from the winning spec.
   *
   * <p>
   * Precedence matches the single-key overloads: the loader-based
   * {@code computeIfAbsentWithSoftExpire(key, loader)} and
   * {@code computeIfAbsentWithSoftExpire(key, CachePolicy)} overloads never
   * consult the registry.
   *
   * @param cacheKey the key to retrieve
   * @param <V>      the value type (erased — the loader's own type governs)
   * @return the cached (possibly stale) or loaded value, or {@code null} if the
   *         loader returned {@code null}
   * @throws ZetaModeException     when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException  when the key matches a blacklist rule
   * @throws IllegalStateException when no registry is wired or no registered
   *                               prefix matches
   */
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey) {
    ZetaLoadingSpec<?> spec = requireRegisteredSpec(cacheKey, "computeIfAbsentWithSoftExpire");
    return (V) hotKeyCache
        .computeIfAbsentWithSoftExpire(cacheKey, spec.toPolicy(cacheKey, StalePolicy.SOFT_REFRESH))
        .orElse(null);
  }

  /**
   * Per-key compute-if-absent with soft-expire, using all-default
   * configuration (no TTL override, null caching enabled,
   * {@link StalePolicy#SOFT_REFRESH}, reporting enabled).
   *
   * <p>
   * <b>Registry precedence:</b> this overload never consults the
   * {@link ZetaLoaderRegistry} — a spec registered for the key's prefix is
   * <i>not</i> applied; global defaults govern and the stale policy is forced
   * to {@link StalePolicy#SOFT_REFRESH}. To inherit a spec's policy while
   * swapping only the data source, derive it explicitly —
   * {@code spec.withLoader(custom).toPolicy(key, StalePolicy.SOFT_REFRESH)}
   * handed to {@link #computeIfAbsentWithSoftExpire(String, CachePolicy)}.
   *
   * <p>
   * Behaviorally identical to {@link #computeIfAbsent(String, Supplier)}:
   * both build a {@link StalePolicy#SOFT_REFRESH} policy with default knobs,
   * and the soft-expire storage path delegates to the plain compute path.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value supplier for cache misses / refreshes
   * @param <V>      the value type
   * @return the cached (possibly stale) or loaded value, or {@code null} if the
   *         loader returned {@code null}
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey, Supplier<V> loader) {
    Objects.requireNonNull(loader, "loader must not be null");
    return (V) hotKeyCache
        .computeIfAbsentWithSoftExpire(cacheKey, CachePolicy.of(loader, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
        .orElse(null);
  }

  /**
   * Per-key compute-if-absent with soft-expire and an explicit
   * {@link CachePolicy} — the full-control form. With a globally disabled
   * soft TTL (entries carrying {@code softExpireAtMs = 0} never read as
   * stale) this call behaves exactly like plain {@link #computeIfAbsent} —
   * no background refresh is ever armed.
   *
   * <p>
   * The policy's TTL suppliers are evaluated lazily — at most once per
   * call, and only when an entry is created, promoted, renewed, or a
   * background refresh is scheduled — so SpEL-based TTL expressions cost
   * nothing on a plain cache hit. When {@link CachePolicy#nullCaching()} is
   * {@code false}, a {@code null} loader result leaves no cache entry, so the
   * next call re-invokes the loader. The policy also carries the reader and
   * the report-allow flag, consolidating all per-invocation parameters.
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation cache policy (carries reader,
   *                 TTLs, null-caching, stale-policy, fail-fast (failOnError),
   *                 and report-allow flag)
   * @param <T>      the value type
   * @return the cached (possibly stale) or loaded value as an {@link Optional}
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> computeIfAbsentWithSoftExpire(String cacheKey, CachePolicy policy) {
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("computeIfAbsentWithSoftExpire");
    requireAppDetector("computeIfAbsentWithSoftExpire");
    return hotKeyCache.computeIfAbsentWithSoftExpire(cacheKey, policy);
  }

  /**
   * Look up a cached value without loading or triggering hot-key detection.
   *
   * @param cacheKey the key to look up
   * @param <T>      the value type
   * @return an {@link Optional} containing the raw value if present
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public <T> Optional<T> peek(@NotNull @NotBlank String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("peek");
    return hotKeyCache.peek(cacheKey);
  }

  /**
   * Batch variant of {@link #peek(String)}. Returns a map of key → value for
   * all keys that are present in L1. Missing keys are silently omitted.
   *
   * @param cacheKeys the keys to look up
   * @return a map of present key-value pairs (never {@code null})
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public Map<String, Object> peekAll(Collection<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<String, Object> result = new HashMap<>();
    cacheKeys.forEach(key -> {
      Optional<Object> value = peek(key);
      value.ifPresent(v -> result.put(key, v));
    });

    return result;
  }

  /**
   * Annotation-path peek for the Spring Cache adapter: look up a cached value
   * <b>and</b> feed hot-key detection and Worker reporting when a hit is
   * served — preserving the core invariant that every read triggers both,
   * while paying a single rule evaluation and a single hash lookup per call
   * (the plain {@link #peek} + {@link #evaluateRule} + {@link #tag} sequence
   * evaluates the rules and probes the L1 twice). The rule handling mirrors
   * the facade read path: a whitelist match detects without reporting, a
   * true miss does neither, and a blocked key throws.
   *
   * <p>
   * Intended for {@code ZetaSpringCache.lookup}; callers outside the
   * annotation integration should prefer {@link #peek} (side-effect-free) or
   * {@link #get} (loads on miss).
   *
   * @param cacheKey the key to look up
   * @return the unwrapped user value,
   *         {@link io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue#INSTANCE}
   *         for a cached-null sentinel hit (surface Spring's null marker so
   *         Spring skips the method), or {@code null} on a true miss;
   *         detection/reporting fire only on the two hit forms
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  @Internal
  public Object peekAndTag(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("peekAndTag");
    return hotKeyCache.peekAndTag(cacheKey);
  }

  /**
   * Atomically replace the cached value only if the current value equals
   * the expected value. See {@link HotKeyCache#compareAndSet} for details.
   *
   * @param cacheKey the key to replace
   * @param expected the expected current value (nullable)
   * @param newValue the new value to cache (nullable)
   * @return {@code true} if the replacement was performed
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public boolean compareAndSet(String cacheKey, @Nullable Object expected, @Nullable Object newValue) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("compareAndSet");
    return hotKeyCache.compareAndSet(cacheKey, expected, newValue);
  }

  /**
   * Atomically invalidate the cached value only if the current value equals
   * the expected value.
   * <p>
   * No version bump or broadcast is performed — the invalidation is purely
   * local. If cross-instance invalidation is needed, pair with a subsequent
   * call to {@link #invalidate(String, CachePolicy)}.
   *
   * @param cacheKey the key to invalidate
   * @param expected the expected value (nullable)
   * @return {@code true} if the invalidation was performed
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public boolean compareAndInvalidate(String cacheKey, @Nullable Object expected) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("compareAndInvalidate");
    return hotKeyCache.compareAndInvalidate(cacheKey, expected);
  }

  /**
   * Atomically set the cache value and return the previous value — the
   * logical equivalent of
   * {@link java.util.concurrent.atomic.AtomicReference#getAndSet}.
   *
   * <p>
   * Existing entry metadata ({@code dataVersion}, {@code decisionVersion},
   * TTLs) is preserved exactly as-is; only the value payload is replaced. No
   * version bump or broadcast occurs. When the key is absent, the new value
   * is inserted with the configured default TTLs.
   *
   * @param cacheKey the key to update
   * @param newValue the new value to cache (nullable)
   * @param <T>      the type of the previously cached value
   * @return the previous value, or empty if none existed
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  public <T> Optional<T> getAndSet(String cacheKey, Object newValue) {
    return getAndSet(cacheKey, newValue, CachePolicy.defaults());
  }

  /**
   * Atomically set the cache value and return the previous value, with an
   * explicit {@link CachePolicy} controlling the TTLs of a <b>newly created</b>
   * entry (only applies when the key was absent; an existing entry keeps its
   * metadata). See {@link #getAndSet(String, Object)} for the swap semantics.
   *
   * <p>
   * <b>Honored components:</b> {@code hardTtlMs}, {@code softTtlMs}
   * (evaluated once per call). <b>Ignored components:</b> {@code skipBroadcast}
   * (this path never broadcasts), {@code nullCaching}, {@code stalePolicy},
   * {@code reader}, {@code reportEnabled}, {@code failOnError}.
   *
   * <p>
   * Use with caution: unlike {@link #compareAndSet}, this method does not
   * perform any concurrency control (CAS). If multiple threads call this
   * method concurrently, the final value is the last writer's. For
   * conditional swaps, prefer {@link #compareAndSet}.
   *
   * @param cacheKey the key to update
   * @param newValue the new value to cache (nullable)
   * @param policy   the cache policy supplying the new entry's TTL overrides
   * @param <T>      the type of the previously cached value
   * @return the previous value, or empty if none existed
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  public <T> Optional<T> getAndSet(String cacheKey, Object newValue, CachePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    requireAppCache("getAndSet");
    return hotKeyCache.getAndSet(cacheKey, newValue, hardTtlMs, softTtlMs);
  }

  /**
   * Atomically insert the given value into the cache only if the key is not
   * already present — a fast, lock-free-at-key-level operation that never
   * triggers a data-source writer, version bump, or cross-instance broadcast.
   *
   * <p>
   * If the key already exists in L1 (including as a {@code NullValue}
   * sentinel), the method returns {@code false} and the existing entry is
   * left untouched. If the key is absent, a fresh cache entry in
   * {@code KeyState.NORMAL} is created with the configured default TTLs and
   * the method returns {@code true}.
   *
   * @param cacheKey the key to insert
   * @param value    the value to cache (must not be {@code null}; to cache a
   *                 {@code null} result, use the NullValue sentinel
   *                 explicitly or rely on the {@code @NullCaching} annotation)
   * @return {@code true} if the entry was inserted, {@code false} if the key
   *         was already present
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  public boolean putIfAbsent(String cacheKey, Object value) {
    return putIfAbsent(cacheKey, value, CachePolicy.defaults());
  }

  /**
   * Atomically insert the given value into the cache only if the key is not
   * already present, with an explicit {@link CachePolicy} controlling the new
   * entry's TTLs. See {@link #putIfAbsent(String, Object)} for the
   * insertion semantics.
   *
   * <p>
   * <b>Honored components:</b> {@code hardTtlMs}, {@code softTtlMs}
   * (evaluated once per call). <b>Ignored components:</b> {@code skipBroadcast}
   * (this path never broadcasts), {@code nullCaching}, {@code stalePolicy},
   * {@code reader}, {@code reportEnabled}, {@code failOnError}.
   *
   * @param cacheKey the key to insert
   * @param value    the value to cache (must not be {@code null})
   * @param policy   the cache policy supplying the new entry's TTL overrides
   * @return {@code true} if the entry was inserted, {@code false} if the key
   *         was already present
   * @throws ZetaBlockedException when the key matches a blocklist rule
   */
  public boolean putIfAbsent(String cacheKey, Object value, CachePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    requireAppCache("putIfAbsent");
    return hotKeyCache.putIfAbsent(cacheKey, value, hardTtlMs, softTtlMs);
  }

  /**
   * Write-through: execute the writer, then update L1 and broadcast.
   * Uses effective hard/soft TTL from configuration.
   *
   * @param cacheKey the key to write
   * @param value    the value to cache
   * @param writer   the data-source mutation to execute before caching
   * @param <T>      the value type
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer) {
    putThrough(cacheKey, value, writer, CachePolicy.defaults());
  }

  /**
   * Write-through with an explicit {@link CachePolicy} — the full-control
   * form: execute the writer, then update L1 and (unless
   * {@code policy.skipBroadcast()} is {@code true}) broadcast to peers.
   *
   * <p>
   * <b>Honored components:</b> {@code hardTtlMs}, {@code softTtlMs}
   * (evaluated once per call; 0 = use configured default;
   * {@link Long#MAX_VALUE} for a permanent entry — see the class-level note
   * on the version constraint for permanent entries), and
   * {@code skipBroadcast}. <b>Ignored components:</b> {@code nullCaching},
   * {@code stalePolicy}, {@code reader}, {@code reportEnabled},
   * {@code failOnError}.
   *
   * @param cacheKey the key to write
   * @param value    the value to cache
   * @param writer   the data-source mutation to execute before caching
   * @param policy   the cache policy supplying TTL overrides and broadcast
   *                 control
   * @param <T>      the value type
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public <T> void putThrough(String cacheKey, T value, Runnable writer, CachePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(value, "value must not be null");
    Objects.requireNonNull(writer, "writer must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    requireAppCache("putThrough");
    hotKeyCache.putThrough(cacheKey, value, writer, hardTtlMs, softTtlMs, !policy.skipBroadcast());
  }

  /**
   * Write a value directly into the local L1 cache without version bump,
   * without broadcast, without hot-key detection, and without reporting.
   * <p>
   * Existing entry metadata is preserved. If no entry exists, a fresh
   * {@link io.github.hyshmily.zeta.model.CacheEntry} is created with
   * {@link io.github.hyshmily.zeta.model.KeyState#NORMAL}.
   * <p>
   * Never throws {@code UnsupportedOperationException} in Worker-only mode;
   * silently no-ops instead.
   *
   * @param cacheKey the key to store
   * @param value    the value to cache
   */
  public void putLocal(String cacheKey, Object value) {
    putLocal(cacheKey, value, CachePolicy.defaults());
  }

  /**
   * Write a value directly into the local L1 cache with an explicit
   * {@link CachePolicy} controlling the entry's TTLs. Delegates to
   * {@link #putLocal(String, Object)} semantics — no version bump, no
   * broadcast, no hot-key detection, and no reporting.
   *
   * <p>
   * <b>Honored components:</b> {@code hardTtlMs}, {@code softTtlMs}
   * (evaluated once per call). <b>Ignored components:</b> everything else —
   * this path is strictly local.
   *
   * <p>
   * In Worker-only mode this method silently no-ops.
   *
   * @param cacheKey the key to store
   * @param value    the value to cache
   * @param policy   the cache policy supplying the entry's TTL overrides
   */
  public void putLocal(String cacheKey, Object value, CachePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(value, "value must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    if (hotKeyCache == null) {
      return; // Worker-only mode: silently no-op
    }
    hotKeyCache.putLocal(cacheKey, value, hardTtlMs, softTtlMs);
  }

  /**
   * Invalidate a single key from L1 and broadcast REFRESH to peers.
   * The next {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidate(String cacheKey) {
    invalidate(cacheKey, CachePolicy.defaults());
  }

  /**
   * Invalidate a single key from L1 with explicit broadcast control via
   * {@link CachePolicy}: when {@code policy.skipBroadcast()} is {@code true}
   * the invalidation stays local (no REFRESH message to peers).
   *
   * <p>
   * <b>Honored components:</b> {@code skipBroadcast}. <b>Ignored
   * components:</b> everything else — invalidation carries no TTL, reader,
   * or null-caching semantics.
   *
   * @param cacheKey the key to invalidate
   * @param policy   the cache policy supplying broadcast control
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidate(String cacheKey, CachePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("invalidate");
    hotKeyCache.invalidate(cacheKey, !policy.skipBroadcast());
  }

  /**
   * Invalidate a collection of keys from L1 and broadcast INVALIDATE for each.
   *
   * @param cacheKeys the keys to invalidate
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidate(Collection<String> cacheKeys) {
    invalidate(cacheKeys, CachePolicy.defaults());
  }

  /**
   * Invalidate a collection of keys from L1 with explicit broadcast control
   * via {@link CachePolicy} — see {@link #invalidate(String, CachePolicy)}.
   *
   * <p>
   * <b>Honored components:</b> {@code skipBroadcast}. <b>Ignored
   * components:</b> everything else.
   *
   * @param cacheKeys the keys to invalidate
   * @param policy    the cache policy supplying broadcast control
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidate(Collection<String> cacheKeys, CachePolicy policy) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("invalidate");
    hotKeyCache.invalidate(cacheKeys, !policy.skipBroadcast());
  }

  /**
   * Execute a mutation, then invalidate L1 and broadcast to peers.
   * If the mutation throws, invalidation is skipped.
   *
   * @param cacheKey the key to invalidate after mutation
   * @param mutation the mutation to execute before invalidation
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(String cacheKey, Runnable mutation) {
    invalidateAfterPut(cacheKey, mutation, CachePolicy.defaults());
  }

  /**
   * Execute a mutation, then invalidate L1 with explicit broadcast control
   * via {@link CachePolicy}. If the mutation throws, invalidation is skipped.
   *
   * <p>
   * <b>Honored components:</b> {@code skipBroadcast}. <b>Ignored
   * components:</b> everything else.
   *
   * @param cacheKey the key to invalidate after mutation
   * @param mutation the mutation to execute before invalidation
   * @param policy   the cache policy supplying broadcast control
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(String cacheKey, Runnable mutation, CachePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(mutation, "mutation must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("invalidateAfterPut");
    requireAppDetector("invalidateAfterPut");
    hotKeyCache.invalidateAfterPut(cacheKey, mutation, !policy.skipBroadcast());
  }

  /**
   * Execute mutations for multiple keys, then invalidate each from L1 and
   * broadcast. Each key gets its own mutation. If a single mutation fails,
   * that key is skipped (invalidation and broadcast are not performed).
   *
   * @param mutations a map of key → mutation pairs to execute before invalidation
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(Map<String, ? extends Runnable> mutations) {
    invalidateAfterPut(mutations, CachePolicy.defaults());
  }

  /**
   * Execute mutations for multiple keys, then invalidate each from L1 with
   * explicit broadcast control via {@link CachePolicy} — see
   * {@link #invalidateAfterPut(String, Runnable, CachePolicy)}.
   *
   * <p>
   * <b>Honored components:</b> {@code skipBroadcast}. <b>Ignored
   * components:</b> everything else.
   *
   * @param mutations a map of key → mutation pairs
   * @param policy    the cache policy supplying broadcast control
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(Map<String, ? extends Runnable> mutations, CachePolicy policy) {
    Objects.requireNonNull(mutations, "mutations must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("invalidateAfterPut");
    requireAppDetector("invalidateAfterPut");
    hotKeyCache.invalidateAfterPut(mutations, !policy.skipBroadcast());
  }

  /**
   * Invalidate all entries from the L1 cache without broadcasting.
   * <p>
   * This is an emergency flush — all cached values are removed immediately.
   * No cross-instance sync messages are sent. Subsequent {@link #get} calls
   * will reload data from the reader.
   * <p>
   * Use with caution: flushing the local cache increases load on the backend
   * until entries are re-cached.
   *
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidateAllLocal() {
    requireAppCache("invalidateAllLocal");
    hotKeyCache.invalidateAllLocal();
  }

  /**
   * Estimated number of entries currently in the L1 cache.
   * <p>
   * This is a lightweight, best-effort estimate from the underlying Caffeine
   * cache, suitable for monitoring dashboards and capacity planning.
   *
   * @return estimated entry count, or {@code 0} in Worker-only mode
   */
  public long estimatedSize() {
    if (hotKeyCache == null) {
      return 0L;
    }
    return hotKeyCache.estimatedSize();
  }

  /**
   * Return a snapshot of basic L1 cache statistics.
   * <p>
   * Hit/miss/eviction counters are populated because the default L1 cache
   * enables Caffeine's {@code recordStats()}; they are {@code 0} only when a
   * custom {@code Cache} bean is supplied without stats recording.
   * {@code estimatedSize} is always available.
   *
   * @return a {@link ZetaCacheStats} record, or {@code null} in Worker-only mode;
   *         hit/miss counters are {@code 0} if stats recording is not enabled
   */
  public ZetaCacheStats stats() {
    if (hotKeyCache == null) {
      return null;
    }
    return hotKeyCache.stats();
  }

  /**
   * Return the underlying Caffeine cache for direct access.
   *
   * <p>
   * Useful for Caffeine-specific operations such as {@code asMap()},
   * {@code policy()}, and {@code cleanUp()}. Use with caution — bypassing
   * the HotKey orchestration layer can lead to inconsistent cache state.
   *
   * @return the raw Caffeine {@link Cache} instance
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public Cache<String, Object> getLocalCache() {
    requireAppCache("getLocalCache");
    return hotKeyCache.getLocalCache();
  }

  /**
   * Attempt to acquire a distributed lock with the provider's default
   * retry counts.
   *
   * <p>
   * Equivalent to {@code tryLock(key, expire, unit, -1, -1, -1)}.
   *
   * <p>
   * Usage:
   * 
   * <pre>{@code
   * try (AutoReleaseLock lock = hotKey.tryLock("order:42", 5, TimeUnit.SECONDS)) {
   *   if (lock != null) {
   *     // critical section
   *   }
   * }
   * }</pre>
   *
   * @param key    the lock key
   * @param expire the time-to-live for the lock
   * @param unit   the time unit for {@code expire}
   * @return a lock handle if acquired, or {@code null} if the lock is held
   *         by another caller or the provider is unavailable
   */
  @Nullable
  public AutoReleaseLock tryLock(String key, long expire, TimeUnit unit) {
    Assert.hasText(key, "key must not be empty");
    Assert.isTrue(expire >= 0, "expire must not be negative");
    Objects.requireNonNull(unit, "unit must not be null");
    return lockProvider != null ? lockProvider.tryLock(key, expire, unit) : null;
  }

  /**
   * Attempt to acquire a distributed lock with explicit retry counts — the
   * full-control form of {@link #tryLock(String, long, TimeUnit)}.
   *
   * <p>
   * Any negative count falls back to the provider's configured default.
   * Returns {@code null} when the lock is held by another caller or when
   * no Redis is available (graceful degradation).
   *
   * @param key                 the lock key
   * @param expire              the time-to-live for the lock
   * @param unit                the time unit for {@code expire}
   * @param tryLockLockCount    the number of {@code SET NX} retries;
   *                            negative → default
   * @param tryLockInquiryCount the number of {@code GET} inquiries;
   *                            negative → default
   * @param tryLockUnlockCount  the number of {@code DEL} retries;
   *                            negative → default
   * @return a lock handle if acquired, or {@code null} if the lock is held
   *         by another caller or the provider is unavailable
   */
  @Nullable
  public AutoReleaseLock tryLock(
      String key,
      long expire,
      TimeUnit unit,
      int tryLockLockCount,
      int tryLockInquiryCount,
      int tryLockUnlockCount) {
    Assert.hasText(key, "key must not be empty");
    Assert.isTrue(expire >= 0, "expire must not be negative");
    Objects.requireNonNull(unit, "unit must not be null");
    return lockProvider != null
        ? lockProvider.tryLock(key, expire, unit, tryLockLockCount, tryLockInquiryCount, tryLockUnlockCount)
        : null;
  }

  /**
   * Acquire a distributed lock, execute the action, and release
   * with the provider's default retry counts.
   *
   * <p>
   * Equivalent to {@code tryLockAndRun(key, expire, unit, action, -1, -1, -1)}.
   *
   * @param key    the lock key
   * @param expire the time-to-live for the lock
   * @param unit   the time unit for {@code expire}
   * @param action the action to execute while holding the lock
   * @return {@code true} if the lock was acquired and the action ran,
   *         {@code false} otherwise
   */
  public boolean tryLockAndRun(String key, long expire, TimeUnit unit, Runnable action) {
    Assert.hasText(key, "key must not be empty");
    Assert.isTrue(expire >= 0, "expire must not be negative");
    Objects.requireNonNull(unit, "unit must not be null");
    Objects.requireNonNull(action, "action must not be null");
    if (lockProvider == null) {
      return false;
    }
    try (AutoReleaseLock lock = tryLock(key, expire, unit)) {
      if (lock != null) {
        action.run();
        return true;
      }
      return false;
    }
  }

  /**
   * Acquire a distributed lock with explicit retry counts, execute the
   * action, and release — the full-control form of
   * {@link #tryLockAndRun(String, long, TimeUnit, Runnable)}.
   *
   * <p>
   * Any negative count falls back to the provider's configured default.
   *
   * @param key                 the lock key
   * @param expire              the time-to-live for the lock
   * @param unit                the time unit for {@code expire}
   * @param action              the action to execute while holding the lock
   * @param tryLockLockCount    the number of {@code SET NX} retries;
   *                            negative → default
   * @param tryLockInquiryCount the number of {@code GET} inquiries;
   *                            negative → default
   * @param tryLockUnlockCount  the number of {@code DEL} retries;
   *                            negative → default
   * @return {@code true} if the lock was acquired and the action ran,
   *         {@code false} otherwise
   */
  public boolean tryLockAndRun(
      String key,
      long expire,
      TimeUnit unit,
      Runnable action,
      int tryLockLockCount,
      int tryLockInquiryCount,
      int tryLockUnlockCount) {
    Assert.hasText(key, "key must not be empty");
    Assert.isTrue(expire >= 0, "expire must not be negative");
    Objects.requireNonNull(unit, "unit must not be null");
    Objects.requireNonNull(action, "action must not be null");
    try (AutoReleaseLock lock = tryLock(key, expire, unit, tryLockLockCount, tryLockInquiryCount, tryLockUnlockCount)) {
      if (lock != null) {
        action.run();
        return true;
      }
      return false;
    }
  }

  /**
   * Cadence factor applied to the resolved soft TTL when computing the timed
   * refresh interval. With the default ±5% TTL jitter, a plain soft-TTL
   * interval would fire before the entry is stale roughly half the time and
   * the refresh would be skipped; the ×1.1 factor guarantees the entry is
   * stale at every tick, fulfilling the {@link #registerRefresh} contract.
   */
  private static final double REFRESH_INTERVAL_SOFT_TTL_FACTOR = 1.1;

  /**
   * Compute the timed-refresh interval from the resolved soft TTL:
   * {@code resolvedSoftTtlMs × 1.1}, clamped to at least 1 ms (see
   * {@link #registerRefresh} and {@link #REFRESH_INTERVAL_SOFT_TTL_FACTOR}).
   *
   * @param resolvedSoftTtlMs the effective soft TTL in milliseconds
   * @return the refresh interval in milliseconds
   */
  static long refreshIntervalMs(long resolvedSoftTtlMs) {
    // Integer-exact ceil(softTtl * 1.1) = softTtl + ceil(softTtl / 10): floating
    // point is avoided on purpose — 100 * 1.1 evaluates to 110.00000000000001
    // and a naive Math.ceil bumps the documented interval by a full ms, while
    // Math.round would violate the >= 1.1x staleness guarantee for tiny TTLs.
    return Math.max(1, resolvedSoftTtlMs + (resolvedSoftTtlMs + 9) / 10);
  }

  /**
   * Resolve the hard/soft TTL overrides from a {@link CachePolicy}, evaluating
   * each lazy supplier exactly once, and assert both are non-negative.
   * Centralizes the boundary decomposition shared by every policy-form
   * write/refresh entry point; the assertion messages are intentional contract
   * text and must not be altered.
   *
   * @param policy the cache policy supplying the TTL overrides
   * @return the resolved hard and soft TTL in milliseconds
   */
  private static ResolvedTtls resolveTtlsOrAssert(CachePolicy policy) {
    long hardTtlMs = policy.hardTtlMs().getAsLong();
    long softTtlMs = policy.softTtlMs().getAsLong();
    Assert.isTrue(hardTtlMs >= 0, "hardTtlMs must not be negative");
    Assert.isTrue(softTtlMs >= 0, "softTtlMs must not be negative");
    return new ResolvedTtls(hardTtlMs, softTtlMs);
  }

  /** Resolved hard/soft TTL pair extracted from a {@link CachePolicy}. */
  private record ResolvedTtls(long hardTtlMs, long softTtlMs) {
  }

  /**
   * Lazy-initialize the refresh scheduler.
   * Uses double-checked locking for thread safety.
   */
  private ScheduledExecutorService getScheduler() {
    ScheduledExecutorService s = refreshScheduler;
    if (s == null) {
      synchronized (this) {
        s = refreshScheduler;
        if (s == null) {
          s = new SafeScheduledExecutorService(2, new ZetaThreadFactory("zeta-refresh"));
          refreshScheduler = s;
        }
      }
    }
    return s;
  }

  /**
   * Register a timed background refresh for the given key with the configured
   * default TTLs. See {@link #registerRefresh(String, Supplier, CachePolicy)}
   * for the scheduling contract.
   *
   * @param key      the cache key to refresh
   * @param supplier the value supplier for refresh
   * @param <T>      the value type
   */
  public <T> void registerRefresh(String key, Supplier<T> supplier) {
    registerRefresh(key, supplier, CachePolicy.defaults());
  }

  /**
   * Register a timed background refresh for the given key with an explicit
   * {@link CachePolicy} supplying the TTL overrides.
   *
   * <p>
   * Schedules {@link #getWithSoftExpire} at an interval slightly longer than
   * the soft TTL ({@code softTtlMs × 1.1}, see
   * {@link #REFRESH_INTERVAL_SOFT_TTL_FACTOR}) to guarantee the entry is stale
   * when the timer fires — even under the ±5% TTL jitter — so every cycle
   * triggers an async refresh via {@code triggerBackgroundRefresh}.
   *
   * <p>
   * <b>Honored components:</b> {@code hardTtlMs} and {@code softTtlMs}
   * (resolved <i>once at registration time</i> — the tick reuses the frozen
   * values, so dynamic suppliers are not re-evaluated per firing),
   * {@code nullCaching}, {@code reportEnabled}, and {@code failOnError}.
   * The tick always runs with {@link StalePolicy#SOFT_REFRESH} regardless of
   * {@code policy.stalePolicy()}. <b>The soft TTL also serves as the base
   * interval</b>; 0 = use the configured default.
   *
   * <p>
   * The scheduler is created lazily on first registration (no threads before
   * first use). Uses a 2-thread pool to prevent a slow supplier from blocking
   * other refresh keys. If a previous registration exists for the same key,
   * it is cancelled and replaced.
   *
   * @param key      the cache key to refresh
   * @param supplier the value supplier for refresh
   * @param policy   the cache policy supplying TTL overrides and tick semantics
   * @param <T>      the value type
   */
  public <T> void registerRefresh(String key, Supplier<T> supplier, CachePolicy policy) {
    Assert.hasText(key, "key must not be empty");
    Objects.requireNonNull(supplier, "supplier must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    requireAppCache("registerRefresh");
    long intervalMs = refreshIntervalMs(hotKeyCache.resolveEffectiveSoftTtl(softTtlMs));
    // Resolve the scheduler and freeze the per-tick policy outside the map's
    // bin lock; the tick no longer rebuilds the CachePolicy on every firing.
    ScheduledExecutorService scheduler = getScheduler();
    var ref = new Object() {
      CachePolicy tick = CachePolicy.of(
          supplier,
          hardTtlMs,
          softTtlMs,
          policy.nullCaching(),
          policy.reportEnabled(),
          StalePolicy.SOFT_REFRESH);
    };
    if (policy.failOnError()) {
      ref.tick = ref.tick.withFailOnError();
    }
    // Atomic replace-and-cancel: separate put + cancel lets two concurrent
    // registrations cancel each other's replacement (T1 cancels F2, T2 cancels
    // F1) and leave a dead future registered for the key. Under compute, each
    // caller cancels only the future it actually displaced, so the last
    // writer's future always survives.
    refreshFutures.compute(key, (k, prev) -> {
      if (prev != null) {
        prev.cancel(false);
      }
      return scheduler.scheduleWithFixedDelay(
          () -> getWithSoftExpire(key, ref.tick),
          intervalMs,
          intervalMs,
          TimeUnit.MILLISECONDS);
    });
  }

  /**
   * Cancel the timed refresh for the given key.
   *
   * @param key the cache key to stop refreshing
   */
  public void unregisterRefresh(String key) {
    Assert.hasText(key, "key must not be empty");
    ScheduledFuture<?> f = refreshFutures.remove(key);
    if (f != null) {
      f.cancel(false);
    }
  }

  /**
   * Cancel all timed refreshes and shut down the refresh scheduler.
   * Called automatically by Spring when this bean is destroyed.
   */
  @Override
  public void destroy() {
    refreshFutures.values().forEach(f -> f.cancel(false));
    refreshFutures.clear();
    ScheduledExecutorService s = refreshScheduler;
    if (s != null) {
      s.shutdown();
    }
  }

  /**
   * Refresh the given key: load via the supplier, evict the local entry, then
   * cache the loaded value through write-through (version bump + broadcast).
   * Uses default TTLs. No broadcast is sent for the eviction itself.
   *
   * <p>
   * <b>Ordering note:</b> the load runs <i>before</i> the eviction so the
   * local L1 miss window stays as short as possible. The refresh is not
   * atomic against concurrent writers: a {@link #putThrough} landing between
   * the load and the final cache update is overwritten by the (possibly
   * older) loaded value, stamped with the newest version — last-writer-wins,
   * tolerated per the same convergence contract as ADR-0013. Callers that
   * need read-modify-write atomicity should use {@link #compareAndSet}
   * instead.
   *
   * @param cacheKey the key to refresh
   * @param loader   the value supplier
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void refresh(String cacheKey, Supplier<?> loader) {
    refresh(cacheKey, loader, CachePolicy.defaults());
  }

  /**
   * Refresh the given key with an explicit {@link CachePolicy}: load via the
   * supplier, evict the local entry, then cache the loaded value through
   * write-through (version bump + broadcast). No broadcast is sent for the
   * eviction itself.
   *
   * <p>
   * <b>Honored components:</b> {@code hardTtlMs}, {@code softTtlMs}
   * (evaluated once per call), and {@code skipBroadcast} (suppresses the
   * peer broadcast of the write-through). <b>Ignored components:</b>
   * {@code reader}, {@code nullCaching}, {@code stalePolicy},
   * {@code reportEnabled}, {@code failOnError}.
   *
   * <p>
   * <b>Ordering note:</b> the load runs <i>before</i> the eviction so the
   * local L1 miss window stays as short as possible; the refresh is not
   * atomic against concurrent writers (see {@link #refresh(String, Supplier)}).
   *
   * @param cacheKey the key to refresh
   * @param loader   the value supplier
   * @param policy   the cache policy supplying TTL overrides and broadcast
   *                 control
   * @param <V>      the value type
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public <V> void refresh(String cacheKey, Supplier<V> loader, CachePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(loader, "loader must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    requireAppCache("refresh");
    V value = loader.get();
    hotKeyCache.invalidate(cacheKey, false);
    // A null loader result must not NPE inside putThrough after the entry was
    // already evicted — the eviction alone is the refresh contract for a
    // missing value (the next read reloads through the application reader).
    if (value != null) {
      hotKeyCache.putThrough(cacheKey, value, () -> {
      }, hardTtlMs, softTtlMs, !policy.skipBroadcast());
    }
  }

  /**
   * Batch variant of {@link #refresh(String, Supplier)}. Refreshes all entries
   * by evicting locally then loading and caching via the provided suppliers.
   *
   * @param loaders a map of key → value supplier
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void refreshAll(Map<String, Supplier<?>> loaders) {
    Objects.requireNonNull(loaders, "loaders must not be null");
    requireAppCache("refreshAll");
    loaders.forEach(this::refresh);
  }

  /**
   * Check whether a key is currently tracked as a local hot key in L1.
   *
   * @param cacheKey the key to inspect
   * @return {@code true} if the key exists in L1 with
   *         {@link io.github.hyshmily.zeta.model.KeyState#HOT}
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public boolean isLocalHotKey(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("isHot");
    requireAppDetector("isHot");
    return hotKeyCache.isHot(cacheKey);
  }

  /**
   * Batch variant of {@link #isLocalHotKey(String)}. Returns a map of key →
   * hot status for all given keys.
   *
   * <p>
   * <b>Batch execution:</b> Iterates sequentially; for large batches
   * consider parallelizing in caller code.
   *
   * @param cacheKeys the keys to inspect
   * @return a map of key → whether it is a local hot key
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public Map<String, Boolean> areLocalHotKeys(Collection<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<String, Boolean> result = new HashMap<>();
    cacheKeys.forEach(key -> result.put(key, isLocalHotKey(key)));
    return result;
  }

  /**
   * Increment the local TopK detector directly, bypassing the buffer and the
   * reportToWorker-to-Worker path. Useful for bulk-loading historical access
   * patterns
   * or correcting frequency counts.
   *
   * @param cacheKey the key to record
   * @param count    the number of accesses to add
   */
  public void notifyLocalDetectorDirect(String cacheKey, long count) {
    requireAppDetector("notifyLocalDetectorDirect");
    appHotKeyDetector.addDirect(cacheKey, count);
  }

  /**
   * Batch-increment the local TopK detector directly, bypassing buffer and
   * reports.
   *
   * @param keyCounts a map of key → access count
   */
  public void notifyLocalDetectorDirect(Map<String, Long> keyCounts) {
    requireAppDetector("notifyLocalDetectorDirect");
    appHotKeyDetector.addDirect(keyCounts);
  }

  /**
   * Notify the local TopK detector that a key was accessed, without triggering
   * a reportToWorker to the Worker. Used by {@code @Intercept} path to keep the
   * local
   * frequency sketch accurate without flooding the Worker with reports.
   * Null keys are silently ignored.
   *
   * @param cacheKey the accessed key (maybe {@code null}, silently ignored)
   */
  public void notifyLocalDetector(String cacheKey) {
    requireAppDetector("notifyLocalDetector");
    appHotKeyDetector.add(cacheKey);
  }

  /**
   * Notify the local detector of key access with a custom delta, routing
   * through the buffered counter. Null keys are silently ignored.
   *
   * @param cacheKey the accessed key (maybe {@code null}, silently ignored)
   * @param count    the number of accesses to record
   */
  public void notifyLocalDetector(String cacheKey, long count) {
    requireAppDetector("notifyLocalDetector");
    appHotKeyDetector.add(cacheKey, count);
  }

  /**
   * Batch-notify the local detector of multiple key accesses, routing through
   * the buffered counter. Null keys in the map are silently ignored.
   *
   * @param keyCounts a map of key → access count
   */
  public void notifyLocalDetector(Map<String, Long> keyCounts) {
    requireAppDetector("notifyLocalDetector");
    appHotKeyDetector.add(keyCounts);
  }

  /**
   * Return the top N hot keys from the local detector, ordered by frequency.
   * Useful when callers need more or fewer items than the configured TopK
   * capacity.
   *
   * @param n the number of top keys to return
   * @return the top N items, or an empty list if no detector is available
   */
  public List<Item> returnLocalTopNHotKeys(int n) {
    return appHotKeyDetector != null ? appHotKeyDetector.listTopN(n) : List.of();
  }

  /**
   * Return a blocking queue of recently expelled hot keys from the local
   * detector.
   * Consumers can drain this queue to react to keys that are no longer hot.
   *
   * <p>
   * <b>Competing consumer:</b> the built-in scheduler drains the same
   * queue — up to {@code 100_000} items per TopK every 10 seconds (see
   * {@code ZetaSchedulingConfiguration#drainExpelled}, enabled by default via
   * {@code zeta.scheduling.enabled=true}) as a memory-protection measure. A
   * user consumer therefore competes with that drain and may not observe
   * every expelled item; events can be silently consumed by the periodic
   * drain before the user's {@code poll()} runs. To receive the full stream,
   * either disable the scheduled drain ({@code zeta.scheduling.enabled=false}
   * — at the cost of unbounded queue growth if nobody drains) or poll this
   * queue more frequently than the 10s drain cadence.
   *
   * @return the expelled queue, or an empty queue if no detector is available
   */
  public BlockingQueue<Item> returnLocalExpelledHotKeys() {
    return appHotKeyDetector != null ? appHotKeyDetector.expelled() : new LinkedBlockingQueue<>();
  }

  /**
   * Return the total number of data streams (accesses) tracked by the local
   * detector.
   *
   * @return the total count, or {@code 0} if no detector is available
   */
  public long returnLocalTotalDataStreams() {
    return appHotKeyDetector != null ? appHotKeyDetector.total() : 0L;
  }

  /**
   * Return the current top-K hot keys (key + count) from the local detector,
   * ordered by frequency.
   *
   * @return the local top-K list, or an empty list if no detector is available;
   *         the returned list is a point-in-time snapshot
   */
  public List<Item> returnLocalHotKeys() {
    return appHotKeyDetector != null ? appHotKeyDetector.list() : List.of();
  }

  /**
   * Add a key pattern to the blacklist. Keys matching this pattern will be
   * blocked from cache get/put operations (returns {@link Optional#empty()}).
   * <p>
   * The pattern is auto-detected by {@link RuleMatcher#of}: exact match,
   * prefix (trailing {@code *}), wildcard (containing {@code *} or {@code ?}),
   * or regex (prefixed with {@code regex:}).
   *
   * @param keyPattern the key pattern to blacklist
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void addBlacklist(String keyPattern) {
    Assert.hasText(keyPattern, "keyPattern must not be empty");
    requireAppCache("addBlacklist");
    hotKeyCache.addBlacklist(keyPattern);
  }

  /**
   * Batch variant of {@link #addBlacklist(String)}. Adds multiple key patterns
   * to the blacklist.
   *
   * @param keyPatterns the key patterns to blacklist
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void addBlacklist(Collection<String> keyPatterns) {
    Objects.requireNonNull(keyPatterns, "keyPatterns must not be null");
    requireAppCache("addBlacklist");
    keyPatterns.forEach(hotKeyCache::addBlacklist);
  }

  /**
   * Remove a key pattern from the blacklist.
   *
   * @param keyPattern the key pattern to remove from the blacklist
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void removeBlacklist(String keyPattern) {
    Assert.hasText(keyPattern, "keyPattern must not be empty");
    requireAppCache("removeBlacklist");
    hotKeyCache.unBlacklist(keyPattern);
  }

  /**
   * Batch variant of {@link #removeBlacklist(String)}. Removes multiple key
   * patterns from the blacklist.
   *
   * @param keyPatterns the key patterns to remove from the blacklist
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void removeBlacklist(Collection<String> keyPatterns) {
    Objects.requireNonNull(keyPatterns, "keyPatterns must not be null");
    requireAppCache("removeBlacklist");
    keyPatterns.forEach(hotKeyCache::unBlacklist);
  }

  /**
   * Add a key pattern to the whitelist. Keys matching this pattern will
   * skip reportToWorker recording (no Worker reportToWorker sent) but still
   * participate in
   * normal cache get/put and local hot-key detection.
   * <p>
   * The pattern is auto-detected by {@link RuleMatcher#of}.
   *
   * @param keyPattern the key pattern to whitelist
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void addWhitelist(String keyPattern) {
    Assert.hasText(keyPattern, "keyPattern must not be empty");
    requireAppCache("addWhitelist");
    hotKeyCache.addWhitelist(keyPattern);
  }

  /**
   * Batch variant of {@link #addWhitelist(String)}. Adds multiple key patterns
   * to the whitelist.
   *
   * @param keyPatterns the key patterns to whitelist
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void addWhitelist(Collection<String> keyPatterns) {
    Objects.requireNonNull(keyPatterns, "keyPatterns must not be null");
    requireAppCache("addWhitelist");
    keyPatterns.forEach(hotKeyCache::addWhitelist);
  }

  /**
   * Remove a key pattern from the whitelist.
   *
   * @param keyPattern the key pattern to remove from the whitelist
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void removeWhitelist(String keyPattern) {
    Assert.hasText(keyPattern, "keyPattern must not be empty");
    requireAppCache("removeWhitelist");
    hotKeyCache.unWhitelist(keyPattern);
  }

  /**
   * Batch variant of {@link #removeWhitelist(String)}. Removes multiple key
   * patterns from the whitelist.
   *
   * @param keyPatterns the key patterns to remove from the whitelist
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void removeWhitelist(Collection<String> keyPatterns) {
    Objects.requireNonNull(keyPatterns, "keyPatterns must not be null");
    requireAppCache("removeWhitelist");
    keyPatterns.forEach(hotKeyCache::unWhitelist);
  }

  /**
   * Evaluate all rules against the given key and return the first matching
   * action.
   *
   * @param cacheKey the key to evaluate
   * @return the matching {@link Rule.RuleAction}, or {@code ALLOW} if no rule
   *         matches
   *         or no cache is available (Worker-only mode)
   */
  public Rule.RuleAction evaluateRule(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    if (hotKeyCache == null) {
      return Rule.RuleAction.ALLOW;
    }
    return hotKeyCache.evaluateRule(cacheKey);
  }

  /**
   * Batch variant of {@link #evaluateRule(String)}. Evaluates all rules against
   * the given keys and returns the first matching action for each.
   *
   * @param cacheKeys the keys to evaluate
   * @return a map of key → matching {@link Rule.RuleAction} (or {@code ALLOW} if
   *         no rule matches)
   */
  public Map<String, Rule.RuleAction> evaluateRules(Collection<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<String, Rule.RuleAction> result = new HashMap<>();
    cacheKeys.forEach(key -> {
      Rule.RuleAction action = evaluateRule(key);
      result.put(key, action);
    });

    return result;
  }

  /**
   * Quickly check whether the given key is blocked by any blacklist rule.
   * <p>
   * Equivalent to {@code evaluateRule(key) == BLOCK}, provided as a convenience
   * for guard clauses before expensive operations.
   *
   * @param cacheKey the key to check
   * @return {@code true} if a blacklist rule matches the key, {@code false} if no
   *         rule matches or no cache is available (Worker-only mode)
   */
  public boolean isBlacklisted(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    if (hotKeyCache == null) {
      return false;
    }
    return hotKeyCache.isBlacklisted(cacheKey);
  }

  /**
   * Batch variant of {@link #isBlacklisted(String)}. Checks multiple keys
   * against blacklist rules.
   *
   * @param cacheKeys the keys to check
   * @return a map of key → whether it is blocked by a blacklist rule
   */
  public Map<String, Boolean> isBlacklisted(Collection<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<String, Boolean> result = new HashMap<>();
    cacheKeys.forEach(key -> {
      boolean isBlacklisted = isBlacklisted(key);
      result.put(key, isBlacklisted);
    });

    return result;
  }

  /**
   * Quickly check whether the given key is whitelisted (skips Worker reporting).
   * <p>
   * Equivalent to {@code evaluateRule(key) == ALLOW_NO_REPORT}, provided as a
   * convenience for debugging and monitoring.
   *
   * @param cacheKey the key to check
   * @return {@code true} if a whitelist rule matches the key, {@code false} if no
   *         rule matches or no cache is available (Worker-only mode)
   */
  public boolean isWhitelisted(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    if (hotKeyCache == null) {
      return false;
    }
    return hotKeyCache.isWhitelisted(cacheKey);
  }

  /**
   * Batch variant of {@link #isWhitelisted(String)}. Checks multiple keys
   * against whitelist rules.
   *
   * @param cacheKeys the keys to check
   * @return a map of key → whether it is whitelisted (skips Worker reporting)
   */
  public Map<String, Boolean> isWhitelisted(Collection<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<String, Boolean> result = new HashMap<>();
    cacheKeys.forEach(key -> {
      boolean isWhitelisted = isWhitelisted(key);
      result.put(key, isWhitelisted);
    });

    return result;
  }

  /**
   * Return a snapshot of all current rules in evaluation order.
   *
   * @return list of rules, or an empty list if no cache is available;
   *         the returned list is unmodifiable
   */
  public List<Rule> getAllRules() {
    if (hotKeyCache == null) {
      return List.of();
    }
    return hotKeyCache.getAllRules();
  }

  /**
   * Remove all blacklist and whitelist rules.
   *
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void clearAllRules() {
    requireAppCache("clearAllRules");
    hotKeyCache.clearAllRules();
  }

  /**
   * Broadcast all local rules to peer instances via the sync exchange.
   * Useful for initial synchronization when a new instance joins the cluster.
   *
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void broadcastAllLocalRulesManually() {
    requireAppCache("broadcastAllLocalRulesManually");
    hotKeyCache.broadcastAllLocalRulesManually();
  }

  /**
   * Whether this instance has app-side cache available.
   *
   * <p>
   * Returns {@code true} in App-only and Coexistence modes,
   * {@code false} in Worker-only mode. Callers can use this to guard
   * cache-dependent operations without catching {@link ZetaModeException}.
   *
   * @return {@code true} if cache-dependent APIs (get, put, invalidate, etc.) are
   *         usable
   */
  public boolean isApp() {
    return hotKeyCache != null;
  }

  /**
   * Returns a human-readable label of the current deployment mode.
   */
  private String currentModeLabel() {
    if (hotKeyCache != null) {
      return "App-only mode";
    }
    // The facade is only ever created with a null cache in Worker-only
    // deployments (ZetaFacadeAutoConfiguration wires it via ObjectProvider);
    // a cache-less facade in an app deployment would be a wiring error.
    return "Worker-only mode (no app-side cache)";
  }

  private void requireAppCache(String operation) {
    if (hotKeyCache == null) {
      throw new ZetaModeException(operation, currentModeLabel(), "App-mode cache");
    }
  }

  /**
   * Requires app-side TopK detector; throws {@link ZetaModeException} when
   * the app-side detector is unavailable (Worker-only mode).
   *
   * @param operation the API operation name
   */
  private void requireAppDetector(String operation) {
    if (appHotKeyDetector == null) {
      throw new ZetaModeException(operation, currentModeLabel(), "App-mode TopK detector");
    }
  }
}
