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

import io.github.hyshmily.zeta.cache.fluentAPI.ZetaReadQuery;
import io.github.hyshmily.zeta.cache.fluentAPI.ZetaWriteCommand;
import io.github.hyshmily.zeta.cache.loader.ZetaLoaderRegistry;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.model.InvalidatePolicy;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.model.WritePolicy;
import io.github.hyshmily.zeta.model.HotKey;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sync.distributedlock.AutoReleaseLock;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * Public facade of the HotKey library — the sole API entry point, split into
 * two planes:
 *
 * <ul>
  *   <li><b>Data plane</b> (this interface): cache reads, writes,
  *       invalidations, detection tagging, and distributed locks. All methods
  *       follow the <b>two-arity convention</b> — a common-case convenience
  *       form (implemented here as a {@code default} method) and a
  *       full-control form whose last parameter is the family's closed
  *       policy record ({@link ReadPolicy}, {@link WritePolicy},
  *       {@link InvalidatePolicy}).
  *       Intermediate positional overloads intentionally do not exist.</li>
   *   <li><b>Administrative plane</b> ({@link #rules()}, {@link #refresh()},
   *       {@link #stats()}, {@link #detector()}): low-frequency operations —
   *       rule CRUD, timed refresh registration, observability, and local-TopK
   *       detection queries/feeds — behind small interfaces that can be
   *       replaced independently (Caffeine's {@code Cache.policy()}
   *       pattern).</li>
 * </ul>
 *
 * <p>
 * <b>Thread safety:</b> all methods are thread-safe.
 *
 * <p>
 * <b>Deployment modes:</b> depending on the runtime mode some services may be
 * absent. In Worker-only mode the data plane throws {@link ZetaModeException}
 * (or returns empty/zero where documented) and the administrative views
 * degrade the same way.
 *
 * <p>
 * The default implementation is {@link DefaultZeta} (wired by
 * {@code ZetaFacadeAutoConfiguration}); the engine behind it is
 * {@code HotKeyCache} — the Caffeine {@code Cache}/{@code BoundedLocalCache}
 * layering.
 */
public interface Zeta {
  /**
   * How much of the access-recording pipeline a {@link #tag(String, TagMode)}
   * call performs. Replaces the former boolean pair — call sites now read as
   * intent instead of positional flags.
   */
  enum TagMode {
    /** Local HeavyKeeper count and Worker report (default). */
    RECORD,
    /** Local count only; skip the Worker report. */
    DETECT_ONLY,
    /** Worker report only; skip the local count. */
    REPORT_ONLY,
    /** Neither (explicit no-op). */
    SILENT;

    /** Whether this mode skips the local HeavyKeeper increment. */
    public boolean skipDetection() {
      return this == REPORT_ONLY || this == SILENT;
    }

    /** Whether this mode skips the Worker report. */
    public boolean skipReport() {
      return this == DETECT_ONLY || this == SILENT;
    }

    /**
     * Map a legacy skip-pair to the mode expressing it.
     *
     * @param skipDetection whether to skip the local count
     * @param skipReport    whether to skip the Worker report
     * @return the corresponding mode
     */
    public static TagMode of(boolean skipDetection, boolean skipReport) {
      if (skipDetection) {
        return skipReport ? SILENT : REPORT_ONLY;
      }
      return skipReport ? DETECT_ONLY : RECORD;
    }
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
  public void tag(String cacheKey);

  /**
   * Tag a cache key with an explicit recording mode.
   *
   * <p>
   * It is the transport for the {@code @Intercept} annotation path
   * ({@code CacheExtensionAspect}); application code should prefer
   * {@link #tag(String)}.
   *
   * @param cacheKey the key to tag
   * @param mode     how much of the pipeline to run (never {@code null})
   */
  public void tag(String cacheKey, TagMode mode);

  /**
   * No-reader get: resolves the key against the {@link ZetaLoaderRegistry}
   * (ADR-0070) and loads through the registered
   * {@link io.github.hyshmily.zeta.cache.loader.CacheLoader} on a
   * miss — the Caffeine {@code LoadingCache} style, where the loader is
   * registered once instead of passed at every call site.
   *
   * <p>
   * <b>Precedence — no conflict with explicit readers:</b> the
   * {@code get(key, reader)}, {@code get(key, ReadPolicy)} and
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
   * {@link #get(String, ReadPolicy)}, so SingleFlight deduplication, circuit
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
  public <T> Optional<T> get(String cacheKey);

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
  public <T> Optional<T> getWithSoftExpire(String cacheKey);

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
   * {@link #get(String, ReadPolicy)}.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader);

  /**
   * Get with an explicit {@link ReadPolicy}. On this (non-atomic) path the
   * policy's TTL suppliers are evaluated lazily — at most once per call and
   * only when an entry is created, promoted, or a soft-expire refresh is
   * scheduled (the {@link ReadPolicy} contract), so SpEL-based TTL
   * expressions cost nothing on a plain hit; a valid {@code NullValue}
   * sentinel is served as an empty hit without re-invoking the reader, and
   * the access is still counted for hot-key detection. When
   * {@link ReadPolicy#nullCaching()} is {@code false}, a {@code null} reader
   * result leaves no cache entry. The policy carries the reader
   * ({@link ReadPolicy#withReader(Supplier)} may inject one) and the
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
  public <T> Optional<T> get(String cacheKey, ReadPolicy policy);

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
   * handed to {@link #getWithSoftExpire(String, ReadPolicy)}.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses / refreshes
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached (possibly stale) or loaded
   *         value
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader);

  /**
   * Get with soft-expire and an explicit {@link ReadPolicy}. Serves valid
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
  public <T> Optional<T> getWithSoftExpire(String cacheKey, ReadPolicy policy);

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
   * @return a map of key → loaded or cached value; keys with no value are omitted
   * @throws ZetaModeException     when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException  when any key matches a blacklist rule
   * @throws IllegalStateException when no registry is wired or any key matches no
   *                               registered prefix
   */
  public <T> Map<String, T> getAll(Iterable<String> cacheKeys);

  /**
   * Batch no-reader get with soft-expire: the batch counterpart of
   * {@link #getWithSoftExpire(String)} — every group is executed with
   * {@link StalePolicy#SOFT_REFRESH} forced, exactly like the single-key
   * overload; grouping, routing, and fail-fast semantics match
   * {@link #getAll(Iterable)}.
   *
   * @param cacheKeys the keys to retrieve
   * @param <T>       the value type (erased — each loader's own type governs)
   * @return a map of key → cached (possibly stale) or loaded value; keys with no value are omitted
   * @throws ZetaModeException     when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException  when any key matches a blacklist rule
   * @throws IllegalStateException when no registry is wired or any key matches no
   *                               registered prefix
   */
  public <T> Map<String, T> getAllWithSoftExpire(Iterable<String> cacheKeys);

  /**
   * Batch get: load each missing key via the reader with all-default
   * configuration (no TTL override, Worker reporting enabled, reader failures
   * swallowed as per-key misses).
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param <T>       the value type
   * @return a map of key → loaded or cached value; keys with no value are omitted
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, T> getAll(Iterable<String> cacheKeys, Function<? super String, ? extends T> reader);

  /**
   * Batch get with an explicit {@link ReadPolicy} — the full-control form.
   * All keys share the same policy. The policy's TTLs are evaluated once for
   * the whole batch (the batch pipeline has no per-key lazy-TTL path); the
   * explicit {@code reader} argument governs loading ({@code policy.reader()}
   * is ignored), and {@code null} results are always cached as short-TTL
   * sentinels.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param policy    the shared per-invocation read policy
   * @param <T>       the value type
   * @return a map of key → loaded or cached value; keys with no value are omitted
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   * @throws RuntimeException     the first reader failure cause when
   *                              {@code policy.failOnError()} is {@code true}
   */
  public <T> Map<String, T> getAll(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    ReadPolicy policy
  );

  /**
   * Batch get with soft-expire: load each missing key via the reader with
   * all-default configuration (no TTL override, Worker reporting enabled,
   * reader failures swallowed as per-key misses).
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses / refreshes
   * @param <T>       the value type
   * @return a map of key → cached (possibly stale) or loaded value; keys with no value are omitted
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   */
  public <T> Map<String, T> getAllWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader
  );

  /**
   * Batch get with soft-expire and an explicit {@link ReadPolicy} — the
   * full-control form. TTL/report/failure semantics match
   * {@link #getAll(Iterable, Function, ReadPolicy)}.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses / refreshes
   * @param policy    the shared per-invocation read policy
   * @param <T>       the value type
   * @return a map of key → cached (possibly stale) or loaded value; keys with no value are omitted
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when any key matches a blacklist rule
   * @throws RuntimeException     the first reader failure cause when
   *                              {@code policy.failOnError()} is {@code true}
   */
  public <T> Map<String, T> getAllWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    ReadPolicy policy
  );

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
   * {@code computeIfAbsent(key, ReadPolicy)}
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
  public <V> V computeIfAbsent(String cacheKey);

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
   * {@code spec.withLoader(custom).toReadPolicy(key)} handed to
   * {@link #computeIfAbsentOptional(String, ReadPolicy)}.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value supplier for cache misses
   * @param <V>      the value type
   * @return the cached or loaded value, or {@code null} if the loader returned
   *         {@code null}
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <V> V computeIfAbsent(String cacheKey, Supplier<V> loader);

  /**
   * {@link Optional}-returning compute-if-absent.
   *
   * <p>Named for its return type so that callers can tell at the call site
   * whether absence is signalled by an empty {@link Optional} or a {@code null}
   * return: the {@code V}-returning {@code computeIfAbsent} family and this
   * method share one base name with different null contracts.
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation read policy (carries reader,
   *                 TTLs, null-caching, stale-policy, fail-fast (failOnError),
   *                 and report-allow flag)
   * @param <T>      the value type
   * @return an {@link Optional} containing the cached or loaded value
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> computeIfAbsentOptional(String cacheKey, ReadPolicy policy);

  /**
   * No-reader compute-if-absent with soft-expire: the V-returning counterpart
   * of {@link #getWithSoftExpire(String)} — {@link StalePolicy#SOFT_REFRESH}
   * forced, TTL/semantics from the winning spec.
   *
   * <p>
   * Precedence matches the single-key overloads: the loader-based
   * {@code computeIfAbsentWithSoftExpire(key, loader)} and
   * {@code computeIfAbsentWithSoftExpire(key, ReadPolicy)} overloads never
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
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey);

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
   * {@code spec.withLoader(custom).toReadPolicy(key, StalePolicy.SOFT_REFRESH)}
   * handed to {@link #computeIfAbsentWithSoftExpireOptional(String, ReadPolicy)}.
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
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey, Supplier<V> loader);

  /**
   * {@link Optional}-returning compute-if-absent-with-soft-expire.
   *
   * <p>Named for its return type so that callers can tell at the call site
   * whether absence is signalled by an empty {@link Optional} or a {@code null}
   * return: the {@code V}-returning {@code computeIfAbsentWithSoftExpire}
   * family and this method share one base name with different null contracts.
   * With a globally disabled
   * soft TTL (entries carrying {@code softExpireAtMs = 0} never read as
   * stale) this call behaves exactly like plain {@link #computeIfAbsent} —
   * no background refresh is ever armed.
   *
   * <p>
   * The policy's TTL suppliers are evaluated lazily — at most once per
   * call, and only when an entry is created, promoted, renewed, or a
   * background refresh is scheduled — so SpEL-based TTL expressions cost
   * nothing on a plain cache hit. When {@link ReadPolicy#nullCaching()} is
   * {@code false}, a {@code null} loader result leaves no cache entry, so the
   * next call re-invokes the loader. The policy also carries the reader and
   * the report-allow flag, consolidating all per-invocation parameters.
   *
   * @param cacheKey the key to retrieve
   * @param policy   the resolved per-invocation read policy (carries reader,
   *                 TTLs, null-caching, stale-policy, fail-fast (failOnError),
   *                 and report-allow flag)
   * @param <T>      the value type
   * @return the cached (possibly stale) or loaded value as an {@link Optional}
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  public <T> Optional<T> computeIfAbsentWithSoftExpireOptional(String cacheKey, ReadPolicy policy);

  /**
   * Look up a cached value without loading or triggering hot-key detection.
   *
   * @param cacheKey the key to look up
   * @param <T>      the value type
   * @return an {@link Optional} containing the raw value if present
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public <T> Optional<T> peek(@NotNull @NotBlank String cacheKey);

  /**
   * Annotation-path peek for the Spring Cache adapter: look up a cached value
   * <b>and</b> feed hot-key detection and Worker reporting when a hit is
   * served — preserving the core invariant that every read triggers both,
   * while paying a single rule evaluation and a single hash lookup per call
   * (the plain {@link #peek(String)} + {@link RuleAdmin#evaluateRule(String)} + {@link #tag(String)} sequence
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
  public Object peekAndTag(String cacheKey);

  /**
   * Write-through with an explicit {@link WritePolicy} — the full-control
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
  public <T> void putThrough(String cacheKey, T value, Runnable writer, WritePolicy policy);

  /**
   * Write a value directly into the local L1 cache with an explicit
   * {@link WritePolicy} controlling the entry's TTLs. Delegates to
   * {@link #putLocal(String, Object)} semantics — no version bump, no
   * broadcast, no hot-key detection, and no reporting.
   *
   * <p>
   * In Worker-only mode this method silently no-ops.
   *
   * @param cacheKey the key to store
   * @param value    the value to cache
   * @param policy   the write policy supplying the entry's TTL overrides
   */
  public void putLocal(String cacheKey, Object value, WritePolicy policy);

  /**
   * Invalidate a single key from L1 with explicit broadcast control via
   * {@link InvalidatePolicy}: when {@code policy.skipBroadcast()} is
   * {@code true} the invalidation stays local (no REFRESH message to peers).
   *
   * @param cacheKey the key to invalidate
   * @param policy   the invalidation policy supplying broadcast control
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidate(String cacheKey, InvalidatePolicy policy);

  /**
   * Invalidate a collection of keys from L1 with explicit broadcast control
   * via {@link InvalidatePolicy} — see {@link #invalidate(String, InvalidatePolicy)}.
   *
   * @param cacheKeys the keys to invalidate
   * @param policy    the invalidation policy supplying broadcast control
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidate(Collection<String> cacheKeys, InvalidatePolicy policy);

  /**
   * Execute a mutation, then invalidate L1 with explicit broadcast control
   * via {@link InvalidatePolicy}. If the mutation throws, invalidation is skipped.
   *
   * @param cacheKey the key to invalidate after mutation
   * @param mutation the mutation to execute before invalidation
   * @param policy   the invalidation policy supplying broadcast control
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(String cacheKey, Runnable mutation, InvalidatePolicy policy);

  /**
   * Execute mutations for multiple keys, then invalidate each from L1 with
   * explicit broadcast control via {@link InvalidatePolicy} — see
   * {@link #invalidateAfterPut(String, Runnable, InvalidatePolicy)}.
   *
   * @param mutations a map of key → mutation pairs
   * @param policy    the invalidation policy supplying broadcast control
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void invalidateAfterPut(Map<String, ? extends Runnable> mutations, InvalidatePolicy policy);

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
  public void invalidateAllLocal();

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
  public AutoReleaseLock tryLock(String key, long expire, TimeUnit unit);

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
  public AutoReleaseLock tryLock(
    String key,
    long expire,
    TimeUnit unit,
    int tryLockLockCount,
    int tryLockInquiryCount,
    int tryLockUnlockCount
  );

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
  public boolean tryLockAndRun(String key, long expire, TimeUnit unit, Runnable action);

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
    int tryLockUnlockCount
  );

  /**
   * Refresh the given key with an explicit {@link WritePolicy}: load via the
   * supplier, evict the local entry, then cache the loaded value through
   * write-through (version bump + broadcast). No broadcast is sent for the
   * eviction itself.
   *
   * <p>
   * <b>Ordering note:</b> the load runs <i>before</i> the eviction so the
   * local L1 miss window stays as short as possible; the refresh is not
   * atomic against concurrent writers (see {@link #refresh(String, Supplier)}).
   *
   * @param cacheKey the key to refresh
   * @param loader   the value supplier
   * @param policy   the write policy supplying TTL overrides and broadcast
   *                 control
   * @param <V>      the value type
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public <V> void refresh(String cacheKey, Supplier<V> loader, WritePolicy policy);

  /**
   * Batch variant of {@link #refresh(String, Supplier)}. Refreshes all entries
   * by evicting locally then loading and caching via the provided suppliers.
   *
   * @param loaders a map of key → value supplier
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public void refreshAll(Map<String, Supplier<?>> loaders);

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
  public boolean isApp();

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
  public default <T> ZetaReadQuery<T> read(String cacheKey) {
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
  public default <T> ZetaWriteCommand<T> write(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    return new ZetaWriteCommand<>(this, cacheKey);
  }

  /**
   * Batch variant of {@link #peek(String)}. Returns a map of key → value for
   * all keys that are present in L1. Missing keys are silently omitted.
   *
   * @param cacheKeys the keys to look up
   * @return a map of present key-value pairs (never {@code null})
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public default Map<String, Object> peekAll(Collection<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<String, Object> result = new HashMap<>();
    cacheKeys.forEach(key -> {
      Optional<Object> value = peek(key);
      value.ifPresent(v -> result.put(key, v));
    });

    return result;
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
  public default <T> void putThrough(String cacheKey, T value, Runnable writer) {
    putThrough(cacheKey, value, writer, WritePolicy.defaults());
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
  public default void putLocal(String cacheKey, Object value) {
    putLocal(cacheKey, value, WritePolicy.defaults());
  }

  /**
   * Invalidate a single key from L1 and broadcast REFRESH to peers.
   * The next {@link #get} will re-fetch from the reader.
   *
   * @param cacheKey the key to invalidate
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public default void invalidate(String cacheKey) {
    invalidate(cacheKey, InvalidatePolicy.defaults());
  }

  /**
   * Invalidate a collection of keys from L1 and broadcast INVALIDATE for each.
   *
   * @param cacheKeys the keys to invalidate
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public default void invalidate(Collection<String> cacheKeys) {
    invalidate(cacheKeys, InvalidatePolicy.defaults());
  }

  /**
   * Execute a mutation, then invalidate L1 and broadcast to peers.
   * If the mutation throws, invalidation is skipped.
   *
   * @param cacheKey the key to invalidate after mutation
   * @param mutation the mutation to execute before invalidation
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public default void invalidateAfterPut(String cacheKey, Runnable mutation) {
    invalidateAfterPut(cacheKey, mutation, InvalidatePolicy.defaults());
  }

  /**
   * Execute mutations for multiple keys, then invalidate each from L1 and
   * broadcast. Each key gets its own mutation. If a single mutation fails,
   * that key is skipped (invalidation and broadcast are not performed).
   *
   * @param mutations a map of key → mutation pairs to execute before invalidation
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public default void invalidateAfterPut(Map<String, ? extends Runnable> mutations) {
    invalidateAfterPut(mutations, InvalidatePolicy.defaults());
  }

  /**
   * Register a timed background refresh for the given key with the configured
   * default TTLs. See {@link #registerRefresh(String, Supplier, ReadPolicy)}
   * for the scheduling contract.
   *
   * @param key      the cache key to refresh
   * @param supplier the value supplier for refresh
   * @param <T>      the value type
   */
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
   * need read-modify-write atomicity should use
   * {@link #putThrough(String, Object, Runnable, WritePolicy)} instead
   * (last-writer-wins; no CAS is offered).
   *
   * @param cacheKey the key to refresh
   * @param loader   the value supplier
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  public default void refresh(String cacheKey, Supplier<?> loader) {
    refresh(cacheKey, loader, WritePolicy.defaults());
  }

  /**
   * Rule administration view: blacklist/whitelist CRUD, rule evaluation, and
   * rule-set snapshot/broadcast. In Worker-only mode the returned view
   * rejects mutations with {@link ZetaModeException} and answers queries
   * with the documented defaults (ALLOW / false / empty).
   */
  RuleAdmin rules();

  /**
   * Timed-refresh administration view: per-key scheduled background refresh
   * registration. In Worker-only mode registration throws
   * {@link ZetaModeException}.
   */
  RefreshAdmin refresh();

  /**
   * Observability view: L1 size/statistics and read-only snapshots.
   * In Worker-only mode {@code stats()} returns {@code null},
   * {@code estimatedSize()} returns {@code 0}, and {@code snapshotValues()} /
   * {@code localKeysWithPrefix()} return empty collections.
   */
  StatsAdmin stats();

  /**
   * Detection-observation view: local-TopK queries and detector feeds. In
   * Worker-only mode feeds and {@code isLocalHotKey} throw
   * {@link ZetaModeException} while pure queries answer with empty defaults.
   */
  DetectorAdmin detector();

  /**
   * Rule administration: blacklist blocks keys from cache operations,
   * whitelist lets keys skip Worker reporting. Patterns are auto-detected by
   * {@link RuleMatcher#of}: exact, prefix (trailing {@code *}), wildcard, or
   * regex (prefixed with {@code regex:}).
   */
  interface RuleAdmin {
    /** Add a key pattern to the blacklist. */
    void addBlacklist(String keyPattern);

    /** Remove a key pattern from the blacklist. */
    void removeBlacklist(String keyPattern);

    /** Add a key pattern to the whitelist. */
    void addWhitelist(String keyPattern);

    /** Remove a key pattern from the whitelist. */
    void removeWhitelist(String keyPattern);

    /**
     * Evaluate all rules against the given key and return the first matching
     * action, or {@link Rule.RuleAction#ALLOW} when no rule matches.
     */
    Rule.RuleAction evaluateRule(String cacheKey);

    /** {@code evaluateRule(key) == BLOCK} convenience guard. */
    boolean isBlacklisted(String cacheKey);

    /** {@code evaluateRule(key) == ALLOW_NO_REPORT} convenience check. */
    boolean isWhitelisted(String cacheKey);

    /** Snapshot of all current rules in evaluation order (unmodifiable). */
    List<Rule> getAllRules();

    /** Remove all blacklist and whitelist rules. */
    void clearAllRules();

    /** Broadcast all local rules to peer instances via the sync exchange. */
    void broadcastAllLocalRulesManually();

    /** Batch variant of {@link #addBlacklist(String)}. */
    default void addBlacklist(Collection<String> keyPatterns) {
      Objects.requireNonNull(keyPatterns, "keyPatterns must not be null");
      keyPatterns.forEach(this::addBlacklist);
    }

    /** Batch variant of {@link #removeBlacklist(String)}. */
    default void removeBlacklist(Collection<String> keyPatterns) {
      Objects.requireNonNull(keyPatterns, "keyPatterns must not be null");
      keyPatterns.forEach(this::removeBlacklist);
    }

    /** Batch variant of {@link #addWhitelist(String)}. */
    default void addWhitelist(Collection<String> keyPatterns) {
      Objects.requireNonNull(keyPatterns, "keyPatterns must not be null");
      keyPatterns.forEach(this::addWhitelist);
    }

    /** Batch variant of {@link #removeWhitelist(String)}. */
    default void removeWhitelist(Collection<String> keyPatterns) {
      Objects.requireNonNull(keyPatterns, "keyPatterns must not be null");
      keyPatterns.forEach(this::removeWhitelist);
    }

    /** Batch variant of {@link #evaluateRule(String)}. */
    default Map<String, Rule.RuleAction> evaluateRules(Collection<String> cacheKeys) {
      Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
      Map<String, Rule.RuleAction> result = new HashMap<>();
      cacheKeys.forEach(key -> result.put(key, evaluateRule(key)));
      return result;
    }

    /** Batch variant of {@link #isBlacklisted(String)}. */
    default Map<String, Boolean> isBlacklisted(Collection<String> cacheKeys) {
      Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
      Map<String, Boolean> result = new HashMap<>();
      cacheKeys.forEach(key -> result.put(key, isBlacklisted(key)));
      return result;
    }

    /** Batch variant of {@link #isWhitelisted(String)}. */
    default Map<String, Boolean> isWhitelisted(Collection<String> cacheKeys) {
      Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
      Map<String, Boolean> result = new HashMap<>();
      cacheKeys.forEach(key -> result.put(key, isWhitelisted(key)));
      return result;
    }
  }

  /**
   * Timed-refresh administration: schedule {@code getWithSoftExpire} ticks at
   * {@code softTtl × 1.1} so every cycle triggers an async refresh for the
   * key. Re-registering a key replaces its schedule; the scheduler is created
   * lazily on first registration.
   */
  interface RefreshAdmin {
    /** Register (or atomically replace) the timed refresh for the key. */
    <T> void registerRefresh(String key, Supplier<T> supplier, ReadPolicy policy);

    /** Cancel the timed refresh for the key (no-op when not registered). */
    void unregisterRefresh(String key);

    /** Register a timed background refresh with the configured default TTLs. */
    default <T> void registerRefresh(String key, Supplier<T> supplier) {
      registerRefresh(key, supplier, ReadPolicy.defaults());
    }
  }

  /**
   * Observability view over the L1 cache.
   *
   * <p>The raw Caffeine handle is deliberately <em>not</em> exposed: L1
   * stores internal wrappers ({@code CacheEntry}, the null sentinel), not
   * user values, and a writable handle lets callers bypass data-version
   * stamping, Worker decision state and peer broadcast. Namespace scans go
   * through {@link #localKeysWithPrefix(String)}, value inspection through
   * {@link #snapshotValues(int)}.
   */
  interface StatsAdmin {
    /** Estimated number of entries currently in the L1 cache. */
    long estimatedSize();

    /**
     * Snapshot of basic L1 statistics; hit/miss counters are {@code 0} when
     * the underlying cache does not record stats.
     */
    ZetaCacheStats snapshot();

    /**
     * Read-only snapshot of up to {@code limit} entries, mapped to the
     * <b>user values</b> actually stored (internal wrappers such as
     * {@code CacheEntry} and the null sentinel are unwrapped; null-valued
     * sentinels appear as {@code null}).
     *
     * <p>This is the supported way to inspect L1 contents. It deliberately
     * exposes neither the internal entry model nor a writable cache handle —
     * writing through a raw cache handle would bypass version stamping and
     * peer broadcast.
     *
     * @param limit maximum number of entries to return; values {@code <= 0}
     *              yield an empty map
     * @return an immutable snapshot, never {@code null}
     */
    Map<String, Object> snapshotValues(int limit);

    /**
     * Snapshot of the L1 keys carrying the given prefix — the read-only
     * replacement for raw-handle namespace scans (e.g. annotation-cache
     * {@code clear()}).
     *
     * @param prefix the key prefix to match (never {@code null})
     * @return the matching keys at call time (point-in-time snapshot, never {@code null})
     */
    List<String> localKeysWithPrefix(String prefix);
  }

  /**
   * Detection-observation view: local-TopK queries and detector feeds. This
   * is the supported home for everything the deprecated direct detection
   * methods ({@code isLocalHotKey}, {@code notify*}, {@code localTopKeys},
   * {@code areLocalHotKeys}, {@code returnLocalExpelledHotKeys},
   * {@code returnLocalTotalDataStreams}) used to do.
   *
   * <p>In Worker-only mode feeds and {@code isLocalHotKey} throw
   * {@link ZetaModeException} while pure queries answer with empty defaults
   * ({@code false} is reserved for thrown guards — see each method).
   */
  interface DetectorAdmin {
    /**
     * Check whether a key is currently tracked as a local hot key in L1.
     *
     * @param cacheKey the key to inspect
     * @return {@code true} if the key exists in L1 with
     *         {@link io.github.hyshmily.zeta.model.KeyState#HOT}
     * @throws ZetaModeException when no cache is available (Worker-only mode)
     */
    boolean isLocalHotKey(String cacheKey);

    /**
     * Increment the local TopK detector directly, bypassing the buffer and
     * the report-to-Worker path. Useful for bulk-loading historical access
     * patterns or correcting frequency counts.
     *
     * @param cacheKey the key to record
     * @param count    the number of accesses to add
     * @throws ZetaModeException when no detector is available (Worker-only mode)
     */
    void notifyLocalDetectorDirect(String cacheKey, long count);

    /**
     * Batch-increment the local TopK detector directly, bypassing buffer and
     * reports.
     *
     * @param keyCounts a map of key → access count
     * @throws ZetaModeException when no detector is available (Worker-only mode)
     */
    void notifyLocalDetectorDirect(Map<String, Long> keyCounts);

    /**
     * Notify the local TopK detector that a key was accessed, without
     * triggering a report to the Worker. Null keys are silently ignored.
     *
     * @param cacheKey the accessed key (maybe {@code null}, silently ignored)
     * @throws ZetaModeException when no detector is available (Worker-only mode)
     */
    void notifyLocalDetector(String cacheKey);

    /**
     * Notify the local detector of key access with a custom delta, routing
     * through the buffered counter. Null keys are silently ignored.
     *
     * @param cacheKey the accessed key (maybe {@code null}, silently ignored)
     * @param count    the number of accesses to record
     * @throws ZetaModeException when no detector is available (Worker-only mode)
     */
    void notifyLocalDetector(String cacheKey, long count);

    /**
     * Batch-notify the local detector of multiple key accesses, routing
     * through the buffered counter. Null keys in the map are silently ignored.
     *
     * @param keyCounts a map of key → access count
     * @throws ZetaModeException when no detector is available (Worker-only mode)
     */
    void notifyLocalDetector(Map<String, Long> keyCounts);

    /**
     * Read-only snapshot of the top {@code n} local hot keys, ordered by
     * frequency.
     *
     * @param n the number of top keys to return
     * @return the top N snapshot, or an empty list if no detector is available
     */
    List<HotKey> localTopKeys(int n);

    /**
     * Read-only snapshot of the current local top-K hot keys, ordered by
     * frequency.
     *
     * @return the local top-K snapshot, or an empty list if no detector is available
     */
    List<HotKey> localTopKeys();

    /**
     * A blocking queue of recently expelled hot keys from the local detector.
     * See the competing-consumer contract on the deprecated direct method.
     *
     * @return the expelled queue, or an empty queue if no detector is available
     */
    BlockingQueue<Item> returnLocalExpelledHotKeys();

    /**
     * The total number of data streams (accesses) tracked by the local
     * detector.
     *
     * @return the total count, or {@code 0} if no detector is available
     */
    long returnLocalTotalDataStreams();

    /**
     * Batch variant of {@link #isLocalHotKey(String)}.
     *
     * @param cacheKeys the keys to inspect
     * @return a map of key → whether it is a local hot key
     * @throws ZetaModeException when no cache is available (Worker-only mode)
     */
    default Map<String, Boolean> areLocalHotKeys(Collection<String> cacheKeys) {
      Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
      Map<String, Boolean> result = new HashMap<>();
      cacheKeys.forEach(key -> result.put(key, isLocalHotKey(key)));
      return result;
    }
  }
}
