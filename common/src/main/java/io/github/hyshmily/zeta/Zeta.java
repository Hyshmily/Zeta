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
import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * Public facade of the Zeta library — the sole API entry point, shaped as
 * the Caffeine-style minimal surface: three data verbs
 * (<b>get / put / invalidate</b>), each with one closed options record
 * ({@link GetPolicy}, {@link PutPolicy},
 * {@link InvalidatePolicy}), their batch forms ({@code getAll}, {@code putAll},
 * the collection and clear invalidations), plus the value-only renewal verbs
 * ({@code refresh} / {@code refreshAll}), a peek probe and the low-frequency
 * administrative views.
 *
 * <ul>
 *   <li><b>Data plane</b> (this interface): reads return the generic value
 *       directly — {@code null} = miss ("no value this time"); a loader's
 *       {@code null} result is <b>never cached</b> (no entry is written, the
 *       next read re-invokes the loader). A {@code null} options argument
 *       selects the same policy as the verb's no-options overload. Batch reads
 *       omit keys with no value (missing key = miss). A refresh renews a value
 *       without touching the entry's lifecycle policy; a TTL change is a
 *       {@code put}.</li>
 *   <li><b>Raw escape hatch</b>: {@link #unsafe()} exposes the single L1
 *       table (Caffeine {@code Cache<String, CacheEntry>}) for callers whose
 *       needs do not fit the verbs — eviction introspection, bulk scans,
 *       diagnostic reads. One-off loaders and readers travel with the call;
 *       there is no second cache API to learn.</li>
 *   <li><b>Administrative plane</b> ({@link #stats()}, {@link #detector()},
 *       {@link #unsafe()}): L1 statistics and local-TopK detection queries
 *       behind small views that can be replaced independently (Caffeine's
 *       {@code Cache.policy()} pattern).</li>
 * </ul>
 *
 * <p>
 * <b>Sync semantics:</b> {@code sync} knobs ({@link PutPolicy#synced()},
 * {@link InvalidatePolicy#localOnly()}) control <b>cross-instance
 * synchronization</b> — a fire-and-forget fan-out tolerated by TTL convergence,
 * never a blocking wait for peer acknowledgement.
 *
 * <p>
 * <b>Argument validation:</b> blank or null keys, null required callbacks or collections,
 * and negative TTLs or top-K limits are rejected before any cache work — {@link IllegalArgumentException}
 * for a null or blank key, a non-positive tag count, a negative TTL, a negative top-K limit or another invalid
 * value; {@link NullPointerException} for a null required callback, value or collection
 * argument. A read policy's TTL suppliers are lazy (see {@link GetPolicy}); a negative
 * result is rejected when the read first needs that TTL, because a plain cache hit never
 * evaluates the supplier at all.
 *
 * <p>
 * <b>Thread safety:</b> all methods are thread-safe. A read is linearized at its own
 * lookup and is not re-evaluated against later mutations: a {@code get} whose loader is
 * already running may return the value it loaded even though a concurrent {@code put},
 * {@code refresh} or invalidation has since landed. The newer mutation owns the L1 entry;
 * the in-flight caller is neither cancelled nor made to wait for the mutation.
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
 * {@code HotKeyCache} over one Caffeine {@code Cache<String, CacheEntry>}.
 */
@SuppressWarnings("unused")
public interface Zeta {
  /**
   * How much of the access-recording pipeline a tag call performs: the call site states its
   * intent instead of passing positional flags.
   */
  enum TagMode {
    /** Shared counter: local add and Worker report (default). */
    RECORD,
    /** Direct local add only; skip the Worker report. */
    DETECT_ONLY,
    /** Direct Worker report only; skip the local add. */
    REPORT_ONLY,
    /** Neither (validated no-op). */
    SILENT,
  }

  /**
   * Tag a cache key with an explicit access count and recording mode — the canonical
   * single-key tagging primitive.
   *
   * <p>{@link TagMode#RECORD} feeds the shared {@code WaveCounter}, so the hint is merged
   * with concurrent reads and reported on the normal tide; {@code DETECT_ONLY} updates the
   * local sketch directly, {@code REPORT_ONLY} publishes directly and {@code SILENT} is a
   * no-op. No mode touches the L1 cache or invokes a reader.
   *
   * <p>Fire-and-forget: a saturated or shedding detector drops the hint instead of throwing.
   *
   * @param cacheKey the key to tag (must not be null or blank)
   * @param count    the number of accesses to add (must be positive)
   * @param mode     how much of the pipeline to run (never {@code null})
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when {@code cacheKey} is null/blank or {@code count} is not positive
   * @throws NullPointerException     when {@code mode} is {@code null}
   */
  void tag(String cacheKey, long count, TagMode mode);

  /**
   * Tag a cache key once on the full pipeline ({@link TagMode#RECORD}).
   *
   * @param cacheKey the key to tag (must not be null or blank)
   */
  default void tag(String cacheKey) {
    tag(cacheKey, 1L, TagMode.RECORD);
  }

  /**
   * Tag a cache key once with an explicit recording mode.
   *
   * @param cacheKey the key to tag (must not be null or blank)
   * @param mode     how much of the pipeline to run (never {@code null})
   */
  default void tag(String cacheKey, TagMode mode) {
    tag(cacheKey, 1L, mode);
  }

  /**
   * Tag a cache key {@code count} times on the full pipeline ({@link TagMode#RECORD}).
   *
   * @param cacheKey the key to tag (must not be null or blank)
   * @param count    the number of accesses to add (must be positive)
   */
  default void tag(String cacheKey, long count) {
    tag(cacheKey, count, TagMode.RECORD);
  }

  /**
   * Batch form of {@link #tag(String, long, TagMode)}: record weighted access counts for
   * multiple keys under one shared mode.
   *
   * <p>Validation is all-or-nothing: every key and count is checked before the snapshot
   * reaches the counter. A null/blank key or a non-positive count rejects the whole call;
   * an empty map is a no-op after the mode and deployment checks. The facade copies the
   * map while validating, so a concurrent caller mutation cannot inject an unchecked row.
   *
   * @param keyCounts a map of cacheKey to a positive access count (never {@code null})
   * @param mode      how much of the pipeline to run (never {@code null})
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when any key is null/blank or any count is not positive
   * @throws NullPointerException     when {@code keyCounts}, {@code mode} or a count value is {@code null}
   */
  void tagAll(Map<String, Long> keyCounts, TagMode mode);

  /**
   * Batched weighted tagging on the full pipeline ({@link TagMode#RECORD}).
   *
   * @param keyCounts a map of cacheKey to a positive access count (never {@code null})
   */
  default void tagAll(Map<String, Long> keyCounts) {
    tagAll(keyCounts, TagMode.RECORD);
  }

  /**
   * Batch tag a collection of keys once each under one shared mode. Duplicate keys are
   * summed into one count; the facade builds the snapshot once and delegates to
   * {@link #tagAll(Map, TagMode)}, so the same all-or-nothing validation applies.
   *
   * @param cacheKeys the keys to tag; every element must be non-blank (never {@code null})
   * @param mode      how much of the pipeline to run (never {@code null})
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when any element is null or blank
   * @throws NullPointerException     when {@code cacheKeys} or {@code mode} is {@code null}
   */
  default void tagAll(Collection<String> cacheKeys, TagMode mode) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Objects.requireNonNull(mode, "mode must not be null");
    Map<String, Long> counts = new LinkedHashMap<>((int) (cacheKeys.size() / 0.75f) + 1);
    for (String cacheKey : cacheKeys) {
      counts.merge(cacheKey, 1L, Long::sum);
    }
    tagAll(counts, mode);
  }

  /**
   * Look up a cached value without loading or triggering hot-key detection.
   * {@code null} = miss (same contract as {@link #get}).
   *
   * @param cacheKey the key to look up
   * @param <T>      the value type
   * @return the cached value, or {@code null} on a miss
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   */
  <T> @Nullable T peek(String cacheKey);

  /**
   * One-off get with a typed loader: the loader (including its
   * {@link CacheLoader#isCacheable admission} decision) travels with the
   * call; options carry only TTL/semantics overrides. The admission filter
   * is derived from the loader's {@code isCacheable}, so a caller cannot
   * override it through the read options; {@code GetPolicy} carries no
   * filter at all.
   *
   * <p>The engine calls {@link CacheLoader#reload} (not {@link CacheLoader#load})
   * for its background refresh.
   *
   * <p><b>Loader thread contract.</b> With {@link GetPolicy#callerRunsLoader()}
   * set, a load this call performs runs on the calling thread and the read
   * returns only after that invocation completes. A same-key follower that joins
   * another call's in-flight load performs no load of its own. A loader that
   * owns caller-bound state — a one-shot invocation chain, a transaction, a
   * security context — therefore requires caller-runs and must not expect its
   * {@code load} to be replayed.
   *
   * <p>The options' TTL suppliers are lazy: a plain cache hit never evaluates
   * them, and a negative result is rejected with
   * {@link IllegalArgumentException} when the read first needs that TTL.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value loader for cache misses / refreshes
   * @param options  the per-invocation read options ({@code null} =
   *                 {@link GetPolicy#defaults()})
   * @param <V>      the value type
   * @return the cached or loaded value; {@code null} = miss
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   */
  <V> @Nullable V get(String cacheKey, CacheLoader<String, V> loader, @Nullable GetPolicy options);

  /**
   * One-off get with a plain reader — the convenience form with
   * {@code isCacheable} always true (everything non-null is cached). To
   * serve-but-not-store specific values, pass a full
   * {@link CacheLoader} with {@code isCacheable} instead.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses
   * @param options  the per-invocation read options ({@code null} =
   *                 {@link GetPolicy#defaults()})
   * @param <V>      the value type
   * @return the cached or loaded value; {@code null} = miss
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   */
  <V> @Nullable V get(String cacheKey, Supplier<V> reader, @Nullable GetPolicy options);

  /**
   * Get with a typed loader and the default read options.
   *
   * @param cacheKey the key to retrieve
   * @param loader   the value loader
   * @param <V>      the value type
   * @return the cached or loaded value; {@code null} = miss
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  default <V> @Nullable V get(String cacheKey, CacheLoader<String, V> loader) {
    return get(cacheKey, loader, GetPolicy.defaults());
  }

  /**
   * Get with a plain reader and the default read options.
   *
   * @param cacheKey the key to retrieve
   * @param reader   the value supplier for cache misses
   * @param <V>      the value type
   * @return the cached or loaded value; {@code null} = miss
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  default <V> @Nullable V get(String cacheKey, Supplier<V> reader) {
    return get(cacheKey, reader, GetPolicy.defaults());
  }

  /**
   * Batch get with a reader — the full-control form. All keys share the same
   * options; the options' TTLs are evaluated and validated once for the whole
   * batch — before any key is read, even when {@code cacheKeys} is empty — and
   * the positional reader governs loading. Null and blank keys are skipped and —
   * like a miss — omitted from the result. No admission filter is applied —
   * every non-{@code null} value is stored.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param options   the shared per-invocation read options ({@code null} =
   *                  {@link GetPolicy#defaults()})
   * @param <V>       the value type
   * @return a map of key → loaded or cached value; keys with no value are
   *         omitted
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when a TTL in {@code options} is negative
   * @throws RuntimeException         the first reader failure cause; the remaining keys
   *                                  are still processed and their failures suppressed
   */
  <V> Map<String, V> getAll(
    Collection<String> cacheKeys,
    Function<? super String, ? extends V> reader,
    @Nullable GetPolicy options
  );

  /**
   * Batch get with the default read options — see
   * {@link #getAll(Collection, Function, GetPolicy)}.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param <V>       the value type
   * @return a map of key → loaded or cached value; keys with no value are
   *         omitted
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   * @throws RuntimeException  the first reader failure cause; the remaining keys
   *                           are still processed and their failures suppressed
   */
  default <V> Map<String, V> getAll(Collection<String> cacheKeys, Function<? super String, ? extends V> reader) {
    return getAll(cacheKeys, reader, GetPolicy.defaults());
  }

  /**
   * Pure cache write. There is no business writer and no load: "mutate the
   * source, then update the cache" orchestration belongs to the caller.
   * The default is local-only;
   * {@link PutPolicy#synced()} upgrades the call to a version-stamped
   * cluster write: the writer bumps a {@code dataVersion} and broadcasts the
   * value in a fire-and-forget PUT, which peers apply version-guarded without
   * reloading. Values that are not remotely
   * serializable or exceed {@code zeta.sync.put-max-bytes} degrade to a
   * value-less REFRESH inside the same broadcast.
   * Transaction-aware: inside a Spring transaction the write waits for
   * commit (rollback drops it).
   *
   * <p>
   * {@code put(key, null)} throws {@link NullPointerException} — null is
   * never cached. On a Worker-managed (HOT/COOL) entry only the value is
   * updated — the Worker's TTL regime is kept. Values are stored as-is (no
   * value codec).
   *
   * @param cacheKey the key to write
   * @param value    the value to cache (never {@code null})
   * @param options  the write options ({@code null} = {@link PutPolicy#defaults()})
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws NullPointerException     when value is {@code null} (null is never
   *                                  cached)
   * @throws IllegalArgumentException when a TTL is negative, or an effective
   *                                  permanent hard TTL ({@link Long#MAX_VALUE})
   *                                  is combined with sync
   */
  void put(String cacheKey, Object value, @Nullable PutPolicy options);

  /**
   * Pure cache write with default options (no TTL override, local-only).
   * Equivalent to {@code put(cacheKey, value, PutPolicy.defaults())}.
   *
   * @param cacheKey the key to write
   * @param value    the value to cache (never {@code null})
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws NullPointerException when value is {@code null}
   */
  default void put(String cacheKey, Object value) {
    put(cacheKey, value, PutPolicy.defaults());
  }

  /**
   * Batch form of {@link #put(String, Object, PutPolicy)}: one policy for the
   * whole map, one transaction boundary, and one contiguous version block per
   * chunk of {@link io.github.hyshmily.zeta.util.clock.StampClock#MAX_ATOMIC_RANGE}
   * keys.
   *
   * <p><b>Validation is all-or-nothing.</b> The map, every key and every value
   * are checked before any cache work; a null/blank key or null value rejects
   * the whole call, so a caller error never writes a prefix of the batch. An
   * empty map is a no-op after the shared options are validated: an invalid TTL
   * or an unsyncable permanent write still fails.
   *
   * <p><b>Not atomic.</b> A failure after some keys were applied leaves those
   * keys written. The guarantee is version ordering, not all-or-nothing: a
   * concurrent single write is ordered against the whole chunk (older than every
   * chunk stamp or newer than every one), never interleaved. Use a surrounding
   * transaction when all-or-nothing is required.
   *
   * <p>{@code sync} broadcasts each applied value independently. There is no
   * batch PUT wire message; a large batch therefore produces one PUT per key.
   * The entries are copied into an insertion-ordered snapshot while they are
   * validated, so a concurrent mutation of the caller's map is not observed and
   * cannot inject an unchecked row into the write.
   *
   * @param entries the keys and values to write; every key must be non-blank and
   *                every value non-null
   * @param options the shared write options ({@code null} =
   *                {@link PutPolicy#defaults()})
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when a key is null/blank or a TTL is
   *                                  negative, or a synced write resolves to a
   *                                  permanent hard TTL
   * @throws NullPointerException     when the map or a value is null
   */
  void putAll(Map<String, ?> entries, @Nullable PutPolicy options);

  /**
   * Batch write with default options (no TTL override, local-only).
   * Equivalent to {@code putAll(entries, PutPolicy.defaults())}.
   *
   * @param entries the keys and values to write
   */
  default void putAll(Map<String, ?> entries) {
    putAll(entries, PutPolicy.defaults());
  }

  /**
   * Invalidate a single key from L1. Sync is <b>on</b> by default
   * (correctness semantics: peers drop their copy immediately); turning it
   * off with {@link InvalidatePolicy#localOnly()} leaves cluster convergence
   * to hard-TTL expiry and is only correct for single-instance deployments.
   *
   * <p><b>Peer convergence is best-effort and bounded by TTL, not by latency.</b>
   * Outside an active transaction this node's entry is removed synchronously; inside one,
   * the default {@code immediate=false} defers the removal to {@code afterCommit} (pass
   * {@link InvalidatePolicy#immediate(boolean)} to bypass the deferral). The deferral also
   * covers {@link InvalidatePolicy.Scope#REMOTE}: a rollback sends no peer invalidation at
   * all. Peers are told through a fire-and-forget broadcast with no publisher confirms.
   * A peer that misses the message stays stale until
   * the entry's remaining hard TTL expires — default 5min, 1h while a Worker holds the
   * key HOT. A read already in flight when the removal runs may still return the value it
   * loaded, but that value is not stored.
   *
   * @param cacheKey the key to invalidate
   * @param options  the invalidation options ({@code null} =
   *                 {@link InvalidatePolicy#defaults()})
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  void invalidate(String cacheKey, @Nullable InvalidatePolicy options);

  /**
   * Invalidate one key with the default options (cluster sync on).
   *
   * @param cacheKey the key to invalidate
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  default void invalidate(String cacheKey) {
    invalidate(cacheKey, InvalidatePolicy.defaults());
  }

  /**
   * Invalidate a collection of keys from L1 — see
   * {@link #invalidate(String, InvalidatePolicy)}. Null and blank elements are
   * skipped, not rejected (an all-invalid collection is a no-op), so a batch
   * built from a query result needs no pre-filtering.
   *
   * @param cacheKeys the keys to invalidate
   * @param options   the invalidation options ({@code null} =
   *                  {@link InvalidatePolicy#defaults()})
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  void invalidate(Collection<String> cacheKeys, @Nullable InvalidatePolicy options);

  /**
   * Invalidate a collection of keys with the default options (cluster sync on).
   *
   * @param cacheKeys the keys to invalidate; null and blank elements are skipped
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  default void invalidate(Collection<String> cacheKeys) {
    invalidate(cacheKeys, InvalidatePolicy.defaults());
  }

  /**
   * Clear the whole L1 cache across the cluster: remove every local entry and
   * send exactly one {@code INVALIDATE_ALL} message, independent of how many
   * keys this instance holds.
   *
   * <p>This is the table-wide flush. It is deliberately <b>not</b> a namespace clear:
   * the annotation verbs keep every operation scoped to its cache namespace, and
   * {@link #clearPrefix(String, InvalidatePolicy)} /
   * {@link #clearNamespace(String, InvalidatePolicy)} sweep one namespace
   * without touching the others. Use this method only when every namespace must
   * go.
   *
   * <p>Scope selects the blast radius: {@link InvalidatePolicy.Scope#LOCAL}
   * sweeps this JVM only and never broadcasts;
   * {@link InvalidatePolicy.Scope#REMOTE} broadcasts without sweeping. Either
   * scope defers to {@code afterCommit} inside an active transaction unless the
   * policy's {@code immediate} flag is set.
   *
   * <p>A local sweep keeps a write allocated after the clear stamp, exactly as peers keep
   * it; the local sweep and the peer guard share the same version rule. A local sweep also
   * drops the matching in-flight loads, so a read that starts after it cannot join a load
   * that began before it; a {@link InvalidatePolicy.Scope#REMOTE} clear applies the same
   * kill on each receiving peer.
   *
   * @param options the clear options ({@code null} = {@link InvalidatePolicy#defaults()})
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  void clearAll(@Nullable InvalidatePolicy options);

  /**
   * Clear the whole L1 cache across the cluster with the default invalidation
   * options — see {@link #clearAll(InvalidatePolicy)}.
   *
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  default void clearAll() {
    clearAll(InvalidatePolicy.defaults());
  }

  /**
   * Clear every local entry whose physical key starts with {@code prefix}, and
   * optionally broadcast one {@code INVALIDATE_PREFIX} to peers.
   *
   * <p>This is the scoped counterpart of {@link #clearAll(InvalidatePolicy)}:
   * it sweeps one key namespace without evicting unrelated namespaces. The
   * prefix is a <b>physical key prefix</b>; programmatic keys are stored exactly
   * as passed, while annotation keys are {@code cache + NUL + encodedKey}, where
   * the encoding is the canonical default-key form (see
   * {@link #clearNamespace(String, InvalidatePolicy)} for the annotation form).
   *
   * <p>{@link InvalidatePolicy.Scope#LOCAL} sweeps locally without broadcasting;
   * {@link InvalidatePolicy.Scope#BOTH} sweeps locally and broadcasts;
   * {@link InvalidatePolicy.Scope#REMOTE} broadcasts without touching local
   * entries. Either scope defers to {@code afterCommit} inside an active
   * transaction unless the policy's {@code immediate} flag is set. A local sweep
   * is version-guarded: an entry written after the clear stamp survives, exactly
   * as a peer's post-clear write does. The sweep is best-effort termination, not
   * a linearizable barrier; the recorded prefix watermark refuses later fills
   * that started before the clear, and a local sweep drops the prefix's in-flight
   * loads so a read that starts after it cannot join one (a
   * {@link InvalidatePolicy.Scope#REMOTE} clear applies the same kill on the
   * receiving peer).
   *
   * @param prefix  the physical key prefix; must not be null or blank
   * @param options the invalidation options ({@code null} = defaults)
   * @throws IllegalArgumentException when {@code prefix} is null or blank
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   */
  void clearPrefix(String prefix, @Nullable InvalidatePolicy options);

  /**
   * Clear every local entry under {@code prefix}, including peers, with the
   * default invalidation options.
   *
   * @param prefix the physical key prefix; must not be null or blank
   */
  default void clearPrefix(String prefix) {
    clearPrefix(prefix, InvalidatePolicy.defaults());
  }

  /**
   * Clear one annotation namespace: equivalent to
   * {@code clearPrefix(cache + NUL, options)}.
   *
   * <p>The namespace delimiter is an implementation detail; callers address
   * the same {@code cache} value they used on {@code @ZetaCached},
   * {@code @ZetaPut} or {@code @ZetaInvalidate}.
   *
   * @param cache   the annotation namespace; must not be null or blank
   * @param options the invalidation options ({@code null} = defaults)
   * @throws IllegalArgumentException when {@code cache} is null or blank
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   */
  default void clearNamespace(String cache, @Nullable InvalidatePolicy options) {
    if (cache == null || cache.isBlank()) {
      throw new IllegalArgumentException("cache must not be blank");
    }
    clearPrefix(CacheKeysPolicy.namespacePrefix(cache), options);
  }

  /**
   * Clear one annotation namespace, including peers, with the default
   * invalidation options.
   *
   * @param cache the annotation namespace; must not be null or blank
   */
  default void clearNamespace(String cache) {
    clearNamespace(cache, InvalidatePolicy.defaults());
  }

  /**
   * Reload the given key from the source and apply it through the write-through
   * guard: the version is allocated <i>before</i> the load, then the loaded
   * value is applied with a fire-and-forget broadcast when {@code sync} is set.
   *
   * <p><b>Value-only renewal.</b> A resident entry keeps its lifecycle policy:
   * a {@code HOT}/{@code COOL} entry keeps its state, Worker decision lease and
   * hard deadline (only the value, version and soft window move), and a
   * {@code NORMAL} entry keeps its stored normal hard/soft TTL baseline. Only
   * an absent or hard-expired key is rebuilt from the configured default TTLs.
   * Changing a TTL is a {@code put} (or invalidate + get), never a refresh.
   *
   * <p><b>Ordering note:</b> the version is allocated before the load, so a
   * concurrent synced {@code put} landing during the load holds the newer
   * version and this refresh's stale apply is skipped instead of regressing
   * it; the skipped value is not broadcast either. A {@code null} loader result
   * <b>skips the refresh</b>: the current entry stays — a refresh that learned
   * nothing must not evict the value it was asked to renew.
   *
   * @param cacheKey the key to refresh
   * @param reader   the value supplier, run on the caller thread (or the
   *                 after-commit thread inside a transaction)
   * @param sync     whether to broadcast the applied value to peers
   * @param <V>      the value type
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when the key is blank, or a synced refresh
   *                                  resolves to a permanent hard TTL
   * @throws NullPointerException     when the reader is null
   */
  <V> void refresh(String cacheKey, Supplier<V> reader, boolean sync);

  /**
   * Reload the given key through {@link CacheLoader#reload} and apply it under
   * the same value-only contract as {@link #refresh(String, Supplier, boolean)}.
   * The loader's {@link CacheLoader#isCacheable admission} is honored: a
   * declined value (like a {@code null} reload) skips the apply and leaves the
   * resident entry untouched.
   *
   * <p>{@code reload}, not {@code load}, is the refresh seam: a refresh can run
   * on an after-commit or caller-owned scheduler thread where the original
   * call's transaction, security context and MDC are gone. Pass a
   * {@link Supplier} when the load needs the caller's context.
   *
   * @param cacheKey the key to refresh
   * @param loader   the loader whose {@code reload} produces the value
   * @param sync     whether to broadcast the applied value to peers
   * @param <V>      the value type
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when the key is blank, or a synced refresh
   *                                  resolves to a permanent hard TTL
   * @throws NullPointerException     when the loader is null
   */
  <V> void refresh(String cacheKey, CacheLoader<String, V> loader, boolean sync);

  /**
   * Refresh the given key locally.
   * Equivalent to {@code refresh(cacheKey, reader, false)}.
   *
   * @param cacheKey the key to refresh
   * @param reader   the value supplier
   * @param <V>      the value type
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  default <V> void refresh(String cacheKey, Supplier<V> reader) {
    refresh(cacheKey, reader, false);
  }

  /**
   * Refresh the given key locally through {@code CacheLoader#reload}.
   * Equivalent to {@code refresh(cacheKey, loader, false)}.
   *
   * @param cacheKey the key to refresh
   * @param loader   the loader whose {@code reload} produces the value
   * @param <V>      the value type
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  default <V> void refresh(String cacheKey, CacheLoader<String, V> loader) {
    refresh(cacheKey, loader, false);
  }

  /**
   * Batch variant of {@link #refresh(String, Supplier, boolean)}: refreshes
   * all entries with the same sync flag. A {@code CacheLoader} batch form is not
   * offered because two {@code Map}-shaped overloads erase to the same
   * signature; call {@link #refresh(String, CacheLoader, boolean)} per key for
   * loader-based renewal.
   *
   * <p>Per-key isolation: one failing loader does not abort the batch — a failed
   * key is collected and the fan-out keeps submitting while the executor
   * accepts. The first failure is rethrown after the fan-out settles, with the
   * rest attached as suppressed. A submission failure stops further submission
   * but still waits for every task already accepted before rethrowing.
   *
   * <p>Argument validation (blank key, null loader) runs before any task is
   * submitted, so a caller error never enters the fan-out. The validated entries
   * are snapshotted before submission: a concurrent map mutation cannot inject
   * an unvalidated key or loader between validation and dispatch. The mode and
   * sync checks run even for an empty map.
   *
   * <p>The fan-out runs on the injected refresh executor, whose admission
   * policy owns back-pressure; a rejected submission stops the fan-out and is
   * reported after every accepted task settles. A single-entry batch runs
   * inline on the calling thread.
   *
   * @param loaders a map of key → value supplier
   * @param sync    whether to broadcast each applied value to peers
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when a key is null/blank, or a synced
   *                                  refresh resolves to a permanent hard TTL
   * @throws NullPointerException     when the map or a loader is null
   */
  void refreshAll(Map<String, Supplier<?>> loaders, boolean sync);

  /**
   * Batch refresh with the local-only default.
   * Equivalent to {@code refreshAll(loaders, false)}.
   *
   * @param loaders a map of key → value supplier
   */
  default void refreshAll(Map<String, Supplier<?>> loaders) {
    refreshAll(loaders, false);
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
  boolean isCacheAvailable();

  /**
   * Raw escape hatch over the single L1 table (Caffeine's
   * {@code Cache.policy()} pattern). The facade stops one step short of the
   * Caffeine handle on purpose: everything the data verbs do not cover —
   * eviction-policy introspection, bulk scans, diagnostic entry reads — goes
   * through this view instead of growing the verb surface.
   *
   * <p>Deletes, puts and loads that must respect version stamping, peer
   * broadcast or admission have no business here; use the data verbs.
   *
   * @return the raw-table view
   */
  UnsafeAdmin unsafe();

  /**
   * L1 statistics snapshot: request hit/miss counters, eviction count and
   * estimated size. In Worker-only mode throws {@link ZetaModeException}
   * (fail fast; a null return would manufacture NPEs at callers). Raw Caffeine
   * stats and size go through {@link #unsafe()}.
   */
  ZetaCacheStats stats();

  /**
   * Detection-observation view: local-TopK queries. Feeds do not go through
   * this view — they go through {@link #tag(String, long, TagMode)}. In Worker-only
   * mode {@code isLocalHotKey} throws {@link ZetaModeException} while pure
   * queries answer with empty defaults.
   */
  DetectorAdmin detector();

  /**
   * Raw access to the single L1 Caffeine table. The raw handle protects
   * nothing: the caller owns every guarantee {@link #raw()} says it skips.
   *
   * <p>Reading is unmodified — {@code getIfPresent} returns the live
   * {@link CacheEntry} wrapper (unwrap the value yourself; a HOT entry past
   * its soft TTL still reads as present until the async refresh lands). A write
   * skips version allocation, peer broadcast, admission and the Worker
   * decision stamp, so a raw write can diverge from peers until the entry's
   * hard TTL expires. Do not build a second caching layer on top.
   */
  interface UnsafeAdmin {
    /**
     * The live L1 table: physical keys — exactly what the facade verbs were
     * given — and {@link CacheEntry} wrappers rather than user values.
     *
     * @return the live table (never {@code null})
     * @throws ZetaModeException when no cache is available (Worker-only mode)
     */
    Cache<String, CacheEntry> raw();

    /**
     * Companion to a raw removal or overwrite: mark the given keys' in-flight
     * loads superseded so a leader that began before the raw mutation does not
     * commit its pre-mutation value. Touches nothing else — no L1 effect, no
     * broadcast; in-flight loads still complete.
     * The data verbs wire the same forget into every removal; a raw mutation is
     * the one path that cannot.
     *
     * @param cacheKeys the keys whose dedup entries to drop (never {@code null})
     * @throws IllegalArgumentException when any key is null or blank
     * @throws ZetaModeException        when no cache is available (Worker-only mode)
     */
    void forgetDedup(String... cacheKeys);
  }

  /**
   * Detection-observation view: local-TopK queries. This is a pure query
   * surface — feeds go through {@link #tag(String, long, TagMode)}, never through
   * this view.
   *
   * <p>In Worker-only mode {@code isLocalHotKey} throws
   * {@link ZetaModeException}; a valid pure query answers with an empty
   * default.
   */
  interface DetectorAdmin {
    /**
     * Check whether a key is currently hot on this instance — either resident
     * in L1 with {@link io.github.hyshmily.zeta.model.KeyState#HOT} (and not
     * logically expired) or tracked by the local TopK detector (promotable
     * but possibly not yet resident).
     *
     * @param cacheKey the key to inspect
     * @return {@code true} if the key is L1-HOT or detector-tracked
     * @throws ZetaModeException when no cache is available (Worker-only mode)
     */
    boolean isLocalHotKey(String cacheKey);

    /**
     * Read-only snapshot of the top {@code limit} local hot keys, ordered by
     * frequency. A negative limit is rejected in every deployment mode, before
     * the detector lookup; {@code 0} returns an empty list.
     *
     * @param limit the maximum number of top keys to return; must not be negative
     * @return the top N snapshot, or an empty list if no detector is available
     * @throws IllegalArgumentException when {@code limit} is negative
     */
    List<Item> localTopKeys(int limit);
  }
}
