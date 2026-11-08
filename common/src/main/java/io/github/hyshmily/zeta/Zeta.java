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
import io.github.hyshmily.zeta.model.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Public facade of the Zeta library — the sole API entry point, shaped as
 * the Caffeine-style minimal surface: three data verbs
 * (<b>get / put / invalidate</b>), each with one closed options record
 * ({@link GetPolicy}, {@link PutPolicy},
 * {@link InvalidatePolicy}), their batch forms ({@code getAll}, {@code putAll},
 * the collection and clear invalidations), plus a peek probe and the low-frequency
 * administrative views.
 *
 * <ul>
 *   <li><b>Data plane</b> (this interface): reads return the generic value
 *       directly — {@code null} = miss ("no value this time"); a loader's
 *       {@code null} result is <b>never cached</b> (no entry is written, the
 *       next read re-invokes the loader). {@code null} options arguments are
 *       interpreted as that family's {@code defaults()}. Batch reads omit
 *       keys with no value (missing key = miss).</li>
 *   <li><b>Raw escape hatch</b>: {@link #unsafe()} exposes the single L1
 *       table (Caffeine {@code Cache<String, CacheEntry>}) for callers whose
 *       needs do not fit the verbs — eviction introspection, bulk scans,
 *       diagnostic reads. One-off loaders and readers travel with the call;
 *       there is no second cache API to learn.</li>
 *   <li><b>Administrative plane</b> ({@link #refresh()}, {@link #stats()},
 *       {@link #detector()}, {@link #unsafe()}): timed refresh registration,
 *       L1 statistics, and local-TopK detection queries behind small views
 *       that can be replaced independently (Caffeine's
 *       {@code Cache.policy()} pattern).</li>
 * </ul>
 *
 * <p>
 * <b>Sync semantics:</b> {@code sync} knobs ({@link PutPolicy#synced()},
 * {@link InvalidatePolicy#localOnly()}) control <b>cross-instance
 * synchronization</b> — fire-and-forget fan-out tolerated by TTL convergence,
 * never a blocking wait for peer acknowledgement (design decisions: sync messages).
 *
 * <p>
 * <b>Argument validation:</b> blank or null keys, null required callbacks or collections,
 * and negative TTLs are rejected before any cache work — {@link IllegalArgumentException}
 * for a null or blank key, a non-positive tag count, a negative TTL or another invalid
 * value; {@link NullPointerException} for a null required callback, value or collection
 * argument.
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
   * How much of the access-recording pipeline a {@link #tag(String, TagMode)} call performs: the
   * call site states its intent instead of passing positional flags.
   */
  enum TagMode {
    /** Local HeavyKeeper add and Worker report (default). */
    RECORD,
    /** Local add only; skip the Worker report. */
    DETECT_ONLY,
    /** Worker report only; skip the local add. */
    REPORT_ONLY,
    /** Neither (explicit no-op). */
    SILENT;

    /**
     * Declaration order encodes the two skip flags as {@code (skipDetection << 1) | skipReport};
     * {@link #of} and the two predicates rely on that layout.
     */
    private static final TagMode[] VALUES = values();

    /** Whether this mode skips the local HeavyKeeper increment. */
    public boolean skipDetection() {
      return (ordinal() & 2) != 0;
    }

    /** Whether this mode skips the Worker report. */
    public boolean skipReport() {
      return (ordinal() & 1) != 0;
    }

    /**
     * Map a {@code (skipDetection, skipReport)} pair to the equivalent mode.
     *
     * @param skipDetection whether to skip the local add
     * @param skipReport    whether to skip the Worker report
     * @return the corresponding mode
     */
    public static TagMode of(boolean skipDetection, boolean skipReport) {
      return VALUES[(skipDetection ? 2 : 0) | (skipReport ? 1 : 0)];
    }
  }

  /**
   * Tag a cache key as potentially hot, reporting it to the Worker without
   * performing a cache read.
   *
   * <p>
   * Updates the local HeavyKeeper sketch and enqueues the key for batch
   * reporting to the Worker, exactly as {@link #get} would, but without
   * touching the L1 cache or invoking any reader/loader. Fire-and-forget:
   * a saturated or shedding detector drops the hint instead of throwing, so
   * audit-style tagging never breaks the business method.
   *
   * @param cacheKey the key to tag
   */
  void tag(String cacheKey);

  /**
   * Tag a cache key with an explicit recording mode — the low-level form for
   * callers that must suppress detection or reporting independently.
   * Application code should prefer {@link #tag(String)}.
   *
   * @param cacheKey the key to tag
   * @param mode     how much of the pipeline to run (never {@code null})
   */
  void tag(String cacheKey, TagMode mode);

  /**
   * Tag a key {@code count} times in one call — the bulk form of
   * {@link #tag(String)}, for a batch read or a paginated workload where one
   * business call represents several hits.
   *
   * <p>The counts land in the shared access counter and feed both local
   * detection and Worker reporting at tide time, so a batch replay behaves
   * exactly like the equivalent number of {@link #tag(String)} calls.
   *
   * @param cacheKey the key to tag
   * @param count    the number of accesses to add (must be positive)
   * @throws IllegalArgumentException when count is not positive
   */
  void tag(String cacheKey, long count);

  /**
   * Look up a cached value without loading or triggering hot-key detection.
   * {@code null} = miss (same contract as {@link #get}).
   *
   * @param cacheKey the key to look up
   * @param <T>      the value type
   * @return the cached value, or {@code null} on a miss
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   */
  <T> T peek(String cacheKey);

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
   * @param cacheKey the key to retrieve
   * @param loader   the value loader for cache misses / refreshes
   * @param options  the per-invocation read options ({@code null} =
   *                 {@link GetPolicy#defaults()})
   * @param <V>      the value type
   * @return the cached or loaded value; {@code null} = miss
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   */
  <V> V get(String cacheKey, CacheLoader<String, V> loader, GetPolicy options);

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
  <V> V get(String cacheKey, Supplier<V> reader, GetPolicy options);

  /**
   * Batch get with a reader — the full-control form. All keys share the same
   * options; the options' TTLs are evaluated once for the whole batch (the
   * batch pipeline has no per-key lazy-TTL path) and the positional reader
   * governs loading. Null and blank keys are skipped and — like a miss —
   * omitted from the result. No admission filter is applied — every
   * non-{@code null} value is stored.
   *
   * @param cacheKeys the keys to retrieve
   * @param reader    the value function for cache misses
   * @param options   the shared per-invocation read options ({@code null} =
   *                  {@link GetPolicy#defaults()})
   * @param <V>       the value type
   * @return a map of key → loaded or cached value; keys with no value are
   *         omitted
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws RuntimeException     the first reader failure cause; the remaining keys
   *                              are still processed and their failures suppressed
   */
  <V> Map<String, V> getAll(
    Collection<String> cacheKeys,
    Function<? super String, ? extends V> reader,
    GetPolicy options
  );

  /**
   * Pure cache write. There is no business writer and no load: "mutate the
   * source, then update the cache" orchestration belongs to the caller.
   * The default is local-only;
   * {@link PutPolicy#synced()} upgrades the call to a version-stamped
   * cluster write: the writer bumps a {@code dataVersion} and broadcasts the
   * value in a fire-and-forget PUT (design decisions: sync messages), which peers apply
   * version-guarded without reloading. Values that are not remotely
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
  void put(String cacheKey, Object value, PutPolicy options);

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
   * empty map is a no-op.
   *
   * <p><b>Not atomic.</b> A failure after some keys were applied leaves those
   * keys written. The guarantee is version ordering, not all-or-nothing: a
   * concurrent single write is ordered against the whole chunk (older than every
   * chunk stamp or newer than every one), never interleaved. Use a surrounding
   * transaction when all-or-nothing is required.
   *
   * <p>{@code sync} broadcasts each applied value independently. There is no
   * batch PUT wire message; a large batch therefore produces one PUT per key.
   * The map must not be mutated while this call is in progress (a deferred batch
   * is frozen, an inline one is iterated once).
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
  void putAll(Map<String, ?> entries, PutPolicy options);

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
   * all. Peers are told through a fire-and-forget broadcast with no publisher confirms
   * (design decisions: sync messages). A peer that misses the message stays stale until
   * the entry's remaining hard TTL expires — default 5min, 1h while a Worker holds the
   * key HOT. A read already in flight when the removal runs may still return the value it
   * loaded, but that value is not stored.
   *
   * @param cacheKey the key to invalidate
   * @param options  the invalidation options ({@code null} =
   *                 {@link InvalidatePolicy#defaults()})
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  void invalidate(String cacheKey, InvalidatePolicy options);

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
  void invalidate(Collection<String> cacheKeys, InvalidatePolicy options);

  /**
   * Clear the whole L1 cache across the cluster: remove every local entry and
   * send exactly one {@code INVALIDATE_ALL} message, independent of how many
   * keys this instance holds.
   *
   * <p>This is the table-wide flush (design decisions: invalidation, design
   * decisions: cache table). It is deliberately <b>not</b> a namespace clear:
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
  void clearAll(InvalidatePolicy options);

  /**
   * Clear every local entry whose physical key starts with {@code prefix}, and
   * optionally broadcast one {@code INVALIDATE_PREFIX} to peers.
   *
   * <p>This is the scoped counterpart of {@link #clearAll(InvalidatePolicy)}:
   * it sweeps one key namespace without evicting unrelated namespaces. The
   * prefix is a <b>physical key prefix</b>; programmatic keys are stored exactly
   * as passed, while annotation keys are {@code cache + NUL + logicalKey} (see
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
  void clearPrefix(String prefix, InvalidatePolicy options);

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
  default void clearNamespace(String cache, InvalidatePolicy options) {
    if (cache == null || cache.isBlank()) {
      throw new IllegalArgumentException("cache must not be blank");
    }
    clearPrefix(CacheKeysPolicy.namespacePrefix(cache), options);
  }

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
   * Clear one annotation namespace, including peers, with the default
   * invalidation options.
   *
   * @param cache the annotation namespace; must not be null or blank
   */
  default void clearNamespace(String cache) {
    clearNamespace(cache, InvalidatePolicy.defaults());
  }

  /**
   * Refresh the given key: allocate the version <i>before</i> the load, then
   * apply the loaded value through the write-through guard (version bump +
   * fire-and-forget broadcast when {@code options.sync()}).
   *
   * <p>
   * <b>Ordering note:</b> the version is allocated before the load, so a
   * concurrent synced {@code put} landing during the load holds the newer
   * version and this refresh's stale apply is skipped instead of regressing
   * it; the skipped value is not broadcast either. A {@code null} loader result
   * <b>skips the refresh</b> (DEBUG): the current entry stays — a
   * refresh that learned nothing must not evict the value it was asked to renew.
   *
   * @param cacheKey the key to refresh
   * @param loader   the value supplier
   * @param options  the write options ({@code null} = {@link PutPolicy#defaults()})
   * @param <V>      the value type
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when a TTL in {@code options} is negative, or
   *                                  a synced write resolves to a permanent hard TTL
   */
  <V> void refresh(String cacheKey, Supplier<V> loader, PutPolicy options);

  /**
   * Refresh the given key with default options (cluster sync on).
   *
   * @param cacheKey the key to refresh
   * @param loader   the value supplier
   * @param <V>      the value type
   * @throws ZetaModeException when no cache is available (Worker-only mode)
   */
  default <V> void refresh(String cacheKey, Supplier<V> loader) {
    refresh(cacheKey, loader, PutPolicy.synced());
  }

  /**
   * Batch variant of {@link #refresh(String, Supplier)}. Refreshes all entries
   * by loading and applying via the provided suppliers. Per-key isolation:
   * one failing loader does not abort the batch — a failed key is collected and
   * the fan-out keeps submitting while the executor accepts. The first failure
   * is rethrown after the fan-out settles, with the rest attached as suppressed.
   * Argument validation (blank key, null loader) runs before any task is
   * submitted, so a caller error never enters the fan-out.
   *
   * @param loaders a map of key → value supplier
   * @throws ZetaModeException        when no cache is available (Worker-only mode)
   * @throws IllegalArgumentException when the effective default hard TTL is permanent
   *                                  (every entry written by {@code refreshAll} is synced)
   */
  void refreshAll(Map<String, Supplier<?>> loaders);

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
   * Timed-refresh administration view: per-key scheduled background refresh
   * registration. In Worker-only mode registration throws
   * {@link ZetaModeException}.
   */
  RefreshAdmin refresh();

  /**
   * L1 statistics snapshot: request hit/miss counters, eviction count and
   * estimated size. In Worker-only mode throws {@link ZetaModeException}
   * (fail fast; a null return would manufacture NPEs at callers). Raw Caffeine
   * stats and size go through {@link #unsafe()}.
   */
  ZetaCacheStats stats();

  /**
   * Detection-observation view: local-TopK queries. Feeds do not go through
   * this view — they go through {@link #tag(String, TagMode)}. In Worker-only
   * mode {@code isLocalHotKey} throws {@link ZetaModeException} while pure
   * queries answer with empty defaults.
   */
  DetectorAdmin detector();

  /**
   * Timed-refresh administration: schedule ticks at {@code softTtl × 1.1} so
   * every cycle triggers an async refresh for the key. Re-registering a key
   * replaces its schedule; the scheduler is created lazily on first
   * registration.
   */
  interface RefreshAdmin {
    /** Register (or atomically replace) the timed refresh for the key. */
    <T> void registerRefresh(String key, Supplier<T> supplier, GetPolicy options);

    /** Cancel the timed refresh for the key (no-op when not registered). */
    void unregisterRefresh(String key);

    /** Register a timed background refresh with the configured default TTLs. */
    default <T> void registerRefresh(String key, Supplier<T> supplier) {
      registerRefresh(key, supplier, GetPolicy.defaults());
    }
  }

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
     * commit its pre-mutation value (design decisions: dedup lifecycle). Touches
     * nothing else — no L1 effect, no broadcast; in-flight loads still complete.
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
   * surface — feeds go through {@link #tag(String, TagMode)}, never through
   * this view.
   *
   * <p>In Worker-only mode {@code isLocalHotKey} throws
   * {@link ZetaModeException}; the pure queries answer with empty defaults.
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
     * frequency.
     *
     * @param limit the number of top keys to return
     * @return the top N snapshot, or an empty list if no detector is available
     */
    List<Item> localTopKeys(int limit);
  }
}
