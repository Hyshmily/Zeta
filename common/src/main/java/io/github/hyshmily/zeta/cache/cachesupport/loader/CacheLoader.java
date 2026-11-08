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
package io.github.hyshmily.zeta.cache.cachesupport.loader;

import org.jspecify.annotations.Nullable;

/**
 * Root loader SPI (design decisions: api surface): one object, three seams.
 * <ul>
 *   <li>{@link #load} answers "where does the data come from" — invoked on
 *       foreground cache misses.</li>
 *   <li>{@link #reload} answers the same question for a background refresh —
 *       invoked outside the caller's original call stack, possibly on another
 *       thread; defaults to {@link #load}.</li>
 *   <li>{@link #isCacheable} answers "may this value be stored" — the
 *       admission decision consulted before the engine writes a non-{@code null}
 *       value to L1 on the loader's behalf. The read path derives the admission
 *       filter from it, so a caller cannot smuggle an admission decision through
 *       the read options.</li>
 * </ul>
 *
 * <p><b>Cluster sync plane:</b> the cluster loader is supplied by the
 * application and may be absent. When present, the same SPI fetches
 * authoritative values for Worker HOT warm-up and peer REFRESH; when absent,
 * HOT promotes the current L1 entry when it has a usable one and a peer REFRESH
 * degrades to a local invalidation (a soft-expire for a Worker-managed entry).
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>Implementations must be thread-safe.</li>
 *   <li>Returning {@code null} signals "key absent at the data source" — a
 *       {@code null} result is never cached and never consults
 *       {@link #isCacheable}: the next read re-invokes the loader.</li>
 *   <li>Throwing an unchecked exception signals a load failure: the read path propagates it
 *       to the caller (cluster handlers treat it as a skipped load).</li>
 *   <li>A loader must not re-enter the same key while it is loading it: the
 *       nested read would join the in-flight load it is already inside. The
 *       engine detects that same-thread re-entry and fails it fast with
 *       {@link IllegalStateException}; it never waits on itself. Loading other
 *       keys is allowed, and a direct write during a load is safe — the version
 *       guard orders it.</li>
 * </ul>
 *
 * <p>Typical use:
 * <pre>{@code
 * User user = zeta.get("user:" + id, key -> repo.findById(id), GetPolicy.defaults());
 * // plain lambda loader: isCacheable defaults to true
 * }</pre>
 *
 * @param <K> the key type accepted by this loader ({@code String} on the
 *            facade plane)
 * @param <V> the value type produced by this loader
 * @see io.github.hyshmily.zeta.Zeta
 */
@FunctionalInterface
public interface CacheLoader<K, V> {
  /**
   * Load the value for the given key from the application data source — the
   * foreground miss path. Runs on the calling thread when the read asks for
   * caller-runs, otherwise on the load executor.
   *
   * @param key the key to load; never {@code null}
   * @return the loaded value, or {@code null} if the key does not exist
   */
  @Nullable
  V load(K key);

  /**
   * Reload the value for the given key — the <b>background-refresh</b> entry
   * point (soft-expire refresh / engine-triggered reload). Invoked outside the
   * caller's original call stack and possibly on another thread, including the
   * caller's own thread when the refresh executor runs inline
   * ({@code CallerRunsPolicy}, a synchronous executor).
   *
   * <p>Implementations that carry caller-bound state — a one-shot invocation
   * chain, a thread-confined transaction or security context — override this
   * method to produce the value without that state. Resuming such state from a
   * refresh is never valid: the state belongs to a call that has already
   * returned or is still in progress.
   *
   * <p>Defaults to {@link #load}, so plain lambdas and method references keep
   * working as loaders. {@link #load} remains the foreground miss path.
   *
   * <p><b>{@code null} means "no replacement", not "remove".</b> The refresh
   * leg skips the apply and the stale entry keeps serving until its hard TTL:
   * a refresh that learned nothing must not turn a servable value into a miss.
   * A key that is gone for good resolves through the next foreground load
   * after the hard TTL expires.
   *
   * @param key the key to reload; never {@code null}
   * @return the reloaded value, or {@code null} when no replacement should be
   *         applied
   */
  default @Nullable V reload(K key) {
    return load(key);
  }

  /**
   * Whether the engine may store a value it obtained on this loader's behalf.
   * Consulted at every write the engine performs for the loader:
   * <ul>
   *   <li>a foreground load or a background refresh, before the read path
   *       stores the returned value;</li>
   *   <li>Worker HOT warm-up and a peer {@code REFRESH} through the cluster
   *       loader, and a peer {@code PUT}'s carried value when a cluster loader
   *       bean is registered.</li>
   * </ul>
   *
   * <p>{@code false} still serves the value to the caller but skips the L1
   * store: the next read re-invokes the loader. Detection and Worker reporting
   * are unaffected — a rejected key still counts toward hot-key statistics.
   *
   * <p><b>Not a global cacheability switch.</b> An explicit {@code put} stores
   * the caller's value regardless of this predicate, and a TTL promotion or a
   * raw-table write rewrites an existing entry without re-consulting it. The
   * filter covers values the engine obtains on the loader's behalf, not writes
   * the caller performs itself.
   *
   * <p><b>Value-only.</b> The key is not passed, so a per-key rule cannot be
   * expressed here. On the annotation path {@code unless} can inspect the
   * arguments and the result instead.
   *
   * <p><b>Threading and cost.</b> Runs on whichever thread is about to store
   * the value: the read path's load thread, an AMQP listener thread, or a
   * key's ordered sync dispatcher. Implementations must be thread-safe, cheap,
   * non-blocking, side-effect free and independent of caller thread state —
   * this is a predicate, not a second data-source call. An exception thrown
   * here is treated like a loader failure and propagates on the read path.
   *
   * <p>Defaults to {@code true} (admit every value), so a loader implemented
   * as a lambda or method reference need not override it.
   *
   * @param value the non-{@code null} value about to be stored
   * @return whether the engine may store it in L1
   */
  default boolean isCacheable(V value) {
    return true;
  }
}
