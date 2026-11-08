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
package io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight;

import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Deduplicates concurrent in-flight loads for the same key: the first caller runs
 * the reader, every concurrent caller joins its result.
 *
 * <p><b>Leader commit.</b> A reader result is only useful once it is stored, so the
 * elected leader runs {@code leaderCommit} inside the load before the shared future
 * completes; outside a transaction the followers observe a value whose L1 write
 * already happened. The commit callback may itself defer its store to a transaction's
 * {@code afterCommit}, in which case a follower can observe the value before L1 is
 * written and a retry can start a second load. Only the leader commits, so N waiters
 * never queue on one cache bin to write the same entry, and a load killed by
 * {@link #invalidate(String)} commits nothing.
 *
 * <p>The table is a deduplication window, not a cache: the leader retires its flight when the
 * load settles — before the shared future completes, so a woken waiter's retry cannot re-elect
 * it — and a later miss re-runs the reader. {@code maxSize} and {@code ttlSec} bound only
 * entries that are still in flight. {@link #invalidatePrefix(String)} extends the same kill to
 * one key namespace, so a read that starts after a scoped clear cannot join a load that began
 * before it. Thread-safe.
 */
public interface SingleFlight {
  /**
   * Whether any breaker shard is currently degrading loads (OPEN, or HALF_OPEN with no probe
   * slot). This is the degraded-mode signal the read path uses to allow a stale fallback and to
   * withhold fail-fast.
   *
   * @return {@code true} if at least one shard is refusing loads
   */
  boolean isBreakerOpen();

  /**
   * Whether the given key's breaker shard is currently degrading loads (OPEN, or HALF_OPEN with
   * no probe slot).
   *
   * @param cacheKey the key whose shard to check
   * @return {@code true} if the key's breaker is refusing loads
   */
  boolean isBreakerOpen(String cacheKey);

  /**
   * Number of reader suppliers currently executing — not the size of the dedup table.
   *
   * @return the number of executing loaders (never negative)
   */
  long estimatedInFlightSize();

  /**
   * Cumulative loads that ended in the dedup layer's own
   * {@link java.util.concurrent.TimeoutException}, counted once per leader.
   *
   * @return the timeout count (monotonic)
   */
  default long loadTimeoutCount() {
    return 0L;
  }

  /**
   * Load one value, deduplicating same-key callers.
   *
   * <p>A reader must not call back into this method for the same key: the flight
   * it would join is the one already executing it. Such a same-thread re-entry
   * is rejected with {@link IllegalStateException} instead of joining a future
   * that only this call can complete. Re-entering a different key is fine, but a
   * reader running on the load executor can still join a leader queued on that
   * same executor; the join's budget starts only when the leader task runs, so
   * avoid building cross-key join cycles from a load reader.
   *
   * @param cacheKey     the key to load
   * @param reader       the value supplier, run at most once per election
   * @param callerRuns   {@code true} runs the elected leader's reader on the calling
   *                     thread (caller-bound state such as a transaction survives the
   *                     miss); {@code false} runs it on the load executor
   * @param leaderCommit runs on the leader's thread with the loaded value before the
   *                     shared future completes; the place the caller stores the value
   *                     (never invoked for a follower or a superseded load). It may
   *                     defer the store to a transaction afterCommit.
   * @param <T>          the value type
   * @return the explicit outcome (never {@code null}; reader failures surface as
   *         {@code FAILED}, never as a throw)
   */
  <T> LoaderOutcome<T> load(String cacheKey, Supplier<T> reader, boolean callerRuns, Consumer<? super T> leaderCommit);

  /**
   * Run a load on the calling thread without publishing a shared flight — for a caller whose
   * transaction can observe uncommitted data. Other threads must not join a flight whose value
   * may still roll back, so no follower shares this result; a concurrent caller runs its own
   * load. Breaker admission and settlement still apply, so the outcome set is unchanged.
   *
   * @param cacheKey     the key to load
   * @param reader       the value supplier, run exactly once on the calling thread
   * @param leaderCommit runs before the outcome is returned, on the calling thread
   * @param <T>          the value type
   * @return the explicit outcome (never {@code null})
   */
  <T> LoaderOutcome<T> loadIsolated(String cacheKey, Supplier<T> reader, Consumer<? super T> leaderCommit);

  /**
   * Batch form: elects every key before joining any, so a batch of misses loads in
   * parallel instead of one key at a time.
   *
   * @param cacheKeys    the keys to load
   * @param reader       the per-key value function
   * @param leaderCommit per-key leader commit (see the single-key form)
   * @param <T>          the value type
   * @return key → outcome, in input order
   */
  default <T> Map<String, LoaderOutcome<T>> load(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    BiConsumer<String, ? super T> leaderCommit
  ) {
    Map<String, LoaderOutcome<T>> outcomes = new LinkedHashMap<>();
    for (String key : cacheKeys) {
      outcomes.put(key, load(key, () -> reader.apply(key), false, v -> leaderCommit.accept(key, v)));
    }
    return outcomes;
  }

  /**
   * Drop the key's dedup entry and mark its load superseded: a leader still running
   * must not commit, and a later caller starts a fresh load instead of replaying the
   * pre-mutation result.
   *
   * @param cacheKey the key whose dedup entry to drop
   */
  void invalidate(String cacheKey);

  /**
   * Drop every dedup entry whose key starts with {@code prefix} and mark those loads
   * superseded, so a read that starts after a scoped clear cannot join a load that began
   * before it. Flights outside the prefix are untouched by the built-in implementation.
   *
   * <p>The default is conservative: it invalidates every flight. A custom implementation
   * remains correct without a prefix index; the built-in one narrows the kill.
   *
   * @param prefix the physical key prefix
   */
  default void invalidatePrefix(String prefix) {
    invalidateAll();
  }

  /**
   * Drop every dedup entry and mark every in-flight load superseded (whole-table clear).
   */
  void invalidateAll();
}
