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
package io.github.hyshmily.zeta.hotkeydetector.heavykeeper;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Top‑K hot key detection interface.
 *
 * <p>Implementations track the most frequently accessed keys using a
 * sketch-based algorithm (e.g. HeavyKeeper) and provide access to the
 * current ranking (sorted by estimated frequency descending), the expelled
 * items (keys that lost their seat — evicted when the set is full, or
 * dropped by the periodic decay), and the decay-adjusted tracked request
 * volume.
 *
 * <p>The interface supports both single-key and batch access recording,
 * periodic frequency decay ({@link #fading}), and read-only introspection
 * ({@link #list}, {@link #contains}, {@link #total}).
 *
 * <p>Implementations are expected to be thread-safe.
 */
public interface TopK {
  /**
   * Record one or more accesses for the given key.
   *
   * @param key   the accessed key
   * @param delta the number of accesses to record (must be positive)
   * @return the result indicating whether the key became hot and whether another key was evicted
   */
  AddResult addDirect(String key, long delta);

  /**
   * Return the current TopK list sorted by frequency (descending).
   *
   * @return list of {@link Item} entries, never {@code null}
   */
  List<Item> list();

  /**
   * Return the top N hot keys.
   *
   * @param limit maximum number of keys to return
   * @return list of at most {@code limit} {@link Item} entries
   */
  List<Item> listTopN(int limit);

  /**
   * Return the queue of items that have lost their TopK seat — evicted to make
   * room for a hotter key, or dropped by the periodic decay.
   * Consumers should drain this queue periodically for asynchronous processing.
   *
   * @return a blocking queue of evicted items
   */
  BlockingQueue<Item> expelled();

  /**
   * Decay all frequency estimates to age out historical data and prevent
   * stale frequency accumulation.
   *
   * <p>Typically invoked periodically by a scheduler. Each implementation
   * decays its estimates: {@code HeavyKeeper} rotates a sliding window of
   * sketch counters (zeroing the oldest window) and re-synchronises membership
   * counts from the sketch under its admission lock, dropping members below the
   * admission threshold. This ensures the TopK set reflects recent access
   * patterns rather than cumulative historical popularity.
   */
  void fading();

  /**
   * Return the approximate number of distinct keys currently tracked (0 to k).
   *
   * @return estimated number of keys in the top-K set
   */
  int estimatedSize();

  /**
   * Return the tracked access volume, <b>decay-adjusted rather than
   * since-startup</b>.
   *
   * <p>Not a monotonic lifetime counter: each {@link #fading()} cycle halves the
   * running total before adding the next window's volume, so the returned value
   * converges on a weighted recent-traffic figure (roughly the last
   * {@code windowCount × decayInterval} of activity) and moves <em>down</em> as
   * traffic subsides. It is still an aggregate "how much traffic is this
   * instance seeing" signal, which is how the {@code zeta.topk.total} gauge and
   * the actuator's {@code totalRequests} use it — but it must not be read as a
   * cumulative request count, and it is not used in any hot-key threshold or
   * ratio.
   *
   * @return decay-adjusted tracked access volume
   */
  long total();

  /**
   * Check whether the given key is currently in the TopK set.
   *
   * @param key the key to check
   * @return {@code true} if the key is present in the current TopK ranking
   */
  boolean contains(String key);
}
