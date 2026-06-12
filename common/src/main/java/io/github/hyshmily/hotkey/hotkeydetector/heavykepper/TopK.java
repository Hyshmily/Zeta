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
package io.github.hyshmily.hotkey.hotkeydetector.heavykepper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Top‑K hot key detection interface.
 *
 * <p>Implementations track the most frequently accessed keys using a
 * sketch-based algorithm (e.g. HeavyKeeper) and provide access to the
 * current ranking, expelled items, and total request count.
 */
public interface TopK {

  /**
   * Record one or more accesses for the given key.
   *
   * @param key       the accessed key
   * @param increment the number of accesses to record
   * @return the  result indicating whether the key became hot and whether another key was evicted
   */
  AddResult addDirect(String key, int increment);

  /**
   * Record accesses for multiple keys.
   * <p>Always updates both the sketch counters and the TopK heap.
   *
   * @param keyCounts map of keys to their access counts
   * @return list of {@link AddResult} for keys that entered the TopK set
   */
  List<AddResult> add(Map<String, Long> keyCounts);
  /**
   * Return the current TopK list sorted by frequency (descending).
   *
   * @return list of {@link Item} entries, never {@code null}
   */
  List<Item> list();

  /**
   * Return the top N hot keys.
   *
   * @param n maximum number of keys to return
   * @return list of at most {@code n} {@link Item} entries
   */
  List<Item> listTopN(int n);

  /**
   * Return the queue of items that have been evicted from the TopK set.
   * Consumers should drain this queue periodically for asynchronous processing.
   *
   * @return a blocking queue of evicted items
   */
  BlockingQueue<Item> expelled();

  /**
   * Decay all frequency counts to age out historical data.
   * Typically called periodically by a scheduler.
   */
  void fading();

  /**
   * Return the total number of data streams (accesses) tracked since startup.
   *
   * @return total access count
   */
  long total();

  /**
   * Check whether the given key is currently in the TopK set.
   *
   * @param key the key to check
   * @return {@code true} if the key is present in the current TopK ranking
   */
  default boolean contains(String key) {
    return list()
      .stream()
      .anyMatch(item -> item.key().equals(key));
  }

}
