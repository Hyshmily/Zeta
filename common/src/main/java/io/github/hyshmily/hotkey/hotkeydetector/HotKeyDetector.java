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
package io.github.hyshmily.hotkey.hotkeydetector;

import io.github.hyshmily.hotkey.hotkeydetector.doublebuffer.BufferedCounter;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.AddResult;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.HeavyKeeper;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Unified facade for hot key detection.
 *
 * <p>Provides both direct (synchronous, no buffering) and buffered (asynchronous,
 * batched) paths for recording key accesses, along with read-only introspection
 * of the underlying HeavyKeeper state.
 */
public class HotKeyDetector implements TopK {

    private final HeavyKeeper heavyKeeper;
    private final BufferedCounter bufferedCounter;

    /**
     * Creates a detector that wraps the given HeavyKeeper instance.
     *
     * <p>The buffered path ({@link #add(String)}, {@link #addAll(Map)})
     * aggregates counts and flushes them to {@code heavyKeeper.add(Map)},
     * which always updates both the sketch and the TopK heap.
     *
     * @param heavyKeeper the underlying sketch-based TopK implementation
     */
    public HotKeyDetector(HeavyKeeper heavyKeeper) {
        this.heavyKeeper = heavyKeeper;
        this.bufferedCounter = new BufferedCounter(heavyKeeper::add);
    }


    /**
     * Record an access directly to the sketch and TopK heap.
     *
     * @param key       the accessed key
     * @param increment the frequency increment
     * @return result indicating whether the key became hot
     */
    @Override
    public AddResult addDirect(String key, int increment) {
        return heavyKeeper.addDirect(key, increment);
    }

    /**
     * Record accesses for multiple keys. Delegates to the underlying HeavyKeeper.
     *
     * @param keyCounts map of keys to their access counts
     * @return list of {@link AddResult} for keys that entered the TopK set
     */
    @Override
    public List<AddResult> add(Map<String, Long> keyCounts) {
        return heavyKeeper.add(keyCounts);
    }

    /**
     * Record single access for the given key through the buffer.
     *
     * @param key the accessed key
     */
    public void add(String key) {
        bufferedCounter.count(key, 1);
    }

    /**
     * Record multiple accesses for the given key through the buffer.
     *
     * @param key   the accessed key
     * @param delta the number of accesses
     */
    public void add(String key, long delta) {
        bufferedCounter.count(key, delta);
    }

    /**
     * Record accesses for multiple keys through the buffer.
     *
     * @param keyCounts map of keys to their access counts
     */
    public void addAll(Map<String, Long> keyCounts) {
        for (Map.Entry<String, Long> entry : keyCounts.entrySet()) {
            bufferedCounter.count(entry.getKey(), entry.getValue());
        }
    }


    /**
     * Return all keys currently in the TopK set, sorted by estimated count descending.
     */
    @Override
    public List<Item> list() {
        return heavyKeeper.list();
    }

    /**
     * Return the top N hot keys from the underlying HeavyKeeper.
     *
     * @param n maximum number of keys to return
     * @return list of at most {@code n} {@link Item} entries, never null
     */
    @Override
    public List<Item> listTopN(int n) {
        return heavyKeeper.listTopN(n);
    }

    /**
     * Check whether the given key is currently in the TopK set.
     *
     * @param key the key to check
     * @return {@code true} if the key is present in the current TopK ranking
     */
    @Override
    public boolean contains(String key) {
        return heavyKeeper.contains(key);
    }

    /**
     * Return the total number of data streams tracked since startup or last reset.
     *
     * @return total access count
     */
    @Override
    public long total() {
        return heavyKeeper.total();
    }

    /**
     * Return the blocking queue holding items that have been evicted from the TopK set.
     *
     * @return a blocking queue of evicted items
     */
    @Override
    public BlockingQueue<Item> expelled() {
        return heavyKeeper.expelled();
    }
    /**
     * Decay all frequency counts to age out historical data.
     * Delegates to the underlying HeavyKeeper.
     */
    @Override
    public void fading() {
        heavyKeeper.fading();
    }

}
