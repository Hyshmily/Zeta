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
package io.github.hyshmily.zeta.worker.detection;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates Top-K candidates and triggers pre-warming broadcasts for stable hot keys.
 *
 * <p>A hot key must appear in the Top-K list {@code preWarmMinAppearances} consecutive times
 * before it is considered "confirmed" and send for pre-warming. Once confirmed, it won't
 * be send again until it drops out of the Top-K for a sustained period (auto-cooling).
 *
 * <p>This hysteresis prevents flapping: a key that briefly enters and leaves the Top-K won't
 * type unnecessary broadcasts.
 *
 * <p><b>Known limitation — local‑only validation:</b> All validation is
 * purely local to this Worker instance — cross‑checking this Worker's
 * HeavyKeeper TopK against its own sliding‑window TopK. There is
 * <b>no cross‑Worker consensus, quorum, or coordination</b>. A globally
 * hot key that is split across multiple shards may never reach any single
 * Worker's TopK list.
 */
@RequiredArgsConstructor
@Slf4j
public class TopKValidator {

  /** Algorithm that tracks approximate Top-K frequencies. */
  private final TopK topK;

  /** Broadcasts pre-warm messages to other worker instances. */
  private final WorkerBroadcaster broadcaster;

  /** How many of the top entries to consider for pre-warming each validation cycle. */
  private final int preWarmTopN;

  /** How many consecutive Top-K appearances are required before a key is confirmed as hot. */
  private final int preWarmMinAppearances;

  /**
   * Keys that have already been confirmed and send for pre-warming.
   * Now backed by Caffeine to provide automatic expiration and size limit.
   */
  private final Cache<String, Boolean> confirmedHotKeys = Caffeine.newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build();

  /**
   * Tracks how many consecutive validation cycles each candidate key has appeared
   * in the Top-K list. Reset when the key drops out of the Top-K.
   */
  private final Map<String, AtomicInteger> topKAppearances = new ConcurrentHashMap<>();

  /** Counters for monitoring. */
  private long validationCount = 0;
  private long broadcastCount = 0;
  private long coolCount = 0;

  /**
   * Runs one validation cycle.
   *
   * <p>Inspects the current Top-K list and increments the appearance counter for each
   * candidate key. When a key reaches the required appearance threshold, a pre-warm
   * send is sent to all worker instances.
   *
   * <p>Also performs automatic cooling: confirmed keys that are no longer in the top
   * {@code preWarmTopN} are removed from the confirmed set.
   */
  public void validate() {
    // Performance optimization: use listTopN if available
    List<Item> topKeys = topK.listTopN(preWarmTopN);
    Set<String> currentTopKeys = topKeys.stream().map(Item::key).collect(Collectors.toSet());

    // 1. Check new candidates
    for (Item item : topKeys) {
      if (confirmedHotKeys.getIfPresent(item.key()) != null) {
        continue; // Already confirmed
      }

      AtomicInteger appearances = topKAppearances.computeIfAbsent(item.key(), k -> new AtomicInteger(0));

      if (appearances.incrementAndGet() >= preWarmMinAppearances) {
        broadcaster.broadcastHot(item.key());

        confirmedHotKeys.put(item.key(), Boolean.TRUE);
        topKAppearances.remove(item.key());
        broadcastCount++;

        log.debug("TopK pre-warm: key={}, count={}", item.key(), item.count());
      }
    }

    // 2. Auto-cooling: remove confirmed keys no longer in the top set
    Set<String> confirmedKeys = confirmedHotKeys.asMap().keySet();
    for (String key : confirmedKeys) {
      if (!currentTopKeys.contains(key)) {
        confirmedHotKeys.invalidate(key);
        coolCount++;
        log.debug("Auto cooled: key={}", key);
      }
    }

    // 3. Clean up counters for dropped keys
    topKAppearances.keySet().retainAll(currentTopKeys);

    // 4. Monitor logging (every 10 cycles)
    if (++validationCount % 10 == 0) {
      log.info(
        "TopKValidator stats: confirmed={}, candidates={}, broadcasts={}, coolings={}",
        confirmedHotKeys.estimatedSize(),
        topKAppearances.size(),
        broadcastCount,
        coolCount
      );
    }
  }

  /**
   * Manually marks a key as confirmed (already pre-warmed), so it won't be
   * send again until it cools down.
   *
   * @param key the cache key to mark as confirmed
   */
  public void markConfirmed(String key) {
    confirmedHotKeys.put(key, Boolean.TRUE);
  }

  /**
   * Manually removes a key from the confirmed set, allowing it to be pre-warmed
   * again if it re-enters the Top-K.
   *
   * @param key the cache key to mark as cooled
   */
  public void markCooled(String key) {
    confirmedHotKeys.invalidate(key);
  }
}
