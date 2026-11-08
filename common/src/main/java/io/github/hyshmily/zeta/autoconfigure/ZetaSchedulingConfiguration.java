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
package io.github.hyshmily.zeta.autoconfigure;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.util.Internal;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * Scheduled tasks for periodic TopK maintenance.
 *
 * <p>Handles decaying (fading) the HeavyKeeper counters, draining
 * expelled hot-key entries, promoting TopK-admitted keys off the read path,
 * and periodically running the L1 cache maintenance so expired entries are
 * physically reclaimed even when the cache is idle (Segcache-style eager
 * expiration).  Uses {@link List List&#60;TopK&#62;} injection
 * so every registered TopK implementation is maintained — the app-side
 * {@code HotKeyDetector} is not a {@code TopK} bean, so the sketch is decayed
 * exactly once per tick even when a custom implementation is registered
 * alongside it.  (This configuration is
 * disabled entirely in Worker-only mode, so the Worker's own TopK is
 * maintained by the Worker's scheduling.)
 *
 * <p>A tick is registered only when its collaborator exists: no L1 table means
 * no cleanup tick, no engine means no promotion tick. The optional
 * collaborators are resolved once at construction, so a registered tick never
 * unpacks a container handle or tests the same absence again.
 *
 * <p>Tasks are submitted directly to the shared {@code hotKeyScheduler} via
 * {@link ScheduledExecutorService#scheduleWithFixedDelay}, so HotKey's
 * maintenance work never shares the host application's global single-threaded
 * {@code taskScheduler}.
 *
 * <p>Enabled by default; controlled via {@code zeta.scheduling.enabled}.
 */
@Internal
@AutoConfiguration(after = ZetaAutoConfiguration.class)
@ConditionalOnProperty(name = "zeta.scheduling.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "false", matchIfMissing = true)
@ConditionalOnBean(TopK.class)
@Slf4j
public class ZetaSchedulingConfiguration {

  /** Interval between TopK-admission promotion drains (sub-second keeps promotion near-immediate). */
  private static final long ADMISSION_DRAIN_INTERVAL_MILLIS = 500;

  /** Interval between HeavyKeeper decay passes (fixed-delay per instance). */
  private static final long HOT_KEY_DECAY_INTERVAL_SECONDS = 20;

  /** Interval between expelled-key queue drain passes. */
  private static final long EXPEL_DRAIN_INTERVAL_SECONDS = 10;

  /** Interval between L1 expired-entry cleanup passes. */
  private static final long L1_CLEANUP_INTERVAL_SECONDS = 5;

  /** Maximum number of expelled-key samples carried into one DEBUG line. */
  private static final int SAMPLE_LIMIT = 20;

  private final @Nullable Cache<String, CacheEntry> l1Cache;
  private final ScheduledExecutorService scheduler;
  private final @Nullable HotKeyCache hotKeyCache;
  private final List<TopK> topKInstances;

  public ZetaSchedulingConfiguration(
    List<TopK> topKInstances,
    ObjectProvider<HotKeyCache> hotKeyCacheProvider,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService scheduler,
    ObjectProvider<Cache<String, CacheEntry>> l1CacheProvider
  ) {
    this.scheduler = scheduler;
    this.topKInstances = topKInstances;
    this.l1Cache = l1CacheProvider.getIfAvailable();
    this.hotKeyCache = hotKeyCacheProvider.getIfAvailable();
  }

  @PostConstruct
  void scheduleTasks() {
    if (!topKInstances.isEmpty()) {
      scheduleGuarded("cleanHotKeys", this::cleanHotKeys, HOT_KEY_DECAY_INTERVAL_SECONDS, TimeUnit.SECONDS);
      scheduleGuarded("drainExpelled", this::drainExpelled, EXPEL_DRAIN_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }
    if (l1Cache != null) {
      scheduleGuarded(
        "cleanUpExpiredEntries",
        this::cleanUpExpiredEntries,
        L1_CLEANUP_INTERVAL_SECONDS,
        TimeUnit.SECONDS
      );
    }
    if (hotKeyCache != null) {
      scheduleGuarded(
        "promoteAdmittedKeys",
        this::promoteAdmittedKeys,
        ADMISSION_DRAIN_INTERVAL_MILLIS,
        TimeUnit.MILLISECONDS
      );
    }
  }

  /**
   * Register one scheduler tick behind a single failure guard. The
   * {@code hotKeyScheduler} is a safe scheduled executor whose wrapper already
   * routes a throwable through the exception-handler chain; this guard keeps a
   * substituted plain executor from silently losing the cadence and names the
   * tick in the ERROR — one owner so every tick has the same failure shape.
   *
   * @param tickName the tick's name (for the ERROR line)
   * @param tick     the tick body
   * @param delay    the fixed delay between runs
   * @param unit     the delay unit
   */
  private void scheduleGuarded(String tickName, Runnable tick, long delay, TimeUnit unit) {
    scheduler.scheduleWithFixedDelay(
      () -> {
        try {
          tick.run();
        } catch (Throwable e) {
          log.error("Scheduled {} failed", tickName, e);
        }
      },
      delay,
      delay,
      unit
    );
  }

  /**
   * Drain TopK admission events off the read path. Detection enqueues a key
   * when it newly enters the TopK set; this tick rewrites the resident entry
   * to HOT. The delay bounds the promotion lag to roughly one tick.
   */
  void promoteAdmittedKeys() {
    int promoted = hotKeyCache.promoteAdmitted();
    if (promoted > 0) {
      log.debug("Promoted {} admitted hot key(s) to HOT", promoted);
    }
  }

  /**
   * Periodically decay the HeavyKeeper counters for all registered TopK instances.
   */
  void cleanHotKeys() {
    topKInstances.forEach(TopK::fading);
  }

  /**
   * Periodically drain expelled hot keys from all registered TopK instances.
   * Logs a truncated summary of up to {@value #SAMPLE_LIMIT} sample keys when
   * DEBUG is enabled.
   *
   * <p><b>This drain owns the expelled queue.</b> The queue is observable read-only through the
   * {@code /actuator/hotkey} endpoint but is never handed out for consumption: a second consumer
   * would compete with this drain and silently lose items to it. The drain is a memory-protection
   * measure — without it, an undrained queue grows unbounded — and it empties every queue on each
   * pass. Do not build on this queue for guaranteed event delivery.
   */
  void drainExpelled() {
    if (!log.isDebugEnabled()) {
      for (TopK topK : topKInstances) {
        topK.expelled().clear();
      }
      return;
    }
    List<Item> sampleItems = new ArrayList<>(SAMPLE_LIMIT);
    int totalDrained = 0;
    for (TopK topK : topKInstances) {
      var queue = topK.expelled();
      totalDrained += queue.size();
      queue.drainTo(sampleItems, SAMPLE_LIMIT - sampleItems.size());
      queue.clear();
    }
    if (totalDrained > 0) {
      List<String> sampleKeys = sampleItems.stream().map(Item::key).toList();
      log.debug(
        "Drained {} expelled hot keys from {} TopK instance(s): {}{}",
        totalDrained,
        topKInstances.size(),
        String.join(",", sampleKeys),
        totalDrained > sampleKeys.size() ? "..." : ""
      );
    }
  }

  /**
   * Periodically run the L1 Caffeine maintenance to physically reclaim
   * expired entries.
   *
   * <p>Reads are served from a logical hard-expire check, so expired entries
   * are never returned; but without a background maintenance trigger the
   * Caffeine timing wheel only reclaims them on cache operations (writes,
   * reads, forced eviction). Under low traffic, entries that expire and are
   * never read again would otherwise linger in memory until the next
   * operation. This task bounds the reclamation delay at
   * {@link #L1_CLEANUP_INTERVAL_SECONDS} — the Segcache-style eager
   * expiration idea — at the cost of an incremental maintenance pass every
   * interval (Caffeine maintenance is amortized O(1) per entry).
   */
  void cleanUpExpiredEntries() {
    l1Cache.cleanUp();
  }
}
