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
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * Scheduled tasks for periodic TopK maintenance.
 *
 * <p>Handles decaying (fading) the HeavyKeeper counters, draining
 * expelled hot-key entries, and periodically running the L1 cache
 * maintenance so expired entries are physically reclaimed even when the
 * cache is idle (Segcache-style eager expiration).  Uses {@link List
 * List&#60;TopK&#62;} injection
 * to support both the app-side and Worker-side TopK instances when they
 * coexist in the same JVM.
 *
 * <p>Rather than relying on Spring's {@code @Scheduled} (which uses the
 * global single-threaded {@code taskScheduler}), tasks are submitted
 * directly to the shared {@code hotKeyScheduler} via {@link
 * ScheduledExecutorService#scheduleWithFixedDelay}.  This keeps HotKey's
 * maintenance tasks isolated from the host application's own scheduled
 * jobs.
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

  /** Interval between L1 expired-entry cleanup passes. */
  private static final long L1_CLEANUP_INTERVAL_SECONDS = 5;

  private final List<TopK> topKInstances;
  private final ScheduledExecutorService scheduler;
  private final Optional<Cache<String, Object>> l1Cache;

  public ZetaSchedulingConfiguration(
    List<TopK> topKInstances,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService scheduler,
    Optional<Cache<String, Object>> l1Cache
  ) {
    this.topKInstances = topKInstances;
    this.scheduler = scheduler;
    this.l1Cache = l1Cache;
  }

  @PostConstruct
  void scheduleTasks() {
    scheduler.scheduleWithFixedDelay(this::cleanHotKeys, 20, 20, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(this::drainExpelled, 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(
      this::cleanUpExpiredEntries,
      L1_CLEANUP_INTERVAL_SECONDS,
      L1_CLEANUP_INTERVAL_SECONDS,
      TimeUnit.SECONDS
    );
  }

  /**
   * Periodically decay the HeavyKeeper counters for all registered TopK instances.
   * Runs every 20 seconds (fixed interval, see {@link #scheduleTasks()}).
   */
  void cleanHotKeys() {
    try {
      topKInstances.forEach(TopK::fading);
      log.debug("HeavyKeeper tick: decayed for {} TopK instance(s)", topKInstances.size());
    } catch (Exception e) {
      log.error("Scheduled cleanHotKeys failed", e);
    }
  }

  /**
   * Periodically drain expelled hot keys from all registered TopK instances.
   * Logs a truncated summary of up to 20 sample keys. Runs every 10 seconds.
   */
  void drainExpelled() {
    try {
      int totalDrained = 0;
      List<String> sampleKeys = new ArrayList<>();
      for (TopK topK : topKInstances) {
        List<Item> items = new ArrayList<>();
        topK.expelled().drainTo(items, 100_000);
        totalDrained += items.size();
        items.stream().map(Item::key).limit(20).forEach(sampleKeys::add);
      }
      if (totalDrained > 0) {
        String keys = sampleKeys.stream().limit(20).collect(Collectors.joining(","));
        boolean truncated = totalDrained > 20;
        log.info(
          "Drained {} expelled hot keys from {} TopK instance(s): {}{}",
          totalDrained,
          topKInstances.size(),
          keys,
          truncated ? "..." : ""
        );
      }
    } catch (Exception e) {
      log.error("Scheduled drainExpelled failed", e);
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
   *
   * <p>No-op when no L1 {@code Cache<String, Object>} bean is present (e.g. a
   * consumer substituted the cache with their own bean type).
   */
  void cleanUpExpiredEntries() {
    try {
      l1Cache.ifPresent(Cache::cleanUp);
    } catch (Exception e) {
      log.error("Scheduled cleanUpExpiredEntries failed", e);
    }
  }
}
