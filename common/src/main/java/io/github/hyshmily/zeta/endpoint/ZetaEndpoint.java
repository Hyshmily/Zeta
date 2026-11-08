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
package io.github.hyshmily.zeta.endpoint;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.AccessCounter;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.lang.Nullable;

/**
 * Actuator {@code /actuator/hotkey} endpoint that exposes runtime diagnostics.
 *
 * <p>The response includes the local TopK ranking, L1 cache size, SingleFlight
 * in-flight size, reporter queue state, refresh leases, version-clock skew,
 * identity, and cluster-health/isolation counters. Each section is produced only
 * when the corresponding service is available in the current deployment mode.
 *
 * <p>Runs on the management plane (port, exposure, and roles honored via the
 * standard {@code management.*} configuration), unlike a plain MVC controller.
 *
 * <p><b>Security:</b> This endpoint returns sensitive runtime data including
 * actual cache key names and cluster topology. Protect it via Spring Security
 * (e.g. {@code management.endpoint.hotkey.roles=ADMIN}) to prevent information
 * leakage in production environments.
 */
@Internal
@Builder
@Endpoint(id = "hotkey")
public class ZetaEndpoint {

  /** L1 Caffeine cache instance. */
  private final Cache<String, CacheEntry> caffeineCache;
  /** Background soft-expire refresh executor (limiter observability). */
  private final BackgroundRefresher backgroundRefresher;
  /**
   * Sync-plane listener supplying appName-isolation counters for the "sync" section;
   * {@code null} when the sync plane is absent.
   */
  private final CacheSyncListener syncListener;
  /**
   * Decision-plane listener supplying appName-isolation counters for the "worker" section;
   * {@code null} when Worker mode is off.
   */
  private final WorkerListener workerListener;

  /**
   * Shared read-path access counter (design decisions: access counting); {@code null} when absent.
   * Its pending-key figure is reported as {@code reportPendingKeys}; without it the reporter's
   * own count is the fallback.
   */
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  private final AccessCounter accessCounter;

  /** App-to-Worker report router and publisher. */
  private final KeyReporter hotKeyReporter;
  /** SingleFlight deduplication guard for concurrent source loads. */
  private final SingleFlight singleFlight;

  /** Node-local data-version stamp clock (skew observability). */
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  private final StampClock stampClock;

  /** Cluster health view (may be {@code null} in Worker-only mode). */
  private final HealthView healthView;
  /** App-side TopK detector (HeavyKeeper) for local hot-key frequency tracking. */
  private final TopK hotKeyDetector;

  /**
   * Collect all diagnostic metrics into a three-section response map:
   * <ul>
   *   <li><b>local</b> — app-side detection, cache, reporting, refresh leases, version
   *   <li><b>worker</b> — worker-side health and foreign-app isolation
   *   <li><b>sync</b> — appName-isolation counters, node-id collisions
   * </ul>
   *
   * @param limit maximum TopK entries per section; {@code null} (absent query
   *              parameter) means the default of 100
   * @return a {@link LinkedHashMap} with identity fields and sectioned metrics
   */
  @ReadOperation
  public Map<String, Object> hotKeyInfo(@Nullable Integer limit) {
    int topLimit = limit != null ? limit : 100;
    Map<String, Object> info = new LinkedHashMap<>();

    info.put("instanceId", NodeIdentity.getInstanceId());
    info.put("jvmId", NodeIdentity.getJvmId());

    Map<String, Object> local = buildLocalSection(topLimit);
    if (!local.isEmpty()) {
      info.put("local", local);
    }

    Map<String, Object> worker = buildWorkerSection();
    if (!worker.isEmpty()) {
      info.put("worker", worker);
    }

    Map<String, Object> sync = buildSyncSection();
    if (!sync.isEmpty()) {
      info.put("sync", sync);
    }

    return info;
  }

  /**
   * Append a listener's foreign-application isolation counters (design decisions: sync messages) to its section.
   *
   * @param section        the endpoint section to append to
   * @param drops          cumulative messages dropped for a foreign application name
   * @param lastForeignApp the most recently seen foreign app name, or {@code null}
   */
  private static void putForeignAppStats(Map<String, Object> section, long drops, String lastForeignApp) {
    section.put("foreignAppDrops", drops);
    if (lastForeignApp != null) {
      section.put("lastForeignApp", lastForeignApp);
    }
  }

  /**
   * Build the "local" section. The bar for a field is the facade's: it must answer one
   * question an operator can act on. Configuration echoes (TTL values, queue capacities,
   * sketch parameters), derived values ({@code capacity - size}) and internals (the expelled
   * queue's contents, the send-dedup window) fail that bar — they are reconstructible from
   * configuration and the reported values, or already readable elsewhere, and they bury the
   * handful of numbers that move.
   *
   * @param limit maximum TopK entries to include
   * @return a map of local diagnostic metrics (never {@code null})
   */
  private Map<String, Object> buildLocalSection(int limit) {
    Map<String, Object> local = new LinkedHashMap<>();

    if (hotKeyDetector != null) {
      // The detector already owns the bounded, sorted snapshot; doing list()
      // plus a manual subList would sort the whole membership and copy it twice.
      local.put("topK", toTopKEntries(hotKeyDetector.listTopN(Math.max(1, limit))));
      local.put("totalRequests", hotKeyDetector.total());
    }

    if (caffeineCache != null) {
      local.put("cacheSize", caffeineCache.estimatedSize());
    }

    if (singleFlight != null) {
      local.put("inFlightSize", singleFlight.estimatedInFlightSize());
    }

    if (hotKeyReporter != null) {
      // Depth against drops: is the reporter behind (depth rising) or already losing work
      // (full rising)? The expiry causes stay as their own meters
      // (zeta.reporter.queue.expired.*); their sum is a derived count this endpoint does not repeat.
      KeyReporter.DispatcherStats stats = hotKeyReporter.dispatcherStats();
      if (stats != null) {
        local.put("reportQueueDepth", stats.depth());
        local.put("reportQueueFullCount", stats.dropped());
      }
    }

    if (accessCounter != null) {
      // Pending keys come from the shared access counter (design decisions: access counting).
      local.put("reportPendingKeys", accessCounter.pendingKeyCount());
    } else if (hotKeyReporter != null) {
      local.put("reportPendingKeys", hotKeyReporter.pendingKeyCount());
    }

    if (backgroundRefresher != null) {
      var refreshLimiter = backgroundRefresher.getRefreshLimiter();
      if (refreshLimiter != null) {
        local.put("refreshPoolAvailable", refreshLimiter.availablePermits());
      }
      // Lease-on-failure observability (design decisions: expiry and refresh): the keys a
      // failed refresh is still holding servable, oldest first. One map replaces three
      // aggregate counters — it answers "which keys, how stale" rather than "how many events".
      local.put("staleDebt", backgroundRefresher.leasedStaleness(Math.max(1, Math.min(limit, 100))));
    }

    if (stampClock != null) {
      local.put("versionClockSkewMillis", stampClock.skewMillis());
    }

    return local;
  }

  /**
   * Build the "sync" section of the actuator response containing appName-isolation
   * counters and node-id collisions.
   *
   * @return a map of sync diagnostic metrics (never {@code null}; empty when the
   *         sync plane is absent)
   */
  private Map<String, Object> buildSyncSection() {
    if (syncListener == null) {
      return Map.of();
    }
    Map<String, Object> sync = new LinkedHashMap<>();
    putForeignAppStats(sync, syncListener.foreignAppDrops(), syncListener.lastForeignApp());
    sync.put("nodeIdCollisions", syncListener.nodeIdCollisions());
    return sync;
  }

  /**
   * Build the "worker" section of the actuator response containing worker-side
   * diagnostics (cluster health, foreign-application isolation).
   *
   * @return a map of worker diagnostic metrics (never {@code null})
   */
  private Map<String, Object> buildWorkerSection() {
    Map<String, Object> worker = new LinkedHashMap<>();

    if (healthView != null) {
      worker.put("health", healthView.isClusterHealthy() ? "healthy" : "unhealthy");
      // Heartbeat freshness in elapsed-monotonic form (the raw timestamp is a
      // monotonic-clock value, meaningless outside this process); -1 = no
      // heartbeat ever received. Gives the HealthView#getLastAnyHeartbeatTime
      // signal an observation surface for debugging Reporter routing dropouts.
      long last = healthView.getLastAnyHeartbeatTime();
      worker.put("msSinceLastAnyHeartbeat", last == 0 ? -1 : TimeSource.monotonicMillis() - last);
    }

    if (workerListener != null) {
      putForeignAppStats(worker, workerListener.foreignAppDrops(), workerListener.lastForeignApp());
      worker.put("coolDispatchRejections", workerListener.coolDispatchRejections());
    }

    return worker;
  }

  /**
   * Convert a list of TopK {@link Item} records into a list of key-add maps
   * suitable for JSON serialisation in the actuator response.
   *
   * @param items the TopK items to convert
   * @return a list of maps, each containing {@code "key"} and {@code "add"}
   */
  private static List<Map<String, Object>> toTopKEntries(List<Item> items) {
    List<Map<String, Object>> entries = new ArrayList<>(items.size());
    for (Item item : items) {
      entries.add(Map.of("key", item.key(), "add", item.count()));
    }
    return entries;
  }
}
