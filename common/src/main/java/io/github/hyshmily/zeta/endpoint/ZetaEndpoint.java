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
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.EntryLifecycle;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.dispatcher.DispatcherStats;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.InstanceIdGenerator;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.version.VersionController;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

/**
 * Actuator {@code /actuator/hotkey} endpoint that exposes runtime diagnostics
 * and rule management operations.
 *
 * <p>The response includes both app-side and Worker-side TopK rankings, L1
 * cache metrics, SingleFlight in-flight sizes, recently expelled keys,
 * algorithm configuration, TTL settings, version tracking state, send
 * dedup, per-key dispatcher gate/backlog (ADR-0072 D-1), identity, and
 * instance-level health. Each section is produced only
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

  /** App-side TopK detector (HeavyKeeper) for local hot-key frequency tracking. */
  private final TopK hotKeyDetector;
  /** L1 Caffeine cache instance. */
  private final Cache<String, CacheEntry> caffeineCache;
  /** SingleFlight deduplication guard for concurrent L2 reads. */
  private final SingleFlight singleFlight;
  /** HotKey configuration view (live reads, per scrape). */
  private final EndpointSettings properties;
  /** App-to-Worker reportToWorker aggregator. */
  private final KeyReporter hotKeyReporter;
  /** Blacklist/whitelist rule evaluator. */
  private final RuleMatcher ruleMatcher;
  /** Entry lifecycle: guards, demotion, draft factory, TTL policy. */
  private final EntryLifecycle entryLifecycle;

  /** Background soft-expire refresh executor (limiter observability). */
  private final BackgroundRefresher backgroundRefresher;

  /** Version tracking controller (Redis-backed, with local fallback). */
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  private final VersionController versionController;

  /** Cross-instance cache sync publisher (AMQP). */
  private final CacheSyncPublisher cacheSyncPublisher;
  /** Worker-side hot-key state machine. */
  private final ZetaBayesianSM zetaBayesianSM;
  /** Cluster health view (may be {@code null} in Worker-only mode). */
  private final HealthView healthView;

  /**
   * Sync-plane listener whose ordered dispatcher gate is reported in the "sync" section
   * (ADR-0072 D-1); {@code null} when the sync plane is absent.
   */
  private final CacheSyncListener syncListener;

  /**
   * Decision-plane listener whose ordered dispatcher gate is reported in the "worker" section
   * (ADR-0072 D-1); {@code null} when Worker mode is off.
   */
  private final WorkerListener workerListener;

  /**
   * Collect all diagnostic metrics into a three-section response map:
   * <ul>
   *   <li><b>local</b> — app-side detection, cache, reporting, rules, TTLs, version
   *   <li><b>worker</b> — worker-side TopK, health, state machine, decision-plane dispatch gate
   *   <li><b>sync</b> — send dedup cache, sync-plane dispatch gate
   * </ul>
   * Each section is populated only when the required components are available.
   *
   * @param limit maximum TopK entries per section; {@code null} (absent query
   *              parameter) means the default of 100
   * @return a {@link LinkedHashMap} with identity fields and sectioned metrics
   */
  @ReadOperation
  public Map<String, Object> hotKeyInfo(Integer limit) {
    int topLimit = limit != null ? limit : 100;
    Map<String, Object> info = new LinkedHashMap<>();

    info.put("instanceId", InstanceIdGenerator.get());
    info.put("nodeId", InstanceIdGenerator.getNodeId());

    Map<String, Object> local = buildLocalSection(topLimit);
    if (!local.isEmpty()) {
      info.put("local", local);
    }

    Map<String, Object> worker = buildWorkerSection(topLimit);
    if (!worker.isEmpty()) {
      info.put("worker", worker);
    }

  if (cacheSyncPublisher != null || syncListener != null) {
    Map<String, Object> sync = new LinkedHashMap<>();
    if (cacheSyncPublisher != null) {
      sync.put("dedupCacheSize", cacheSyncPublisher.getDedupCacheSize());
    }
    putDispatchStats(sync, syncListener == null ? null : syncListener.dispatcherStats());
    if (syncListener != null) {
      sync.put("foreignAppDrops", syncListener.foreignAppDrops());
      if (syncListener.lastForeignApp() != null) {
        sync.put("lastForeignApp", syncListener.lastForeignApp());
      }
    }
    if (!sync.isEmpty()) {
      info.put("sync", sync);
    }
  }

  return info;
}

/**
 * Flatten a dispatcher gate snapshot into {@code dispatch*} keys of the given section
 * (ADR-0072 D-1). A {@code null} or not-yet-initialized dispatcher contributes nothing, so a
 * deployment mode without that plane simply omits the keys.
 *
 * @param section the endpoint section to append to
 * @param stats   the gate snapshot, or {@code null} when the plane is absent
 */
private static void putDispatchStats(Map<String, Object> section, DispatcherStats stats) {
  if (stats == null) {
    return;
  }
  section.put("dispatchPendingUnits", stats.pendingUnits());
  section.put("dispatchRemainingUnits", stats.remainingUnits());
  section.put("dispatchMaxPendingUnits", stats.maxPendingUnits());
  section.put("dispatchActiveKeys", stats.activeKeys());
  section.put("dispatchBacklogged", stats.backlogged());
  section.put("dispatchDropped", stats.dropped());
  section.put("dispatchRejected", stats.rejected());
}

  /**
   * Build the "local" section of the actuator response containing app-side
   * diagnostics (TopK rankings, cache metrics, SingleFlight, reporter, rules,
   * TTL settings, version tracking).
   *
   * @return a map of local diagnostic metrics (never {@code null})
   */
  private Map<String, Object> buildLocalSection(int limit) {
    Map<String, Object> local = new LinkedHashMap<>();

    if (hotKeyDetector != null) {
      List<Item> topKList = hotKeyDetector.list();
      int actualLimit = Math.min(topKList.size(), Math.max(1, limit));
      local.put("topK", toTopKEntries(topKList.subList(0, actualLimit)));
      local.put("topKCount", actualLimit);
      local.put("totalRequests", hotKeyDetector.total());
      local.put("recentlyExpelled", hotKeyDetector.expelled().stream().map(Item::key).limit(10).toList());
      local.put("expelledQueueSize", hotKeyDetector.expelled().size());
      local.put("expelledQueueRemaining", hotKeyDetector.expelled().remainingCapacity());
      local.putAll(heavyKeeperConfig(hotKeyDetector));
    }

    if (caffeineCache != null) {
      local.put("cacheSize", caffeineCache.estimatedSize());
      local.put("cacheMaxSize", properties.cacheMaxSize());
      local.put("cacheMaxWeight", properties.cacheMaxWeight());
    }

    if (singleFlight != null) {
      local.put("inflightSize", singleFlight.estimatedInflightSize());
      local.put("inflightMaxSize", properties.inflightMaxSize());
      local.put("inflightTtlSec", properties.inflightTtlSeconds());
      local.put("inflightTimeoutSec", properties.inflightTimeoutSeconds());
    }

    if (hotKeyReporter != null) {
      local.put("reportQueueDepth", hotKeyReporter.dispatcherDepth());
      local.put("reportQueueCapacity", hotKeyReporter.dispatcherCapacity());
      local.put("reportExpiredCount", hotKeyReporter.dispatcherExpired());
      local.put("reportQueueFullCount", hotKeyReporter.dispatcherDropped());
      local.put("reportPendingKeys", hotKeyReporter.getPendingKeyCount());
    }

    if (ruleMatcher != null) {
      local.put(
        "rules",
        ruleMatcher
          .getAllRules()
          .stream()
          .map(rule ->
            Map.of(
              "id",
              rule.getId(),
              "action",
              rule.getAction(),
              "type",
              rule.getType(),
              "pattern",
              rule.getPattern(),
              "createdAt",
              rule.getCreatedAt()
            )
          )
          .toList()
      );
    }

    if (entryLifecycle != null) {
      local.put("hardTtlMs", entryLifecycle.ttlPolicy().getEffectiveHardTtlMs());
      local.put("softTtlMs", entryLifecycle.ttlPolicy().getEffectiveSoftTtlMs());
      local.put("hotHardTtlMs", entryLifecycle.ttlPolicy().getEffectiveHotHardTtlMs());
      local.put("hotSoftTtlMs", entryLifecycle.ttlPolicy().getEffectiveHotSoftTtlMs());
      local.put("nullValueTtlSec", properties.nullValueTtlSeconds());
      if (backgroundRefresher != null && backgroundRefresher.getRefreshLimiter() != null) {
        local.put("refreshPoolAvailable", backgroundRefresher.getRefreshLimiter().availablePermits());
      }
    }

    if (versionController != null) {
      local.put("versionRedisEnabled", versionController.isRedisConfigured());
      local.put("versionDegradedCount", versionController.getDegradedVersionCount());
    }

    return local;
  }

  /**
   * Build the "worker" section of the actuator response containing worker-side
   * diagnostics (worker TopK rankings, shard health, state machine tracked keys).
   *
   * @return a map of worker diagnostic metrics (never {@code null})
   */
  private Map<String, Object> buildWorkerSection(int limit) {
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

    if (zetaBayesianSM != null) {
      worker.put("trackedKeys", zetaBayesianSM.getTrackedKeys());
    }

    if (workerListener != null) {
      putDispatchStats(worker, workerListener.dispatcherStats());
      worker.put("foreignAppDrops", workerListener.foreignAppDrops());
      if (workerListener.lastForeignApp() != null) {
        worker.put("lastForeignApp", workerListener.lastForeignApp());
      }
    }

    return worker;
  }

  /**
   * Convert a list of TopK {@link Item} records into a list of key-count maps
   * suitable for JSON serialisation in the actuator response.
   *
   * @param items the TopK items to convert
   * @return a list of maps, each containing {@code "key"} and {@code "count"}
   */
  private static List<Map<String, Object>> toTopKEntries(List<Item> items) {
    List<Map<String, Object>> entries = new ArrayList<>(items.size());
    for (Item item : items) {
      entries.add(Map.of("key", item.key(), "count", item.count()));
    }
    return entries;
  }

  /**
   * Extract HeavyKeeper-specific configuration from a TopK instance.
   * Returns an empty map if the detector is not a HeavyKeeper.
   *
   * @param topK the TopK detector (may be any implementation)
   * @return a map of HeavyKeeper config keys, or an empty map
   */
  private static Map<String, Object> heavyKeeperConfig(TopK topK) {
    if (topK instanceof HeavyKeeper hk) {
      return Map.of(
        "topKCapacity",
        hk.getK(),
        "sketchWidth",
        hk.getWidth(),
        "sketchDepth",
        hk.getDepth(),
        "minCountThreshold",
        hk.getMinCount()
      );
    }
    return Map.of();
  }
}
