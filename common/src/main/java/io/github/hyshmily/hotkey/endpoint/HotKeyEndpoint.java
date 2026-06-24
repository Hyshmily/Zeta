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
package io.github.hyshmily.hotkey.endpoint;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.local.CacheSyncPublisher;
import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
import io.github.hyshmily.hotkey.util.version.VersionController;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

/**
 * Actuator {@code /actuator/hotkey} endpoint that exposes runtime diagnostics
 * and rule management operations.
 *
 * <p>The response includes both app-side and Worker-side TopK rankings, L1
 * cache metrics, SingleFlight in-flight sizes, recently expelled keys,
 * algorithm configuration, TTL settings, version tracking state, broadcast
 * dedup, identity, and instance-level health. Each section is produced only
 * when the corresponding service is available in the current deployment mode.
 */
@Endpoint(id = "hotkey")
@RequiredArgsConstructor
public class HotKeyEndpoint {

  /** App-side TopK detector (HeavyKeeper) for local hot-key frequency tracking. */
  private final TopK hotKeyDetector;
  /** Worker-side TopK detector — separate instance fed by {@code ReportConsumer}. */
  private final TopK workerTopK;
  /** L1 Caffeine cache instance. */
  private final Cache<String, Object> caffeineCache;
  /** SingleFlight deduplication guard for concurrent L2 reads. */
  private final SingleFlight singleFlight;
  /** HotKey configuration properties. */
  private final HotKeyProperties properties;
  /** App-to-Worker report aggregator. */
  private final HotKeyReporter hotKeyReporter;
  /** Blacklist/whitelist rule evaluator. */
  private final RuleMatcher ruleMatcher;
  /** Cache TTL manager for soft/hard expiry. */
  private final CacheExpireManager expireManager;
  /** Version tracking controller (Redis-backed, with local fallback). */
  private final VersionController versionController;
  /** Cross-instance cache sync publisher (AMQP). */
  private final CacheSyncPublisher cacheSyncPublisher;
  /** Worker-side hot-key state machine. */
  private final HotKeyStateMachine hotKeyStateMachine;
  /** Cluster health view provider (optional — unavailable in Worker-only mode). */
  private final ObjectProvider<ClusterHealthView> healthViewProvider;

  /**
   * Obtain the cluster health view if available, or {@code null} if the
   * health view provider is not present (e.g. in Worker-only mode).
   *
   * @return the {@link ClusterHealthView} instance, or {@code null}
   */
  private ClusterHealthView healthView() {
    return healthViewProvider.getIfAvailable();
  }

  /**
   * Collect all diagnostic metrics into a three-section response map:
   * <ul>
   *   <li><b>local</b> — app-side detection, cache, reporting, rules, TTLs, version
   *   <li><b>worker</b> — worker-side TopK, health, state machine
   *   <li><b>sync</b> — broadcast dedup cache
   * </ul>
   * Each section is populated only when the required components are available.
   *
   * @return a {@link LinkedHashMap} with identity fields and sectioned metrics
   */
  @ReadOperation
  public Map<String, Object> hotKeyInfo() {
    Map<String, Object> info = new LinkedHashMap<>();

    info.put("instanceId", InstanceIdGenerator.get());
    info.put("nodeId", InstanceIdGenerator.getNodeId());

    Map<String, Object> local = buildLocalSection();
    if (!local.isEmpty()) {
      info.put("local", local);
    }

    Map<String, Object> worker = buildWorkerSection();
    if (!worker.isEmpty()) {
      info.put("worker", worker);
    }

    if (cacheSyncPublisher != null) {
      info.put("sync", Map.of("dedupCacheSize", cacheSyncPublisher.getDedupCacheSize()));
    }

    return info;
  }

  /**
   * Build the "local" section of the actuator response containing app-side
   * diagnostics (TopK rankings, cache metrics, SingleFlight, reporter, rules,
   * TTL settings, version tracking).
   *
   * @return a map of local diagnostic metrics (never {@code null})
   */
  private Map<String, Object> buildLocalSection() {
    Map<String, Object> local = new LinkedHashMap<>();

    if (hotKeyDetector != null) {
      List<Item> topKList = hotKeyDetector.list();
      local.put("topK", toTopKEntries(topKList));
      local.put("topKCount", topKList.size());
      local.put("totalRequests", hotKeyDetector.total());
      local.put("recentlyExpelled", hotKeyDetector.expelled().stream().map(Item::key).limit(10).toList());
      local.put("expelledQueueSize", hotKeyDetector.expelled().size());
      local.put("expelledQueueRemaining", hotKeyDetector.expelled().remainingCapacity());
      local.putAll(heavyKeeperConfig(hotKeyDetector));
    }

    if (caffeineCache != null) {
      local.put("cacheSize", caffeineCache.estimatedSize());
      local.put("cacheMaxSize", properties.getLocalCacheMaxSize());
    }

    if (singleFlight != null) {
      local.put("inflightSize", singleFlight.estimatedInflightSize());
      local.put("inflightMaxSize", properties.getInflightMaxSize());
      local.put("inflightTtlSec", properties.getInflightTtlSeconds());
      local.put("inflightTimeoutSec", properties.getInflightTimeoutSeconds());
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

    if (expireManager != null) {
      local.put("softExpireEnabled", expireManager.isSoftExpireEnabled());
      local.put("hardTtlMs", expireManager.getEffectiveHardTtlMs());
      local.put("softTtlMs", expireManager.getEffectiveSoftTtlMs());
      local.put("hotHardTtlMs", expireManager.getEffectiveHotHardTtlMs());
      local.put("hotSoftTtlMs", expireManager.getEffectiveHotSoftTtlMs());
      if (expireManager.getRefreshLimiter() != null) {
        local.put("refreshPoolAvailable", expireManager.getRefreshLimiter().availablePermits());
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
  private Map<String, Object> buildWorkerSection() {
    Map<String, Object> worker = new LinkedHashMap<>();

    if (workerTopK != null) {
      List<Item> topKList = workerTopK.list();
      worker.put("topK", toTopKEntries(topKList));
      worker.put("topKCount", topKList.size());
      worker.put("totalRequests", workerTopK.total());
      worker.put("recentlyExpelled", workerTopK.expelled().stream().map(Item::key).limit(10).toList());
      worker.putAll(heavyKeeperConfig(workerTopK));
    }

    ClusterHealthView v = healthView();
    if (v != null) {
      worker.put("health", v.isClusterHealthy() ? "healthy" : "unhealthy");
    }

    if (hotKeyStateMachine != null) {
      worker.put("trackedKeys", hotKeyStateMachine.getTrackedKeys());
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
