package io.github.hyshmily.hotkey.endpoint;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.HeavyKeeper;
import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.sync.VersionController;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.monitor.WorkerHealthMonitor;
import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
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

  private final TopK hotKeyDetector;
  private final TopK workerTopK;
  private final Cache<String, Object> caffeineCache;
  private final SingleFlight singleFlight;
  private final HotKeyProperties properties;
  private final HotKeyReporter hotKeyReporter;
  private final RuleMatcher ruleMatcher;
  private final WorkerHealthMonitor workerHealthMonitor;
  private final CacheExpireManager expireManager;
  private final VersionController versionController;
  private final CacheSyncPublisher cacheSyncPublisher;
  private final HotKeyStateMachine hotKeyStateMachine;

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

    if (workerHealthMonitor != null) {
      Map<Integer, Map<String, Object>> healthData = workerHealthMonitor.getWorkerHealth();
      worker.put("shards", healthData.keySet().stream().sorted().toList());
      worker.put("health", healthData);
    }

    if (hotKeyStateMachine != null) {
      worker.put("trackedKeys", hotKeyStateMachine.getTrackedKeys());
    }

    return worker;
  }

  private static List<Map<String, Object>> toTopKEntries(List<Item> items) {
    List<Map<String, Object>> entries = new ArrayList<>(items.size());
    for (Item item : items) {
      entries.add(Map.of("key", item.key(), "count", item.count()));
    }
    return entries;
  }

  private static Map<String, Object> heavyKeeperConfig(TopK topK) {
    if (topK instanceof HeavyKeeper hk) {
      return Map.of(
        "topKCapacity", hk.getK(),
        "sketchWidth", hk.getWidth(),
        "sketchDepth", hk.getDepth(),
        "minCountThreshold", hk.getMinCount()
      );
    }
    return Map.of();
  }
}
