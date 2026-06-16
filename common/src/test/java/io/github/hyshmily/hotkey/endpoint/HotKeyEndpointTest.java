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
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.HeavyKeeper;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.Rule;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.sync.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.VersionController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Aggregate verification for {@link HotKeyEndpoint}.
 *
 * <p>Covers every field in every section (local, worker, sync), HeavyKeeper-specific
 * algorithm parameters, null-safety for all components, and partial-deployment scenarios.
 */
@SuppressWarnings("unchecked")
class HotKeyEndpointTest {

  private TopK hotKeyDetector;
  private TopK workerTopK;
  private Cache<String, Object> caffeineCache;
  private SingleFlight singleFlight;
  private HotKeyProperties properties;
  private HotKeyReporter hotKeyReporter;
  private RuleMatcher ruleMatcher;
  private RingManager workerHealthMonitor;
  private CacheExpireManager expireManager;
  private VersionController versionController;
  private CacheSyncPublisher cacheSyncPublisher;
  private HotKeyStateMachine hotKeyStateMachine;
  private ObjectProvider<ClusterHealthView> healthViewProvider;
  private ClusterHealthView healthView;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    hotKeyDetector = mock(TopK.class);
    workerTopK = mock(TopK.class);
    caffeineCache = mock(Cache.class);
    singleFlight = mock(SingleFlight.class);
    properties = new HotKeyProperties();
    hotKeyReporter = mock(HotKeyReporter.class);
    ruleMatcher = new RuleMatcher(Optional.empty(), Optional.empty());
    workerHealthMonitor = mock(RingManager.class);
    expireManager = mock(CacheExpireManager.class);
    versionController = mock(VersionController.class);
    cacheSyncPublisher = mock(CacheSyncPublisher.class);
    hotKeyStateMachine = mock(HotKeyStateMachine.class);
    healthViewProvider = mock(ObjectProvider.class);
    healthView = mock(ClusterHealthView.class);
    when(healthViewProvider.getIfAvailable()).thenReturn(healthView);
    when(healthView.isClusterHealthy()).thenReturn(true);
  }

  /* helpers */

  private HotKeyEndpoint endpointWithAll() {
    return new HotKeyEndpoint(
      hotKeyDetector, workerTopK, caffeineCache, singleFlight, properties,
      hotKeyReporter, ruleMatcher, workerHealthMonitor, expireManager,
      versionController, cacheSyncPublisher, hotKeyStateMachine, healthViewProvider);
  }

  private void mockTopK(TopK topK, List<Item> items, long total) {
    when(topK.list()).thenReturn(items);
    when(topK.total()).thenReturn(total);
    when(topK.expelled()).thenReturn(new LinkedBlockingQueue<>());
  }

  /* all components present */

  /**
   * Verifies that hotKeyInfo returns all three sections (local, worker, sync) with their respective fields when all components are present.
   */
  @Test
  void hotKeyInfo_shouldContainLocalWorkerAndSyncSections() {
    mockTopK(hotKeyDetector, List.of(new Item("k1", 10), new Item("k2", 7)), 200L);
    mockTopK(workerTopK, List.of(new Item("wk1", 5)), 80L);
    when(caffeineCache.estimatedSize()).thenReturn(42L);
    when(singleFlight.estimatedInflightSize()).thenReturn(3L);
    when(hotKeyReporter.dispatcherDepth()).thenReturn(7);
    when(hotKeyReporter.dispatcherCapacity()).thenReturn(10000);
    when(hotKeyReporter.dispatcherExpired()).thenReturn(2L);
    when(hotKeyReporter.dispatcherDropped()).thenReturn(1L);
    when(hotKeyReporter.getPendingKeyCount()).thenReturn(5L);
    when(expireManager.isSoftExpireEnabled()).thenReturn(true);
    when(expireManager.getEffectiveHardTtlMs()).thenReturn(300000L);
    when(expireManager.getEffectiveSoftTtlMs()).thenReturn(30000L);
    when(expireManager.getEffectiveHotHardTtlMs()).thenReturn(3600000L);
    when(expireManager.getEffectiveHotSoftTtlMs()).thenReturn(300000L);
    when(expireManager.getRefreshLimiter()).thenReturn(new Semaphore(50));
    when(versionController.isRedisConfigured()).thenReturn(true);
    when(versionController.getDegradedVersionCount()).thenReturn(0L);
    when(cacheSyncPublisher.getDedupCacheSize()).thenReturn(15L);
    when(hotKeyStateMachine.getTrackedKeys()).thenReturn(7);

    Map<String, Object> info = endpointWithAll().hotKeyInfo();

    assertThat(info).containsKeys("instanceId", "nodeId");
    assertThat(info).containsKeys("local", "worker", "sync");

    // ── local ──
    Map<String, Object> local = (Map<String, Object>) info.get("local");
    assertThat(local).containsEntry("topKCount", 2);
    assertThat(local).containsEntry("totalRequests", 200L);
    assertThat(local).containsKey("topK");
    assertThat(local).containsKey("recentlyExpelled");
    assertThat(local).containsEntry("cacheSize", 42L);
    assertThat(local).containsEntry("cacheMaxSize", properties.getLocalCacheMaxSize());
    assertThat(local).containsEntry("inflightSize", 3L);
    assertThat(local).containsEntry("inflightMaxSize", properties.getInflightMaxSize());
    assertThat(local).containsEntry("inflightTtlSec", properties.getInflightTtlSeconds());
    assertThat(local).containsEntry("inflightTimeoutSec", properties.getInflightTimeoutSeconds());
    assertThat(local).containsEntry("reportQueueDepth", 7);
    assertThat(local).containsEntry("reportQueueCapacity", 10000);
    assertThat(local).containsEntry("reportExpiredCount", 2L);
    assertThat(local).containsEntry("reportQueueFullCount", 1L);
    assertThat(local).containsEntry("reportPendingKeys", 5L);
    assertThat(local).containsKey("rules");
    assertThat(local).containsEntry("softExpireEnabled", true);
    assertThat(local).containsEntry("hardTtlMs", 300000L);
    assertThat(local).containsEntry("softTtlMs", 30000L);
    assertThat(local).containsEntry("hotHardTtlMs", 3600000L);
    assertThat(local).containsEntry("hotSoftTtlMs", 300000L);
    assertThat(local).containsEntry("refreshPoolAvailable", 50);
    assertThat(local).containsEntry("versionRedisEnabled", true);
    assertThat(local).containsEntry("versionDegradedCount", 0L);

    // ── worker ──
    Map<String, Object> worker = (Map<String, Object>) info.get("worker");
    assertThat(worker).containsEntry("topKCount", 1);
    assertThat(worker).containsEntry("totalRequests", 80L);
    assertThat(worker).containsKey("topK");
    assertThat(worker).containsKey("recentlyExpelled");
    assertThat(worker).containsKey("health");
    assertThat(worker).containsEntry("trackedKeys", 7);

    // ── sync ──
    Map<String, Object> sync = (Map<String, Object>) info.get("sync");
    assertThat(sync).containsEntry("dedupCacheSize", 15L);
  }

  /* HeavyKeeper algorithm params */

  /**
   * Verifies that the local section exposes HeavyKeeper-specific algorithm parameters (capacity, sketch width/depth, decay, min count threshold).
   */
  @Test
  void localSection_shouldExposeHeavyKeeperParams() {
    HeavyKeeper hk = new HeavyKeeper(10, 500, 4, 0.9, 5);
    hk.addDirect("k1", 20);
    HotKeyEndpoint ep = new HotKeyEndpoint(
      hk, null, caffeineCache, singleFlight, properties, hotKeyReporter, ruleMatcher,
      null, null, null, null, null, healthViewProvider);
    when(caffeineCache.estimatedSize()).thenReturn(10L);

    Map<String, Object> info = ep.hotKeyInfo();
    Map<String, Object> local = (Map<String, Object>) info.get("local");

    assertThat(local).containsEntry("topKCapacity", 10);
    assertThat(local).containsEntry("sketchWidth", 500);
    assertThat(local).containsEntry("sketchDepth", 4);
    assertThat(local).containsEntry("minCountThreshold", 5);
    assertThat(local).containsEntry("topKCount", 1);
    assertThat(local).containsKey("expelledQueueSize");
    assertThat(local).containsKey("expelledQueueRemaining");
  }

  /* null components */

  /**
   * Verifies that hotKeyInfo omits all three sections when all optional components are null.
   */
  @Test
  void hotKeyInfo_shouldOmitSectionWhenAllComponentsNull() {
    ObjectProvider<ClusterHealthView> nullHvProvider = mock(ObjectProvider.class);
    when(nullHvProvider.getIfAvailable()).thenReturn(null);
    HotKeyEndpoint minimal = new HotKeyEndpoint(
      null, null, null, null, properties, null, null, null, null, null, null, null, nullHvProvider);
    Map<String, Object> info = minimal.hotKeyInfo();
    assertThat(info).doesNotContainKeys("local", "worker", "sync");
    assertThat(info).containsKeys("instanceId", "nodeId");
  }

  /* partial: only local, no worker/sync */

  /**
   * Verifies that hotKeyInfo includes local but omits worker and sync when only local components are provided.
   */
  @Test
  void hotKeyInfo_shouldOmitWorkerAndSyncWhenOnlyLocalComponents() {
    when(hotKeyDetector.list()).thenReturn(List.of());
    when(hotKeyDetector.total()).thenReturn(0L);
    when(hotKeyDetector.expelled()).thenReturn(new LinkedBlockingQueue<>());
    when(caffeineCache.estimatedSize()).thenReturn(5L);
    ObjectProvider<ClusterHealthView> nullHvProvider = mock(ObjectProvider.class);
    when(nullHvProvider.getIfAvailable()).thenReturn(null);
    HotKeyEndpoint ep = new HotKeyEndpoint(
      hotKeyDetector, null, caffeineCache, null, properties, null, null, null, null, null, null, null, nullHvProvider);

    Map<String, Object> info = ep.hotKeyInfo();
    assertThat(info).containsKey("local");
    assertThat(info).doesNotContainKeys("worker", "sync");
  }

  /* partial: only sync, no local/worker */

  /**
   * Verifies that hotKeyInfo includes sync but omits local and worker when only sync components are provided.
   */
  @Test
  void hotKeyInfo_shouldIncludeSyncWhenOnlySyncComponents() {
    when(cacheSyncPublisher.getDedupCacheSize()).thenReturn(5L);
    ObjectProvider<ClusterHealthView> nullHvProvider = mock(ObjectProvider.class);
    when(nullHvProvider.getIfAvailable()).thenReturn(null);
    HotKeyEndpoint ep = new HotKeyEndpoint(
      null, null, null, null, properties, null, null, null, null, null, cacheSyncPublisher, null, nullHvProvider);

    Map<String, Object> info = ep.hotKeyInfo();
    assertThat(info).doesNotContainKeys("local", "worker");
    assertThat(info).containsKey("sync");
    assertThat(((Map<String, Object>) info.get("sync"))).containsEntry("dedupCacheSize", 5L);
  }

  /* worker section with multiple shards */

  /**
   * Verifies that the worker section lists health info for all known Worker nodes.
   */
  @Test
  void workerSection_shouldListWorkerHealth() {
    mockTopK(workerTopK, List.of(new Item("k", 1)), 10L);
    when(hotKeyStateMachine.getTrackedKeys()).thenReturn(3);
    HotKeyEndpoint ep = new HotKeyEndpoint(
      null, workerTopK, null, null, properties, null, null, workerHealthMonitor,
      null, null, null, hotKeyStateMachine, healthViewProvider);

    Map<String, Object> info = ep.hotKeyInfo();
    Map<String, Object> worker = (Map<String, Object>) info.get("worker");
    assertThat(worker).containsEntry("trackedKeys", 3);
    assertThat(worker).containsKey("health");
  }

  /* no rule matcher rules */

  /**
   * Verifies that the local section shows an empty rule list when no rules are configured.
   */
  @Test
  void localSection_shouldShowEmptyRulesWhenNoRules() {
    mockTopK(hotKeyDetector, List.of(), 0L);
    mockTopK(workerTopK, List.of(), 0L);
    when(caffeineCache.estimatedSize()).thenReturn(0L);
    HotKeyEndpoint ep = endpointWithAll();
    Map<String, Object> info = ep.hotKeyInfo();
    Map<String, Object> local = (Map<String, Object>) info.get("local");
    assertThat(local).containsKey("rules");
    assertThat((List<?>) local.get("rules")).isEmpty();
  }

  /* rule matcher with rules */

  /**
   * Verifies that the local section includes configured rules with their type, pattern, and creation timestamp.
   */
  @Test
  void localSection_shouldIncludeRules() {
    ruleMatcher = new RuleMatcher(Optional.empty(), Optional.empty());
    ruleMatcher.addRule(new Rule(Rule.RuleType.PREFIX, "secret:*", Rule.RuleAction.BLOCK));
    ruleMatcher.addRule(new Rule(Rule.RuleType.PREFIX, "health:*", Rule.RuleAction.ALLOW_NO_REPORT));
    mockTopK(hotKeyDetector, List.of(), 0L);
    when(caffeineCache.estimatedSize()).thenReturn(0L);
    HotKeyEndpoint ep = new HotKeyEndpoint(
      hotKeyDetector, null, caffeineCache, null, properties, null, ruleMatcher,
      null, null, null, null, null, healthViewProvider);
    Map<String, Object> info = ep.hotKeyInfo();
    Map<String, Object> local = (Map<String, Object>) info.get("local");
    assertThat(local).containsKey("rules");
    List<Map<String, Object>> rules = (List<Map<String, Object>>) local.get("rules");
    assertThat(rules).hasSize(2);
    assertThat(rules.get(0)).containsKeys("id", "type", "pattern", "createdAt");
  }

  /* null refresh limiter */

  /**
   * Verifies that the local section skips the refresh pool available field when the limiter is null.
   */
  @Test
  void localSection_shouldSkipRefreshPoolWhenLimiterNull() {
    mockTopK(hotKeyDetector, List.of(), 0L);
    mockTopK(workerTopK, List.of(), 0L);
    when(caffeineCache.estimatedSize()).thenReturn(0L);
    when(expireManager.isSoftExpireEnabled()).thenReturn(true);
    when(expireManager.getEffectiveHardTtlMs()).thenReturn(300000L);
    when(expireManager.getEffectiveSoftTtlMs()).thenReturn(30000L);
    when(expireManager.getEffectiveHotHardTtlMs()).thenReturn(3600000L);
    when(expireManager.getEffectiveHotSoftTtlMs()).thenReturn(300000L);
    when(expireManager.getRefreshLimiter()).thenReturn(null);
    HotKeyEndpoint ep = endpointWithAll();
    Map<String, Object> info = ep.hotKeyInfo();
    Map<String, Object> local = (Map<String, Object>) info.get("local");
    assertThat(local).containsEntry("softExpireEnabled", true);
    assertThat(local).doesNotContainKey("refreshPoolAvailable");
  }

  /* non-HeavyKeeper TopK should not expose algorithm params */

  /**
   * Verifies that the local section does NOT contain HeavyKeeper-specific keys when a non-HeavyKeeper TopK impl is used.
   */
  @Test
  void localSection_shouldNotExposeHeavyKeeperParamsForNonHeavyKeeper() {
    TopK customTopK = mock(TopK.class);
    when(customTopK.list()).thenReturn(List.of(new Item("k1", 1)));
    when(customTopK.total()).thenReturn(10L);
    when(customTopK.expelled()).thenReturn(new LinkedBlockingQueue<>());
    when(caffeineCache.estimatedSize()).thenReturn(0L);
    HotKeyEndpoint ep = new HotKeyEndpoint(
      customTopK, null, caffeineCache, null, properties, null, null,
      null, null, null, null, null, healthViewProvider);

    Map<String, Object> info = ep.hotKeyInfo();
    Map<String, Object> local = (Map<String, Object>) info.get("local");

    assertThat(local).containsKey("topKCount");
    assertThat(local).doesNotContainKeys("topKCapacity", "sketchWidth", "sketchDepth", "minCountThreshold");
  }

  /* worker section with null health view */

  /**
   * Verifies that the worker section omits health when the health view provider returns null.
   */
  @Test
  void workerSection_shouldOmitHealthWhenHealthViewNull() {
    mockTopK(workerTopK, List.of(new Item("wk", 1)), 5L);
    ObjectProvider<ClusterHealthView> nullHvProvider = mock(ObjectProvider.class);
    when(nullHvProvider.getIfAvailable()).thenReturn(null);
    HotKeyEndpoint ep = new HotKeyEndpoint(
      null, workerTopK, null, null, properties, null, null, workerHealthMonitor,
      null, null, null, hotKeyStateMachine, nullHvProvider);

    Map<String, Object> info = ep.hotKeyInfo();
    Map<String, Object> worker = (Map<String, Object>) info.get("worker");

    assertThat(worker).doesNotContainKey("health");
  }

  /* expelled queue edge cases */

  /**
   * Verifies that the local section reports expelled queue metrics even when the queue is empty.
   */
  @Test
  void localSection_shouldReportExpelledQueueMetricsWhenEmpty() {
    when(hotKeyDetector.list()).thenReturn(List.of());
    when(hotKeyDetector.total()).thenReturn(0L);
    when(hotKeyDetector.expelled()).thenReturn(new LinkedBlockingQueue<>());
    when(workerTopK.expelled()).thenReturn(new LinkedBlockingQueue<>());
    when(caffeineCache.estimatedSize()).thenReturn(0L);
    HotKeyEndpoint ep = endpointWithAll();
    Map<String, Object> info = ep.hotKeyInfo();
    Map<String, Object> local = (Map<String, Object>) info.get("local");

    assertThat(local).containsKey("expelledQueueSize");
    assertThat(local).containsKey("expelledQueueRemaining");
  }

  /* topK list values match mock data */

  /**
   * Verifies that the local and worker TopK sections correctly reflect the counts from mock data.
   */
  @Test
  void localAndWorkerTopK_shouldRespectMockData() {
    mockTopK(hotKeyDetector, List.of(new Item("k1", 10)), 200L);
    mockTopK(workerTopK, List.of(new Item("wk1", 5)), 80L);
    when(caffeineCache.estimatedSize()).thenReturn(0L);
    Map<String, Object> info = endpointWithAll().hotKeyInfo();
    Map<String, Object> local = (Map<String, Object>) info.get("local");
    Map<String, Object> worker = (Map<String, Object>) info.get("worker");
    assertThat(local).containsEntry("topKCount", 1);
    assertThat(local).containsEntry("totalRequests", 200L);
    assertThat(worker).containsEntry("topKCount", 1);
    assertThat(worker).containsEntry("totalRequests", 80L);
  }
}
