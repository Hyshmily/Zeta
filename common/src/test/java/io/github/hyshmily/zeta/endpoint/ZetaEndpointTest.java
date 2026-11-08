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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.AccessCounter;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Aggregate verification for {@link ZetaEndpoint}.
 *
 * <p>Covers every field in every section (local, worker, sync), HeavyKeeper-specific
 * algorithm parameters, null-safety for all components, and partial-deployment scenarios.
 */
@SuppressWarnings("unchecked")
class ZetaEndpointTest {

  private TopK hotKeyDetector;
  private Cache<String, CacheEntry> caffeineCache;
  private SingleFlight singleFlight;
  private KeyReporter keyReporter;
  private BackgroundRefresher backgroundRefresher;
  private StampClock stampClock;
  private HealthView healthView;
  private AccessCounter accessCounter;
  private CacheSyncListener syncListener;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    hotKeyDetector = mock(TopK.class);
    caffeineCache = mock(Cache.class);
    singleFlight = mock(SingleFlight.class);
    keyReporter = mock(KeyReporter.class);
    backgroundRefresher = mock(BackgroundRefresher.class);
    stampClock = mock(StampClock.class);
    healthView = mock(HealthView.class);
    accessCounter = mock(AccessCounter.class);
    syncListener = mock(CacheSyncListener.class);
    when(healthView.isClusterHealthy()).thenReturn(true);
  }

  /* helpers */

  private ZetaEndpoint endpointWithAll() {
    return ZetaEndpoint.builder()
      .hotKeyDetector(hotKeyDetector)
      .caffeineCache(caffeineCache)
      .singleFlight(singleFlight)
      .hotKeyReporter(keyReporter)
      .backgroundRefresher(backgroundRefresher)
      .stampClock(stampClock)
      .healthView(healthView)
      .accessCounter(accessCounter)
      .syncListener(syncListener)
      .build();
  }

  private void mockTopK(TopK topK, List<Item> items, long total) {
    when(topK.listTopN(100)).thenReturn(items);
    when(topK.total()).thenReturn(total);
  }

  /* all components present */

  /**
   * Verifies that hotKeyInfo returns all three sections (local, worker, sync) with their respective fields when all components are present.
   */
  @Test
  void hotKeyInfo_shouldContainLocalWorkerAndSyncSections() {
    mockTopK(hotKeyDetector, List.of(new Item("k1", 10), new Item("k2", 7)), 200L);
    when(caffeineCache.estimatedSize()).thenReturn(42L);
    when(singleFlight.estimatedInFlightSize()).thenReturn(3L);
    when(keyReporter.dispatcherStats()).thenReturn(new KeyReporter.DispatcherStats(7, 2, 0, 1));
    when(accessCounter.pendingKeyCount()).thenReturn(5L);
    when(backgroundRefresher.getRefreshLimiter()).thenReturn(new Semaphore(50));
    when(stampClock.skewMillis()).thenReturn(0L);
    when(syncListener.foreignAppDrops()).thenReturn(4L);
    when(syncListener.lastForeignApp()).thenReturn("other-app");
    when(syncListener.nodeIdCollisions()).thenReturn(2L);

    Map<String, Object> info = endpointWithAll().hotKeyInfo(100);

    assertThat(info).containsKeys("instanceId", "jvmId");
    assertThat(info).doesNotContainKey("dataVersionNodeId");
    assertThat(info).containsKeys("local", "worker", "sync");

    // ── local ──
    Map<String, Object> local = (Map<String, Object>) info.get("local");
    assertThat(local).containsEntry("totalRequests", 200L);
    assertThat(local).containsKey("topK");
    assertThat(local).containsEntry("cacheSize", 42L);
    assertThat(local).containsEntry("inFlightSize", 3L);
    assertThat(local).containsEntry("reportQueueDepth", 7);
    assertThat(local).containsEntry("reportQueueFullCount", 1L);
    assertThat(local).containsEntry("reportPendingKeys", 5L);
    assertThat(local).containsEntry("refreshPoolAvailable", 50);
    assertThat(local).containsEntry("versionClockSkewMillis", 0L);
    assertThat(local)
      .as("configuration echoes, derived counts and internals are not exposed")
      .doesNotContainKeys(
        "reportExpiredCount",
        "topKCount",
        "recentlyExpelled",
        "expelledQueueSize",
        "expelledQueueRemaining",
        "topKCapacity",
        "sketchWidth",
        "sketchDepth",
        "minCountThreshold",
        "cacheMaxSize",
        "inFlightMaxSize",
        "inFlightTtlSec",
        "inFlightTimeoutSec",
        "reportQueueCapacity",
        "hardTtlMs",
        "softTtlMs",
        "hotHardTtlMs",
        "hotSoftTtlMs",
        "refreshFailureLeased",
        "leaseStaleCapped",
        "leaseSuppressedByClassifier"
      );

    // ── worker ──
    Map<String, Object> worker = (Map<String, Object>) info.get("worker");
    assertThat(worker).containsKey("health");

    // ── sync ──
    Map<String, Object> sync = (Map<String, Object>) info.get("sync");
    assertThat(sync).containsEntry("foreignAppDrops", 4L);
    assertThat(sync).containsEntry("lastForeignApp", "other-app");
    assertThat(sync).containsEntry("nodeIdCollisions", 2L);
    assertThat(sync).doesNotContainKey("dedupCacheSize");
  }

  /* algorithm parameters are configuration, not runtime state */

  /**
   * The detector's algorithm parameters (capacity, sketch width/depth, min add) are
   * configuration: identical between scrapes and readable from {@code configprops}. The
   * response carries only what the detector observes — the ranking and the total.
   */
  @Test
  void localSection_shouldNotExposeDetectorConfiguration() {
    HeavyKeeper hk = new HeavyKeeper(10, 500, 4, 0.9, 5, 10_000, 3, false);
    hk.addDirect("k1", 20);
    ZetaEndpoint ep = ZetaEndpoint.builder()
      .hotKeyDetector(hk)
      .caffeineCache(caffeineCache)
      .singleFlight(singleFlight)
      .hotKeyReporter(keyReporter)
      .healthView(healthView)
      .build();
    when(caffeineCache.estimatedSize()).thenReturn(10L);

    Map<String, Object> info = ep.hotKeyInfo(100);
    Map<String, Object> local = (Map<String, Object>) info.get("local");

    assertThat((List<?>) local.get("topK")).hasSize(1);
    assertThat(local).doesNotContainKeys(
      "topKCapacity",
      "sketchWidth",
      "sketchDepth",
      "minCountThreshold",
      "topKCount",
      "expelledQueueSize",
      "expelledQueueRemaining",
      "recentlyExpelled"
    );
  }

  /* null components */

  /**
   * Verifies that hotKeyInfo omits all three sections when all optional components are null.
   */
  @Test
  void hotKeyInfo_shouldOmitSectionWhenAllComponentsNull() {
    ZetaEndpoint minimal = ZetaEndpoint.builder().build();
    Map<String, Object> info = minimal.hotKeyInfo(100);
    assertThat(info).doesNotContainKeys("local", "worker", "sync");
    assertThat(info).containsKeys("instanceId", "jvmId");
  }

  /* partial: only local, no worker/sync */

  /**
   * Verifies that hotKeyInfo includes local but omits worker and sync when only local components are provided.
   */
  @Test
  void hotKeyInfo_shouldOmitWorkerAndSyncWhenOnlyLocalComponents() {
    when(hotKeyDetector.listTopN(100)).thenReturn(List.of());
    when(hotKeyDetector.total()).thenReturn(0L);
    when(caffeineCache.estimatedSize()).thenReturn(5L);
    ZetaEndpoint ep = ZetaEndpoint.builder().hotKeyDetector(hotKeyDetector).caffeineCache(caffeineCache).build();

    Map<String, Object> info = ep.hotKeyInfo(100);
    assertThat(info).containsKey("local");
    assertThat(info).doesNotContainKeys("worker", "sync");
  }

  /* partial: only sync, no local/worker */

  /**
   * Verifies that hotKeyInfo includes sync but omits local and worker when only sync components are provided.
   */
  @Test
  void hotKeyInfo_shouldIncludeSyncWhenOnlySyncComponents() {
    when(syncListener.foreignAppDrops()).thenReturn(3L);
    ZetaEndpoint ep = ZetaEndpoint.builder().syncListener(syncListener).build();

    Map<String, Object> info = ep.hotKeyInfo(100);
    assertThat(info).doesNotContainKeys("local", "worker");
    assertThat(info).containsKey("sync");
    assertThat(((Map<String, Object>) info.get("sync"))).containsEntry("foreignAppDrops", 3L);
  }

  /* worker section with multiple shards */

  /**
   * Verifies that the worker section lists health info for all known Worker nodes.
   */
  @Test
  void workerSection_shouldListWorkerHealth() {
    ZetaEndpoint ep = ZetaEndpoint.builder().healthView(healthView).build();

    Map<String, Object> info = ep.hotKeyInfo(100);
    Map<String, Object> worker = (Map<String, Object>) info.get("worker");
    assertThat(worker).containsKey("health");
  }

  /* null refresh limiter */

  /**
   * Verifies that the local section skips the refresh pool available field when the limiter is null.
   */
  @Test
  void localSection_shouldSkipRefreshPoolWhenLimiterNull() {
    mockTopK(hotKeyDetector, List.of(), 0L);
    when(caffeineCache.estimatedSize()).thenReturn(0L);
    when(backgroundRefresher.getRefreshLimiter()).thenReturn(null);
    ZetaEndpoint ep = endpointWithAll();
    Map<String, Object> info = ep.hotKeyInfo(100);
    Map<String, Object> local = (Map<String, Object>) info.get("local");
    assertThat(local).doesNotContainKey("refreshPoolAvailable");
  }

  /* non-HeavyKeeper TopK should not expose algorithm params */

  /**
   * Verifies that the local section does NOT contain HeavyKeeper-specific keys when a non-HeavyKeeper TopK impl is used.
   */
  @Test
  void localSection_shouldNotExposeHeavyKeeperParamsForNonHeavyKeeper() {
    TopK customTopK = mock(TopK.class);
    when(customTopK.listTopN(100)).thenReturn(List.of(new Item("k1", 1)));
    when(customTopK.total()).thenReturn(10L);
    when(caffeineCache.estimatedSize()).thenReturn(0L);
    ZetaEndpoint ep = ZetaEndpoint.builder()
      .hotKeyDetector(customTopK)
      .caffeineCache(caffeineCache)
      .healthView(healthView)
      .build();

    Map<String, Object> info = ep.hotKeyInfo(100);
    Map<String, Object> local = (Map<String, Object>) info.get("local");

    assertThat((List<?>) local.get("topK")).hasSize(1);
    assertThat(local).doesNotContainKeys("topKCapacity", "sketchWidth", "sketchDepth", "minCountThreshold");
  }

  /* worker section with null health view */

  /**
   * Verifies that the worker section omits health when the health view provider returns null.
   */
  @Test
  void workerSection_shouldBeOmittedWhenNoWorkerComponentExists() {
    ZetaEndpoint ep = ZetaEndpoint.builder().build();

    Map<String, Object> info = ep.hotKeyInfo(100);
    assertThat(info).doesNotContainKey("worker");
  }

  /* topK list values match mock data */

  /**
   * Verifies that the local and worker TopK sections correctly reflect the counts from mock data.
   */
  @Test
  void localAndWorkerTopK_shouldRespectMockData() {
    mockTopK(hotKeyDetector, List.of(new Item("k1", 10)), 200L);
    when(caffeineCache.estimatedSize()).thenReturn(0L);
    Map<String, Object> info = endpointWithAll().hotKeyInfo(100);
    Map<String, Object> local = (Map<String, Object>) info.get("local");
    assertThat((List<?>) local.get("topK")).hasSize(1);
    assertThat(local).containsEntry("totalRequests", 200L);
  }
}
