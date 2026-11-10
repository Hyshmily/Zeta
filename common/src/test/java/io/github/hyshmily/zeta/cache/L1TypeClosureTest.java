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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pin for the F1 L1 type closure (design decisions: entry model): every L1 slot holds a
 * {@link CacheEntry} — no bare user values, no representation that needs an
 * {@code instanceof} discrimination. A raw write is unrepresentable: the
 * compiler rejects it.
 */
class L1TypeClosureTest {

  private Cache<String, CacheEntry> caffeineCache;
  private HotKeyCache hotKeyCache;
  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() {
    HotKeyDetector hotKeyDetector = mock(HotKeyDetector.class);
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ZetaProperties ttlConfig = new ZetaProperties();
    HealthView healthView = mock(HealthView.class);
    EntryLifecycle entryLifecycle = new EntryLifecycleImpl(caffeineCache, ttlConfig, healthView);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    hotKeyCache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      mock(SingleFlight.class),
      entryLifecycle,
      new DefaultBackgroundRefresher(caffeineCache, Runnable::run, entryLifecycle, 10),
      new AccessCounter(hotKeyDetector, null, scheduler, 50L, AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS),
      new BroadcastBuffer(scheduler, null, 500, 2_000, null),
      mock(StampClock.class),
      healthView
    );
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  void putLocal_slotHoldsCacheEntry() {
    hotKeyCache.putLocal("k", "v", 0L, 0L);

    CacheEntry stored = caffeineCache.getIfPresent("k");
    assertThat(stored).isNotNull();
    assertThat(stored.getValue()).isEqualTo("v");
    assertThat(stored.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void unwrapForStats_absentSlotIsNull() {
    assertThat(HotKeyCache.<Object>unwrap(null)).isNull();
  }
}
