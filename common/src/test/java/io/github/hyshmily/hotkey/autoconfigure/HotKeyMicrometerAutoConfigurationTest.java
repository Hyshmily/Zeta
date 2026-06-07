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
package io.github.hyshmily.hotkey.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.CacheSyncPublisher;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import io.github.hyshmily.hotkey.hotkeycache.VersionController;
import io.github.hyshmily.hotkey.monitor.WorkerHealthMonitor;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link HotKeyMicrometerAutoConfiguration}.
 */
class HotKeyMicrometerAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(HotKeyMicrometerAutoConfiguration.class));

  private HotKeyMicrometerAutoConfiguration config;
  private SimpleMeterRegistry registry;

  @BeforeEach
  void setUp() {
    config = new HotKeyMicrometerAutoConfiguration();
    registry = new SimpleMeterRegistry();
  }

  @Test
  void configLoadsWhenMeterBinderIsOnClasspath() {
    runner.run(ctx -> assertThat(ctx).hasSingleBean(HotKeyProperties.class));
  }

  @Test
  void caffeineMeterBinder_registersMetrics() {
    Cache<String, Object> cache = Caffeine.newBuilder().recordStats().build();
    cache.put("a", 1);
    cache.getIfPresent("a");
    cache.getIfPresent("missing");

    @SuppressWarnings("unchecked")
    ObjectProvider<Cache<String, Object>> provider = mock(ObjectProvider.class);
    doAnswer(inv -> {
      ((Consumer<Cache<String, Object>>) inv.getArgument(0)).accept(cache);
      return null;
    }).when(provider).ifAvailable(any());

    MeterBinder binder = config.hotKeyCaffeineMetrics(provider);
    binder.bindTo(registry);

    assertThat(registry.getMeters()).isNotEmpty();
    assertThat(registry.find("cache.gets").tags("result", "hit").functionCounter()).isNotNull();
    assertThat(registry.find("cache.gets").tags("result", "miss").functionCounter()).isNotNull();
    assertThat(registry.find("cache.size").gauge()).isNotNull();
  }

  @Test
  void customMeterBinder_registersAllMetrics_whenAllDepsPresent() {
    TopK detector = mockTopK(5, 100L, 3, 97);
    TopK workerTopK = mockTopK(3, 500L, 1, 99);
    SingleFlight sf = mock(SingleFlight.class);
    when(sf.estimatedInflightSize()).thenReturn(2L);
    HotKeyReporter reporter = mock(HotKeyReporter.class);
    when(reporter.dispatcherDepth()).thenReturn(10);
    when(reporter.dispatcherDropped()).thenReturn(5L);
    when(reporter.dispatcherExpired()).thenReturn(3L);
    when(reporter.getPendingKeyCount()).thenReturn(200L);
    CacheExpireManager expireManager = mock(CacheExpireManager.class);
    when(expireManager.getRefreshLimiter()).thenReturn(new Semaphore(8));
    VersionController vc = mock(VersionController.class);
    when(vc.getDegradedVersionCount()).thenReturn(7L);
    CacheSyncPublisher csp = mock(CacheSyncPublisher.class);
    when(csp.getDedupCacheSize()).thenReturn(15L);
    WorkerHealthMonitor whm = mock(WorkerHealthMonitor.class);
    when(whm.isAnyWorkerAlive()).thenReturn(true);
    HotKeyStateMachine sm = mock(HotKeyStateMachine.class);
    when(sm.getTrackedKeys()).thenReturn(12);

    MeterBinder binder = config.hotKeyCustomMetrics(
      providerThatReturns(detector),
      providerThatReturns(workerTopK),
      providerThatReturns(sf),
      providerThatReturns(reporter),
      providerThatReturns(expireManager),
      providerThatReturns(vc),
      providerThatReturns(csp),
      providerThatReturns(sm),
      providerThatReturns(whm)
    );
    binder.bindTo(registry);

    assertGaugeValue("hotkey.topk.size", "type", "local", 5.0);
    assertGaugeValue("hotkey.topk.total", "type", "local", 100.0);
    assertGaugeExists("hotkey.expelled.queue.size");
    assertGaugeExists("hotkey.expelled.queue.remaining");
    assertGaugeValue("hotkey.singleflight.inflight", 2.0);
    assertGaugeValue("hotkey.reporter.queue.depth", 10.0);
    assertGaugeValue("hotkey.reporter.queue.dropped.total", 5.0);
    assertGaugeValue("hotkey.reporter.queue.expired.total", 3.0);
    assertGaugeValue("hotkey.reporter.pending.keys", 200.0);
    assertGaugeValue("hotkey.expire.refresh.available", 8.0);
    assertGaugeValue("hotkey.version.degraded.total", 7.0);
    assertGaugeValue("hotkey.sync.dedup.size", 15.0);
    assertGaugeValue("hotkey.topk.size", "type", "worker", 3.0);
    assertGaugeValue("hotkey.topk.total", "type", "worker", 500.0);
    assertGaugeValue("hotkey.worker.alive", 1.0);
    assertGaugeValue("hotkey.worker.tracked.keys", 12.0);
  }

  @Test
  void customMeterBinder_handlesNoDeps() {
    MeterBinder binder = config.hotKeyCustomMetrics(
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null)
    );
    binder.bindTo(registry);

    assertThat(registry.getMeters()).isEmpty();
  }

  @Test
  void customMeterBinder_handlesNullRefreshLimiter() {
    CacheExpireManager expireManager = mock(CacheExpireManager.class);
    when(expireManager.getRefreshLimiter()).thenReturn(null);

    MeterBinder binder = config.hotKeyCustomMetrics(
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(expireManager),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null)
    );
    binder.bindTo(registry);

    assertThat(registry.find("hotkey.expire.refresh.available").gauge()).isNull();
    assertThat(registry.getMeters()).isEmpty();
  }

  @Test
  void customMeterBinder_registersLocalOnly() {
    TopK detector = mockTopK(3, 50L, 0, 100);
    SingleFlight sf = mock(SingleFlight.class);
    when(sf.estimatedInflightSize()).thenReturn(1L);

    MeterBinder binder = config.hotKeyCustomMetrics(
      providerThatReturns(detector),
      providerThatReturns(null),
      providerThatReturns(sf),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null)
    );
    binder.bindTo(registry);

    assertGaugeValue("hotkey.topk.size", "type", "local", 3.0);
    assertGaugeValue("hotkey.topk.total", "type", "local", 50.0);
    assertGaugeExists("hotkey.expelled.queue.size");
    assertGaugeExists("hotkey.expelled.queue.remaining");
    assertGaugeValue("hotkey.singleflight.inflight", 1.0);

    assertThat(registry.find("hotkey.topk.size").tags("type", "worker").gauge()).isNull();
    assertThat(registry.find("hotkey.reporter.queue.depth").gauge()).isNull();
    assertThat(registry.find("hotkey.worker.alive").gauge()).isNull();
    assertThat(registry.find("hotkey.worker.tracked.keys").gauge()).isNull();
    assertThat(registry.find("hotkey.sync.dedup.size").gauge()).isNull();
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private <T> ObjectProvider<T> providerThatReturns(T value) {
    ObjectProvider<T> provider = mock(ObjectProvider.class);
    doAnswer(invocation -> {
      if (value != null) {
        ((Consumer<T>) invocation.getArgument(0)).accept(value);
      }
      return null;
    }).when(provider).ifAvailable(any());
    return provider;
  }

  private TopK mockTopK(int listSize, long total, int expelledSize, int expelledRemaining) {
    TopK topK = mock(TopK.class);
    List<Item> items = mock(List.class);
    when(items.size()).thenReturn(listSize);
    when(topK.list()).thenReturn(items);
    when(topK.total()).thenReturn(total);
    BlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    for (int i = 0; i < expelledSize; i++) {
      queue.add(mock(Item.class));
    }
    when(topK.expelled()).thenReturn(queue);
    return topK;
  }

  private void assertGaugeExists(String name) {
    assertThat(registry.find(name).gauge()).as("gauge %s should exist", name).isNotNull();
  }

  private void assertGaugeValue(String name, double expected) {
    assertThat(registry.find(name).gauge().value()).as("gauge %s value", name).isEqualTo(expected);
  }

  private void assertGaugeValue(String name, String tagKey, String tagValue, double expected) {
    assertThat(registry.find(name).tags(tagKey, tagValue).gauge().value())
      .as("gauge %s{%s=%s} value", name, tagKey, tagValue)
      .isEqualTo(expected);
  }
}
