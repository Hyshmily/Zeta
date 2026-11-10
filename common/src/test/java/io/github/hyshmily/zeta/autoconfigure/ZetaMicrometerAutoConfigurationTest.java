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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.cache.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Set;
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
 * Tests for {@link ZetaMicrometerAutoConfiguration}.
 */
class ZetaMicrometerAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(ZetaMicrometerAutoConfiguration.class)
  );

  private ZetaMicrometerAutoConfiguration config;
  private SimpleMeterRegistry registry;

  @BeforeEach
  void setUp() {
    config = new ZetaMicrometerAutoConfiguration();
    registry = new SimpleMeterRegistry();
  }

  /**
   * Verifies that the Micrometer auto-configuration loads when MeterBinder is on the classpath.
   */
  @Test
  void configLoadsWhenMeterBinderIsOnClasspath() {
    runner.run(ctx -> assertThat(ctx).hasSingleBean(ZetaProperties.class));
  }

  /**
   * A user-defined {@link MeterBinder} must not suppress Zeta's binders: the conditions are
   * name-based, so any unrelated application binder leaves both Zeta binders registered.
   */
  @Test
  void userMeterBinder_doesNotSuppressZetaBinders() {
    runner
      .withUserConfiguration(UserMeterBinderConfig.class)
      .run(ctx -> {
        assertThat(ctx).hasBean("hotKeyCaffeineMetrics");
        assertThat(ctx).hasBean("hotKeyCustomMetrics");
      });
  }

  @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
  static class UserMeterBinderConfig {

    @org.springframework.context.annotation.Bean
    MeterBinder applicationMeterBinder() {
      return registry -> {};
    }
  }

  /**
   * Verifies that the Caffeine cache MeterBinder registers hit/miss and size metrics.
   */
  @Test
  void caffeineMeterBinder_registersMetrics() {
    Cache<String, CacheEntry> cache = Caffeine.newBuilder().recordStats().build();
    cache.put("a", EntryDraft.blank(null).value(1).version(0).hardExpiryAt(Long.MAX_VALUE).build());
    cache.getIfPresent("a");
    cache.getIfPresent("missing");

    @SuppressWarnings("unchecked")
    ObjectProvider<Cache<String, CacheEntry>> provider = mock(ObjectProvider.class);
    doAnswer(inv -> {
      ((Consumer<Cache<String, CacheEntry>>) inv.getArgument(0)).accept(cache);
      return null;
    })
      .when(provider)
      .ifAvailable(any());

    MeterBinder binder = config.hotKeyCaffeineMetrics(provider);
    binder.bindTo(registry);

    assertThat(registry.getMeters()).isNotEmpty();
    assertThat(registry.find("cache.gets").tags("result", "hit").functionCounter()).isNotNull();
    assertThat(registry.find("cache.gets").tags("result", "miss").functionCounter()).isNotNull();
    assertThat(registry.find("cache.size").gauge()).isNotNull();
  }

  /**
   * Verifies that the custom MeterBinder registers all zeta-specific metrics when all optional dependencies are present.
   */
  @Test
  void customMeterBinder_registersAllMetrics_whenAllDepsPresent() {
    TopK detector = mockTopK(5, 100L, 3);
    SingleFlight sf = mock(SingleFlight.class);
    when(sf.estimatedInFlightSize()).thenReturn(2L);
    KeyReporter reporter = mock(KeyReporter.class);
    when(reporter.dispatcherStats()).thenReturn(new KeyReporter.DispatcherStats(10, 0, 0, 5));
    // design decisions: access counting: pending keys read from the shared counter's real reservoir.
    io.github.hyshmily.zeta.cache.AccessCounter accessCounter = new io.github.hyshmily.zeta.cache.AccessCounter(
      mock(io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector.class),
      null,
      mock(java.util.concurrent.ScheduledExecutorService.class),
      50L,
      500L
    );
    accessCounter.add("pending-1");
    accessCounter.add("pending-2");
    BackgroundRefresher backgroundRefresher = mock(BackgroundRefresher.class);
    when(backgroundRefresher.getRefreshLimiter()).thenReturn(new Semaphore(8));
    StampClock clock = mock(StampClock.class);
    when(clock.skewMillis()).thenReturn(7L);
    CacheSyncPublisher csp = mock(CacheSyncPublisher.class);
    HealthView healthView = mock(HealthView.class);
    when(healthView.isClusterHealthy()).thenReturn(true);
    when(healthView.getAliveWorkerIds()).thenReturn(Set.of("worker-1"));
    BroadcastBuffer broadcastBuffer = mock(BroadcastBuffer.class);
    when(broadcastBuffer.sendFailures()).thenReturn(2L);
    when(broadcastBuffer.saturationDrops()).thenReturn(1L);
    when(broadcastBuffer.invalidatesDropped()).thenReturn(4L);

    MeterBinder binder = config.hotKeyCustomMetrics(
      providerThatReturns(detector),
      providerThatReturns(sf),
      providerThatReturns(reporter),
      providerThatReturns(accessCounter),
      providerThatReturns(broadcastBuffer),
      providerThatReturns(backgroundRefresher),
      providerThatReturns(clock),
      providerThatReturns(csp),
      providerThatReturns(healthView),
      providerThatReturns(null),
      providerThatReturns(null)
    );
    binder.bindTo(registry);

    assertGaugeValue("zeta.topk.size", "type", "local", 5.0);
    assertGaugeValue("zeta.topk.total", "type", "local", 100.0);
    assertGaugeExists("zeta.expelled.queue.size");
    assertGaugeValue("zeta.singleflight.inflight", 2.0);
    assertGaugeValue("zeta.reporter.queue.depth", 10.0);
    assertCounterValue("zeta.reporter.queue.dropped.total", 5.0);
    assertCounterValue("zeta.reporter.queue.expired.dead.total", 0.0);
    assertCounterValue("zeta.reporter.queue.expired.stale.total", 0.0);
    // Two distinct keys counted into the real reservoir (design decisions: access counting).
    assertGaugeValue("zeta.reporter.pending.keys", 2.0);
    assertGaugeValue("zeta.expire.refresh.available", 8.0);
    assertGaugeValue("zeta.version.clock.skew", 7.0);
    assertGaugeValue("zeta.worker.alive", 1.0);
    // Stall-cause meters: only the causes with no other series.
    assertCounterValue("zeta.stall.broadcast_storm.stopped.total", 3.0);
    assertCounterValue("zeta.sync.invalidate.dropped.total", 4.0);
    assertGaugeValue("zeta.stall.source_degraded.stopped", 0.0);
    assertCounterValue("zeta.stall.source_degraded.timeouts.total", 0.0);
    // Removed duplicates/derivations must not come back.
    assertThat(registry.find("zeta.expelled.queue.remaining").gauge()).isNull();
    assertThat(registry.find("zeta.reporter.queue.expired.total").gauge()).isNull();
    assertThat(registry.find("zeta.sync.dedup.size").gauge()).isNull();
    assertThat(registry.find("zeta.stall.report_backpressure.delayed").gauge()).isNull();
    assertThat(registry.find("zeta.stall.report_backpressure.stopped.total").gauge()).isNull();
    assertThat(registry.find("zeta.stall.worker_partition.stopped").gauge()).isNull();
  }

  /**
   * Verifies that the custom MeterBinder registers no metrics when all optional dependencies are absent.
   */
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
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null)
    );
    binder.bindTo(registry);

    assertThat(registry.getMeters()).isEmpty();
  }

  /**
   * An unstarted dispatcher reads as unknown depth ({@code -1}) and zero monotonic totals: a
   * counter must never report a negative value.
   */
  @Test
  void customMeterBinder_unstartedDispatcherUsesTheFallbacks() {
    KeyReporter reporter = mock(KeyReporter.class);
    when(reporter.dispatcherStats()).thenReturn(null);

    MeterBinder binder = config.hotKeyCustomMetrics(
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(reporter),
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

    assertGaugeValue("zeta.reporter.queue.depth", -1.0);
    assertCounterValue("zeta.reporter.queue.dropped.total", 0.0);
    assertCounterValue("zeta.reporter.queue.expired.dead.total", 0.0);
    assertCounterValue("zeta.reporter.queue.expired.stale.total", 0.0);
  }

  /**
   * Verifies that the custom MeterBinder gracefully handles a null refresh limiter from BackgroundRefresherImpl.
   */
  @Test
  void customMeterBinder_handlesNullRefreshLimiter() {
    BackgroundRefresher backgroundRefresher = mock(BackgroundRefresher.class);
    when(backgroundRefresher.getRefreshLimiter()).thenReturn(null);

    MeterBinder binder = config.hotKeyCustomMetrics(
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(backgroundRefresher),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null),
      providerThatReturns(null)
    );
    binder.bindTo(registry);

    assertThat(registry.find("zeta.expire.refresh.available").gauge()).isNull();
    // Lease counters live on the refresher seam itself (not the limiter), so
    // they register even when the limiter is absent.
    assertThat(registry.find("zeta.expire.refresh.failure.leased.total").functionCounter()).isNotNull();
    assertThat(registry.find("zeta.expire.refresh.lease.debt.keys").gauge()).isNotNull();
    assertThat(registry.getMeters()).hasSize(2);
  }

  /**
   * Verifies that the custom MeterBinder registers only local metrics when worker/reporter/sync dependencies are absent.
   */
  @Test
  void customMeterBinder_registersLocalOnly() {
    TopK detector = mockTopK(3, 50L, 0);
    SingleFlight sf = mock(SingleFlight.class);
    when(sf.estimatedInFlightSize()).thenReturn(1L);

    MeterBinder binder = config.hotKeyCustomMetrics(
      providerThatReturns(detector),
      providerThatReturns(sf),
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

    assertGaugeValue("zeta.topk.size", "type", "local", 3.0);
    assertGaugeValue("zeta.topk.total", "type", "local", 50.0);
    assertGaugeExists("zeta.expelled.queue.size");
    assertGaugeValue("zeta.singleflight.inflight", 1.0);

    assertThat(registry.find("zeta.topk.size").tags("type", "worker").gauge()).isNull();
    assertThat(registry.find("zeta.reporter.queue.depth").gauge()).isNull();
    assertThat(registry.find("zeta.worker.alive").gauge()).isNull();
    assertThat(registry.find("zeta.sync.dedup.size").gauge()).isNull();
  }

  @SuppressWarnings("unchecked")
  private <T> ObjectProvider<T> providerThatReturns(T value) {
    ObjectProvider<T> provider = mock(ObjectProvider.class);
    doAnswer(invocation -> {
      if (value != null) {
        ((Consumer<T>) invocation.getArgument(0)).accept(value);
      }
      return null;
    })
      .when(provider)
      .ifAvailable(any());
    return provider;
  }

  private TopK mockTopK(int listSize, long total, int expelledSize) {
    TopK topK = mock(TopK.class);
    List<Item> items = mock(List.class);
    when(items.size()).thenReturn(listSize);
    when(topK.list()).thenReturn(items);
    // The size gauge reads the O(1) estimate, never the (materialising) ranking list.
    when(topK.estimatedSize()).thenReturn(listSize);
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

  private void assertCounterValue(String name, double expected) {
    assertThat(registry.find(name).functionCounter().count()).as("counter %s value", name).isEqualTo(expected);
  }
}
