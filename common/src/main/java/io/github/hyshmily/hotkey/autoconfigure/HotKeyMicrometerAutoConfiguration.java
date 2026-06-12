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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.sync.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.VersionController;
import io.github.hyshmily.hotkey.util.SystemLoadMonitor;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for exposing HotKey metrics via Micrometer.
 *
 * <p>Registers two {@link MeterBinder} beans when Micrometer is on the classpath:
 * <ul>
 *   <li>{@code hotKeyCaffeineMetrics} — standard Caffeine cache metrics (hit rate, eviction, etc.)
 *       under the {@code hotkeydetector.l1} metric prefix.</li>
 *   <li>{@code hotKeyCustomMetrics} — HotKey-specific business metrics covering TopK detection,
 *       SingleFlight, Reporter, ExpireManager, VersionController, SyncPublisher, Worker TopK,
 *       Worker health, and StateMachine.</li>
 * </ul>
 *
 * <p>All custom metrics use the {@code hotkeydetector} namespace. Any missing dependency silently
 * skips the corresponding gauge registration, mirroring the same null-safe approach used
 * by {@link io.github.hyshmily.hotkey.endpoint.HotKeyEndpoint}.
 */
@AutoConfiguration(after = HotKeyAutoConfiguration.class)
@ConditionalOnClass(MeterBinder.class)
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyMicrometerAutoConfiguration {

  /**
   * Registers standard Caffeine cache metrics (named {@code cache.*} with a
   * {@code cache=hotkeydetector.l1} tag) using {@link CaffeineCacheMetrics}. This exposes
   * hit/miss counts, eviction count/weight, estimated size, and max size.
   *
   * <p>Uses {@link ObjectProvider} so the bean is created safely even when the
   * L1 cache is absent (e.g. Worker-only mode).
   *
   * @param hotLocalCacheProvider provider for the L1 Caffeine cache
   * @return a {@link MeterBinder} that registers Caffeine cache metrics
   */
  @Bean
  @ConditionalOnMissingBean
  public MeterBinder hotKeyCaffeineMetrics(
    @Qualifier("hotLocalCache") ObjectProvider<Cache<String, Object>> hotLocalCacheProvider
  ) {
    return registry ->
      hotLocalCacheProvider.ifAvailable(cache -> CaffeineCacheMetrics.monitor(registry, cache, "hotkeydetector.l1"));
  }

  /**
   * Registers HotKey-specific business metrics for local and worker-side components.
   *
   * <p>Metrics registered (each gated by component availability):
   * <table>
   *   <tr><th>Metric name</th><th>Source</th></tr>
   *   <tr><td>{@code hotkeydetector.topk.size}</td><td>TopK current ranking count (tagged type=local/worker)</td></tr>
   *   <tr><td>{@code hotkeydetector.topk.total}</td><td>TopK total requests tracked (tagged type=local/worker)</td></tr>
   *   <tr><td>{@code hotkeydetector.expelled.queue.size}</td><td>Expelled queue backlog</td></tr>
   *   <tr><td>{@code hotkeydetector.expelled.queue.remaining}</td><td>Expelled queue remaining capacity</td></tr>
   *   <tr><td>{@code hotkeydetector.singleflight.inflight}</td><td>SingleFlight in-flight dedup count</td></tr>
   *   <tr><td>{@code hotkeydetector.reporter.queue.depth}</td><td>Reporter queue backlog</td></tr>
   *   <tr><td>{@code hotkeydetector.reporter.queue.dropped.total}</td><td>Cumulative dropped batches</td></tr>
   *   <tr><td>{@code hotkeydetector.reporter.queue.expired.total}</td><td>Cumulative expired batches</td></tr>
   *   <tr><td>{@code hotkeydetector.reporter.pending.keys}</td><td>Keys buffered in reporter counter cache</td></tr>
   *   <tr><td>{@code hotkeydetector.expire.refresh.available}</td><td>Available refresh limiter permits</td></tr>
   *   <tr><td>{@code hotkeydetector.version.degraded.total}</td><td>Cumulative version fallback count</td></tr>
   *   <tr><td>{@code hotkeydetector.sync.dedup.size}</td><td>Broadcast dedup cache size</td></tr>
   *   <tr><td>{@code hotkeydetector.worker.alive}</td><td>Whether any worker shard is alive (0/1)</td></tr>
   *   <tr><td>{@code hotkeydetector.worker.tracked.keys}</td><td>Keys tracked by state machine</td></tr>
   * </table>
   *
   * @param hotKeyDetectorProvider      provider for the app-side TopK detector
   * @param workerTopKProvider          provider for the Worker-side TopK detector
   * @param singleFlightProvider        provider for the SingleFlight dedup layer
   * @param reporterProvider            provider for the HotKey reporter
   * @param expireManagerProvider       provider for the cache expiry manager
   * @param versionControllerProvider   provider for the version controller
   * @param cacheSyncPublisherProvider  provider for the cache sync publisher
   * @param stateMachineProvider        provider for the Worker state machine
   * @param healthViewProvider          provider for the cluster health view
   * @param cpuMonitorProvider          provider for the system CPU load monitor
   * @return a {@link MeterBinder} that registers HotKey-specific business metrics
   */
  @Bean
  @ConditionalOnMissingBean
  public MeterBinder hotKeyCustomMetrics(
    @Qualifier("hotKeyDetector") ObjectProvider<TopK> hotKeyDetectorProvider,
    @Qualifier("workerTopK") ObjectProvider<TopK> workerTopKProvider,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<HotKeyReporter> reporterProvider,
    ObjectProvider<CacheExpireManager> expireManagerProvider,
    ObjectProvider<VersionController> versionControllerProvider,
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider,
    ObjectProvider<HotKeyStateMachine> stateMachineProvider,
    ObjectProvider<ClusterHealthView> healthViewProvider,
    ObjectProvider<SystemLoadMonitor> cpuMonitorProvider
  ) {
    return registry -> {
      hotKeyDetectorProvider.ifAvailable(detector -> registerLocalTopKGauges(detector, registry));
      singleFlightProvider.ifAvailable(sf ->
        Gauge.builder("hotkeydetector.singleflight.inflight", sf, s -> (double) s.estimatedInflightSize()).register(registry)
      );
      reporterProvider.ifAvailable(r -> registerReporterGauges(r, registry));
      expireManagerProvider.ifAvailable(em -> {
        if (em.getRefreshLimiter() != null) {
          Gauge.builder("hotkeydetector.expire.refresh.available", em, e ->
            (double) e.getRefreshLimiter().availablePermits()
          ).register(registry);
        }
      });
      versionControllerProvider.ifAvailable(vc ->
        Gauge.builder("hotkeydetector.version.degraded.total", vc, v -> (double) v.getDegradedVersionCount()).register(registry)
      );
      cacheSyncPublisherProvider.ifAvailable(csp ->
        Gauge.builder("hotkeydetector.sync.dedup.size", csp, p -> (double) p.getDedupCacheSize()).register(registry)
      );
      workerTopKProvider.ifAvailable(wtk -> registerWorkerTopKGauges(wtk, registry));
      healthViewProvider.ifAvailable(hv ->
        Gauge.builder("hotkeydetector.worker.alive", hv, v -> v.isClusterHealthy() ? 1.0 : 0.0).register(registry)
      );
      stateMachineProvider.ifAvailable(sm ->
        Gauge.builder("hotkeydetector.worker.tracked.keys", sm, s -> (double) s.getTrackedKeys()).register(registry)
      );
      cpuMonitorProvider.ifAvailable(cpu ->
        Gauge.builder("hotkeydetector.cpu.load", cpu, c -> c.getCpuLoadEMA()).register(registry)
      );
    };
  }

  /**
   * Register Micrometer gauges for the local app-side TopK detector.
   * Exposes top-K size, total requests, expelled queue size and remaining capacity.
   *
   * @param detector the local TopK detector
   * @param registry the Micrometer meter registry
   */
  private static void registerLocalTopKGauges(TopK detector, MeterRegistry registry) {
    Gauge.builder("hotkeydetector.topk.size", detector, t -> t.list().size())
      .tag("type", "local")
      .register(registry);
    Gauge.builder("hotkeydetector.topk.total", detector, t -> (double) t.total())
      .tag("type", "local")
      .register(registry);
    Gauge.builder("hotkeydetector.expelled.queue.size", detector, t -> (double) t.expelled().size()).register(registry);
    Gauge.builder("hotkeydetector.expelled.queue.remaining", detector, t -> (double) t.expelled().remainingCapacity()).register(
      registry
    );
  }

  /**
   * Register Micrometer gauges for the worker-side TopK detector.
   * Exposes top-K size and total requests tagged with type=worker.
   *
   * @param worker   the worker-side TopK detector
   * @param registry the Micrometer meter registry
   */
  private static void registerWorkerTopKGauges(TopK worker, MeterRegistry registry) {
    Gauge.builder("hotkeydetector.topk.size", worker, t -> t.list().size())
      .tag("type", "worker")
      .register(registry);
    Gauge.builder("hotkeydetector.topk.total", worker, t -> (double) t.total())
      .tag("type", "worker")
      .register(registry);
  }

  /**
   * Register Micrometer gauges for the HotKey reporter.
   * Exposes queue depth, dropped/expired batch counts, pending key count,
   * BBR passed/dropped/inFlight/maxInFlight statistics.
   *
   * @param reporter the HotKey reporter
   * @param registry the Micrometer meter registry
   */
  private static void registerReporterGauges(HotKeyReporter reporter, MeterRegistry registry) {
    Gauge.builder("hotkeydetector.reporter.queue.depth", reporter, r -> (double) r.dispatcherDepth()).register(registry);
    Gauge.builder("hotkeydetector.reporter.queue.dropped.total", reporter, r -> (double) r.dispatcherDropped()).register(
      registry
    );
    Gauge.builder("hotkeydetector.reporter.queue.expired.total", reporter, r -> (double) r.dispatcherExpired()).register(
      registry
    );
    Gauge.builder("hotkeydetector.reporter.pending.keys", reporter, r -> (double) r.getPendingKeyCount()).register(registry);
    Gauge.builder("hotkeydetector.reporter.bbr.passed", reporter, r -> (double) r.bbrPassed()).register(registry);
    Gauge.builder("hotkeydetector.reporter.bbr.dropped", reporter, r -> (double) r.bbrDropped()).register(registry);
    Gauge.builder("hotkeydetector.reporter.bbr.inflight", reporter, r -> (double) r.bbrInFlight()).register(registry);
    Gauge.builder("hotkeydetector.reporter.bbr.maxinflight", reporter, r -> (double) r.bbrMaxInFlight()).register(registry);
  }
}
