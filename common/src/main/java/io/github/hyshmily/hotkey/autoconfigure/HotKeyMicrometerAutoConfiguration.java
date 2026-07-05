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
import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.cache.cachesupport.ExpireManager;
import io.github.hyshmily.hotkey.cache.cachesupport.SingleFlight;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.hotkey.reporting.KeyReporter;
import io.github.hyshmily.hotkey.sharding.HealthView;
import io.github.hyshmily.hotkey.sync.local.CacheSyncPublisher;
import io.github.hyshmily.hotkey.util.SystemLoadMonitor;
import io.github.hyshmily.hotkey.util.version.VersionController;
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
 *   <li>{@code hotKeyCaffeineMetrics} — standard Caffeine cache metrics (hit rate, eviction,
 *       estimated size, max size) under the {@code hotkey.l1} metric prefix via
 *       {@link CaffeineCacheMetrics}.</li>
 *   <li>{@code hotKeyCustomMetrics} — HotKey-specific business metrics covering TopK detection
 *       (local and worker), SingleFlight, Reporter (queue depth, drops, BBR stats), ExpireManager
 *       (refresh permits), VersionController (degraded count), SyncPublisher (dedup cache size),
 *       Worker health (alive/dead), StateMachine (tracked keys), and CPU load (EMA).</li>
 * </ul>
 *
 * <p>All custom metrics use the {@code hotkey} namespace. Any missing dependency silently
 * skips the corresponding gauge registration via {@link ObjectProvider}, mirroring the same
 * null-safe approach used by {@link io.github.hyshmily.hotkey.endpoint.HotKeyEndpoint}.
 *
 * <p>Thread-safe: Micrometer's {@link MeterRegistry} is thread-safe; gauge supplier lambdas
 * delegate to thread-safe component methods.
 */
@Internal
@AutoConfiguration(after = HotKeyAutoConfiguration.class)
@ConditionalOnClass(MeterBinder.class)
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyMicrometerAutoConfiguration {

  /**
   * Registers standard Caffeine cache metrics using {@link CaffeineCacheMetrics}.
   *
   * <p>Metrics are registered under the standard {@code cache.*} namespace with a
   * {@code cache=hotkey.l1} tag. Exposes hit/miss counts, eviction count/weight,
   * estimated size, and max size.
   *
   * <p>Uses {@link ObjectProvider} so the bean is created safely even when the
   * L1 cache is absent (e.g. Worker-only mode), in which case no metrics are
   * registered.
   *
   * @param hotLocalCacheProvider provider for the L1 Caffeine cache (may be absent)
   * @return a {@link MeterBinder} that registers Caffeine cache metrics when the
   *         cache is available; otherwise registers nothing
   */
  @Bean
  @ConditionalOnMissingBean
  public MeterBinder hotKeyCaffeineMetrics(
    @Qualifier("hotLocalCache") ObjectProvider<Cache<String, Object>> hotLocalCacheProvider
  ) {
    return registry ->
      hotLocalCacheProvider.ifAvailable(cache -> CaffeineCacheMetrics.monitor(registry, cache, "hotkey.l1"));
  }

  /**
   * Registers HotKey-specific business metrics for local and worker-side components.
   *
   * <p>Each gauge is gated by component availability via {@link ObjectProvider} —
   * if a component is not present in the current deployment mode, its corresponding
   * metric is silently skipped.
   *
   * <p>Metrics registered:
   * <table>
   *   <tr><th>Metric name</th><th>Source</th><th>Tags</th></tr>
   *   <tr><td>{@code hotkey.topk.size}</td><td>TopK current ranking count</td><td>type=local|worker</td></tr>
   *   <tr><td>{@code hotkey.topk.total}</td><td>TopK total requests tracked</td><td>type=local|worker</td></tr>
   *   <tr><td>{@code hotkey.expelled.queue.size}</td><td>Expelled queue backlog</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.expelled.queue.remaining}</td><td>Expelled queue remaining capacity</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.singleflight.inflight}</td><td>SingleFlight in-flight dedup count</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.reporter.queue.depth}</td><td>Reporter queue backlog</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.reporter.queue.dropped.total}</td><td>Cumulative dropped batches</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.reporter.queue.expired.total}</td><td>Cumulative expired batches</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.reporter.pending.keys}</td><td>Keys buffered in reporter counter cache</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.reporter.bbr.*}</td><td>BBR rate limiter (passed/dropped/inflight/maxinflight)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.expire.refresh.available}</td><td>Available refresh limiter permits</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.version.degraded.total}</td><td>Cumulative version fallback count</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.sync.dedup.size}</td><td>Broadcast dedup cache size</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.worker.alive}</td><td>Whether any worker shard is alive</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.worker.tracked.keys}</td><td>Keys tracked by state machine</td><td>&mdash;</td></tr>
   *   <tr><td>{@code hotkey.cpu.load}</td><td>System CPU load EMA</td><td>&mdash;</td></tr>
   * </table>
   *
   * @param hotKeyDetectorProvider      provider for the app-side TopK (may be absent)
   * @param workerTopKProvider          provider for the Worker-side TopK (may be absent)
   * @param singleFlightProvider        provider for the SingleFlight dedup layer (may be absent)
   * @param reporterProvider            provider for the HotKey reporter (may be absent)
   * @param expireManagerProvider       provider for the cache expiry manager (may be absent)
   * @param versionControllerProvider   provider for the version controller (may be absent)
   * @param cacheSyncPublisherProvider  provider for the cache sync publisher (may be absent)
   * @param stateMachineProvider        provider for the Worker state machine (may be absent)
   * @param healthViewProvider          provider for the cluster health view (may be absent)
   * @param cpuMonitorProvider          provider for the system CPU load monitor (may be absent)
   * @return a {@link MeterBinder} that registers HotKey-specific business metrics
   */
  @Bean
  @ConditionalOnMissingBean
  public MeterBinder hotKeyCustomMetrics(
    @Qualifier("hotKeyDetector") ObjectProvider<TopK> hotKeyDetectorProvider,
    @Qualifier("workerTopK") ObjectProvider<TopK> workerTopKProvider,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<KeyReporter> reporterProvider,
    ObjectProvider<ExpireManager> expireManagerProvider,
    ObjectProvider<VersionController> versionControllerProvider,
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider,
    ObjectProvider<HotKeyStateMachine> stateMachineProvider,
    ObjectProvider<HealthView> healthViewProvider,
    ObjectProvider<SystemLoadMonitor> cpuMonitorProvider
  ) {
    return registry -> {
      hotKeyDetectorProvider.ifAvailable(detector -> registerLocalTopKGauges(detector, registry));
      singleFlightProvider.ifAvailable(sf ->
        Gauge.builder("hotkey.singleflight.inflight", sf, s -> (double) s.estimatedInflightSize()).register(registry)
      );
      reporterProvider.ifAvailable(r -> registerReporterGauges(r, registry));
      expireManagerProvider.ifAvailable(em -> {
        if (em.getRefreshLimiter() != null) {
          Gauge.builder("hotkey.expire.refresh.available", em, e ->
            (double) e.getRefreshLimiter().availablePermits()
          ).register(registry);
        }
      });
      versionControllerProvider.ifAvailable(vc ->
        Gauge.builder("hotkey.version.degraded.total", vc, v -> (double) v.getDegradedVersionCount()).register(registry)
      );
      cacheSyncPublisherProvider.ifAvailable(csp ->
        Gauge.builder("hotkey.sync.dedup.size", csp, p -> (double) p.getDedupCacheSize()).register(registry)
      );
      workerTopKProvider.ifAvailable(wtk -> registerWorkerTopKGauges(wtk, registry));
      healthViewProvider.ifAvailable(hv ->
        Gauge.builder("hotkey.worker.alive", hv, v -> v.isClusterHealthy() ? 1.0 : 0.0).register(registry)
      );
      stateMachineProvider.ifAvailable(sm ->
        Gauge.builder("hotkey.worker.tracked.keys", sm, s -> (double) s.getTrackedKeys()).register(registry)
      );
      cpuMonitorProvider.ifAvailable(cpu ->
        Gauge.builder("hotkey.cpu.load", cpu, c -> c.getCpuLoadEMA()).register(registry)
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
    Gauge.builder("hotkey.topk.size", detector, t -> t.list().size())
      .tag("type", "local")
      .register(registry);
    Gauge.builder("hotkey.topk.total", detector, t -> (double) t.total())
      .tag("type", "local")
      .register(registry);
    Gauge.builder("hotkey.expelled.queue.size", detector, t -> (double) t.expelled().size()).register(registry);
    Gauge.builder("hotkey.expelled.queue.remaining", detector, t -> (double) t.expelled().remainingCapacity()).register(
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
    Gauge.builder("hotkey.topk.size", worker, t -> t.list().size())
      .tag("type", "worker")
      .register(registry);
    Gauge.builder("hotkey.topk.total", worker, t -> (double) t.total())
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
  private static void registerReporterGauges(KeyReporter reporter, MeterRegistry registry) {
    Gauge.builder("hotkey.reporter.queue.depth", reporter, r -> (double) r.dispatcherDepth()).register(registry);
    Gauge.builder("hotkey.reporter.queue.dropped.total", reporter, r -> (double) r.dispatcherDropped()).register(
      registry
    );
    Gauge.builder("hotkey.reporter.queue.expired.total", reporter, r -> (double) r.dispatcherExpired()).register(
      registry
    );
    Gauge.builder("hotkey.reporter.pending.keys", reporter, r -> (double) r.getPendingKeyCount()).register(registry);
    Gauge.builder("hotkey.reporter.bbr.passed", reporter, r -> (double) r.bbrPassed()).register(registry);
    Gauge.builder("hotkey.reporter.bbr.dropped", reporter, r -> (double) r.bbrDropped()).register(registry);
    Gauge.builder("hotkey.reporter.bbr.inflight", reporter, r -> (double) r.bbrInFlight()).register(registry);
    Gauge.builder("hotkey.reporter.bbr.maxinflight", reporter, r -> (double) r.bbrMaxInFlight()).register(registry);
  }
}
