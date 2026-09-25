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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.endpoint.ZetaEndpoint;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.dispatcher.DispatcherStats;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.SystemLoadMonitor;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import java.util.function.Supplier;
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
 *       estimated size, max size) under the {@code zeta.l1} metric prefix via
 *       {@link CaffeineCacheMetrics}.</li>
 *   <li>{@code hotKeyCustomMetrics} — HotKey-specific business metrics covering TopK detection
 *       (local and worker), SingleFlight, Reporter (queue depth, drops, BBR stats), ExpireManager
 *       (refresh permits), VersionController (degraded count), SyncPublisher (dedup cache size),
 *       Worker health (alive/dead), StateMachine (tracked keys), and CPU load (EMA).</li>
 * </ul>
 *
 * <p>All custom metrics use the {@code zeta} namespace. Any missing dependency silently
 * skips the corresponding gauge registration via {@link ObjectProvider}, mirroring the same
 * null-safe approach used by {@link ZetaEndpoint}.
 *
 * <p>Thread-safe: Micrometer's {@link MeterRegistry} is thread-safe; gauge supplier lambdas
 * delegate to thread-safe component methods.
 */
@Internal
@AutoConfiguration(after = ZetaAutoConfiguration.class)
@ConditionalOnClass(name = "io.micrometer.core.instrument.binder.MeterBinder")
@EnableConfigurationProperties(ZetaProperties.class)
public class ZetaMicrometerAutoConfiguration {

  /**
   * Registers standard Caffeine cache metrics using {@link CaffeineCacheMetrics}.
   *
   * <p>Metrics are registered under the standard {@code cache.*} namespace with a
   * {@code cache=zeta.l1} tag. Exposes hit/miss counts, eviction count/weight,
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
      hotLocalCacheProvider.ifAvailable(cache -> CaffeineCacheMetrics.monitor(registry, cache, "zeta.l1"));
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
   *   <tr><td>{@code zeta.topk.size}</td><td>TopK current ranking count</td><td>type=local|worker</td></tr>
   *   <tr><td>{@code zeta.topk.total}</td><td>TopK total requests tracked</td><td>type=local|worker</td></tr>
   *   <tr><td>{@code zeta.expelled.queue.size}</td><td>Expelled queue backlog</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.expelled.queue.remaining}</td><td>Expelled queue remaining capacity</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.singleflight.inflight}</td><td>SingleFlight in-flight dedup count</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.depth}</td><td>Reporter queue backlog</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.dropped.total}</td><td>Cumulative dropped batches</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.expired.total}</td><td>Cumulative expired batches</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.pending.keys}</td><td>Keys buffered in reporter counter cache</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.bbr.*}</td><td>BBR rate limiter (passed/dropped/inflight/maxinflight)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.feedloop.interval}</td><td>Feed-loop base flush interval in ms
   *       (shadow: the trajectory that <em>would</em> be applied; ADR-0078)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.feedloop.score}</td><td>Feed-loop score in bp — averaged batch
   *       size vs target (10000 == on target)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.feedloop.batch}</td><td>Feed-loop averaged batch size (keys per
   *       completed flush)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.expired.dead.total}</td><td>Expired batches: dead target Worker</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.expired.stale.total}</td><td>Expired batches: 5s staleness</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.stall.*}</td><td>Stall-cause &times; state gauges &mdash; the "why is it slow"
   *       attribution view (RocksDB {@code write_stall_stats} pattern; see
   *       {@link #registerStallGauges})</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.expire.refresh.available}</td><td>Available refresh limiter permits</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.version.degraded.total}</td><td>Cumulative version fallback count</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.sync.dedup.size}</td><td>Broadcast dedup cache size</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.dispatch.*}</td><td>Per-key dispatcher gate: pending/remaining units, active keys, backlogged, dropped, rejected</td><td>plane=sync|worker</td></tr>
   *   <tr><td>{@code zeta.worker.alive}</td><td>Whether any worker shard is alive</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.worker.tracked.keys}</td><td>Keys tracked by state machine</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.cpu.load}</td><td>System CPU load EMA</td><td>&mdash;</td></tr>
   * </table>
   *
   * @param hotKeyDetectorProvider      provider for the app-side TopK (may be absent)
   * @param singleFlightProvider        provider for the SingleFlight dedup layer (may be absent)
   * @param reporterProvider            provider for the HotKey reporter (may be absent)
   * @param broadcastBufferProvider     provider for the broadcast refresh buffer (may be absent)
   * @param expireManagerProvider       provider for the cache expiry manager (may be absent)
   * @param versionControllerProvider   provider for the version controller (may be absent)
   * @param cacheSyncPublisherProvider  provider for the cache sync publisher (may be absent)
   * @param stateMachineProvider        provider for the Worker state machine (may be absent)
   * @param healthViewProvider          provider for the cluster health view (may be absent)
   * @param syncListenerProvider        provider for the sync-plane listener exposing its ordered
   *                                    dispatcher gate (may be absent)
   * @param workerListenerProvider      provider for the decision-plane listener exposing its ordered
   *                                    dispatcher gate (may be absent)
   * @param cpuMonitorProvider          provider for the system CPU load monitor (may be absent)
   * @return a {@link MeterBinder} that registers HotKey-specific business metrics
   */
  @Bean
  @ConditionalOnMissingBean
  public MeterBinder hotKeyCustomMetrics(
    @Qualifier("hotKeyDetector") ObjectProvider<TopK> hotKeyDetectorProvider,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<KeyReporter> reporterProvider,
    ObjectProvider<BroadcastBuffer> broadcastBufferProvider,
    ObjectProvider<ExpireManager> expireManagerProvider,
    ObjectProvider<VersionController> versionControllerProvider,
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider,
    ObjectProvider<ZetaBayesianSM> stateMachineProvider,
    ObjectProvider<HealthView> healthViewProvider,
    ObjectProvider<SystemLoadMonitor> cpuMonitorProvider,
    ObjectProvider<CacheSyncListener> syncListenerProvider,
    ObjectProvider<WorkerListener> workerListenerProvider
  ) {
    return registry -> {
      hotKeyDetectorProvider.ifAvailable(detector -> registerLocalTopKGauges(detector, registry));
      singleFlightProvider.ifAvailable(sf ->
        Gauge.builder("zeta.singleflight.inflight", sf, s -> (double) s.estimatedInflightSize()).register(registry)
      );
      reporterProvider.ifAvailable(r -> registerReporterGauges(r, registry));
      // Stall-cause gauges: each (cause, state ≠ normal) pair is its own
      // meter — the healthy/normal state registers nothing (the RocksDB
      // write_stall_stats pattern). A missing component silently skips its
      // causes (that failure mode does not exist in this deployment).
      registerStallGauges(
        registry,
        reporterProvider,
        singleFlightProvider,
        broadcastBufferProvider,
        healthViewProvider
      );
      expireManagerProvider.ifAvailable(em -> {
        if (em.getRefreshLimiter() != null) {
          Gauge.builder("zeta.expire.refresh.available", em, e ->
            (double) e.getRefreshLimiter().availablePermits()
          ).register(registry);
        }
      });
      versionControllerProvider.ifAvailable(vc ->
        Gauge.builder("zeta.version.degraded.total", vc, v -> (double) v.getDegradedVersionCount()).register(registry)
      );
      cacheSyncPublisherProvider.ifAvailable(csp ->
        Gauge.builder("zeta.sync.dedup.size", csp, p -> (double) p.getDedupCacheSize()).register(registry)
      );
      healthViewProvider.ifAvailable(hv ->
        Gauge.builder("zeta.worker.alive", hv, v -> v.isClusterHealthy() ? 1.0 : 0.0).register(registry)
      );
      stateMachineProvider.ifAvailable(sm ->
        Gauge.builder("zeta.worker.tracked.keys", sm, s -> (double) s.getTrackedKeys()).register(registry)
      );
      //noinspection Convert2MethodRef
      cpuMonitorProvider.ifAvailable(cpu ->
        Gauge.builder("zeta.cpu.load", cpu, SystemLoadMonitor::getCpuLoadEMA).register(registry)
      );
      // Per-key dispatcher gates (ADR-0072 D-1): registered only when the plane's listener is
      // present — a missing listener means that plane does not exist in this deployment mode.
      syncListenerProvider.ifAvailable(listener -> {
        if (listener.dispatcherStats() != null) {
          registerDispatchGauges(registry, "sync", listener::dispatcherStats);
        }
      });
      workerListenerProvider.ifAvailable(listener -> {
        if (listener.dispatcherStats() != null) {
          registerDispatchGauges(registry, "worker", listener::dispatcherStats);
        }
      });
    };
  }

  /**
   * Register the per-key dispatcher gate gauges for one plane (ADR-0072 D-1).
   *
   * <p>{@code zeta.dispatch.pending.units} is the weighted backlog currently charged to the gate and
   * {@code zeta.dispatch.remaining.units} is the capacity left before submissions start being
   * dropped — reading the two together is what tells "no traffic" (both zero/at capacity) apart
   * from "gate saturated" (remaining near zero), a distinction that previously existed only as a
   * throttled WARN emitted after the gate had already closed.
   *
   * <p>The supplier is re-read on every scrape, so a listener created before its {@code @PostConstruct}
   * initializer ran (or one whose dispatcher is absent) contributes a zeroed snapshot instead of
   * failing the scrape.
   *
   * @param registry the meter registry to register into
   * @param plane    the {@code plane} tag value ({@code sync} or {@code worker})
   * @param supplier re-reads the plane's gate snapshot on each scrape
   */
  private static void registerDispatchGauges(
    MeterRegistry registry,
    String plane,
    Supplier<DispatcherStats> supplier
  ) {
    Gauge.builder("zeta.dispatch.pending.units", supplier, s -> (double) snapshot(s).pendingUnits())
      .tag("plane", plane)
      .register(registry);
    Gauge.builder("zeta.dispatch.remaining.units", supplier, s -> (double) snapshot(s).remainingUnits())
      .tag("plane", plane)
      .register(registry);
    Gauge.builder("zeta.dispatch.active.keys", supplier, s -> (double) snapshot(s).activeKeys())
      .tag("plane", plane)
      .register(registry);
    Gauge.builder("zeta.dispatch.backlogged", supplier, s -> snapshot(s).backlogged() ? 1.0 : 0.0)
      .tag("plane", plane)
      .register(registry);
    Gauge.builder("zeta.dispatch.dropped.total", supplier, s -> (double) snapshot(s).dropped())
      .tag("plane", plane)
      .register(registry);
    Gauge.builder("zeta.dispatch.rejected.total", supplier, s -> (double) snapshot(s).rejected())
      .tag("plane", plane)
      .register(registry);
  }

  /**
   * Read one gate snapshot, substituting a zeroed snapshot for an absent dispatcher so a gauge
   * lambda can never throw during a scrape.
   *
   * @param supplier re-reads the plane's gate snapshot
   * @return the snapshot, or a zeroed one when the plane has no initialized dispatcher
   */
  private static DispatcherStats snapshot(Supplier<DispatcherStats> supplier) {
    DispatcherStats stats = supplier.get();
    return stats == null ? ZERO_STATS : stats;
  }

  /** Zeroed gate snapshot used only while a plane's dispatcher does not exist yet. */
  private static final DispatcherStats ZERO_STATS = new DispatcherStats(0L, 0L, 0, 0L, 0L);

  /**
   * Register Micrometer gauges for the local app-side TopK detector.
   * Exposes top-K size, total requests, expelled queue size and remaining capacity.
   *
   * @param detector the local TopK detector
   * @param registry the Micrometer meter registry
   */
  private static void registerLocalTopKGauges(TopK detector, MeterRegistry registry) {
    Gauge.builder("zeta.topk.size", detector, t -> t.list().size())
      .tag("type", "local")
      .register(registry);
    Gauge.builder("zeta.topk.total", detector, t -> (double) t.total())
      .tag("type", "local")
      .register(registry);
    Gauge.builder("zeta.expelled.queue.size", detector, t -> (double) t.expelled().size()).register(registry);
    Gauge.builder("zeta.expelled.queue.remaining", detector, t -> (double) t.expelled().remainingCapacity()).register(
      registry
    );
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
    Gauge.builder("zeta.reporter.queue.depth", reporter, r -> (double) r.dispatcherDepth()).register(registry);
    Gauge.builder("zeta.reporter.queue.dropped.total", reporter, r -> (double) r.dispatcherDropped()).register(
      registry
    );
    Gauge.builder("zeta.reporter.queue.expired.total", reporter, r -> (double) r.dispatcherExpired()).register(
      registry
    );
    Gauge.builder("zeta.reporter.queue.expired.dead.total", reporter, r -> (double) r.dispatcherExpiredDeadTarget())
      .register(registry);
    Gauge.builder("zeta.reporter.queue.expired.stale.total", reporter, r -> (double) r.dispatcherExpiredStale())
      .register(registry);
    Gauge.builder("zeta.reporter.pending.keys", reporter, r -> (double) r.getPendingKeyCount()).register(registry);
    Gauge.builder("zeta.reporter.bbr.passed", reporter, r -> (double) r.bbrPassed()).register(registry);
    Gauge.builder("zeta.reporter.bbr.dropped", reporter, r -> (double) r.bbrDropped()).register(registry);
    Gauge.builder("zeta.reporter.bbr.inflight", reporter, r -> (double) r.bbrInFlight()).register(registry);
    Gauge.builder("zeta.reporter.bbr.maxinflight", reporter, r -> (double) r.bbrMaxInFlight()).register(registry);
    // ADR-0078 feed-loop interval tuner: registered only when the tuner is
    // configured (report-interval-tuning != off). In shadow mode the interval
    // gauge shows the trajectory the tuner WOULD apply — the deploy-first
    // observation window.
    if (reporter.feedLoopEnabled()) {
      Gauge.builder("zeta.reporter.feedloop.interval", reporter, r -> (double) r.feedLoopIntervalMs()).register(
        registry
      );
      Gauge.builder("zeta.reporter.feedloop.score", reporter, r -> (double) r.feedLoopScoreBp()).register(registry);
      Gauge.builder("zeta.reporter.feedloop.batch", reporter, r -> (double) r.feedLoopBatchSize()).register(registry);
    }
  }

  /**
   * Register the stall-cause gauges — the "why is it slow" attribution view,
   * modeled on RocksDB's {@code WriteStallCause × WriteStallCondition}
   * ticker matrix ({@code db/write_stall_stats}). Each (cause, state) pair
   * that can occur in this deployment gets one meter; the normal state
   * registers nothing, so an absent series means "not stalling".
   *
   * <table>
   *   <tr><th>Metric name</th><th>Meaning</th><th>Source</th></tr>
   *   <tr><td>{@code zeta.stall.report_backpressure.delayed}</td><td>Dispatcher queue depth
   *       (congestion building; clamped at 0 before the reporter starts)</td>
   *       <td>KeyReporter</td></tr>
   *   <tr><td>{@code zeta.stall.report_backpressure.stopped.total}</td><td>Batches lost to a full
   *       queue or staleness expiry (drops are the stopped state of the report path)</td>
   *       <td>KeyReporter</td></tr>
   *   <tr><td>{@code zeta.stall.broadcast_storm.stopped.total}</td><td>Refresh broadcasts lost to
   *       broker send failures or a saturated send executor</td><td>BroadcastBuffer</td></tr>
   *   <tr><td>{@code zeta.stall.redis_degraded.stopped}</td><td>1 while the circuit breaker is
   *       open (loads fast-fail; reads degrade to stale values)</td><td>SingleFlight</td></tr>
   *   <tr><td>{@code zeta.stall.redis_degraded.timeouts.total}</td><td>Cumulative dedup loads
   *       resolved empty by a reader timeout</td><td>SingleFlight</td></tr>
   *   <tr><td>{@code zeta.stall.worker_partition.stopped}</td><td>1 while no Worker shard is
   *       alive (report routing has no target)</td><td>HealthView</td></tr>
   * </table>
   */
  private static void registerStallGauges(
    MeterRegistry registry,
    ObjectProvider<KeyReporter> reporterProvider,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<BroadcastBuffer> broadcastBufferProvider,
    ObjectProvider<HealthView> healthViewProvider
  ) {
    reporterProvider.ifAvailable(r -> {
      Gauge.builder("zeta.stall.report_backpressure.delayed", r, v -> (double) Math.max(0, v.dispatcherDepth()))
        .register(registry);
      Gauge.builder(
        "zeta.stall.report_backpressure.stopped.total",
        r,
        v -> (double) Math.max(0, v.dispatcherDropped() + v.dispatcherExpired())
      ).register(registry);
    });
    broadcastBufferProvider.ifAvailable(bb ->
      Gauge.builder(
        "zeta.stall.broadcast_storm.stopped.total",
        bb,
        b -> (double) (b.sendFailures() + b.saturationDrops())
      ).register(registry)
    );
    singleFlightProvider.ifAvailable(sf -> {
      Gauge.builder("zeta.stall.redis_degraded.stopped", sf, s -> s.isBreakerOpen() ? 1.0 : 0.0).register(registry);
      Gauge.builder("zeta.stall.redis_degraded.timeouts.total", sf, s -> (double) s.getLoadTimeoutCount()).register(
        registry
      );
    });
    healthViewProvider.ifAvailable(hv ->
      Gauge.builder("zeta.stall.worker_partition.stopped", hv, v -> v.getAliveWorkerIds().isEmpty() ? 1.0 : 0.0)
        .register(registry)
    );
  }
}
