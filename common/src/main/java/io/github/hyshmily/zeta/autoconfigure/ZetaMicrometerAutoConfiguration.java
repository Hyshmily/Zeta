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
import io.github.hyshmily.zeta.cache.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.endpoint.ZetaEndpoint;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.reporting.impl.ReportFeedLoop;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import java.util.concurrent.Semaphore;
import java.util.function.ToLongFunction;
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
 *       estimated size, max size) in the standard {@code cache.*} namespace tagged
 *       {@code cache=zeta.l1}, via {@link CaffeineCacheMetrics}.</li>
 *   <li>{@code hotKeyCustomMetrics} — HotKey-specific business metrics covering TopK detection
 *       (local and worker), SingleFlight, Reporter (queue depth, drops), BackgroundRefresher
 *       (refresh permits), StampClock (clock skew), SyncPublisher (send failures),
 *       Worker health (alive/dead).</li>
 * </ul>
 *
 * <p>All custom metrics use the {@code zeta} namespace. Any missing dependency silently
 * skips the corresponding meter registration via {@link ObjectProvider}, mirroring the same
 * null-safe approach used by {@link ZetaEndpoint}. Monotonic totals are registered as
 * {@link FunctionCounter}s so they carry counter semantics (and a {@code _total} suffix) in
 * every registry; live and decaying values — including {@code zeta.topk.total}, which falls
 * with the sketch's decay — are gauges.
 *
 * <p>Thread-safe: Micrometer's {@link MeterRegistry} is thread-safe; meter supplier lambdas
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
   * {@code cache=zeta.l1} tag. Exposes hit/miss counts, eviction add/weight,
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
  @ConditionalOnMissingBean(name = "hotKeyCaffeineMetrics")
  public MeterBinder hotKeyCaffeineMetrics(ObjectProvider<Cache<String, CacheEntry>> hotLocalCacheProvider) {
    return registry ->
      hotLocalCacheProvider.ifAvailable(cache -> CaffeineCacheMetrics.monitor(registry, cache, "zeta.l1"));
  }

  /**
   * Registers HotKey-specific business metrics for local and worker-side components.
   *
   * <p>Each meter is gated by component availability via {@link ObjectProvider} —
   * if a component is not present in the current deployment mode, its corresponding
   * metric is silently skipped.
   *
   * <p>Metrics registered:
   * <table>
   *   <caption>Registered meters</caption>
   *   <tr><th>Metric name</th><th>Source</th><th>Tags</th></tr>
   *   <tr><td>{@code zeta.topk.size}</td><td>TopK current ranking size</td><td>type=local</td></tr>
   *   <tr><td>{@code zeta.topk.total}</td><td>TopK total requests tracked</td><td>type=local</td></tr>
   *   <tr><td>{@code zeta.expelled.queue.size}</td><td>Expelled queue backlog</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.singleflight.inflight}</td><td>Loader suppliers currently executing</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.depth}</td><td>Reporter queue backlog</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.dropped.total}</td><td>Cumulative dropped batches</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.expired.dead.total}</td><td>Expired batches: dead target Worker</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.expired.stale.total}</td><td>Expired batches: 5s staleness</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.pending.keys}</td><td>Keys buffered in reporter counter cache</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.feedloop.interval}</td><td>Feed-loop base flush interval in ms
   *       (shadow: the trajectory that <em>would</em> be applied; design decisions: reporting)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.feedloop.score}</td><td>Feed-loop score in bp — smoothed
   *       arrival rate projected onto the interval in force for the measured window, vs target
   *       (10000 == on target)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.feedloop.batch}</td><td>Feed-loop projected batch (keys per flush
   *       at the interval that produced the score, from the smoothed rate)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.stall.*}</td><td>Stall causes that have no other series &mdash; see
   *       {@link #registerStallMeters}</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.expire.refresh.available}</td><td>Available refresh limiter permits</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.expire.refresh.failure.leased.total}</td><td>Cumulative refresh failures granted a
   *       lease (the stale entry stays servable)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.expire.refresh.lease.debt.keys}</td><td>Keys currently holding a failure
   *       lease</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.version.clock.skew}</td><td>Observed clock lead of the data-stamp clock, in milliseconds</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.worker.alive}</td><td>Whether the heartbeat view passes the cluster gate</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.sync.send.failed.total}</td><td>Cumulative sync-plane publish failures (broker errors)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.sync.nodeid.collision.total}</td><td>Inbound messages from a peer sharing this node's dataVersion node id</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.worker.decision.cool.rejected.total}</td><td>COOL decisions the ordered dispatcher refused (re-applied inline)</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.worker.decision.epoch.regress.skip.total}</td><td>Decisions skipped for a same-nodeId epoch rollback</td><td>&mdash;</td></tr>
   * </table>
   *
   * <p><b>What is deliberately not registered.</b> A meter earns its place by changing an
   * action: derived values ({@code capacity - size}, {@code dead + stale}), configuration
   * echoes, and internal windows are left out — a dashboard reads the one series that
   * already exposes the number instead of a mirror of it.
   *
   * @param hotKeyDetectorProvider      provider for the app-side TopK (may be absent)
   * @param singleFlightProvider        provider for the SingleFlight dedup layer (may be absent)
   * @param reporterProvider            provider for the HotKey reporter (may be absent)
   * @param accessCounterProvider       provider for the shared access counter (may be absent)
   * @param broadcastBufferProvider     provider for the broadcast refresh buffer (may be absent)
   * @param backgroundRefresherProvider provider for the background refresh executor (may be absent)
   * @param stampClockProvider          provider for the data-stamp clock (may be absent)
   * @param cacheSyncPublisherProvider  provider for the cache sync publisher (may be absent)
   * @param healthViewProvider          provider for the cluster health view (may be absent)
   * @param syncListenerProvider        provider for the sync-plane listener (may be absent)
   * @param workerListenerProvider      provider for the Worker decision listener (may be absent)
   * @return a {@link MeterBinder} that registers HotKey-specific business metrics
   */
  @Bean
  @ConditionalOnMissingBean(name = "hotKeyCustomMetrics")
  public MeterBinder hotKeyCustomMetrics(
    @Qualifier("heavyKeeper") ObjectProvider<TopK> hotKeyDetectorProvider,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<KeyReporter> reporterProvider,
    ObjectProvider<io.github.hyshmily.zeta.cache.AccessCounter> accessCounterProvider,
    ObjectProvider<BroadcastBuffer> broadcastBufferProvider,
    ObjectProvider<BackgroundRefresher> backgroundRefresherProvider,
    ObjectProvider<StampClock> stampClockProvider,
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider,
    ObjectProvider<HealthView> healthViewProvider,
    ObjectProvider<CacheSyncListener> syncListenerProvider,
    ObjectProvider<WorkerListener> workerListenerProvider
  ) {
    return registry -> {
      hotKeyDetectorProvider.ifAvailable(detector -> registerLocalTopKMeters(detector, registry));
      singleFlightProvider.ifAvailable(sf ->
        Gauge.builder("zeta.singleflight.inflight", sf, s -> (double) s.estimatedInFlightSize()).register(registry)
      );
      reporterProvider.ifAvailable(r -> registerReporterMeters(r, registry));
      // design decisions: access counting: the counting reservoir lives on the shared access counter —
      // its pending-keys gauge replaces the reporter-owned one.
      accessCounterProvider.ifAvailable(counter ->
        Gauge.builder("zeta.reporter.pending.keys", counter, c -> (double) c.pendingKeyCount()).register(registry)
      );
      // Stall-cause meters: each (cause, state ≠ normal) pair is its own
      // meter — the healthy/normal state registers nothing (the RocksDB
      // write_stall_stats pattern). A missing component silently skips its
      // causes (that failure mode does not exist in this deployment).
      registerStallMeters(registry, singleFlightProvider, broadcastBufferProvider);
      cacheSyncPublisherProvider.ifAvailable(csp ->
        FunctionCounter.builder("zeta.sync.send.failed.total", csp, p -> (double) p.sendFailures()).register(registry)
      );
      backgroundRefresherProvider.ifAvailable(refresher -> {
        var limiter = refresher.getRefreshLimiter();
        if (limiter != null) {
          Gauge.builder("zeta.expire.refresh.available", limiter, Semaphore::availablePermits).register(registry);
        }
        // Lease-on-failure observability (design decisions: expiry and refresh follow-up):
        // granted leases and the current debt. No refusal-reason counter is registered —
        // a refusal is the absence of a grant, so "leased total flat while failures rise"
        // already says a refusal happened, and no dashboard action differs between the
        // classifier and the staleness cap.
        FunctionCounter.builder("zeta.expire.refresh.failure.leased.total", refresher, r ->
          (double) r.getRefreshFailureLeased()
        ).register(registry);
        Gauge.builder("zeta.expire.refresh.lease.debt.keys", refresher, r -> (double) r.leasedKeyCount()).register(
          registry
        );
      });
      stampClockProvider.ifAvailable(clock ->
        Gauge.builder("zeta.version.clock.skew", clock, c -> (double) c.skewMillis()).register(registry)
      );
      healthViewProvider.ifAvailable(hv ->
        Gauge.builder("zeta.worker.alive", hv, v -> v.isClusterHealthy() ? 1.0 : 0.0).register(registry)
      );
      // Decision-plane saturation: a COOL the ordered dispatcher refused is
      // re-applied inline on the consumer thread (never silently lost); the
      // counter is the signal that the dispatcher saturated.
      workerListenerProvider.ifAvailable(wl -> {
        FunctionCounter.builder("zeta.worker.decision.cool.rejected.total", wl, w ->
          (double) w.coolDispatchRejections()
        ).register(registry);
        // Epoch-rollback skip (VersionGuard is a static utility — no bean):
        // persistent skips after a Worker restart mean the epoch file was lost
        // and the new incarnation is muted until the resident entries expire.
        // Surfaced only alongside a decision listener — no listener, no
        // decisions to skip.
        FunctionCounter.builder("zeta.worker.decision.epoch.regress.skip.total", VersionGuard.class, ignored ->
          (double) VersionGuard.epochRegressSkips()
        ).register(registry);
      });
      // dataVersion node-id collisions: two instances sharing a node id can
      // mint equal stamps and skip each other's writes until the next ms/TTL.
      syncListenerProvider.ifAvailable(sl ->
        FunctionCounter.builder("zeta.sync.nodeid.collision.total", sl, s -> (double) s.nodeIdCollisions()).register(
          registry
        )
      );
    };
  }

  @Bean
  @ConditionalOnMissingBean(name = "zetaLateMetrics")
  public org.springframework.beans.factory.SmartInitializingSingleton zetaLateMetrics(
    ObjectProvider<MeterRegistry> registries,
    ObjectProvider<Cache<String, CacheEntry>> hotLocalCacheProvider,
    @Qualifier("heavyKeeper") ObjectProvider<TopK> hotKeyDetectorProvider,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<KeyReporter> reporterProvider,
    ObjectProvider<io.github.hyshmily.zeta.cache.AccessCounter> accessCounterProvider,
    ObjectProvider<BroadcastBuffer> broadcastBufferProvider,
    ObjectProvider<BackgroundRefresher> backgroundRefresherProvider,
    ObjectProvider<StampClock> stampClockProvider,
    ObjectProvider<CacheSyncPublisher> cacheSyncPublisherProvider,
    ObjectProvider<HealthView> healthViewProvider,
    ObjectProvider<CacheSyncListener> syncListenerProvider,
    ObjectProvider<WorkerListener> workerListenerProvider
  ) {
    // A MeterBinder created during the early reporter/access-counter cycle can be bound
    // before the components exist, leaving every zeta.* meter silently absent. Re-run the
    // same idempotent registrations once all singletons are ready; Micrometer returns the
    // existing meter for an identical id, so the early binding is harmless when it worked.
    return () ->
      registries
        .orderedStream()
        .forEach(registry -> {
          boolean alreadyBound = registry
            .getMeters()
            .stream()
            .anyMatch(meter -> meter.getId().getName().startsWith("zeta."));
          if (alreadyBound) {
            return;
          }
          hotKeyCaffeineMetrics(hotLocalCacheProvider).bindTo(registry);
          hotKeyCustomMetrics(
            hotKeyDetectorProvider,
            singleFlightProvider,
            reporterProvider,
            accessCounterProvider,
            broadcastBufferProvider,
            backgroundRefresherProvider,
            stampClockProvider,
            cacheSyncPublisherProvider,
            healthViewProvider,
            syncListenerProvider,
            workerListenerProvider
          ).bindTo(registry);
        });
  }

  /**
   * Register Micrometer meters for the local app-side TopK detector.
   * Exposes top-K size, total requests and expelled queue size.
   *
   * @param detector the local TopK detector
   * @param registry the Micrometer meter registry
   */
  private static void registerLocalTopKMeters(TopK detector, MeterRegistry registry) {
    Gauge.builder("zeta.topk.size", detector, t -> (double) t.estimatedSize())
      .tag("type", "local")
      .register(registry);
    Gauge.builder("zeta.topk.total", detector, t -> (double) t.total())
      .tag("type", "local")
      .register(registry);
    Gauge.builder("zeta.expelled.queue.size", detector, t -> (double) t.expelled().size()).register(registry);
  }

  /**
   * Register Micrometer meters for the HotKey reporter: queue depth and the feed-loop
   * curves read straight from their components. Monotonic totals are counters; live values are
   * gauges. The feed-loop meters are registered only when the tuner exists, so a disabled
   * control loop is visible as a missing series; queue depth reports {@code -1} while the
   * dispatcher has not started.
   *
   * @param reporter the HotKey reporter
   * @param registry the Micrometer meter registry
   */
  private static void registerReporterMeters(KeyReporter reporter, MeterRegistry registry) {
    Gauge.builder("zeta.reporter.queue.depth", reporter, r -> dispatcherStat(r, s -> s.depth(), -1d)).register(
      registry
    );
    FunctionCounter.builder("zeta.reporter.queue.dropped.total", reporter, r ->
      dispatcherStat(r, s -> s.dropped(), 0d)
    ).register(registry);
    // The two expiry causes select different actions: a dead target means topology/health,
    // a stale batch means backpressure. Their sum is not exposed — it can never disagree.
    FunctionCounter.builder("zeta.reporter.queue.expired.dead.total", reporter, r ->
      dispatcherStat(r, s -> s.expiredDeadTarget(), 0d)
    ).register(registry);
    FunctionCounter.builder("zeta.reporter.queue.expired.stale.total", reporter, r ->
      dispatcherStat(r, s -> s.expiredStale(), 0d)
    ).register(registry);
    // zeta.reporter.pending.keys is registered from the shared access counter: the reporter
    // owns no counting reservoir (design decisions: access counting).
    // Feed-loop meters exist only when the tuner is configured: in shadow mode the interval
    // shows the trajectory it WOULD apply — the deploy-first observation window.
    ReportFeedLoop loop = reporter.feedLoop();
    if (loop != null) {
      Gauge.builder("zeta.reporter.feedloop.interval", loop, ReportFeedLoop::intervalMs).register(registry);
      Gauge.builder("zeta.reporter.feedloop.score", loop, ReportFeedLoop::lastScoreBp).register(registry);
      Gauge.builder("zeta.reporter.feedloop.batch", loop, ReportFeedLoop::projectedBatchSize).register(registry);
    }
  }

  /**
   * One dispatcher value, or the fallback when the dispatcher has not started. A gauge reads
   * {@code -1} as "unknown"; a monotonic counter reads {@code 0}, because a counter must never
   * report a negative total.
   */
  private static double dispatcherStat(
    KeyReporter reporter,
    ToLongFunction<KeyReporter.DispatcherStats> extractor,
    double fallbackWhenStopped
  ) {
    KeyReporter.DispatcherStats stats = reporter.dispatcherStats();
    return stats == null ? fallbackWhenStopped : (double) extractor.applyAsLong(stats);
  }

  /**
   * Register the stall-cause meters — the "why is it slow" attribution view,
   * modeled on RocksDB's {@code WriteStallCause × WriteStallCondition}
   * ticker matrix ({@code db/write_stall_stats}). Only causes with no other
   * series are registered: the report path's backlog and drop counts are
   * already {@code zeta.reporter.queue.*}, and "no Worker alive" is already
   * {@code zeta.worker.alive} — re-registering them under {@code zeta.stall}
   * gave a dashboard three names for one number.
   *
   * <table>
   *   <tr><th>Metric name</th><th>Meaning</th><th>Source</th></tr>
   *   <tr><td>{@code zeta.stall.broadcast_storm.stopped.total}</td><td>Refresh broadcasts lost to
   *       broker send failures or a saturated send executor</td><td>BroadcastBuffer</td></tr>
   *   <tr><td>{@code zeta.stall.source_degraded.stopped}</td><td>1 while the circuit breaker is
   *       open (loads fast-fail; reads degrade to stale values)</td><td>SingleFlight</td></tr>
   *   <tr><td>{@code zeta.stall.source_degraded.timeouts.total}</td><td>Cumulative dedup loads
   *       resolved empty by a reader timeout</td><td>SingleFlight</td></tr>
   * </table>
   */
  private static void registerStallMeters(
    MeterRegistry registry,
    ObjectProvider<SingleFlight> singleFlightProvider,
    ObjectProvider<BroadcastBuffer> broadcastBufferProvider
  ) {
    broadcastBufferProvider.ifAvailable(bb ->
      FunctionCounter.builder("zeta.stall.broadcast_storm.stopped.total", bb, b ->
        (double) (b.sendFailures() + b.saturationDrops())
      ).register(registry)
    );
    singleFlightProvider.ifAvailable(sf -> {
      Gauge.builder("zeta.stall.source_degraded.stopped", sf, s -> s.isBreakerOpen() ? 1.0 : 0.0).register(registry);
      FunctionCounter.builder("zeta.stall.source_degraded.timeouts.total", sf, s ->
        (double) s.loadTimeoutCount()
      ).register(registry);
    });
  }
}
