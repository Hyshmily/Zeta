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
import io.github.hyshmily.zeta.cache.AccessCounter;
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
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.ToLongFunction;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
 * every registry; live and decaying values — including {@code zeta.topk.count}, which falls
 * with the sketch's decay — are gauges.
 *
 * <p>Thread-safe: Micrometer's {@link MeterRegistry} is thread-safe; meter supplier lambdas
 * delegate to thread-safe component methods.
 *
 * <p><b>App-side only.</b> The configuration is off in Worker mode
 * ({@code zeta.worker.enabled=true}): every component it reads — the local TopK, the L1 cache,
 * SingleFlight, the reporter, the sync plane — belongs to the application side, and the Worker's
 * own meters are bound by {@code WorkerAutoConfiguration.workerDetectionMeterBinder}. Without the
 * condition a Worker deployment that happens to contain one TopK-typed bean would publish it under
 * {@code type=local}.
 */
@Internal
@AutoConfiguration(
  after = { ZetaAutoConfiguration.class, ZetaFacadeAutoConfiguration.class, ZetaAmqpAutoConfiguration.class }
)
@ConditionalOnClass(name = "io.micrometer.core.instrument.binder.MeterBinder")
@ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "false", matchIfMissing = true)
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
   *   <tr><td>{@code zeta.topk.count}</td><td>TopK total requests tracked</td><td>type=local</td></tr>
   *   <tr><td>{@code zeta.expelled.queue.size}</td><td>Expelled queue backlog</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.singleflight.inflight}</td><td>Loader suppliers currently executing</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.depth}</td><td>Reporter queue backlog</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.queue.dropped.total}</td><td>Cumulative dropped batches</td><td>&mdash;</td></tr>
   *   <tr><td>{@code zeta.reporter.publish.failed.total}</td><td>Cumulative publisher failures</td><td>&mdash;</td></tr>
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
   * @param defaultTopKProvider        the framework's {@code heavyKeeper} bean, the fallback when
   *                                   several TopK beans make the unqualified provider ambiguous
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
    ObjectProvider<TopK> hotKeyDetectorProvider,
    @Qualifier("heavyKeeper") ObjectProvider<TopK> defaultTopKProvider,
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
      TopK detector = hotKeyDetectorProvider.getIfUnique(defaultTopKProvider::getIfAvailable);
      if (detector != null) {
        registerLocalTopKMeters(detector, registry);
      }
      singleFlightProvider.ifAvailable(sf ->
        gauge(registry, "zeta.singleflight.inflight", sf, SingleFlight::estimatedInFlightSize)
      );
      reporterProvider.ifAvailable(r -> registerReporterMeters(r, registry));
      // design decisions: access counting: the counting reservoir lives on the shared access counter —
      // its pending-keys gauge replaces the reporter-owned one.
      accessCounterProvider.ifAvailable(counter ->
        gauge(registry, "zeta.reporter.pending.keys", counter, AccessCounter::pendingKeyCount)
      );
      // Stall-cause meters: each (cause, state ≠ normal) pair is its own
      // meter — the healthy/normal state registers nothing (the RocksDB
      // write_stall_stats pattern). A missing component silently skips its
      // causes (that failure mode does not exist in this deployment).
      registerStallMeters(registry, singleFlightProvider, broadcastBufferProvider);
      cacheSyncPublisherProvider.ifAvailable(csp ->
        counter(registry, "zeta.sync.send.failed.total", csp, CacheSyncPublisher::sendFailures)
      );
      backgroundRefresherProvider.ifAvailable(refresher -> {
        var limiter = refresher.getRefreshLimiter();
        if (limiter != null) {
          gauge(registry, "zeta.expire.refresh.available", limiter, Semaphore::availablePermits);
        }
      });
      stampClockProvider.ifAvailable(clock ->
        gauge(registry, "zeta.version.clock.skew", clock, StampClock::skewMillis)
      );
      healthViewProvider.ifAvailable(hv ->
        gauge(registry, "zeta.worker.alive", hv, v -> v.isClusterHealthy() ? 1L : 0L)
      );
      // Decision-plane saturation: a COOL the ordered dispatcher refused is
      // re-applied inline on the consumer thread (never silently lost); the
      // counter is the signal that the dispatcher saturated.
      workerListenerProvider.ifAvailable(wl -> {
        counter(registry, "zeta.worker.decision.cool.rejected.total", wl, WorkerListener::coolDispatchRejections);
        // Epoch-rollback skip (VersionGuard is a static utility — no bean):
        // persistent skips after a Worker restart mean the epoch file was lost
        // and the new incarnation is muted until the resident entries expire.
        // Surfaced only alongside a decision listener — no listener, no
        // decisions to skip.
        counter(registry, "zeta.worker.decision.epoch.regress.skip.total", VersionGuard.class, ignored ->
          VersionGuard.epochRegressSkips()
        );
      });
      // Sync-plane saturation and identity diagnostics: a dispatcher refusal is applied by
      // the no-hook fallback (never silently lost), and a shared node id means two instances
      // can mint equal stamps and skip each other's writes until the next ms/TTL.
      syncListenerProvider.ifAvailable(sl -> {
        counter(registry, "zeta.sync.dispatch.rejected.total", sl, CacheSyncListener::syncDispatchRejections);
        counter(registry, "zeta.sync.nodeid.collision.total", sl, CacheSyncListener::nodeIdCollisions);
      });
    };
  }

  /**
   * Register one live-value gauge reading a {@code long}. The cast to Micrometer's {@code double}
   * lives here, so every meter site names only its component and its accessor.
   *
   * @param registry the meter registry
   * @param name     the meter name
   * @param target   the object the gauge reads
   * @param value    the accessor
   * @param <T>      the target type
   */
  private static <T> void gauge(MeterRegistry registry, String name, T target, ToLongFunction<T> value) {
    Gauge.builder(name, target, t -> (double) value.applyAsLong(t)).register(registry);
  }

  /**
   * Register one monotonic counter reading a {@code long}.
   *
   * @param registry the meter registry
   * @param name     the meter name
   * @param target   the object the counter reads
   * @param value    the accessor
   * @param <T>      the target type
   */
  private static <T> void counter(MeterRegistry registry, String name, T target, ToLongFunction<T> value) {
    FunctionCounter.builder(name, target, t -> (double) value.applyAsLong(t)).register(registry);
  }

  /**
   * Re-binds both registered Zeta {@link MeterBinder} beans once every singleton is ready.
   *
   * <p>A binder created during the early reporter/access-counter cycle can be initialized before
   * the cache, detector or reporter it reads exists, leaving every {@code zeta.*} meter silently
   * absent. Resolving the two binder beans by their fixed names keeps a user override in the
   * loop; Micrometer returns the existing meter for an identical id, so a successful early bind
   * is harmless. The registry is not probed for an existing {@code zeta.*} meter: that probe would
   * let one early series suppress every unrelated late binder.
   *
   * <p>A registry already owned by a {@link CompositeMeterRegistry} is skipped: the composite
   * forwards every binding to its children, so binding each child directly would duplicate the
   * whole binder set. A child that is not registered with a composite bean is bound on its own.
   *
   * @param registriesProvider every meter registry in the context, in order
   * @param caffeineMetrics    the registered {@code hotKeyCaffeineMetrics} bean, if present
   * @param customMetrics      the registered {@code hotKeyCustomMetrics} bean, if present
   * @return a callback invoked once after singleton instantiation
   */
  @Bean
  @ConditionalOnMissingBean(name = "zetaLateMetrics")
  public SmartInitializingSingleton zetaLateMetrics(
    ObjectProvider<MeterRegistry> registriesProvider,
    @Qualifier("hotKeyCaffeineMetrics") ObjectProvider<MeterBinder> caffeineMetrics,
    @Qualifier("hotKeyCustomMetrics") ObjectProvider<MeterBinder> customMetrics
  ) {
    return () -> {
      List<MeterRegistry> registries = registriesProvider.orderedStream().toList();
      Set<MeterRegistry> compositeChildren = new HashSet<>();
      for (MeterRegistry registry : registries) {
        if (registry instanceof CompositeMeterRegistry composite) {
          compositeChildren.addAll(composite.getRegistries());
        }
      }
      for (MeterRegistry registry : registries) {
        if (compositeChildren.contains(registry)) {
          continue;
        }
        caffeineMetrics.ifAvailable(binder -> binder.bindTo(registry));
        customMetrics.ifAvailable(binder -> binder.bindTo(registry));
      }
    };
  }

  /**
   * Register Micrometer meters for the local app-side TopK detector.
   * Exposes top-K size, total requests and expelled queue size.
   *
   * @param detector the local TopK detector; never {@code null}, because the binder registers these
   *                 meters only when a detector bean exists
   * @param registry the Micrometer meter registry
   */
  private static void registerLocalTopKMeters(TopK detector, MeterRegistry registry) {
    Gauge.builder("zeta.topk.size", detector, t -> (double) t.estimatedSize())
      .tag("type", "local")
      .register(registry);
    // Not ".total": Prometheus Client 1.x strips a reserved suffix from a gauge name, which would
    // silently rename the series to zeta_topk.
    Gauge.builder("zeta.topk.count", detector, t -> (double) t.total())
      .tag("type", "local")
      .register(registry);
    gauge(registry, "zeta.expelled.queue.size", detector, t -> t.expelled().size());
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
    Gauge.builder("zeta.reporter.queue.depth", reporter, r ->
      dispatcherStat(r, KeyReporter.DispatcherStats::depth, -1d)
    ).register(registry);
    FunctionCounter.builder("zeta.reporter.queue.dropped.total", reporter, r ->
      dispatcherStat(r, KeyReporter.DispatcherStats::dropped, 0d)
    ).register(registry);
    FunctionCounter.builder("zeta.reporter.publish.failed.total", reporter, r ->
      dispatcherStat(r, KeyReporter.DispatcherStats::publishFailed, 0d)
    ).register(registry);
    // The two expiry causes select different actions: a dead target means topology/health,
    // a stale batch means backpressure. Their sum is not exposed — it can never disagree.
    FunctionCounter.builder("zeta.reporter.queue.expired.dead.total", reporter, r ->
      dispatcherStat(r, KeyReporter.DispatcherStats::expiredDeadTarget, 0d)
    ).register(registry);
    FunctionCounter.builder("zeta.reporter.queue.expired.stale.total", reporter, r ->
      dispatcherStat(r, KeyReporter.DispatcherStats::expiredStale, 0d)
    ).register(registry);
    // zeta.reporter.pending.keys is registered from the shared access counter: the reporter
    // owns no counting reservoir (design decisions: access counting).
    // Feed-loop meters exist only when the tuner is configured: in shadow mode the interval
    // shows the trajectory it WOULD apply — the deploy-first observation window.
    ReportFeedLoop loop = reporter.feedLoop();
    if (loop != null) {
      gauge(registry, "zeta.reporter.feedloop.interval", loop, ReportFeedLoop::intervalMs);
      gauge(registry, "zeta.reporter.feedloop.score", loop, ReportFeedLoop::lastScoreBp);
      gauge(registry, "zeta.reporter.feedloop.batch", loop, ReportFeedLoop::projectedBatchSize);
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
   *   <tr><td>{@code zeta.sync.invalidate.dropped.total}</td><td>INVALIDATE-family messages
   *       dropped because the send executor refused them (counted per message)</td><td>BroadcastBuffer</td></tr>
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
    broadcastBufferProvider.ifAvailable(bb -> {
      counter(registry, "zeta.stall.broadcast_storm.stopped.total", bb, b -> b.sendFailures() + b.saturationDrops());
      counter(registry, "zeta.sync.invalidate.dropped.total", bb, BroadcastBuffer::invalidatesDropped);
    });
    singleFlightProvider.ifAvailable(sf -> {
      gauge(registry, "zeta.stall.source_degraded.stopped", sf, s -> s.isBreakerOpen() ? 1L : 0L);
      counter(registry, "zeta.stall.source_degraded.timeouts.total", sf, SingleFlight::loadTimeoutCount);
    });
  }
}
