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

import static io.github.hyshmily.zeta.util.TimeSource.monotonicMillis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.cache.AccessCounter;
import io.github.hyshmily.zeta.cache.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.LoadFlights;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.heat.HeatDirectory;
import io.github.hyshmily.zeta.heat.HeatView;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry.WarmLoaderBinder;
import io.github.hyshmily.zeta.policy.ZetaWarmConfigurer;
import io.github.hyshmily.zeta.reporting.CadenceTunable;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.KeepWarm;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.executor.ElasticThreadPool;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * App-side autoconfiguration for the HotKey library.
 *
 * <p>Creates the app-side {@link TopK} detector (HeavyKeeper), L1 Caffeine
 * cache, {@link SingleFlight} deduplication layer, executor, and the
 * {@link HotKeyCache}.
 *
 * <p><b>Condition:</b> this configuration is <em>skipped</em> when the
 * Worker is active ({@code zeta.worker.enabled=true}). It runs when
 * Worker is disabled or the property is absent.
 */
@Internal
@AutoConfiguration
@ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "false", matchIfMissing = true)
@EnableConfigurationProperties(ZetaProperties.class)
@Slf4j
public class ZetaAutoConfiguration {

  /** Thread-name prefix for the interruptible load pool, local to this assembly. */
  private static final String PREFIX_LOAD = "zeta-load";

  /** Thread-name prefix for the peer-REFRESH load pool, local to this assembly. */
  private static final String PREFIX_PEER_REFRESH = "zeta-peer-refresh";

  /** Thread-name prefix for the broadcast send pool (design decisions: buffering and ordering). */
  private static final String PREFIX_SEND = "zeta-send";

  private final ZetaProperties properties;

  public ZetaAutoConfiguration(ZetaProperties properties) {
    this.properties = properties;
  }

  /**
   * Report a {@code CALLER_RUNS} request that only one of the three pools can
   * honour. The load and send pools force abort, so one WARN names the pool that
   * honours the setting instead of repeating the same global value per pool.
   */
  @PostConstruct
  void warnIfCallerRunsIsDowngraded() {
    if (properties.getExecutorRejection() != ZetaProperties.ExecutorRejection.CALLER_RUNS) {
      return;
    }
    log.warn(
      "zeta.local.executor-rejection=CALLER_RUNS is honored only by the hotKeyExecutor pool. The interruptible-load " +
        "and broadcast-send pools always abort: running their task on the submitting thread would move a load " +
        "inside SingleFlight's lock or a broker send onto the commit thread."
    );
  }

  /**
   * Report a refresh bound the executor cannot honour.
   *
   * <p>The refresh semaphore and the read-miss loads share one pool, so a
   * permit count above the effective load-pool maximum is unreachable: the pool,
   * not the semaphore, is the real bound. {@link ZetaProperties#effectiveRefreshMaxPools()}
   * clamps it; this startup check keeps the clamping visible even when the
   * application replaces the refresher bean.
   */
  @PostConstruct
  void warnIfRefreshLimitExceedsPool() {
    int limit = properties.getRefreshMaxPools();
    int max = properties.effectiveLoadMaxPoolSize();
    if (limit > max) {
      log.warn(
        "zeta.local.refresh-max-pools={} exceeds the load pool's max size {}: the limit is clamped to the pool. " +
          "Background refreshes share the pool with read-miss loads, so a higher limit bounds nothing; lower the " +
          "limit to leave headroom or raise zeta.local.load-executor-max-pool-size.",
        limit,
        max
      );
    }
  }

  /**
   * Create the app-side TopK instance (HeavyKeeper) as a standalone bean.
   *
   * <p>The HeavyKeeper uses a Count-Min Sketch augmented with a minimum-add
   * threshold and exponential decay to track the top-K hottest keys with high
   * accuracy and low memory footprint. Configuration parameters (width, depth,
   * decay, minCount, topK capacity, expelled queue capacity) are read from
   * {@link ZetaProperties}. The configured width is auto-aligned up to the
   * nearest power of two so bucket indexing takes the mask fast path (the
   * aligned value is logged at INFO).
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a new HeavyKeeper TopK instance
   */
  @Bean
  @ConditionalOnMissingBean
  public HeavyKeeper heavyKeeper(ZetaProperties properties) {
    return new HeavyKeeper(
      properties.getTopK(),
      properties.getWidth(),
      properties.getDepth(),
      properties.getDecay(),
      properties.getMinCount(),
      properties.getExpelledQueueCapacity(),
      properties.getSketchWindowCount(),
      true
    );
  }

  /**
   * Create the app-side {@link HotKeyDetector} — the write edge of the local
   * sketch.
   *
   * <p>Since design decisions: access counting the detector owns no buffering:
   * the read path counts once in the shared {@link AccessCounter}, whose tide
   * delivers merged batches to {@link HotKeyDetector#ingest(Map)}. It is
   * deliberately <b>not</b> a {@link TopK} bean: queries (actuator, Micrometer,
   * the facade's ranking) inject the {@code HeavyKeeper} bean by that type, so
   * the sketch is decayed exactly once per tick. The detector provides no
   * scheduling either — {@code fading()} and the expelled drain belong to
   * {@link ZetaSchedulingConfiguration}.
   *
   * <p>This bean is the {@code @Qualifier("hotKeyDetector")} target for
   * injection into {@link HeatView}, {@link AccessCounter} and the keep-warm
   * admission sink.
   *
   * @param heavyKeeper the HeavyKeeper TopK instance (never {@code null})
   * @return a new HotKeyDetector instance
   */
  @Bean
  @ConditionalOnMissingBean
  public HotKeyDetector hotKeyDetector(HeavyKeeper heavyKeeper) {
    return new HotKeyDetector(heavyKeeper);
  }

  /**
   * Create the shared {@link AccessCounter} — the single buffered counting
   * entry behind the read path (design decisions: access counting): one tide
   * feeds both the Worker report and the sketch ingest, so the two pipelines
   * cannot disagree.
   *
   * <p>Tide base: the configured {@code zeta.local.report-interval-ms} when a
   * reporter is wired (50ms seed, moved by the reporter's feed-loop tuner
   * inside {@code [report-interval-min-ms, report-interval-max-ms]}), the
   * detector's {@code zeta.local.detect-interval-ms} rhythm otherwise. The
   * sketch ingest keeps its own fixed {@code detect-interval-ms} cadence
   * (default 500ms) by merging reporter-sized tides into an accumulator — the
   * reporting control loop never sets detection latency.
   *
   * @param hotKeyDetector   the sketch facade (never {@code null})
   * @param hotKeyReporterProvider provider for the Worker reporter, absent when reporting is
   *                         disabled (no RabbitMQ / App-only deployment)
   * @param hotKeyScheduler  the shared scheduler driving the tide (never {@code null})
   * @param properties       the HotKey configuration properties (never {@code null})
   * @return a new AccessCounter instance
   */
  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public AccessCounter accessCounter(
    HotKeyDetector hotKeyDetector,
    ObjectProvider<KeyReporter> hotKeyReporterProvider,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService hotKeyScheduler,
    ZetaProperties properties
  ) {
    KeyReporter reporter = hotKeyReporterProvider.getIfAvailable();
    AccessCounter counter = new AccessCounter(
      hotKeyDetector,
      reporter,
      hotKeyScheduler,
      properties.getReportIntervalMs(),
      properties.getDetectIntervalMs()
    );
    // The design decisions: reporting feed-loop tuner stays with the reporter but its computed
    // interval applies to the counter's tide (design decisions: access counting) — wire the seam.
    if (reporter instanceof CadenceTunable tunable) {
      tunable.setCadenceAdjuster(counter::adjustDeliverIntervalMs);
    }
    return counter;
  }

  /**
   * Create the per-prefix circuit-breaker registry protecting remote cache-load calls.
   *
   * <p>One sick datasource trips only its own prefix shard — a global breaker
   * would silence every key when a single source fails. Shards share one slide
   * thread and the live breaker settings.
   *
   * <p>Registered as a Spring-managed {@link AutoCloseable} bean — the container infers
   * {@code close()} as its destroy method, so every shard and the shared slide
   * scheduler are shut down on context shutdown and hot-restart (new classloader)
   * scenarios do not leak threads or breaker instances.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a new breaker registry
   */
  @Bean(destroyMethod = "close")
  @ConditionalOnMissingBean(CircuitBreakerRegistry.class)
  public CircuitBreakerRegistry circuitBreakerRegistry(ZetaProperties properties) {
    return new CircuitBreakerRegistry(properties.getCircuitBreaker(), properties.getCircuitBreaker().getMaxPrefixes());
  }

  /**
   * Create the SingleFlight deduplication layer for concurrent cache-load requests.
   *
   * <p>When multiple threads request the same key simultaneously (a cache miss), the
   * first caller triggers the load and subsequent callers wait for the same result
   * rather than duplicating the load. Configuration parameters (max in-flight entries,
   * TTL, timeout) are read from {@link ZetaProperties}.
   *
   * @param properties      the HotKey configuration properties (never {@code null})
   * @param loadExecutor    the interruptible pool reserved for
   *                        {@link io.github.hyshmily.zeta.util.InterruptingAsync}
   *                        loads (never {@code null})
   * @param breakerRegistry the per-prefix breaker shards protecting remote load calls (never {@code null})
   * @return a new SingleFlight instance
   */
  @Bean
  @ConditionalOnMissingBean
  public SingleFlight singleFlight(
    ZetaProperties properties,
    @Qualifier("interruptibleLoadExecutor") Executor loadExecutor,
    CircuitBreakerRegistry breakerRegistry
  ) {
    return new LoadFlights(
      properties.getInFlightMaxSize(),
      properties.getInFlightTtlSeconds(),
      properties.getInFlightTimeoutSeconds(),
      loadExecutor,
      breakerRegistry
    );
  }

  /**
   * Create the per-namespace TTL and warming policy registry.
   *
   * <p>The annotation layer registers each resolved method's baselines and heat triggers here;
   * the prewarm and keep-warm paths read them back. TTLs stay single-owned by the declaration
   * layers, and a namespace whose methods disagree becomes non-prewarmable rather than guessing.
   *
   * @param properties             the HotKey configuration properties (never {@code null})
   * @param clusterLoaderProvider  the optional deployment-wide warmable loader
   * @return a new policy registry
   */
  @Bean
  @ConditionalOnMissingBean
  public NamespacePolicyRegistry namespacePolicyRegistry(
    ZetaProperties properties,
    ObjectProvider<CacheLoader<String, Object>> clusterLoaderProvider
  ) {
    return new NamespacePolicyRegistry(properties, clusterLoaderProvider.getIfAvailable());
  }

  /**
   * Create the plan-time namespace format registry. It owns only each namespace's key format and
   * locked PLAIN type; loaders and triggers stay in the warm registry, so a plan can be built
   * while a bean is proxied without instantiating a user configurer.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a new format registry
   */
  @Bean
  @ConditionalOnMissingBean
  public NamespaceKeys namespaceKeys(ZetaProperties properties) {
    return new NamespaceKeys(properties.getKeyFormat());
  }

  /**
   * Create the entry lifecycle manager: TOCTOU invalidation guards and the single draft factory.
   *
   * @param hotLocalCache the L1 Caffeine cache (never {@code null})
   * @param properties    the HotKey configuration properties (never {@code null})
   * @return a new EntryLifecycleImpl instance
   */
  @Bean
  @ConditionalOnMissingBean
  public EntryLifecycle entryLifecycle(Cache<String, CacheEntry> hotLocalCache, ZetaProperties properties) {
    return new EntryLifecycleImpl(hotLocalCache, properties);
  }

  /**
   * Create the background soft-expire refresh executor: per-key dedup, a
   * global refresh limiter, timeout protection, and a closed retry window on
   * failure. Shares the entry lifecycle, so an in-flight refresh is
   * refused by the same invalidation tombstones as every other fill.
   *
   * @param hotLocalCache     the L1 Caffeine cache (never {@code null})
   * @param loadExecutor      the interruptible pool reserved for
   *                          {@link io.github.hyshmily.zeta.util.InterruptingAsync}
   *                          refresh loads (never {@code null})
   * @param entryLifecycle    the entry lifecycle (TTL policy and tombstones, never {@code null})
   * @param properties        the HotKey configuration properties (never {@code null})
   * @return a new DefaultBackgroundRefresher instance
   */
  @Bean
  @ConditionalOnMissingBean
  public BackgroundRefresher backgroundRefresher(
    Cache<String, CacheEntry> hotLocalCache,
    @Qualifier("interruptibleLoadExecutor") Executor loadExecutor,
    EntryLifecycle entryLifecycle,
    ZetaProperties properties
  ) {
    return new DefaultBackgroundRefresher(
      hotLocalCache,
      loadExecutor,
      entryLifecycle,
      properties.effectiveRefreshMaxPools(),
      properties.getRefreshTimeoutMs()
    );
  }

  /**
   * Create the dedicated pool for the bulk-refresh fan-out
   * ({@code Zeta.refreshAll}).
   *
   * <p><b>Why a dedicated pool.</b> Each fan-out task blocks on stamp allocation
   * plus business IO on its own thread, so the fan-out must run neither on the
   * JVM-global common pool (it would starve every unrelated {@code CompletableFuture}
   * / parallel-stream user) nor on {@code interruptibleLoadExecutor} (a bulk refresh
   * of many keys would park the threads that serve read-miss SingleFlight loads and
   * background refreshes). {@code ZetaFacadeAutoConfiguration} binds this pool to the
   * facade's refresh fan-out. Uses the shared bounded Tomcat-style ordering
   * (core → max → queue → reject) with the configured rejection policy.
   *
   * <p>This is the one pool where {@code CALLER_RUNS} is honored: a bulk refresh is
   * caller-driven work, so back-pressuring the caller that asked for it is the
   * right degradation.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a configured {@link ElasticThreadPool}
   */
  @Bean(name = "hotKeyExecutor", destroyMethod = "shutdownNow")
  @ConditionalOnMissingBean(name = "hotKeyExecutor")
  public Executor hotKeyExecutor(ZetaProperties properties) {
    return newPool(
      properties,
      ZetaConstants.Threads.PREFIX_KEY,
      Saturation.CALLER_RUNS_OK,
      properties.getExecutorCorePoolSize(),
      properties.getExecutorMaxPoolSize(),
      properties.getExecutorQueueCapacity()
    );
  }

  /**
   * Create the <b>interruptible</b> executor used exclusively by
   * {@link io.github.hyshmily.zeta.util.InterruptingAsync}.
   *
   * <p><b>Why a dedicated pool (design decisions: invalidation).</b> {@code InterruptingAsync} owns the
   * interrupt flag of the threads it runs on: it interrupts a timed-out load in
   * place and clears the flag on task exit. Its contract requires an executor
   * whose threads are never used for anything else, so a stalled data source
   * degrades reads without occupying write-path threads. Write-path work runs
   * inline or at afterCommit on the caller/commit thread (design decisions: invalidation).
   *
   * <p>This pool serves {@link #singleFlight} and {@link #backgroundRefresher} —
   * the only two components that run interruptible loads.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a configured {@link ElasticThreadPool} reserved for interruptible loads
   */
  @Bean(name = "interruptibleLoadExecutor", destroyMethod = "shutdownNow")
  @ConditionalOnMissingBean(name = "interruptibleLoadExecutor")
  public Executor interruptibleLoadExecutor(ZetaProperties properties) {
    return newPool(
      properties,
      PREFIX_LOAD,
      Saturation.FORCE_ABORT,
      properties.effectiveLoadCorePoolSize(),
      properties.effectiveLoadMaxPoolSize(),
      properties.effectiveLoadQueueCapacity()
    );
  }

  /**
   * Create the executor for blocking peer REFRESH loads.
   *
   * <p>A peer REFRESH is background work triggered by another instance, not by a local caller.
   * Sharing the interruptible load pool let a REFRESH burst park the read-miss SingleFlight loads
   * and the local renewal behind it. The pool is bounded and aborts on saturation; the sync
   * context turns that rejection into a versioned local invalidation instead of blocking the
   * dispatcher. Its concurrency reuses the refresh limiter's bound.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a bounded executor reserved for peer refresh loads
   */
  @Bean(name = "peerRefreshExecutor", destroyMethod = "shutdownNow")
  @ConditionalOnMissingBean(name = "peerRefreshExecutor")
  public Executor peerRefreshExecutor(ZetaProperties properties) {
    int concurrency = Math.max(1, properties.effectiveRefreshMaxPools());
    return newPool(
      properties,
      PREFIX_PEER_REFRESH,
      Saturation.FORCE_ABORT,
      concurrency,
      concurrency,
      properties.effectiveLoadQueueCapacity()
    );
  }

  /**
   * Create the executor used to hand buffered peer broadcasts to AMQP.
   *
   * <p><b>Why (design decisions: buffering and ordering).</b> {@link BroadcastBuffer} documents that production
   * wiring must supply a send executor; passing {@code null} made every deferred
   * REFRESH send run on the submitting thread — which, for a transactional
   * {@code putThrough}, is the transaction-commit thread. A slow broker then
   * stalled commit. Buffered sends are network I/O with their own failure mode
   * and must not share a pool with cache writes or loads.
   *
   * <p>Shutdown drains for a bounded window before forcing the pool down, so a
   * broadcast submitted after a committed transaction is not discarded by an
   * immediate {@code shutdownNow()}.
   *
   * @param properties             the HotKey configuration properties (never {@code null})
   * @param syncPublisherProvider optional sync publisher used only to order bean destruction
   * @return a configured {@link ElasticThreadPool} reserved for broadcast sends
   */
  @Bean(name = "zetaSendExecutor", destroyMethod = "shutdownGracefully")
  @ConditionalOnMissingBean(name = "zetaSendExecutor")
  public Executor zetaSendExecutor(
    ZetaProperties properties,
    ObjectProvider<CacheSyncPublisher> syncPublisherProvider
  ) {
    // Destruction edge only: the pool must outlive the publisher it sends through, so it is
    // destroyed first and a queued broadcast is flushed before the Rabbit connection closes.
    syncPublisherProvider.getIfAvailable();
    return newPool(
      properties,
      PREFIX_SEND,
      Saturation.FORCE_ABORT,
      properties.effectiveSendCorePoolSize(),
      properties.effectiveSendMaxPoolSize(),
      properties.effectiveSendQueueCapacity()
    );
  }

  /**
   * Whether a pool may fall back to {@link ThreadPoolExecutor.CallerRunsPolicy}
   * when {@code zeta.local.executor-rejection} says so.
   *
   * <p>{@code CALLER_RUNS} moves a task from the pool onto whoever submitted it,
   * which is only safe when the submitting thread is already the right place to
   * run it. Two pools are not:
   *
   * <ul>
   *   <li>the load pool submits inside {@code SingleFlight}'s
   *       {@code computeIfAbsent} mapping, so the reader would run on the caller
   *       while it still holds the bin lock, and {@code orTimeout} would
   *       interrupt the caller's own thread on a slow data source;</li>
   *   <li>the send pool submits from the commit thread (deferred broadcasts) and
   *       from the business thread (invalidations), so a saturated broker would
   *       stall transaction commit — the exact failure design decisions: buffering and ordering exists to
   *       prevent.</li>
   * </ul>
   *
   * <p>Both therefore ignore the setting and always abort; a configuration that
   * asks for it on their behalf is reported at startup rather than silently
   * downgraded.
   */
  private enum Saturation {
    /** Honor {@code zeta.local.executor-rejection}; {@code CALLER_RUNS} is meaningful. */
    CALLER_RUNS_OK,
    /** Always abort, whatever the setting says. */
    FORCE_ABORT,
  }

  /**
   * Build a bounded thread pool with Tomcat-style ordering
   * (core → max → queue → reject).
   *
   * <p>The bounded queue limits concurrent AMQP channel usage: RabbitMQ's
   * {@code CachingConnectionFactory} associates channels with platform threads,
   * so unbounded concurrency causes channel-open timeouts.
   *
   * @param properties    the HotKey configuration properties (never {@code null})
   * @param threadPrefix  the thread-name prefix identifying the pool's domain
   * @param saturation    whether this pool may honor {@code CALLER_RUNS}
   * @param corePoolSize  the pool's core size
   * @param maxPoolSize   the pool's maximum size
   * @param queueCapacity the pool's queue capacity
   * @return a configured {@link ElasticThreadPool}
   */
  private ElasticThreadPool newPool(
    ZetaProperties properties,
    String threadPrefix,
    Saturation saturation,
    int corePoolSize,
    int maxPoolSize,
    int queueCapacity
  ) {
    boolean callerRuns =
      saturation == Saturation.CALLER_RUNS_OK &&
      properties.getExecutorRejection() == ZetaProperties.ExecutorRejection.CALLER_RUNS;
    // ABORT (default) throws on saturation — upstream read paths swallow the
    // failure as a cache miss. CALLER_RUNS back-pressures the submitting thread
    // instead of dropping the async work.
    var rejectionHandler = callerRuns
      ? (RejectedExecutionHandler) new ThreadPoolExecutor.CallerRunsPolicy()
      : (RejectedExecutionHandler) (r, exe) -> {
          log.debug(
            "Zeta executor task rejected: pool={}, active={}, poolSize={}/{}, queueSize={}",
            threadPrefix,
            exe.getActiveCount(),
            exe.getPoolSize(),
            exe.getMaximumPoolSize(),
            exe.getQueue().size()
          );
          throw new RejectedExecutionException("HotKey executor queue full: " + threadPrefix);
        };
    var executor = new ElasticThreadPool(
      corePoolSize,
      maxPoolSize,
      60L,
      TimeUnit.SECONDS,
      queueCapacity,
      new ZetaThreadFactory(threadPrefix),
      rejectionHandler
    );
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  /**
   * Create the sync plane's send outlet: deferred PUT/REFRESH records plus the
   * immediate INVALIDATE family, including the cancel-before-send rule.
   */
  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public BroadcastBuffer broadcastBuffer(
    @Qualifier("hotKeyScheduler") ScheduledExecutorService hotKeyScheduler,
    @Qualifier("zetaSendExecutor") Executor sendExecutor,
    ObjectProvider<CacheSyncPublisher> syncPublisherProvider,
    CacheSyncProperties syncProperties
  ) {
    return new BroadcastBuffer(
      hotKeyScheduler,
      syncPublisherProvider.getIfAvailable(),
      syncProperties.getFlushDelayMs(),
      sendExecutor,
      BroadcastBuffer.DEFAULT_MAX_PENDING_PAYLOAD_BYTES
    );
  }

  /**
   * Create the combined heat view over the cluster lease table and the local detector.
   *
   * @param heatDirectoryProvider the cluster lease table
   * @param hotKeyDetector        the local TopK detector
   * @return the heat view
   */
  @Bean
  @ConditionalOnMissingBean
  public HeatView heatView(
    ObjectProvider<HeatDirectory> heatDirectoryProvider,
    @Qualifier("hotKeyDetector") HotKeyDetector hotKeyDetector
  ) {
    return new HeatView(heatDirectoryProvider.getIfAvailable(), hotKeyDetector);
  }

  /**
   * Create the {@link HotKeyCache}.
   *
   * @param hotLocalCache       the L1 Caffeine cache (never {@code null})
   * @param singleFlight        the deduplication layer (never {@code null})
   * @param entryLifecycle      the entry lifecycle manager (never {@code null})
   * @param backgroundRefresher the background refresh coordinator (never {@code null})
   * @param accessCounter       the shared read-path counter (never {@code null})
   * @param broadcastBuffer     the sync-plane send outlet (never {@code null})
   * @param stampClock          the node-local data-stamp clock (never {@code null})
   * @return a new HotKeyCache instance with node-local stamping
   */
  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public HotKeyCache hotKeyCache(
    Cache<String, CacheEntry> hotLocalCache,
    SingleFlight singleFlight,
    EntryLifecycle entryLifecycle,
    BackgroundRefresher backgroundRefresher,
    AccessCounter accessCounter,
    BroadcastBuffer broadcastBuffer,
    StampClock stampClock
  ) {
    return new HotKeyCache(
      hotLocalCache,
      singleFlight,
      entryLifecycle,
      backgroundRefresher,
      accessCounter,
      broadcastBuffer,
      stampClock
    );
  }

  /**
   * Create the local hard-renewal scheduler. {@link WarmRegistryFinalizer} installs its signals
   * after every singleton exists and the registry is frozen, so replacing or omitting this bean
   * leaves the other components untouched.
   *
   * @param hotLocalCache the L1 table
   * @param heatView      the heat query
   * @param policies      the namespace policy registry
   * @param refresher     the shared refresh executor
   * @param hotKeyScheduler the shared scheduler
   * @param stampClock    the data-stamp clock
   * @param entryLifecycle the entry lifecycle owning the shared TTL arithmetic
   * @param properties    the live configuration
   * @return the renewal scheduler
   */
  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public KeepWarm keepWarm(
    Cache<String, CacheEntry> hotLocalCache,
    HeatView heatView,
    NamespacePolicyRegistry policies,
    BackgroundRefresher refresher,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService hotKeyScheduler,
    StampClock stampClock,
    EntryLifecycle entryLifecycle,
    ZetaProperties properties
  ) {
    return new KeepWarm(
      hotLocalCache,
      heatView,
      policies,
      refresher,
      hotKeyScheduler,
      stampClock,
      entryLifecycle.ttlPolicy(),
      properties
    );
  }

  /**
   * Collect every warming declaration after all singletons exist, freeze the registry, and then
   * attach the renewal scheduler.
   *
   * <p>It lives here, in the app-side configuration, rather than in the annotation module, so a
   * programmatic deployment without Spring AOP still runs its {@link ZetaWarmConfigurer} beans and
   * the same validation and freeze. The scheduler is injected rather than provided as a separate
   * lifecycle bean: two unordered {@link org.springframework.beans.factory.SmartInitializingSingleton}s
   * would let the scheduler observe the registry before it was frozen.
   *
   * @param policies      the namespace warming registry
   * @param keys          the plan-time key-format registry
   * @param configurers   the user configurers
   * @param binders       the annotation layer's warm-spec binders, when that layer is present
   * @param hotKeyCache   the L1 engine
   * @param accessCounter the shared read counter
   * @param hotKeyDetector the local detector
   * @param keepWarm      the renewal scheduler, when present
   * @return the finalizer
   */
  @Bean
  @ConditionalOnMissingBean
  public WarmRegistryFinalizer warmRegistryFinalizer(
    NamespacePolicyRegistry policies,
    NamespaceKeys keys,
    ObjectProvider<ZetaWarmConfigurer> configurers,
    ObjectProvider<WarmLoaderBinder> binders,
    HotKeyCache hotKeyCache,
    AccessCounter accessCounter,
    @Qualifier("hotKeyDetector") HotKeyDetector hotKeyDetector,
    ObjectProvider<KeepWarm> keepWarm
  ) {
    return new WarmRegistryFinalizer(
      policies,
      keys,
      configurers,
      binders,
      hotKeyCache,
      accessCounter,
      hotKeyDetector,
      keepWarm
    );
  }

  /**
   * Create the L1 Caffeine cache instance.
   *
   * <p>Time-based expiry operates at the <em>Caffeine</em> level via a custom
   * {@link Expiry} implementation, computing remaining nanoseconds from the
   * entry's {@code hardExpireAtMs} monotonic deadline — the same axis
   * {@code TtlPolicy} stamps its deadlines on, and the same kind of clock
   * Caffeine's own ticker counts with. Entries with
   * {@code hardExpireAtMs == Long.MAX_VALUE} are purely logical-expiry
   * — Caffeine never evicts them by time; they live until size eviction or
   * manual invalidation. Reads never extend the expiry duration (no read-based
   * refresh), ensuring predictable TTL behavior.
   *
   * <p>Eviction strategy: {@code zeta.local.cache.max-size} bounds the table. A
   * normal entry created without a per-call override carries the configured
   * {@code zeta.local.hard-ttl-ms} as its hard deadline ({@code 0} means
   * permanent, which the {@link Expiry} above maps to no time eviction).
   *
   * <p>Stats recording is always enabled ({@code recordStats()}) so that
   * {@code Zeta#stats()} and the {@code cache.*} Micrometer metrics report
   * hit/miss/eviction counters. A custom {@code Cache<String, CacheEntry>} bean
   * replacing this one must enable {@code recordStats()} itself for those
   * counters to be populated.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @param customizerProvider ordered provider of application {@link ZetaCacheCustomizer}
   *                           beans, applied just before {@code build()} (design decisions: api surface)
   * @return a configured Caffeine {@link Cache} instance
   */
  @Bean
  @ConditionalOnMissingBean
  public Cache<String, CacheEntry> hotLocalCache(
    ZetaProperties properties,
    ObjectProvider<ZetaCacheCustomizer> customizerProvider
  ) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    builder.maximumSize(properties.getCache().getMaxSize());
    // Stats recording enables hit/miss/eviction counters for Zeta#stats() and the cache.*
    // Micrometer metrics; overhead is a few LongAdder increments per cache operation.
    builder.recordStats();
    // Caffeine.newBuilder() starts as <Object, Object>; expireAfter(...) narrows the same
    // builder instance in place, so this typed reference carries every setting written above.
    Caffeine<String, CacheEntry> typed = builder.expireAfter(
      new Expiry<>() {
        /**
         * Compute the time-to-live for a newly created cache entry.
         * Returns {@link Long#MAX_VALUE} for pure logical-expiry entries
         * (where {@code hardExpireAtMs == Long.MAX_VALUE}); otherwise
         * computes the remaining monotonic time.
         *
         * @param key              the cache key
         * @param value            the cache value (expected to be a {@link CacheEntry})
         * @param currentTimeNanos the current time in nanoseconds (provided by Caffeine)
         * @return the expiry duration in nanoseconds, or {@link Long#MAX_VALUE} for no expiry
         */
        @Override
        public long expireAfterCreate(@NonNull String key, @NonNull CacheEntry value, long currentTimeNanos) {
          return remainingHardTtlNanos(value.getHardExpireAtMs());
        }

        /**
         * Re-compute the expiry duration after an entry is updated.
         * Preserves pure logical expiry across updates; otherwise
         * recalculates from the entry's {@code hardExpireAtMs}.
         *
         * @param key              the cache key
         * @param value            the cache value (expected to be a {@link CacheEntry})
         * @param currentTimeNanos the current time in nanoseconds (provided by Caffeine)
         * @param currentDuration  the current expiry duration in nanoseconds
         * @return the updated expiry duration in nanoseconds, or {@link Long#MAX_VALUE} for no expiry
         */
        @Override
        public long expireAfterUpdate(
          @NonNull String key,
          @NonNull CacheEntry value,
          long currentTimeNanos,
          long currentDuration
        ) {
          return remainingHardTtlNanos(value.getHardExpireAtMs());
        }

        /**
         * Preserve the current expiry duration on read — reads never
         * extend or shorten the entry's time-to-live.
         *
         * @param key              the cache key
         * @param value            the cache value
         * @param currentTimeNanos the current time in nanoseconds (provided by Caffeine)
         * @param currentDuration  the current expiry duration in nanoseconds
         * @return the unchanged expiry duration in nanoseconds
         */
        @Override
        public long expireAfterRead(
          @NonNull String key,
          @NonNull CacheEntry value,
          long currentTimeNanos,
          long currentDuration
        ) {
          return currentDuration;
        }
      }
    );
    // Application customizers run last, in order, immediately before build() — they may add
    // orthogonal listeners/executors/schedulers but cannot replace the capacity or expiry knobs
    // set above (Caffeine setters are single-use).
    customizerProvider.orderedStream().forEach(customizer -> customizer.customize(typed));
    return typed.build();
  }

  /**
   * Remaining Caffeine expiry in nanos for an entry whose logical hard expiry
   * sits at {@code hardExpireAtMs}: {@link Long#MAX_VALUE} for pure
   * logical-expiry entries (Caffeine never time-evicts them — the Expiry
   * Javadoc defines {@code Long.MAX_VALUE} as "no expiration"); otherwise the
   * remaining monotonic time, floored at 1ms so a just-expired entry still
   * gets one scheduling quantum instead of an immediate re-expiry storm.
   */
  private static long remainingHardTtlNanos(long hardExpireAtMs) {
    if (hardExpireAtMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }

    long remainingMs = hardExpireAtMs - monotonicMillis();
    return TimeUnit.MILLISECONDS.toNanos(Math.max(1, remainingMs));
  }
}
