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

import static io.github.hyshmily.zeta.util.TimeSource.currentTimeMillis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.CacheDependencies;
import io.github.hyshmily.zeta.cache.CentralDispatcher;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.impl.SingleFlightImpl;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.rule.RuleEvaluator;
import io.github.hyshmily.zeta.rule.RuleRepository;
import io.github.hyshmily.zeta.rule.RuleService;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.executor.StandardThreadExecutor;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import java.util.Optional;
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
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * App-side autoconfiguration for the HotKey library.
 *
 * <p>Creates the app-side {@link TopK} detector (HeavyKeeper), L1 Caffeine
 * cache, {@link SingleFlight} deduplication layer, executor, and the
 * primary {@link HotKeyCache} (without Redis version tracking when Redis
 * is absent).
 *
 * <p><b>Condition:</b> this configuration is <em>skipped</em> when the
 * Worker is active ({@code zeta.worker.enabled=true}). It runs when
 * Worker is disabled or the property is absent.
 */
@Internal
@AutoConfiguration(afterName = "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration")
@ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "false", matchIfMissing = true)
@EnableConfigurationProperties(ZetaProperties.class)
@Slf4j
public class ZetaAutoConfiguration {

  /**
   * Create the app-side TopK instance (HeavyKeeper) as a standalone bean.
   *
   * <p>The HeavyKeeper uses a Count-Min Sketch augmented with a minimum-count
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
   * Create the app-side {@link HotKeyDetector} facade that wraps the HeavyKeeper TopK
   * and schedules periodic decay.
   *
   * <p>The detector provides the primary hot-key detection API ({@code add()}, {@code list()},
   * {@code total()}) and schedules the periodic HeavyKeeper decay using the shared scheduler.
   * This bean is the {@code @Qualifier("hotKeyDetector")} target for injection into
   * {@link HotKeyCache} and other components.
   *
   * @param heavyKeeper      the HeavyKeeper TopK instance (never {@code null})
   * @param hotKeyScheduler  the shared scheduler for periodic tasks (never {@code null})
   * @return a new HotKeyDetector instance
   */
  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public HotKeyDetector hotKeyDetector(
    HeavyKeeper heavyKeeper,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService hotKeyScheduler
  ) {
    return new HotKeyDetector(heavyKeeper, hotKeyScheduler);
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
    return new SingleFlightImpl(
      properties.getInFlightMaxSize(),
      properties.getInFlightTtlSeconds(),
      properties.getInFlightTimeoutSeconds(),
      loadExecutor,
      breakerRegistry
    );
  }

  /**
   * Create the entry lifecycle manager: TOCTOU invalidation guards,
   * Decision-Validity demotion and the single draft factory.
   *
   * @param hotLocalCache     the L1 Caffeine cache (never {@code null})
   * @param properties        the HotKey configuration properties (never {@code null})
   * @param clusterHealthView the cluster health view for decision validity (never {@code null})
   * @return a new EntryLifecycleImpl instance
   */
  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public EntryLifecycle entryLifecycle(
    Cache<String, CacheEntry> hotLocalCache,
    ZetaProperties properties,
    HealthView clusterHealthView
  ) {
    return new EntryLifecycleImpl(hotLocalCache, properties, clusterHealthView);
  }

  /**
   * Create the background soft-expire refresh executor: per-key dedup, a
   * global refresh limiter, timeout protection, and lease-on-failure
   * degradation. Shares the TTL policy with the entry lifecycle.
   *
   * @param hotLocalCache     the L1 Caffeine cache (never {@code null})
   * @param loadExecutor      the interruptible pool reserved for
   *                          {@link io.github.hyshmily.zeta.util.InterruptingAsync}
   *                          refresh loads (never {@code null})
   * @param entryLifecycle    the entry lifecycle providing the shared TTL policy (never {@code null})
   * @param properties        the HotKey configuration properties (never {@code null})
   * @return a new DefaultBackgroundRefresher instance
   */
  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public BackgroundRefresher backgroundRefresher(
    Cache<String, CacheEntry> hotLocalCache,
    @Qualifier("interruptibleLoadExecutor") Executor loadExecutor,
    EntryLifecycle entryLifecycle,
    ZetaProperties properties
  ) {
    return new DefaultBackgroundRefresher(
      hotLocalCache,
      loadExecutor,
      entryLifecycle.ttlPolicy(),
      properties.getRefreshMaxPools()
    );
  }

  /**
   * Create the dedicated thread-pool executor for asynchronous cache operations.
   *
   * <p>This executor handles all async operations in the HotKey data path: cache loading
   * via SingleFlight, soft-expiry refresh tasks, and cross-instance send callbacks.
   * Uses a bounded thread pool with Tomcat-style ordering (core → max → queue → reject)
   * to limit concurrent AMQP channel usage (RabbitMQ's {@code CachingConnectionFactory}
   * associates channels with platform threads, so unbounded virtual-thread concurrency
   * causes channel-open timeouts). The pool is configured with core/max pool size,
   * bounded queue capacity, and a rejection policy that throws
   * {@link RejectedExecutionException} when the total in-flight tasks exceed
   * {@code queueCapacity + maxPoolSize}.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a configured {@link StandardThreadExecutor}
   */
  @Bean(name = "hotKeyExecutor", destroyMethod = "shutdownNow")
  @ConditionalOnMissingBean(name = "hotKeyExecutor")
  public Executor hotKeyExecutor(ZetaProperties properties) {
    return newPool(properties, ZetaConstants.Threads.PREFIX_HOTKEY);
  }

  /**
   * Thread-name prefix for the interruptible load pool. Declared locally rather
   * than in {@link ZetaConstants} because it belongs to this assembly only.
   */
  private static final String PREFIX_LOAD = "zeta-load";

  /**
   * Thread-name prefix for the broadcast send pool (ADR-0037 send isolation).
   */
  private static final String PREFIX_SEND = "zeta-send";

  /**
   * Create the <b>interruptible</b> executor used exclusively by
   * {@link io.github.hyshmily.zeta.util.InterruptingAsync}.
   *
   * <p><b>Why a dedicated pool (ADR-0086).</b> {@code InterruptingAsync} owns the
   * interrupt flag of the threads it runs on: it interrupts a timed-out load in
   * place and clears the flag on task exit. Its own contract requires an executor
   * whose threads are never used for anything else. Sharing {@code hotKeyExecutor}
   * violated that contract and coupled two failure domains: when a slow data
   * source stalled, timed-out load tasks kept occupying pool threads (JDBC and
   * most Redis clients swallow {@code interrupt}), the pool saturated, and the
   * <em>write-through</em> tasks submitted by
   * {@code TransactionSupport.runAfterCommit} were rejected. The transaction
   * was already committed at that point, so the L1 update, the Redis version
   * INCR and the peer REFRESH broadcast were silently dropped with only a WARN.
   * Separate pools make a slow data source degrade reads instead of losing writes.
   *
   * <p>This pool serves {@link #singleFlight} and {@link #backgroundRefresher} —
   * the only two components that run interruptible loads.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a configured {@link StandardThreadExecutor} reserved for interruptible loads
   */
  @Bean(name = "interruptibleLoadExecutor", destroyMethod = "shutdownNow")
  @ConditionalOnMissingBean(name = "interruptibleLoadExecutor")
  public Executor interruptibleLoadExecutor(ZetaProperties properties) {
    return newPool(properties, PREFIX_LOAD);
  }

  /**
   * Create the executor used to hand buffered peer broadcasts to AMQP.
   *
   * <p><b>Why (ADR-0037).</b> {@link BroadcastBuffer} documents that production
   * wiring must supply a send executor; passing {@code null} made every deferred
   * REFRESH send run on the submitting thread — which, for a transactional
   * {@code putThrough}, is the transaction-commit thread. A slow broker then
   * stalled commit. Buffered sends are network I/O with their own failure mode
   * and must not share a pool with cache writes or loads.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a configured {@link StandardThreadExecutor} reserved for broadcast sends
   */
  @Bean(name = "zetaSendExecutor", destroyMethod = "shutdownNow")
  @ConditionalOnMissingBean(name = "zetaSendExecutor")
  public Executor zetaSendExecutor(ZetaProperties properties) {
    return newPool(properties, PREFIX_SEND);
  }

  /**
   * Build a bounded thread pool with Tomcat-style ordering
   * (core → max → queue → reject).
   *
   * <p>The bounded queue limits concurrent AMQP channel usage: RabbitMQ's
   * {@code CachingConnectionFactory} associates channels with platform threads,
   * so unbounded concurrency causes channel-open timeouts.
   *
   * @param properties   the HotKey configuration properties (never {@code null})
   * @param threadPrefix the thread-name prefix identifying the pool's domain
   * @return a configured {@link StandardThreadExecutor}
   */
  private StandardThreadExecutor newPool(ZetaProperties properties, String threadPrefix) {
    // ABORT (default) throws on saturation — upstream read paths swallow the
    // failure as a cache miss. CALLER_RUNS back-pressures the submitting thread
    // instead of dropping the async work.
    var rejectionHandler =
      properties.getExecutorRejection() == ZetaProperties.ExecutorRejection.CALLER_RUNS
        ? (RejectedExecutionHandler) new ThreadPoolExecutor.CallerRunsPolicy()
        : (RejectedExecutionHandler) (r, exe) -> {
            log.warn(
              "Zeta executor task rejected: pool={}, corePool={}, maxPool={}, queueCapacity={}",
              threadPrefix,
              properties.getExecutorCorePoolSize(),
              properties.getExecutorMaxPoolSize(),
              properties.getExecutorQueueCapacity()
            );
            throw new RejectedExecutionException("HotKey executor queue full");
          };
    var executor = new StandardThreadExecutor(
      properties.getExecutorCorePoolSize(),
      properties.getExecutorMaxPoolSize(),
      60L,
      TimeUnit.SECONDS,
      properties.getExecutorQueueCapacity(),
      new ZetaThreadFactory(threadPrefix),
      rejectionHandler
    );
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  /**
   * Create the rule engine for key matching against user-defined rules.
   *
   * <p>This variant is wired with an empty Redis provider and an optional sync
   * publisher, since no {@link StringRedisTemplate} is available in the non-Redis
   * deployment mode. Rules are kept in-memory only and do not survive restarts.
   * When Redis is available, {@link ZetaRedisAutoConfiguration#ruleMatcher}
   * takes over with Redis persistence.
   *
   * @return a new rule engine instance (in-memory only)
   */
  @Bean
  @ConditionalOnMissingBean(
    value = RuleMatcherImpl.class,
    type = "org.springframework.data.redis.core.StringRedisTemplate"
  )
  public RuleMatcherImpl ruleMatcher() {
    return new RuleMatcherImpl(Optional.empty());
  }

  /**
   * Create the rule administration service over the rule engine:
   * blacklist/whitelist CRUD, evaluation, and rule-set snapshot.
   *
   * @param ruleRepository the rule mutation seam (never {@code null})
   * @param ruleEvaluator  the rule evaluation seam (never {@code null})
   * @return a new RuleService instance
   */
  @Bean
  @ConditionalOnMissingBean
  public RuleService ruleService(RuleRepository ruleRepository, RuleEvaluator ruleEvaluator) {
    return new RuleService(ruleRepository, ruleEvaluator);
  }

  /**
   * Create the deferred send buffer for putThrough cache-sync messages
   * (value-carrying PUT and version-only REFRESH broadcasts).
   */
  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public BroadcastBuffer broadcastBuffer(
    @Qualifier("hotKeyScheduler") ScheduledExecutorService hotKeyScheduler,
    @Qualifier("zetaSendExecutor") Executor sendExecutor,
    Optional<CacheSyncPublisher> syncPublisher,
    CacheSyncProperties syncProperties
  ) {
    return new BroadcastBuffer(
      hotKeyScheduler,
      syncPublisher,
      syncProperties.getFlushDelayMs(),
      syncProperties.getMaxDeferMs(),
      sendExecutor
    );
  }

  /**
   * Create the {@link CentralDispatcher} that aggregates reporting and broadcasting.
   */
  @Bean
  @ConditionalOnMissingBean
  public CentralDispatcher centralDispatcher(
    Optional<KeyReporter> hotKeyReporter,
    Optional<CacheSyncPublisher> syncPublisher,
    BroadcastBuffer broadcastBuffer,
    HotKeyDetector hotKeyDetector
  ) {
    return new CentralDispatcher(
      hotKeyReporter.orElse(null),
      syncPublisher.orElse(null),
      broadcastBuffer,
      hotKeyDetector
    );
  }

  /**
   * Shared singleton {@link VersionController} (fallback variant, no Redis).
   *
   * <p>Only active when {@code RedisTemplate} is absent; otherwise
   * {@code ZetaRedisAutoConfiguration#versionController} takes over. Exposing the
   * controller as a bean (instead of {@code new} per consumer) unifies the
   * {@code versionFloorCache} and the degraded-version counter across
   * {@code HotKeyCache}, the worker decision handler, the actuator endpoint and
   * the Micrometer gauges.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @param snowflakeIdGenerator the Snowflake generator for degraded versions
   * @return a node-local version controller
   */
  @Bean
  @ConditionalOnMissingBean(value = VersionController.class, type = "org.springframework.data.redis.core.RedisTemplate")
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public VersionController versionController(ZetaProperties properties, SnowflakeIdGenerator snowflakeIdGenerator) {
    return new VersionControllerImpl(Optional.empty(), properties.getVersionKeyTtlMinutes(), snowflakeIdGenerator);
  }

  /**
   * Create the {@link HotKeyCache} (non-Redis variant).
   *
   * <p>Only active when {@code RedisTemplate} is absent; otherwise
   * {@link ZetaRedisAutoConfiguration#hotKeyCache} takes over. Creates a
   * {@link VersionController} with an empty Redis provider (falling back to
   * node-local counters) and default ring manager / health view when none are
   * available from the application context.
   *
   * @param hotKeyDetector            the app-side TopK detector (never {@code null})
   * @param hotLocalCache             the L1 Caffeine cache (never {@code null})
   * @param singleFlight              the deduplication layer (never {@code null})
   * @param entryLifecycle            the entry lifecycle manager (never {@code null})
   * @param backgroundRefresher       the background refresh executor (never {@code null})
   * @param ruleEvaluator             the dedicated HotKey executor (never {@code null})
   * @param centralDispatcher         the central dispatcher for send coordination
   * @param properties                the HotKey configuration properties (never {@code null})
   * @param snowflakeIdGenerator               the rule matcher instance (never {@code null})
   * @param healthViewProvider        provider for the cluster health view (creates default if absent)
   * @return a new HotKeyCache instance with node-local version tracking
   */
  @Bean
  @ConditionalOnMissingBean(type = "org.springframework.data.redis.core.RedisTemplate")
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public HotKeyCache hotKeyCache(
    @Qualifier("hotKeyDetector") HotKeyDetector hotKeyDetector,
    Cache<String, CacheEntry> hotLocalCache,
    SingleFlight singleFlight,
    EntryLifecycle entryLifecycle,
    BackgroundRefresher backgroundRefresher,
    CentralDispatcher centralDispatcher,
    ZetaProperties properties,
    RuleEvaluator ruleEvaluator,
    ObjectProvider<HealthView> healthViewProvider,
    SnowflakeIdGenerator snowflakeIdGenerator,
    ObjectProvider<VersionController> versionControllerProvider
  ) {
    return new HotKeyCache(
      new CacheDependencies(
        hotKeyDetector,
        hotLocalCache,
        singleFlight,
        entryLifecycle,
        backgroundRefresher,
        centralDispatcher,
        ruleEvaluator,
        versionControllerProvider.getIfAvailable(() ->
          new VersionControllerImpl(Optional.empty(), properties.getVersionKeyTtlMinutes(), snowflakeIdGenerator)
        ),
        healthViewOrDefault(healthViewProvider, properties)
      )
    );
  }

  /**
   * Shared default {@link HealthView} assembly for every auto-configuration that
   * consumes a cluster health view without forcing one to exist: the application's
   * own {@code HealthView} bean when present, otherwise a local default built from
   * the heartbeat timeout / degrade-after-failures settings. One definition for the
   * non-Redis cache, the Redis-enhanced cache, and the reporter beans.
   *
   * @param healthViewProvider provider for an application-supplied health view
   * @param properties         the HotKey configuration properties (default source)
   * @return the resolved {@link HealthView} (never {@code null})
   */
  static HealthView healthViewOrDefault(ObjectProvider<HealthView> healthViewProvider, ZetaProperties properties) {
    return healthViewProvider.getIfAvailable(() ->
      new HealthViewImpl(properties.getHeartbeat().getTimeoutMs(), properties.getHeartbeat().getDegradeAfterFailures())
    );
  }

  /**
   * Create the L1 Caffeine cache instance.
   *
   * <p>Time-based expiry operates at the <em>Caffeine</em> level via a custom
   * {@link Expiry} implementation, computing remaining nanoseconds from the
   * entry's {@code hardExpireAtMs} wall-clock deadline. Entries with
   * {@code hardExpireAtMs == Long.MAX_VALUE} are purely logical-expiry
   * — Caffeine never evicts them by time; they live until size eviction or
   * manual invalidation. Reads never extend the expiry duration (no read-based
   * refresh), ensuring predictable TTL behavior.
   *
   * <p>Eviction strategy: {@code max-size} limits entry count. Time-based TTL for
   * entries without an explicit hard-expire timestamp defaults to
   * {@code zeta.local.default-hard-ttl-ms}.
   *
   * <p>Stats recording is always enabled ({@code recordStats()}) so that
   * {@code Zeta#stats()} and the {@code cache.*} Micrometer metrics report
   * hit/miss/eviction counters. A custom {@code Cache<String, CacheEntry>} bean
   * replacing this one must enable {@code recordStats()} itself for those
   * counters to be populated.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @param customizerProvider ordered provider of application {@link ZetaCacheCustomizer}
   *                           beans, applied just before {@code build()} (ADR-0070)
   * @return a configured Caffeine {@link Cache} instance
   */
  @Bean
  @ConditionalOnMissingBean
  public Cache<String, CacheEntry> hotLocalCache(
    ZetaProperties properties,
    ObjectProvider<ZetaCacheCustomizer> customizerProvider
  ) {
    var cfg = properties.getCache();
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    builder.maximumSize(cfg.getMaxSize());
    // Stats recording enables hit/miss/eviction counters for Zeta#stats() and the cache.*
    // Micrometer metrics; overhead is a few LongAdder increments per cache operation.
    builder.recordStats();
    // Narrowing call: captures the String/CacheEntry-typed reference for the
    // removalListener/customizer/build calls below (same mutated instance).
    Caffeine<String, CacheEntry> typed = builder.expireAfter(
      new Expiry<>() {
        /**
         * Compute the time-to-live for a newly created cache entry.
         * Returns {@link Long#MAX_VALUE} for pure logical-expiry entries
         * (where {@code hardExpireAtMs == Long.MAX_VALUE}); otherwise
         * computes the remaining wall-clock time.
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
    // Typing note: Caffeine's narrowing setters (expireAfter) mutate
    // this same builder instance in place and only narrow the static type, so
    // the captured `typed` reference above carries every setting.
    // Application customizers run last, in order, immediately before build() —
    // they may add orthogonal listeners/executors/schedulers but cannot replace
    // the capacity or expiry knobs set above (Caffeine setters are single-use).
    customizerProvider.orderedStream().forEach(customizer -> customizer.customize(typed));
    return typed.build();
  }

  /**
   * Remaining Caffeine expiry in nanos for an entry whose logical hard expiry
   * sits at {@code hardExpireAtMs}: {@link Long#MAX_VALUE} for pure
   * logical-expiry entries (Caffeine never time-evicts them — the Expiry
   * Javadoc defines {@code Long.MAX_VALUE} as "no expiration"); otherwise the
   * remaining wall-clock time, floored at 1ms so a just-expired entry still
   * gets one scheduling quantum instead of an immediate re-expiry storm.
   */
  private static long remainingHardTtlNanos(long hardExpireAtMs) {
    if (hardExpireAtMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }

    long remainingMs = hardExpireAtMs - currentTimeMillis();
    return TimeUnit.MILLISECONDS.toNanos(Math.max(1, remainingMs));
  }
}
