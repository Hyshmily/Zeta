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
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.cache.HotKeyCircuitBreaker;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.local.CacheSyncPublisher;
import io.github.hyshmily.hotkey.util.version.VersionController;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * App-side autoconfiguration for the HotKey library.
 *
 * <p>Creates the app-side {@link TopK} detector (HeavyKeeper), L1 Caffeine
 * cache, {@link SingleFlight} deduplication layer, executor, and the
 * primary {@link HotKeyCache} (without Redis version tracking when Redis
 * is absent).
 *
 * <p><b>Condition:</b> this configuration is <em>skipped</em> when the
 * Worker is active ({@code hotkey.worker.enabled=true}). It runs when
 * Worker is disabled or the property is absent.
 */
@AutoConfiguration(after = RedisAutoConfiguration.class)
@ConditionalOnProperty(prefix = "hotkey.worker", name = "enabled", havingValue = "false", matchIfMissing = true)
@EnableConfigurationProperties(HotKeyProperties.class)
@Slf4j
public class HotKeyAutoConfiguration {

  /**
   * Create the app-side TopK instance (HeavyKeeper) as a standalone bean.
   *
   * <p>The HeavyKeeper uses a Count-Min Sketch augmented with a minimum-count
   * threshold and exponential decay to track the top-K hottest keys with high
   * accuracy and low memory footprint. Configuration parameters (width, depth,
   * decay, minCount, topK capacity, expelled queue capacity) are read from
   * {@link HotKeyProperties}.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a new HeavyKeeper TopK instance
   */
  @Bean
  @ConditionalOnMissingBean
  public HeavyKeeper heavyKeeper(HotKeyProperties properties) {
    return new HeavyKeeper(
      properties.getTopK(),
      properties.getWidth(),
      properties.getDepth(),
      properties.getDecay(),
      properties.getMinCount(),
      properties.getExpelledQueueCapacity()
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
   * Create the SingleFlight deduplication layer for concurrent cache-load requests.
   *
   * <p>When multiple threads request the same key simultaneously (a cache miss), the
   * first caller triggers the load and subsequent callers wait for the same result
   * rather than duplicating the load. Configuration parameters (max in-flight entries,
   * TTL, timeout) are read from {@link HotKeyProperties}.
   *
   * @param properties     the HotKey configuration properties (never {@code null})
   * @param hotKeyExecutor the dedicated HotKey executor for async load execution (never {@code null})
   * @return a new SingleFlight instance
   */
  @Bean
  @ConditionalOnMissingBean
  public SingleFlight singleFlight(HotKeyProperties properties, @Qualifier("hotKeyExecutor") Executor hotKeyExecutor) {
    return new SingleFlight(
      properties.getInflightMaxSize(),
      properties.getInflightTtlSeconds(),
      properties.getInflightTimeoutSeconds(),
      hotKeyExecutor,
      new HotKeyCircuitBreaker(properties.getCircuitBreaker())
    );
  }

  /**
   * Create the soft/hard expiration manager that manages time-based eviction.
   *
   * <p>The soft-expire mechanism triggers an asynchronous refresh when a configurable
   * portion of the TTL has elapsed, serving stale data while fetching a fresh value
   * in the background. The hard-expire is the absolute maximum TTL enforced at the
   * Caffeine level. The refresh pool size limits concurrent background refresh tasks
   * to prevent resource exhaustion under high cache-miss rates.
   *
   * @param hotLocalCache  the L1 Caffeine cache (never {@code null})
   * @param hotKeyExecutor the dedicated HotKey executor for async refresh tasks (never {@code null})
   * @param properties     the HotKey configuration properties (never {@code null})
   * @return a new CacheExpireManager instance
   */
  @Bean
  @ConditionalOnMissingBean
  public CacheExpireManager expireManager(
    Cache<String, Object> hotLocalCache,
    @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
    HotKeyProperties properties
  ) {
    return new CacheExpireManager(hotLocalCache, hotKeyExecutor, properties, properties.getRefreshMaxPools());
  }

  /**
   * Create the dedicated thread-pool executor for asynchronous cache operations.
   *
   * <p>This executor handles all async operations in the HotKey data path: cache loading
   * via SingleFlight, soft-expiry refresh tasks, and cross-instance broadcast callbacks.
   * Uses a bounded thread pool to limit concurrent AMQP channel usage (RabbitMQ's
   * {@code CachingConnectionFactory} associates channels with platform threads, so
   * unbounded virtual-thread concurrency causes channel-open timeouts). The pool is
   * configured with core/max pool size, bounded queue capacity, and a rejection policy
   * that throws {@link RejectedExecutionException} when the queue is full. On shutdown,
   * in-progress tasks are allowed to complete with a 60-second grace period.
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a configured {@link ThreadPoolTaskExecutor}
   */
  @Bean("hotKeyExecutor")
  @ConditionalOnMissingBean(name = "hotKeyExecutor")
  public Executor hotKeyExecutor(HotKeyProperties properties) {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(properties.getExecutorCorePoolSize());
    executor.setMaxPoolSize(properties.getExecutorMaxPoolSize());
    executor.setQueueCapacity(properties.getExecutorQueueCapacity());
    executor.setThreadNamePrefix(HotKeyConstants.THREAD_PREFIX_HOTKEY);
    executor.setWaitForTasksToCompleteOnShutdown(true);
    executor.setAwaitTerminationSeconds(60);
    executor.setRejectedExecutionHandler((r, exe) -> {
      log.warn(
        "HotKey executor task rejected: corePool={}, maxPool={}, queueCapacity={}",
        properties.getExecutorCorePoolSize(),
        properties.getExecutorMaxPoolSize(),
        properties.getExecutorQueueCapacity()
      );
      throw new RejectedExecutionException("HotKey executor queue full");
    });
    executor.initialize();
    return executor;
  }

  /**
   * Create the {@link RuleMatcher} for key matching against user-defined rules.
   *
   * <p>This variant is wired with an empty Redis provider and an optional sync
   * publisher, since no {@link StringRedisTemplate} is available in the non-Redis
   * deployment mode. Rules are kept in-memory only and do not survive restarts.
   * When Redis is available, {@link HotKeyRedisAutoConfiguration#ruleMatcher}
   * takes over with Redis persistence.
   *
   * @param publisherProvider optional provider for the cache sync publisher (may be absent)
   * @return a new RuleMatcher instance (in-memory only)
   */
  @Bean
  @ConditionalOnMissingBean({ RuleMatcher.class, StringRedisTemplate.class })
  public RuleMatcher ruleMatcher(ObjectProvider<CacheSyncPublisher> publisherProvider) {
    return new RuleMatcher(Optional.empty(), Optional.ofNullable(publisherProvider.getIfAvailable()));
  }

  /**
   * Create the {@link HotKeyCache} (non-Redis variant).
   *
   * <p>Only active when {@code RedisTemplate} is absent; otherwise
   * {@link HotKeyRedisAutoConfiguration#hotKeyCache} takes over. Creates a
   * {@link VersionController} with an empty Redis provider (falling back to
   * node-local counters) and default ring manager / health view when none are
   * available from the application context.
   *
   * @param hotKeyDetector            the app-side TopK detector (never {@code null})
   * @param hotLocalCache             the L1 Caffeine cache (never {@code null})
   * @param singleFlight              the deduplication layer (never {@code null})
   * @param expireManager             the soft/hard expiration manager (never {@code null})
   * @param syncPublisher             optional cache sync publisher (may be absent)
   * @param hotKeyReporter            optional hot key reporter (may be absent; e.g. in Worker-only mode)
   * @param hotKeyExecutor            the dedicated HotKey executor (never {@code null})
   * @param properties                the HotKey configuration properties (never {@code null})
   * @param ruleMatcher               the rule matcher instance (never {@code null})
   * @param healthViewProvider        provider for the cluster health view (creates default if absent)
   * @return a new HotKeyCache instance with node-local version tracking
   */
  @Bean
  @ConditionalOnMissingBean(type = "org.springframework.data.redis.core.RedisTemplate")
  public HotKeyCache hotKeyCache(
    @Qualifier("hotKeyDetector") HotKeyDetector hotKeyDetector,
    Cache<String, Object> hotLocalCache,
    SingleFlight singleFlight,
    CacheExpireManager expireManager,
    Optional<CacheSyncPublisher> syncPublisher,
    Optional<HotKeyReporter> hotKeyReporter,
    @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
    HotKeyProperties properties,
    RuleMatcher ruleMatcher,
    ObjectProvider<ClusterHealthView> healthViewProvider
  ) {
    return new HotKeyCache(
      hotKeyDetector,
      hotLocalCache,
      singleFlight,
      expireManager,
      hotKeyExecutor,
      syncPublisher,
      hotKeyReporter,
      ruleMatcher,
      new VersionController(Optional.empty(), properties.getVersionKeyTtlMinutes()),
      properties,
      new ClusterHealthView(
        properties.getExpectedWorkerCount(),
        properties.getHeartbeat().getTimeoutMs(),
        properties.getHeartbeat().getDegradeAfterFailures()
      )
    );
  }

  /**
   * Fallback {@link HotKey} facade bean.
   *
   * <p>Only active when a {@link HotKeyCache} bean exists but no {@link HotKey}
   * has been defined yet. The primary {@link HotKey} creator is
   * {@link HotKeyFacadeAutoConfiguration} — this fallback covers the case where
   * that auto-configuration is excluded or its bean is overridden.
   *
   * @param hotKeyCache    the HotKeyCache instance (never {@code null})
   * @param appHotKeyDetector the app-side TopK detector (never {@code null})
   * @param workerTopKProvider     the Worker-side TopK algorithm (may be {@code null})
   * @return a new HotKey facade instance
   */
  @Bean
  @ConditionalOnBean(HotKeyCache.class)
  @ConditionalOnMissingBean
  public HotKey hotKey(
    HotKeyCache hotKeyCache,
    @Qualifier("hotKeyDetector") HotKeyDetector appHotKeyDetector,
    @Qualifier("workerTopK") ObjectProvider<TopK> workerTopKProvider
  ) {
    return new HotKey(hotKeyCache, appHotKeyDetector, workerTopKProvider.getIfAvailable());
  }

  /**
   * Create the L1 Caffeine cache instance.
   *
   * <p>Time-based expiry operates at the <em>Caffeine</em> level via a custom
   * {@link Expiry} implementation, computing remaining nanoseconds from
   * . Entries with
   * {@code hardExpireAtMs == Long.MAX_VALUE} are purely logical-expiry
   * — Caffeine never evicts them by time; they live until size eviction or
   * manual invalidation. Reads never extend the expiry duration (no read-based
   * refresh), ensuring predictable TTL behavior.
   *
   * <p>The maximum cache size is configured via {@code hotkey.local.local-cache-max-size}
   * (default 1000 entries). Time-based TTL for entries without an explicit hard-expire
   * timestamp defaults to {@code hotkey.local.local-cache-ttl-minutes} (default 5 minutes).
   *
   * @param properties the HotKey configuration properties (never {@code null})
   * @return a configured Caffeine {@link Cache} instance
   */
  @Bean
  @ConditionalOnMissingBean
  public Cache<String, Object> hotLocalCache(HotKeyProperties properties) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder()
      .maximumSize(properties.getLocalCacheMaxSize())
      .expireAfter(
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
          public long expireAfterCreate(@NonNull Object key, @NonNull Object value, long currentTimeNanos) {
            if (value instanceof CacheEntry entry) {
              if (entry.getHardExpireAtMs() == Long.MAX_VALUE) {
                // Pure logical expiry: Caffeine never time-evicts this entry.
                // See Expiry Javadoc: Long.MAX_VALUE signals "no expiration".
                return Long.MAX_VALUE;
              }
              long remainingMs = entry.getHardExpireAtMs() - System.currentTimeMillis();
              return TimeUnit.MILLISECONDS.toNanos(Math.max(1, remainingMs));
            }
            return TimeUnit.MINUTES.toNanos(properties.getLocalCacheTtlMinutes());
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
            @NonNull Object key,
            @NonNull Object value,
            long currentTimeNanos,
            long currentDuration
          ) {
            if (value instanceof CacheEntry entry) {
              if (entry.getHardExpireAtMs() == Long.MAX_VALUE) {
                // Preserve pure logical expiry across updates (e.g. broadcast refresh).
                return Long.MAX_VALUE;
              }
              long remainingMs = entry.getHardExpireAtMs() - System.currentTimeMillis();
              return TimeUnit.MILLISECONDS.toNanos(Math.max(1, remainingMs));
            }
            return currentDuration;
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
            @NonNull Object key,
            @NonNull Object value,
            long currentTimeNanos,
            long currentDuration
          ) {
            return currentDuration;
          }
        }
      );
    return builder.build();
  }
}
