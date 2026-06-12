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
import io.github.hyshmily.hotkey.algorithm.HeavyKeeper;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.sync.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.VersionController;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
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
public class HotKeyAutoConfiguration {

  /** Logger for this configuration class. */
  private static final HotKeyLogger log = new DefaultLogger(HotKeyAutoConfiguration.class);

  /**
   * Create the app-side TopK detector (HeavyKeeper).
   *
   * @param properties the HotKey configuration properties
   * @return a new HeavyKeeper TopK instance
   */
  @Bean
  @ConditionalOnMissingBean
  public TopK hotKeyDetector(HotKeyProperties properties) {
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
   * Create the SingleFlight deduplication layer for concurrent cache-load requests.
   *
   * @param properties    the HotKey configuration properties
   * @param hotKeyExecutor the dedicated HotKey executor
   * @return a new SingleFlight instance
   */
  @Bean
  @ConditionalOnMissingBean
  public SingleFlight singleFlight(HotKeyProperties properties, @Qualifier("hotKeyExecutor") Executor hotKeyExecutor) {
    return new SingleFlight(
      properties.getInflightMaxSize(),
      properties.getInflightTtlSeconds(),
      properties.getInflightTimeoutSeconds(),
      hotKeyExecutor
    );
  }

  /**
   * Create the soft/hard expiration manager that uses a configurable pool of scheduled threads.
   *
   * @param hotLocalCache  the L1 Caffeine cache
   * @param hotKeyExecutor the dedicated HotKey executor
   * @param properties     the HotKey configuration properties
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
   * @param properties the HotKey configuration properties
   * @return a configured ThreadPoolTaskExecutor
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
   * Wired with an empty Redis and sync publisher since no Redis is available in this variant.
   *
   * @param publisherProvider optional provider for the cache sync publisher
   * @return a new RuleMatcher instance
   */
  @Bean
  @ConditionalOnMissingBean({ RuleMatcher.class, org.springframework.data.redis.core.StringRedisTemplate.class })
  public RuleMatcher ruleMatcher(ObjectProvider<CacheSyncPublisher> publisherProvider) {
    return new RuleMatcher(Optional.empty(), Optional.ofNullable(publisherProvider.getIfAvailable()));
  }

  /**
   * Create the {@link HotKeyCache} (non-Redis variant).  Only active when {@code RedisTemplate}
   * is absent; otherwise {@link HotKeyRedisAutoConfiguration#hotKeyCache} takes over.
   *
   * @param hotKeyDetector            the app-side TopK detector
   * @param hotLocalCache             the L1 Caffeine cache
   * @param singleFlight              the deduplication layer
   * @param expireManager             the soft/hard expiration manager
   * @param syncPublisher             optional cache sync publisher
   * @param hotKeyReporter            optional hot key reporter
   * @param hotKeyExecutor            the dedicated HotKey executor
   * @param properties                the HotKey configuration properties
   * @param ruleMatcher               the rule matcher instance
   * @param workerHealthMonitorProvider optional provider for worker health monitor
   * @return a new HotKeyCache instance
   */
  @Bean
  @ConditionalOnMissingBean(type = "org.springframework.data.redis.core.RedisTemplate")
  public HotKeyCache hotKeyCache(
    @Qualifier("hotKeyDetector") TopK hotKeyDetector,
    Cache<String, Object> hotLocalCache,
    SingleFlight singleFlight,
    CacheExpireManager expireManager,
    Optional<CacheSyncPublisher> syncPublisher,
    Optional<HotKeyReporter> hotKeyReporter,
    @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
    HotKeyProperties properties,
    RuleMatcher ruleMatcher,
    ObjectProvider<RingManager> ringManagerProvider,
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
      ringManagerProvider.getIfAvailable(() -> new RingManager(properties.getConsistentHashing().getVirtualNodes())),
      healthViewProvider.getIfAvailable(() -> new ClusterHealthView(0, 3000, 2))
    );
  }

  /**
   * Fallback {@link HotKey} bean.
   *
   * <p>Only active when a {@link HotKeyCache} bean exists but no {@link HotKey}
   * has been defined yet.  The primary {@link HotKey} creator is
   * {@link HotKeyFacadeAutoConfiguration} — this fallback covers the case where
   * that auto-configuration is excluded or its bean is overridden.
   *
   * @param hotKeyCache    the HotKeyCache instance
   * @param hotKeyDetector the app-side TopK detector
   * @return a new HotKey facade instance
   */
  @Bean
  @ConditionalOnBean(HotKeyCache.class)
  @ConditionalOnMissingBean
  public HotKey hotKey(HotKeyCache hotKeyCache, @Qualifier("hotKeyDetector") TopK hotKeyDetector) {
    return new HotKey(hotKeyCache, hotKeyDetector);
  }

  /**
   * Create the L1 Caffeine cache instance.
   *
   * <p>
   * Time-based expiry operates at the <em>Caffeine</em> level, computing
   * remaining nanoseconds from {@code CacheEntry.hardExpireAtMs}. Entries
   * with {@code hardExpireAtMs == Long.MAX_VALUE} are purely logical-expiry
   * — Caffeine never evicts them by time; they live until size eviction or
   * manual invalidation.
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
           */
          @Override
          public long expireAfterCreate(@NonNull Object key, @NonNull Object value, long currentTimeNanos) {
            if (value instanceof CacheEntry entry) {
              if (entry.getHardExpireAtMs() == Long.MAX_VALUE) {
                // Pure logical expiry: Caffeine never time-evicts this entry.
                // See Expiry JavaDoc: Long.MAX_VALUE signals "no expiration".
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
