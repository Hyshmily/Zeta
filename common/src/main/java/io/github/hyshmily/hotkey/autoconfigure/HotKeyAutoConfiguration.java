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
import io.github.hyshmily.hotkey.broadcast.CacheSyncPublisher;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyCache;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * App-side auto-configuration for the HotKey library.
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
@Slf4j
@AutoConfiguration
@ConditionalOnProperty(prefix = "hotkey.worker", name = "enabled", havingValue = "false", matchIfMissing = true)
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyAutoConfiguration {

  /**
   * Create the app-side TopK detector (HeavyKeeper).
   */
  @Bean
  @ConditionalOnMissingBean
  public TopK hotKeyDetector(HotKeyProperties properties) {
    return new HeavyKeeper(
      properties.getTopK(),
      properties.getWidth(),
      properties.getDepth(),
      properties.getDecay(),
      properties.getMinCount()
    );
  }

  /**
   * Create the L1 Caffeine cache with expiry driven by {@code getHardExpireAtMs()}.
   */
  @Bean
  @ConditionalOnMissingBean
  public Cache<String, Object> hotLocalCache(HotKeyProperties properties) {
    long defaultTtlNanos = TimeUnit.MINUTES.toNanos(properties.getLocalCacheTtlMinutes());
    Caffeine<Object, Object> builder = Caffeine.newBuilder()
      .maximumSize(properties.getLocalCacheMaxSize())
      .expireAfter(
        new Expiry<>() {
          @Override
          public long expireAfterCreate(@NonNull Object key, @NonNull Object value, long currentTimeNanos) {
            if (value instanceof CacheEntry entry) {
              if (entry.getHardExpireAtMs() == Long.MAX_VALUE) {
                return defaultTtlNanos;
              }
              long remaining = TimeUnit.MILLISECONDS.toNanos(entry.getHardExpireAtMs() - System.currentTimeMillis());
              return Math.max(1, remaining);
            }
            return defaultTtlNanos;
          }

          @Override
          public long expireAfterUpdate(
            @NonNull Object key,
            @NonNull Object value,
            long currentTimeNanos,
            long currentDuration
          ) {
            return expireAfterCreate(key, value, currentTimeNanos);
          }

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

  /**
   * Create the SingleFlight deduplication layer for concurrent cache-load requests.
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
   */
  @Bean("hotKeyExecutor")
  @ConditionalOnMissingBean(name = "hotKeyExecutor")
  public Executor hotKeyExecutor(HotKeyProperties properties) {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(properties.getExecutorCorePoolSize());
    executor.setMaxPoolSize(properties.getExecutorMaxPoolSize());
    executor.setQueueCapacity(properties.getExecutorQueueCapacity());
    executor.setThreadNamePrefix(HotKeyConstants.THREAD_PREFIX_HOTKEY);
    executor.setRejectedExecutionHandler((_, _) -> {
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
   * Create the {@link HotKeyCache} (non-Redis variant).  Only active when {@code RedisTemplate}
   * is absent; otherwise {@link HotKeyRedisAutoConfiguration#hotKeyCache} takes over.
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
    HotKeyProperties properties
  ) {
    return new HotKeyCache(
      hotKeyDetector,
      hotLocalCache,
      singleFlight,
      expireManager,
      hotKeyExecutor,
      syncPublisher,
      Optional.empty(),
      properties.getVersionKeyTtlMinutes(),
      hotKeyReporter
    );
  }

  /**
   * Fallback {@link HotKey} bean.
   *
   * <p>Only active when a {@link HotKeyCache} bean exists but no {@link HotKey}
   * has been defined yet.  The primary {@link HotKey} creator is
   * {@link HotKeyFacadeAutoConfiguration} — this fallback covers the case where
   * that auto-configuration is excluded or its bean is overridden.
   */
  @Bean
  @ConditionalOnBean(HotKeyCache.class)
  @ConditionalOnMissingBean
  public HotKey hotKey(HotKeyCache hotKeyCache, @Qualifier("hotKeyDetector") TopK hotKeyDetector) {
    return new HotKey(hotKeyCache, hotKeyDetector);
  }
}
