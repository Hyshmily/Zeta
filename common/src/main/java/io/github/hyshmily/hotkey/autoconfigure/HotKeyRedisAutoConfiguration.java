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
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.sync.CacheSyncPublisher;
import io.github.hyshmily.hotkey.cache.*;
import io.github.hyshmily.hotkey.monitor.WorkerHealthMonitor;
import io.github.hyshmily.hotkey.sync.VersionController;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Redis-enhanced auto-configuration that overrides the default
 * {@link HotKeyCache} with one that supports version-based stale detection.
 *
 * <p>Activates when a {@link RedisTemplate} bean is available.
 * {@link StringRedisTemplate} is injected as optional — if absent,
 * version tracking falls back to a node-local counter ({@link
 * io.github.hyshmily.hotkey.util.InstanceIdGenerator#getNodeId()} + {@link
 * java.util.concurrent.atomic.AtomicLong}).
 * Runs <em>after</em>
 * {@link HotKeyAutoConfiguration} so its {@code @ConditionalOnMissingBean}
 * on {@code hotKeyCache} wins over the non-Redis variant, and after
 * {@link RedisAutoConfiguration} so the Redis infrastructure is ready.
 */
@AutoConfiguration(after = { HotKeyAutoConfiguration.class, RedisAutoConfiguration.class })
@ConditionalOnClass(name = "org.springframework.data.redis.core.RedisTemplate")
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyRedisAutoConfiguration {

  /**
   * Create the {@link RuleMatcher} with Redis persistence and broadcast support.
   */
  @Bean
  @ConditionalOnMissingBean
  public RuleMatcher ruleMatcher(
    ObjectProvider<StringRedisTemplate> redisTemplateProvider,
    ObjectProvider<CacheSyncPublisher> publisherProvider
  ) {
    return new RuleMatcher(
      Optional.ofNullable(redisTemplateProvider.getIfAvailable()),
      Optional.ofNullable(publisherProvider.getIfAvailable())
    );
  }

  /**
   * Create the Redis-enhanced {@link HotKeyCache} with version-based stale detection.
   * {@link StringRedisTemplate} is optional — if absent, version tracking falls back
   * to a node-local counter ({@code Long.MIN_VALUE + counter}).
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(RedisTemplate.class)
  public HotKeyCache hotKeyCache(
    @Qualifier("hotKeyDetector") TopK hotKeyDetector,
    Cache<String, Object> hotLocalCache,
    SingleFlight singleFlight,
    CacheExpireManager expireManager,
    Optional<CacheSyncPublisher> syncPublisher,
    Optional<HotKeyReporter> hotKeyReporter,
    @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
    ObjectProvider<StringRedisTemplate> redisTemplateProvider,
    HotKeyProperties properties,
    RuleMatcher ruleMatcher,
    WorkerHealthMonitor workerHealthMonitor
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
      new VersionController(
        Optional.ofNullable(redisTemplateProvider.getIfAvailable()),
        properties.getVersionKeyTtlMinutes()
      ),
      workerHealthMonitor
    );
  }

  /**
   * Fallback {@link HotKey} bean for the Redis-enhanced path.
   *
   * <p>Only active when a {@link HotKeyCache} bean exists but no {@link HotKey}
   * has been defined yet.  This covers the case where the primary
   * {@link HotKeyFacadeAutoConfiguration} is excluded and the {@link HotKeyCache}
   * originates from this configuration.
   */
  @Bean
  @ConditionalOnBean(HotKeyCache.class)
  @ConditionalOnMissingBean
  public HotKey hotKey(HotKeyCache hotKeyCache, @Qualifier("hotKeyDetector") TopK hotKeyDetector) {
    return new HotKey(hotKeyCache, hotKeyDetector);
  }
}
