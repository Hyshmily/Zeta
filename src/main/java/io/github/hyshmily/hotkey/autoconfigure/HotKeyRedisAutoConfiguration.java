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
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.CacheSyncPublisher;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyCache;
import io.github.hyshmily.hotkey.report.HotKeyReporter;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Redis-enhanced auto-configuration that overrides the default
 * {@link HotKeyCache} with one that supports version-based stale detection.
 *
 * <p>Activates when {@code RedisTemplate} is on the classpath and a
 * {@link StringRedisTemplate} bean is available.  Runs <em>after</em>
 * {@link HotKeyAutoConfiguration} so its {@code @ConditionalOnMissingBean}
 * on {@code hotKeyCache} wins over the non-Redis variant, and after
 * {@link RedisAutoConfiguration} so the Redis infrastructure is ready.
 */
@AutoConfiguration(after = {HotKeyAutoConfiguration.class, RedisAutoConfiguration.class})
@ConditionalOnClass(name = "org.springframework.data.redis.core.RedisTemplate")
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyRedisAutoConfiguration {

  /**
   * Create the Redis-enhanced {@link HotKeyCache} with version-based stale detection.
   * Only active when a {@link StringRedisTemplate} bean is available.
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(StringRedisTemplate.class)
  public HotKeyCache hotKeyCache(
    @Qualifier("hotKeyDetector") TopK hotKeyDetector,
    Cache<String, Object> hotLocalCache,
    SingleFlight singleFlight,
    CacheExpireManager expireManager,
    Optional<CacheSyncPublisher> syncPublisher,
    Optional<HotKeyReporter> hotKeyReporter,
    @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
    StringRedisTemplate redisTemplate,
    HotKeyProperties properties
  ) {
    return new HotKeyCache(
      hotKeyDetector,
      hotLocalCache,
      singleFlight,
      expireManager,
      hotKeyExecutor,
      syncPublisher,
      Optional.of(redisTemplate),
      properties.getVersionKeyTtlMinutes(),
      hotKeyReporter
    );
  }

}
