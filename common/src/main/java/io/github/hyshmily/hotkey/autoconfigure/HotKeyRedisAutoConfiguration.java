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
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import io.github.hyshmily.hotkey.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.local.CacheSyncPublisher;
import io.github.hyshmily.hotkey.util.version.VersionController;
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
 * java.util.concurrent.atomic.AtomicLong}) with a {@code Long.MIN_VALUE}
 * base, ensuring degraded versions from different JVMs occupy distinct ranges.
 *
 * <p>Runs <em>after</em> {@link HotKeyAutoConfiguration} so its
 * {@code @ConditionalOnMissingBean} on {@code hotKeyCache} wins over the
 * non-Redis variant, and after {@link RedisAutoConfiguration} so the Redis
 * infrastructure is ready.
 *
 * <p>Also creates a {@link RuleMatcher} with optional Redis persistence,
 * enabling rule CRUD operations to survive app restarts.
 */
@AutoConfiguration(after = { HotKeyAutoConfiguration.class, RedisAutoConfiguration.class })
@ConditionalOnClass(name = "org.springframework.data.redis.core.RedisTemplate")
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeyRedisAutoConfiguration {

  /**
   * Create the {@link RuleMatcher} with optional Redis persistence and broadcast support.
   *
   * <p>When a {@link StringRedisTemplate} is available, rules (blacklist/whitelist)
   * are persisted in Redis under the key {@code hotkey:rules}, surviving app restarts.
   * When a {@link CacheSyncPublisher} is available, rule changes are broadcast to
   * peer instances for cluster-wide consistency.
   *
   * @param redisTemplateProvider optional provider for StringRedisTemplate (Redis persistence);
   *                              may be absent
   * @param publisherProvider     optional provider for the cache sync publisher (cross-instance
   *                              broadcast); may be absent
   * @return a new RuleMatcher instance with optional Redis-backed persistence
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
   *
   * <p>The {@link StringRedisTemplate} is optional — if absent, version tracking falls
   * back to a node-local counter ({@code Long.MIN_VALUE + nodeId + counter}), ensuring
   * graceful degradation when Redis is temporarily unavailable. The full HotKeyCache
   * pipeline includes: TopK detection, L1 Caffeine, SingleFlight dedup, soft/hard
   * expiry, cross-instance sync, reporting, rule matching, and consistent-hash routing.
   *
   * @param hotKeyDetector     the app-side TopK detector for hot-key frequency tracking
   * @param hotLocalCache      the L1 Caffeine cache (shared across all keys)
   * @param singleFlight       the deduplication layer for concurrent cache-load requests
   * @param expireManager      the soft/hard expiration manager for TTL control
   * @param syncPublisher      optional cache sync publisher for cross-instance broadcast
   * @param hotKeyReporter     optional hot key reporter for app-to-Worker data flow
   * @param hotKeyExecutor     the dedicated HotKey async executor
   * @param redisTemplateProvider optional provider for StringRedisTemplate (version tracking);
   *                              may be absent
   * @param properties         the HotKey configuration properties
   * @param ruleMatcher        the rule matcher for blacklist/whitelist evaluation
   * @return a new Redis-backed HotKeyCache instance
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(RedisTemplate.class)
  @ConditionalOnClass(HotKeyDetector.class)
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  public HotKeyCache hotKeyCache(
    @Qualifier("hotKeyDetector") HotKeyDetector hotKeyDetector,
    Cache<String, Object> hotLocalCache,
    SingleFlight singleFlight,
    CacheExpireManager expireManager,
    Optional<CacheSyncPublisher> syncPublisher,
    Optional<HotKeyReporter> hotKeyReporter,
    @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
    ObjectProvider<StringRedisTemplate> redisTemplateProvider,
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
      new VersionController(
        Optional.ofNullable(redisTemplateProvider.getIfAvailable()),
        properties.getVersionKeyTtlMinutes()
      ),
      healthViewProvider.getIfAvailable(() ->
        new ClusterHealthView(
          properties.getExpectedWorkerCount(),
          properties.getHeartbeat().getTimeoutMs(),
          properties.getHeartbeat().getDegradeAfterFailures()
        )
      )
    );
  }

  /**
   * Fallback {@link HotKey} facade bean for the Redis-enhanced path.
   *
   * <p>Only active when a {@link HotKeyCache} bean exists but no {@link HotKey}
   * has been defined yet. This covers the case where the primary
   * {@link HotKeyFacadeAutoConfiguration} is excluded and the {@link HotKeyCache}
   * originates from this configuration. The returned facade provides the full
   * public API including read/write/invalidate operations, TopK introspection,
   * and rule management.
   *
   * @param hotKeyCache    the Redis-backed HotKeyCache instance (never {@code null})
   * @param hotKeyDetector the app-side TopK detector (never {@code null})
   * @return a new HotKey facade instance
   */
  @Bean
  @ConditionalOnBean(HotKeyCache.class)
  @ConditionalOnMissingBean
  public HotKey hotKey(HotKeyCache hotKeyCache, @Qualifier("hotKeyDetector") HotKeyDetector hotKeyDetector) {
    return new HotKey(hotKeyCache, hotKeyDetector, null);
  }
}
