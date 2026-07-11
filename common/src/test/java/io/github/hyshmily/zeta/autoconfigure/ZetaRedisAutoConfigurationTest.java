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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.CentralDispatcher;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Tests for {@link ZetaRedisAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class ZetaRedisAutoConfigurationTest {

  @Mock(lenient = true)
  private ObjectProvider<StringRedisTemplate> redisTemplateProvider;

  @Mock(lenient = true)
  private ObjectProvider<HealthView> healthViewProvider;

  @Mock(lenient = true)
  private ObjectProvider<CacheSyncPublisher> publisherProvider;

  private final ScheduledExecutorService testScheduler = Executors.newSingleThreadScheduledExecutor();

  private final ApplicationContextRunner runner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(ZetaRedisAutoConfiguration.class)
  );

  /**
   * Verifies that the Redis auto-configuration is skipped when RedisTemplate is not on the classpath.
   */
  @Test
  void configIsSkippedWhenRedisTemplateClassNotOnClasspath() {
    runner.run(ctx -> {
      assertThat(ctx).doesNotHaveBean(HotKeyCache.class);
      assertThat(ctx).doesNotHaveBean(Zeta.class);
    });
  }

  /**
   * Verifies that the HotKeyCache bean is created with all required dependencies.
   */
  @Test
  void hotKeyCacheBeanIsCreatedWithRequiredDependencies() {
    ZetaProperties properties = new ZetaProperties();
    HotKeyDetector detector = mock(HotKeyDetector.class);
    Cache<String, Object> localCache = mock(Cache.class);
    SingleFlight singleFlight = mock(SingleFlight.class);
    ExpireManager expireManager = mock(ExpireManager.class);
    Executor executor = mock(Executor.class);

    RuleMatcher ruleMatcher = new RuleMatcherImpl(
      Optional.<StringRedisTemplate>empty(),
      Optional.<CacheSyncPublisher>empty()
    );
    CentralDispatcher dispatcher = new CentralDispatcher(
      Optional.empty(),
      Optional.empty(),
      new BroadcastBuffer(testScheduler, Optional.empty()),
      detector
    );
    ZetaRedisAutoConfiguration config = new ZetaRedisAutoConfiguration();
    HotKeyCache cache = config.hotKeyCache(
      detector,
      localCache,
      singleFlight,
      expireManager,
      executor,
      dispatcher,
      redisTemplateProvider,
      properties,
      ruleMatcher,
      healthViewProvider,
      CacheCompressor.NONE
    );

    assertThat(cache).isNotNull();
  }

  /**
   * Verifies that the HotKeyCache bean is created when optional dependencies are present.
   */
  @Test
  void hotKeyCacheBeanAcceptsOptionalDependencies() {
    ZetaProperties properties = new ZetaProperties();
    HotKeyDetector detector = mock(HotKeyDetector.class);
    Cache<String, Object> localCache = mock(Cache.class);
    SingleFlight singleFlight = mock(SingleFlight.class);
    ExpireManager expireManager = mock(ExpireManager.class);
    Executor executor = mock(Executor.class);
    CacheSyncPublisher publisher = mock(CacheSyncPublisher.class);
    KeyReporter reporter = mock(KeyReporter.class);
    RuleMatcher ruleMatcher = new RuleMatcherImpl(
      Optional.<StringRedisTemplate>empty(),
      Optional.<CacheSyncPublisher>empty()
    );

    CentralDispatcher dispatcher = new CentralDispatcher(
      Optional.of(reporter),
      Optional.of(publisher),
      new BroadcastBuffer(testScheduler, Optional.empty()),
      detector
    );
    ZetaRedisAutoConfiguration config = new ZetaRedisAutoConfiguration();
    HotKeyCache cache = config.hotKeyCache(
      detector,
      localCache,
      singleFlight,
      expireManager,
      executor,
      dispatcher,
      redisTemplateProvider,
      properties,
      ruleMatcher,
      healthViewProvider,
      CacheCompressor.NONE
    );

    assertThat(cache).isNotNull();
  }

  /**
   * Verifies that the ruleMatcher bean is created with StringRedisTemplate and publisher providers.
   */
  @Test
  void ruleMatcher_shouldCreateWithOptionalDependencies() {
    ZetaRedisAutoConfiguration config = new ZetaRedisAutoConfiguration();
    when(redisTemplateProvider.getIfAvailable()).thenReturn(mock(StringRedisTemplate.class));
    when(publisherProvider.getIfAvailable()).thenReturn(mock(CacheSyncPublisher.class));
    RuleMatcher matcher = config.ruleMatcher(redisTemplateProvider, publisherProvider);
    assertThat(matcher).isNotNull();
    assertThat(matcher).isInstanceOf(RuleMatcherImpl.class);
  }

  /**
   * Verifies that the ruleMatcher bean handles absent providers gracefully.
   */
  @Test
  void ruleMatcher_shouldHandleAbsentProviders() {
    ZetaRedisAutoConfiguration config = new ZetaRedisAutoConfiguration();
    RuleMatcher matcher = config.ruleMatcher(redisTemplateProvider, publisherProvider);
    assertThat(matcher).isNotNull();
  }

  /**
   * Verifies that the Redis auto-configuration is skipped when RedisTemplate class is not available at runtime.
   */
  @Test
  void configIsSkippedWhenRedisTemplateBeanNotAvailable() {
    // RedisTemplate class is not on test classpath, so @ConditionalOnClass prevents loading
    // and @ConditionalOnBean(RedisTemplate.class) would not match
    new ApplicationContextRunner()
      .withBean(HotKeyDetector.class, () -> mock(HotKeyDetector.class))
      .withBean(Cache.class, () -> mock(Cache.class))
      .withBean(SingleFlight.class, () -> mock(SingleFlight.class))
      .withBean(ExpireManager.class, () -> mock(ExpireManager.class))
      .withBean("hotKeyExecutor", Executor.class, () -> mock(Executor.class))
      .withBean(ZetaProperties.class, ZetaProperties::new)
      .withConfiguration(AutoConfigurations.of(ZetaRedisAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(HotKeyCache.class);
        assertThat(ctx).doesNotHaveBean(Zeta.class);
      });
  }
}
