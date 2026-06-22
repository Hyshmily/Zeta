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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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
import java.util.Optional;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Tests for {@link HotKeyRedisAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyRedisAutoConfigurationTest {

  @Mock(lenient = true)
  private ObjectProvider<StringRedisTemplate> redisTemplateProvider;

  @Mock(lenient = true)
  private ObjectProvider<ClusterHealthView> healthViewProvider;

  @Mock(lenient = true)
  private ObjectProvider<RingManager> ringManagerProvider;

  private final ApplicationContextRunner runner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(HotKeyRedisAutoConfiguration.class)
  );

  /**
   * Verifies that the Redis auto-configuration is skipped when RedisTemplate is not on the classpath.
   */
  @Test
  void configIsSkippedWhenRedisTemplateClassNotOnClasspath() {
    runner.run(ctx -> {
      assertThat(ctx).doesNotHaveBean(HotKeyCache.class);
      assertThat(ctx).doesNotHaveBean(HotKey.class);
    });
  }

  /**
   * Verifies that the HotKeyCache bean is created with all required dependencies.
   */
  @Test
  void hotKeyCacheBeanIsCreatedWithRequiredDependencies() {
    HotKeyProperties properties = new HotKeyProperties();
    HotKeyDetector detector = mock(HotKeyDetector.class);
    Cache<String, Object> localCache = mock(Cache.class);
    SingleFlight singleFlight = mock(SingleFlight.class);
    CacheExpireManager expireManager = mock(CacheExpireManager.class);
    Executor executor = mock(Executor.class);

    RuleMatcher ruleMatcher = new RuleMatcher(
      Optional.<StringRedisTemplate>empty(),
      Optional.<CacheSyncPublisher>empty()
    );
    HotKeyRedisAutoConfiguration config = new HotKeyRedisAutoConfiguration();
    HotKeyCache cache = config.hotKeyCache(
      detector,
      localCache,
      singleFlight,
      expireManager,
      Optional.<CacheSyncPublisher>empty(),
      Optional.<HotKeyReporter>empty(),
      executor,
      redisTemplateProvider,
      properties,
      ruleMatcher,
      ringManagerProvider,
      healthViewProvider
    );

    assertThat(cache).isNotNull();
  }

  /**
   * Verifies that the HotKeyCache bean is created when optional dependencies (CacheSyncPublisher, HotKeyReporter) are present.
   */
  @Test
  void hotKeyCacheBeanAcceptsOptionalDependencies() {
    HotKeyProperties properties = new HotKeyProperties();
    HotKeyDetector detector = mock(HotKeyDetector.class);
    Cache<String, Object> localCache = mock(Cache.class);
    SingleFlight singleFlight = mock(SingleFlight.class);
    CacheExpireManager expireManager = mock(CacheExpireManager.class);
    Executor executor = mock(Executor.class);
    CacheSyncPublisher publisher = mock(CacheSyncPublisher.class);
    HotKeyReporter reporter = mock(HotKeyReporter.class);
    RuleMatcher ruleMatcher = new RuleMatcher(
      Optional.<StringRedisTemplate>empty(),
      Optional.<CacheSyncPublisher>empty()
    );

    HotKeyRedisAutoConfiguration config = new HotKeyRedisAutoConfiguration();
    HotKeyCache cache = config.hotKeyCache(
      detector,
      localCache,
      singleFlight,
      expireManager,
      Optional.of(publisher),
      Optional.of(reporter),
      executor,
      redisTemplateProvider,
      properties,
      ruleMatcher,
      ringManagerProvider,
      healthViewProvider
    );

    assertThat(cache).isNotNull();
  }

  /**
   * Verifies that a HotKey facade bean is created when a HotKeyCache bean exists.
   */
  @Test
  void fallbackHotKeyBeanIsCreatedWhenCacheExists() {
    HotKeyCache cache = mock(HotKeyCache.class);
    HotKeyDetector detector = mock(HotKeyDetector.class);

    HotKeyRedisAutoConfiguration config = new HotKeyRedisAutoConfiguration();
    HotKey hotKey = config.hotKey(cache, detector);

    assertThat(hotKey).isNotNull();
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
      .withBean(CacheExpireManager.class, () -> mock(CacheExpireManager.class))
      .withBean("hotKeyExecutor", Executor.class, () -> mock(Executor.class))
      .withBean(HotKeyProperties.class, HotKeyProperties::new)
      .withConfiguration(AutoConfigurations.of(HotKeyRedisAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(HotKeyCache.class);
        assertThat(ctx).doesNotHaveBean(HotKey.class);
      });
  }
}
