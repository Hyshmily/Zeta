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
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.cache.cachesupport.ExpireManager;
import io.github.hyshmily.hotkey.cache.cachesupport.SingleFlight;
import io.github.hyshmily.hotkey.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.hotkey.reporting.KeyReporter;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.hotkey.sharding.HealthView;
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
  private ObjectProvider<HealthView> healthViewProvider;

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
    ExpireManager expireManager = mock(ExpireManager.class);
    Executor executor = mock(Executor.class);

    RuleMatcher ruleMatcher = new RuleMatcherImpl(
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
      Optional.<KeyReporter>empty(),
      executor,
      redisTemplateProvider,
      properties,
      ruleMatcher,
      healthViewProvider
    );

    assertThat(cache).isNotNull();
  }

  /**
   * Verifies that the HotKeyCache bean is created when optional dependencies (CacheSyncPublisher, KeyReporter) are present.
   */
  @Test
  void hotKeyCacheBeanAcceptsOptionalDependencies() {
    HotKeyProperties properties = new HotKeyProperties();
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
      healthViewProvider
    );

    assertThat(cache).isNotNull();
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
      .withBean(HotKeyProperties.class, HotKeyProperties::new)
      .withConfiguration(AutoConfigurations.of(HotKeyRedisAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(HotKeyCache.class);
        assertThat(ctx).doesNotHaveBean(HotKey.class);
      });
  }
}
