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
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.cache.SingleFlight;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;

/**
 * Tests for {@link HotKeyAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withPropertyValues("hotkey.local.topK=200")
    .withConfiguration(AutoConfigurations.of(HotKeyAutoConfiguration.class));

  @Test
  void allBeansAreCreatedByDefault() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(TopK.class);
      assertThat(ctx.getBean("hotKeyDetector")).isInstanceOf(TopK.class);
      assertThat(ctx).hasSingleBean(Cache.class);
      assertThat(ctx).hasSingleBean(SingleFlight.class);
      assertThat(ctx).hasSingleBean(CacheExpireManager.class);
      assertThat(ctx).hasSingleBean(Executor.class);
      assertThat(ctx.getBean("hotKeyExecutor")).isInstanceOf(Executor.class);
      assertThat(ctx).hasSingleBean(HotKeyCache.class);
      assertThat(ctx).hasSingleBean(HotKey.class);
    });
  }

  @Test
  void configIsSkippedWhenWorkerEnabled() {
    new ApplicationContextRunner()
      .withPropertyValues("hotkey.worker.enabled=true")
      .withConfiguration(AutoConfigurations.of(HotKeyAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(TopK.class);
        assertThat(ctx).doesNotHaveBean(HotKeyCache.class);
        assertThat(ctx).doesNotHaveBean(HotKey.class);
      });
  }

  @Test
  void hotKeyDetectorCanBeOverridden() {
    new ApplicationContextRunner()
      .withBean("hotKeyDetector", TopK.class, () -> mock(TopK.class))
      .withPropertyValues("hotkey.local.topK=200")
      .withConfiguration(AutoConfigurations.of(HotKeyAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasSingleBean(TopK.class);
        assertThat(ctx.getBean("hotKeyDetector")).isSameAs(ctx.getBean(TopK.class));
      });
  }

  @Test
  void hotLocalCacheCanBeOverridden() {
    new ApplicationContextRunner()
      .withAllowBeanDefinitionOverriding(true)
      .withBean("hotLocalCache", Cache.class, () -> mock(Cache.class))
      .withPropertyValues("hotkey.local.topK=200")
      .withConfiguration(AutoConfigurations.of(HotKeyAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Cache.class));
  }

  @Test
  void singleFlightCanBeOverridden() {
    new ApplicationContextRunner()
      .withBean(SingleFlight.class, () -> mock(SingleFlight.class))
      .withPropertyValues("hotkey.local.topK=200")
      .withConfiguration(AutoConfigurations.of(HotKeyAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(SingleFlight.class));
  }

  @Test
  void expireManagerCanBeOverridden() {
    new ApplicationContextRunner()
      .withBean(CacheExpireManager.class, () -> mock(CacheExpireManager.class))
      .withPropertyValues("hotkey.local.topK=200")
      .withConfiguration(AutoConfigurations.of(HotKeyAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(CacheExpireManager.class));
  }

  @Test
  void hotKeyExecutorCanBeOverridden() {
    new ApplicationContextRunner()
      .withBean("hotKeyExecutor", Executor.class, () -> mock(Executor.class))
      .withPropertyValues("hotkey.local.topK=200")
      .withConfiguration(AutoConfigurations.of(HotKeyAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasSingleBean(Executor.class);
        assertThat(ctx.getBean("hotKeyExecutor")).isSameAs(ctx.getBean(Executor.class));
      });
  }

  @Test
  void hotKeyCacheRespectsRedisTemplateCondition() {
    runner.run(ctx -> {
      // RedisTemplate is not on the test classpath, so non-Redis HotKeyCache is created
      assertThat(ctx).hasSingleBean(HotKeyCache.class);
    });
  }

  @Test
  void hotKeyFallbackIsCreatedWhenHotKeyCacheExists() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(HotKeyCache.class);
      assertThat(ctx).hasSingleBean(HotKey.class);
    });
  }

  @Test
  void hotKeyFallbackRespectsConditionalOnMissingBean() {
    new ApplicationContextRunner()
      .withBean(HotKey.class, () -> mock(HotKey.class))
      .withPropertyValues("hotkey.local.topK=200")
      .withConfiguration(AutoConfigurations.of(HotKeyAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(HotKey.class));
  }
}
