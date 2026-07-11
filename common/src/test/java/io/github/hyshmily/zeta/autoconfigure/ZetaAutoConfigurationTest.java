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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link ZetaAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class ZetaAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withPropertyValues("zeta.local.topK=200")
    .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class));

  /**
   * Verifies that all default beans (TopK, Cache, SingleFlight, ExpireManagerImpl, Executor, HotKeyCache, HotKey) are created.
   */
  @Test
  void allBeansAreCreatedByDefault() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(HotKeyDetector.class);
      assertThat(ctx.getBean("hotKeyDetector")).isInstanceOf(HotKeyDetector.class);
      assertThat(ctx).hasSingleBean(Cache.class);
      assertThat(ctx).hasSingleBean(SingleFlight.class);
      assertThat(ctx).hasSingleBean(ExpireManager.class);
      assertThat(ctx).hasBean("hotKeyExecutor");
      assertThat(ctx.getBean("hotKeyExecutor")).isInstanceOf(Executor.class);
      assertThat(ctx).hasSingleBean(HotKeyCache.class);
      assertThat(ctx).hasSingleBean(Zeta.class);
    });
  }

  /**
   * Verifies that the auto-configuration is skipped when the worker-only mode is enabled.
   */
  @Test
  void configIsSkippedWhenWorkerEnabled() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.worker.enabled=true")
      .withConfiguration(AutoConfigurations.of(ZetaAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(HotKeyDetector.class);
        assertThat(ctx).doesNotHaveBean(HotKeyCache.class);
        assertThat(ctx).doesNotHaveBean(Zeta.class);
      });
  }

  /**
   * Verifies that a custom hotKeyDetector bean overrides the default one.
   */
  @Test
  void hotKeyDetectorCanBeOverridden() {
    new ApplicationContextRunner()
      .withBean("hotKeyDetector", HotKeyDetector.class, () -> mock(HotKeyDetector.class))
      .withPropertyValues("zeta.local.topK=200")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasSingleBean(HotKeyDetector.class);
        assertThat(ctx.getBean("hotKeyDetector")).isSameAs(ctx.getBean(HotKeyDetector.class));
      });
  }

  /**
   * Verifies that a custom local cache bean overrides the default one.
   */
  @Test
  void hotLocalCacheCanBeOverridden() {
    new ApplicationContextRunner()
      .withAllowBeanDefinitionOverriding(true)
      .withBean("hotLocalCache", Cache.class, () -> mock(Cache.class))
      .withPropertyValues("zeta.local.topK=200")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Cache.class));
  }

  /**
   * Verifies that a custom SingleFlight bean overrides the default one.
   */
  @Test
  void singleFlightCanBeOverridden() {
    new ApplicationContextRunner()
      .withBean(SingleFlight.class, () -> mock(SingleFlight.class))
      .withPropertyValues("zeta.local.topK=200")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(SingleFlight.class));
  }

  /**
   * Verifies that a custom ExpireManagerImpl bean overrides the default one.
   */
  @Test
  void expireManagerCanBeOverridden() {
    new ApplicationContextRunner()
      .withBean(ExpireManager.class, () -> mock(ExpireManager.class))
      .withPropertyValues("zeta.local.topK=200")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(ExpireManager.class));
  }

  /**
   * Verifies that a custom Executor bean (named hotKeyExecutor) overrides the default one.
   */
  @Test
  void hotKeyExecutorCanBeOverridden() {
    new ApplicationContextRunner()
      .withBean("hotKeyExecutor", Executor.class, () -> mock(Executor.class))
      .withPropertyValues("zeta.local.topK=200")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasBean("hotKeyExecutor");
        assertThat(ctx.getBean("hotKeyExecutor")).isInstanceOf(Executor.class);
      });
  }

  /**
   * Verifies that a non-Redis HotKeyCache is created when RedisTemplate is not on the test classpath.
   */
  @Test
  void hotKeyCacheRespectsRedisTemplateCondition() {
    runner.run(ctx -> {
      // RedisTemplate is not on the test classpath, so non-Redis HotKeyCache is created
      assertThat(ctx).hasSingleBean(HotKeyCache.class);
    });
  }

  /**
   * Verifies that the HotKey facade bean is created when a HotKeyCache exists.
   */
  @Test
  void hotKeyFallbackIsCreatedWhenHotKeyCacheExists() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(HotKeyCache.class);
      assertThat(ctx).hasSingleBean(Zeta.class);
    });
  }

  /**
   * Verifies that a custom HotKey bean prevents the fallback from being created.
   */
  @Test
  void hotKeyFallbackRespectsConditionalOnMissingBean() {
    new ApplicationContextRunner()
      .withBean(Zeta.class, () -> mock(Zeta.class))
      .withPropertyValues("zeta.local.topK=200")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Zeta.class));
  }

  /**
   * Verifies that the hotLocalCache respects the cache.max-size property.
   */
  @Test
  void hotLocalCache_shouldRespectMaxSizeProperty() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.local.topK=200", "zeta.local.cache.max-size=500")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasSingleBean(Cache.class);
        Cache<String, Object> cache = ctx.getBean(Cache.class);
        // Put more than 500 entries, then cleanUp to force eviction
        for (int i = 0; i < 600; i++) {
          cache.put("k" + i, "v" + i);
        }
        cache.cleanUp();
        assertThat(cache.estimatedSize()).isLessThanOrEqualTo(500);
      });
  }

  /**
   * Verifies that the ruleMatcher bean is created with optional sync publisher.
   */
  @Test
  void ruleMatcher_shouldHandleMissingPublisher() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(RuleMatcher.class);
    });
  }

  /**
   * Verifies that the cache uses weight-based eviction when max-weight is configured.
   */
  @Test
  void hotLocalCache_shouldUseWeightBasedEvictionWhenMaxWeightSet() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.local.topK=200", "zeta.local.cache.max-weight=1000000", "zeta.local.cache.max-size=10")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasSingleBean(Cache.class);
        Cache<String, Object> cache = ctx.getBean(Cache.class);
        // Put a string value; weight should be computed by DefaultWeigher
        cache.put("test", "value");
        cache.cleanUp();
        assertThat(cache.estimatedSize()).isOne();
      });
  }

  /**
   * Verifies that a CacheEntry with Long.MAX_VALUE hardExpireAtMs stays in cache
   * (pure logical expiry — Caffeine never evicts by time).
   */
  @Test
  void localCache_shouldKeepEntryWithMaxValueHardExpire() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.local.topK=200")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> {
        Cache<String, Object> cache = ctx.getBean(Cache.class);
        CacheEntry entry = CacheEntry.builder()
          .value("logical-expiry-value")
          .hardExpireAtMs(Long.MAX_VALUE)
          .dataVersion(1)
          .build();
        cache.put("logical", entry);
        cache.cleanUp();
        assertThat(cache.getIfPresent("logical")).isNotNull();
      });
  }

  /**
   * Verifies that a non-CacheEntry value is accepted (falls through to default TTL).
   */
  @Test
  void localCache_shouldAcceptNonCacheEntryValue() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.local.topK=200")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class, ZetaAutoConfiguration.class))
      .run(ctx -> {
        Cache<String, Object> cache = ctx.getBean(Cache.class);
        cache.put("plain", "plain-string-value");
        cache.cleanUp();
        assertThat(cache.getIfPresent("plain")).isEqualTo("plain-string-value");
      });
  }
}
