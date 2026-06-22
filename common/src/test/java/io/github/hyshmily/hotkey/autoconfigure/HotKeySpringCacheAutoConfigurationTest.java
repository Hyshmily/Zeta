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

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.annotation.HotKeyCacheExtensionAspect;
import io.github.hyshmily.hotkey.cache.annotationsupporter.HotKeyCacheManager;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.AbstractValueAdaptingCache;

class HotKeySpringCacheAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
      .withPropertyValues("hotkey.spring-cache.enabled=true")
      .withBean(HotKey.class, () -> mock(HotKey.class))
      .withConfiguration(AutoConfigurations.of(HotKeySpringCacheAutoConfiguration.class));

  @Test
  void configShouldCreateBothBeansWhenAllConditionsMet() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(HotKeyCacheManager.class);
      assertThat(ctx).hasSingleBean(HotKeyCacheExtensionAspect.class);
    });
  }

  @Test
  void configIsSkippedWhenPropertyDisabled() {
    new ApplicationContextRunner()
        .withPropertyValues("hotkey.spring-cache.enabled=false")
        .withBean(HotKey.class, () -> mock(HotKey.class))
        .withConfiguration(AutoConfigurations.of(HotKeySpringCacheAutoConfiguration.class))
        .run(ctx -> {
          assertThat(ctx).doesNotHaveBean(HotKeyCacheManager.class);
          assertThat(ctx).doesNotHaveBean(HotKeyCacheExtensionAspect.class);
        });
  }

  @Test
  void hotKeyCacheManager_shouldReturnCorrectType() {
    HotKey hotKey = mock(HotKey.class);
    HotKeyProperties properties = new HotKeyProperties();
    HotKeySpringCacheAutoConfiguration config = new HotKeySpringCacheAutoConfiguration();
    HotKeyCacheManager manager = config.hotKeyCacheManager(hotKey, properties);
    assertThat(manager).isNotNull();
    assertThat(manager).isInstanceOf(HotKeyCacheManager.class);
    assertThat(manager).isInstanceOf(CacheManager.class);
  }

  @Test
  void hotKeyCacheExtensionAspect_shouldReturnCorrectType() {
    HotKey hotKey = mock(HotKey.class);
    HotKeyProperties properties = new HotKeyProperties();
    HotKeySpringCacheAutoConfiguration config = new HotKeySpringCacheAutoConfiguration();
    HotKeyCacheExtensionAspect aspect = config.hotKeyCacheExtensionAspect(hotKey, properties);
    assertThat(aspect).isNotNull();
    assertThat(aspect).isInstanceOf(HotKeyCacheExtensionAspect.class);
  }

  @Test
  void configIsSkippedWhenAbstractValueAdaptingCacheNotOnClasspath() {
    new ApplicationContextRunner()
        .withPropertyValues("hotkey.spring-cache.enabled=true")
        .withBean(HotKey.class, () -> mock(HotKey.class))
        .withConfiguration(AutoConfigurations.of(HotKeySpringCacheAutoConfiguration.class))
        .withClassLoader(new FilteredClassLoader(AbstractValueAdaptingCache.class))
        .run(ctx -> {
          assertThat(ctx).doesNotHaveBean(HotKeyCacheManager.class);
          assertThat(ctx).doesNotHaveBean(HotKeyCacheExtensionAspect.class);
        });
  }

  @Test
  void configIsSkippedWhenHotKeyBeanNotAvailable() {
    new ApplicationContextRunner()
        .withPropertyValues("hotkey.spring-cache.enabled=true")
        .withConfiguration(AutoConfigurations.of(HotKeySpringCacheAutoConfiguration.class))
        .run(ctx -> {
          assertThat(ctx).doesNotHaveBean(HotKeyCacheManager.class);
          assertThat(ctx).doesNotHaveBean(HotKeyCacheExtensionAspect.class);
        });
  }
}
