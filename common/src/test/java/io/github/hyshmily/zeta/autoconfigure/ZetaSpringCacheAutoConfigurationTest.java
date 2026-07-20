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

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.CacheExtensionAspect;
import io.github.hyshmily.zeta.annotation.annotationsupporter.ZetaCacheManager;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.AbstractValueAdaptingCache;

class ZetaSpringCacheAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withPropertyValues("zeta.spring-cache.enabled=true")
    .withBean(Zeta.class, () -> mock(Zeta.class))
    .withConfiguration(AutoConfigurations.of(ZetaSpringCacheAutoConfiguration.class));

  @Test
  void configShouldCreateBothBeansWhenAllConditionsMet() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(ZetaCacheManager.class);
      assertThat(ctx).hasSingleBean(CacheExtensionAspect.class);
    });
  }

  @Test
  void configIsSkippedWhenPropertyDisabled() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.spring-cache.enabled=false")
      .withBean(Zeta.class, () -> mock(Zeta.class))
      .withConfiguration(AutoConfigurations.of(ZetaSpringCacheAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(ZetaCacheManager.class);
        assertThat(ctx).doesNotHaveBean(CacheExtensionAspect.class);
      });
  }

  @Test
  void hotKeyCacheManager_shouldReturnCorrectType() {
    Zeta zeta = mock(Zeta.class);
    ZetaProperties properties = new ZetaProperties();
    ZetaSpringCacheAutoConfiguration config = new ZetaSpringCacheAutoConfiguration();
    ZetaCacheManager manager = config.hotKeyCacheManager(zeta, properties);
    assertThat(manager).isNotNull();
    assertThat(manager).isInstanceOf(ZetaCacheManager.class);
    assertThat(manager).isInstanceOf(CacheManager.class);
  }

  @Test
  void hotKeyCacheExtensionAspect_shouldReturnCorrectType() {
    Zeta zeta = mock(Zeta.class);
    ZetaProperties properties = new ZetaProperties();
    ZetaSpringCacheAutoConfiguration config = new ZetaSpringCacheAutoConfiguration();
    CacheExtensionAspect aspect = config.hotKeyCacheExtensionAspect(zeta, properties);
    assertThat(aspect).isNotNull();
    assertThat(aspect).isInstanceOf(CacheExtensionAspect.class);
  }

  @Test
  void configIsSkippedWhenAbstractValueAdaptingCacheNotOnClasspath() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.spring-cache.enabled=true")
      .withBean(Zeta.class, () -> mock(Zeta.class))
      .withConfiguration(AutoConfigurations.of(ZetaSpringCacheAutoConfiguration.class))
      .withClassLoader(new FilteredClassLoader(AbstractValueAdaptingCache.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(ZetaCacheManager.class);
        assertThat(ctx).doesNotHaveBean(CacheExtensionAspect.class);
      });
  }

  @Test
  void configIsSkippedWhenHotKeyBeanNotAvailable() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.spring-cache.enabled=true")
      .withConfiguration(AutoConfigurations.of(ZetaSpringCacheAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(ZetaCacheManager.class);
        assertThat(ctx).doesNotHaveBean(CacheExtensionAspect.class);
      });
  }
}
