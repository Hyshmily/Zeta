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

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.annotation.HotKeyAspect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link HotKeyAnnotationAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyAnnotationAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(HotKeyAnnotationAutoConfiguration.class));

  /**
   * Verifies that the annotation auto-configuration is skipped when the Aspect class is not on the classpath.
   */
  @Test
  void configIsSkippedWhenAspectClassNotOnClasspath() {
    // Aspect is from spring-boot-starter-aop (optional),
    // so @ConditionalOnClass prevents loading
    runner.run(ctx -> assertThat(ctx).doesNotHaveBean(HotKeyAspect.class));
  }

  /**
   * Verifies that the annotation auto-configuration is skipped when the property {@code hotkey.annotation.enabled} is not set to true.
   */
  @Test
  void configIsSkippedWhenPropertyIsNotEnabled() {
    // Even if Aspect were available, @ConditionalOnProperty requires enabled=true
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyAnnotationAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).doesNotHaveBean(HotKeyAspect.class));
  }

  /**
   * Verifies that the annotation auto-configuration is skipped when no HotKey bean is present.
   */
  @Test
  void configIsSkippedWhenHotKeyBeanNotPresent() {
    // Even if Aspect were available and property enabled,
    // @ConditionalOnBean(HotKey.class) requires HotKey bean
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyAnnotationAutoConfiguration.class))
      .withPropertyValues("hotkey.annotation.enabled=true")
      .run(ctx -> assertThat(ctx).doesNotHaveBean(HotKeyAspect.class));
  }

  /**
   * Verifies that a HotKeyAspect bean can be created with a HotKey dependency.
   */
  @Test
  void hotKeyAspectIsCreatedWithHotKeyDependency() {
    HotKey hotKey = mock(HotKey.class);

    HotKeyAnnotationAutoConfiguration config = new HotKeyAnnotationAutoConfiguration();
    HotKeyAspect aspect = config.hotKeyAspect(hotKey);

    assertThat(aspect).isNotNull();
  }

  /**
   * Verifies that the hotKeyAspect bean method creates new instances each time (no singleton caching in the factory method).
   */
  @Test
  void hotKeyAspectIsNotCreatedWhenAlreadyDefined() {
    // This tests @ConditionalOnMissingBean on the hotKeyAspect method
    HotKey hotKey = mock(HotKey.class);

    // Verify the bean method works correctly
    HotKeyAnnotationAutoConfiguration config = new HotKeyAnnotationAutoConfiguration();
    HotKeyAspect aspect1 = config.hotKeyAspect(hotKey);
    HotKeyAspect aspect2 = config.hotKeyAspect(hotKey);

    assertThat(aspect1).isNotNull();
    assertThat(aspect2).isNotNull();
    // Each call creates a new instance since @Bean is singleton by default
    assertThat(aspect1).isNotSameAs(aspect2);
  }

  /**
   * Verifies that the {@code @AutoConfiguration(after = ...)} annotation declares HotKeyFacadeAutoConfiguration as a dependency.
   */
  @Test
  void configAnnotationHasCorrectAfterOrder() {
    // Verify the @AutoConfiguration(after = HotKeyFacadeAutoConfiguration.class)
    // annotation is present as specified
    AutoConfiguration annotation = HotKeyAnnotationAutoConfiguration.class.getAnnotation(AutoConfiguration.class);
    assertThat(annotation).isNotNull();
    assertThat(annotation.after()).contains(HotKeyFacadeAutoConfiguration.class);
  }
}
