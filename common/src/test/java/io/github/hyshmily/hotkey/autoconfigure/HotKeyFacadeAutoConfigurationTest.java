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
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link HotKeyFacadeAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyFacadeAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withBean(HotKeyProperties.class, HotKeyProperties::new)
    .withConfiguration(AutoConfigurations.of(HotKeyFacadeAutoConfiguration.class));

  @AfterEach
  void tearDown() {
    InstanceIdGenerator.setOverride(null);
  }

  @Test
  void hotKeyBeanIsCreatedWithMinimalDependencies() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(HotKey.class);
      assertThat(ctx.getBean(HotKey.class).returnLocalHotKeys()).isEmpty();
    });
  }

  @Test
  void hotKeyBeanIsCreatedWithAllDependencies() {
    new ApplicationContextRunner()
      .withBean(HotKeyProperties.class, HotKeyProperties::new)
      .withBean("hotKeyDetector", TopK.class, () -> mock(TopK.class))
      .withBean("workerTopK", TopK.class, () -> mock(TopK.class))
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .withConfiguration(AutoConfigurations.of(HotKeyFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(HotKey.class));
  }

  @Test
  void hotKeyBeanIsNotCreatedWhenAlreadyDefined() {
    new ApplicationContextRunner()
      .withBean(HotKeyProperties.class, HotKeyProperties::new)
      .withBean(HotKey.class, () -> mock(HotKey.class))
      .withConfiguration(AutoConfigurations.of(HotKeyFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(HotKey.class));
  }

  @Test
  void instanceIdIsSetFromPropertiesWhenConfigured() {
    String instanceId = "test-instance-456";
    InstanceIdGenerator.setOverride(null);
    new ApplicationContextRunner()
      .withBean(HotKeyProperties.class, () -> {
        HotKeyProperties p = new HotKeyProperties();
        p.setInstanceId(instanceId);
        return p;
      })
      .withConfiguration(AutoConfigurations.of(HotKeyFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(InstanceIdGenerator.get()).isEqualTo(instanceId));
  }

  @Test
  void instanceIdIsNotSetWhenPropertyIsBlank() {
    InstanceIdGenerator.setOverride(null);
    runner.run(ctx -> assertThat(InstanceIdGenerator.get()).isNotEqualTo("my-instance"));
  }

  @Test
  void instanceIdIsNotSetWhenPropertyIsNull() {
    InstanceIdGenerator.setOverride(null);
    new ApplicationContextRunner()
      .withBean(HotKeyProperties.class, () -> {
        HotKeyProperties p = new HotKeyProperties();
        p.setInstanceId(null);
        return p;
      })
      .withConfiguration(AutoConfigurations.of(HotKeyFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(InstanceIdGenerator.get()).isNotNull());
  }

  @Test
  void hotKeyBeanAcceptsOnlyAppTopK() {
    new ApplicationContextRunner()
      .withBean(HotKeyProperties.class, HotKeyProperties::new)
      .withBean("hotKeyDetector", TopK.class, () -> mock(TopK.class))
      .withConfiguration(AutoConfigurations.of(HotKeyFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(HotKey.class));
  }

  @Test
  void hotKeyBeanAcceptsOnlyWorkerTopK() {
    new ApplicationContextRunner()
      .withBean(HotKeyProperties.class, HotKeyProperties::new)
      .withBean("workerTopK", TopK.class, () -> mock(TopK.class))
      .withConfiguration(AutoConfigurations.of(HotKeyFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(HotKey.class));
  }

  @Test
  void hotKeyBeanAcceptsOnlyHotKeyCache() {
    new ApplicationContextRunner()
      .withBean(HotKeyProperties.class, HotKeyProperties::new)
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .withConfiguration(AutoConfigurations.of(HotKeyFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(HotKey.class));
  }
}
