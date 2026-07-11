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
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.util.InstanceIdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link ZetaFacadeAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class ZetaFacadeAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withBean(ZetaProperties.class, ZetaProperties::new)
    .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class));

  @AfterEach
  void tearDown() {
    InstanceIdGenerator.setOverride(null);
  }

  /**
   * Verifies that the HotKey facade bean is created with only minimal dependencies (ZetaProperties).
   */
  @Test
  void hotKeyBeanIsCreatedWithMinimalDependencies() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(Zeta.class);
      assertThat(ctx.getBean(Zeta.class).returnLocalHotKeys()).isEmpty();
    });
  }

  /**
   * Verifies that the HotKey facade bean is created when all optional dependencies are provided.
   */
  @Test
  void hotKeyBeanIsCreatedWithAllDependencies() {
    new ApplicationContextRunner()
      .withBean(ZetaProperties.class, ZetaProperties::new)
      .withBean("hotKeyDetector", TopK.class, () -> mock(TopK.class))
      .withBean("workerTopK", TopK.class, () -> mock(TopK.class))
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Zeta.class));
  }

  /**
   * Verifies that a custom HotKey bean is respected and the auto-configured one is not created.
   */
  @Test
  void hotKeyBeanIsNotCreatedWhenAlreadyDefined() {
    new ApplicationContextRunner()
      .withBean(ZetaProperties.class, ZetaProperties::new)
      .withBean(Zeta.class, () -> mock(Zeta.class))
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Zeta.class));
  }

  /**
   * Verifies that the instance ID configured via properties is propagated to the InstanceIdGenerator.
   */
  @Test
  void instanceIdIsSetFromPropertiesWhenConfigured() {
    String instanceId = "test-instance-456";
    InstanceIdGenerator.setOverride(null);
    new ApplicationContextRunner()
      .withBean(ZetaProperties.class, () -> {
        ZetaProperties p = new ZetaProperties();
        p.setInstanceId(instanceId);
        return p;
      })
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(InstanceIdGenerator.get()).isEqualTo(instanceId));
  }

  /**
   * Verifies that a blank instance ID property does not override the InstanceIdGenerator.
   */
  @Test
  void instanceIdIsNotSetWhenPropertyIsBlank() {
    InstanceIdGenerator.setOverride(null);
    runner.run(ctx -> assertThat(InstanceIdGenerator.get()).isNotEqualTo("my-instance"));
  }

  /**
   * Verifies that a null instance ID property does not override the InstanceIdGenerator.
   */
  @Test
  void instanceIdIsNotSetWhenPropertyIsNull() {
    InstanceIdGenerator.setOverride(null);
    new ApplicationContextRunner()
      .withBean(ZetaProperties.class, () -> {
        ZetaProperties p = new ZetaProperties();
        p.setInstanceId(null);
        return p;
      })
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(InstanceIdGenerator.get()).isNotNull());
  }

  /**
   * Verifies that the HotKey bean is created when only the app-level TopK (hotKeyDetector) is present.
   */
  @Test
  void hotKeyBeanAcceptsOnlyAppTopK() {
    new ApplicationContextRunner()
      .withBean(ZetaProperties.class, ZetaProperties::new)
      .withBean("hotKeyDetector", TopK.class, () -> mock(TopK.class))
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Zeta.class));
  }

  /**
   * Verifies that the HotKey bean is created when only the worker-level TopK (workerTopK) is present.
   */
  @Test
  void hotKeyBeanAcceptsOnlyWorkerTopK() {
    new ApplicationContextRunner()
      .withBean(ZetaProperties.class, ZetaProperties::new)
      .withBean("workerTopK", TopK.class, () -> mock(TopK.class))
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Zeta.class));
  }

  /**
   * Verifies that the HotKey bean is created when only the HotKeyCache is present (no TopK beans).
   */
  @Test
  void hotKeyBeanAcceptsOnlyHotKeyCache() {
    new ApplicationContextRunner()
      .withBean(ZetaProperties.class, ZetaProperties::new)
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Zeta.class));
  }

  /**
   * Verifies that the scheduler pool size from properties is respected.
   */
  @Test
  void schedulerPoolSize_shouldRespectProperty() {
    new ApplicationContextRunner()
      .withBean(ZetaProperties.class, ZetaProperties::new)
      .withPropertyValues("zeta.local.schedulerPoolSize=2")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasBean("hotKeyScheduler");
        var scheduler = ctx.getBean("hotKeyScheduler", java.util.concurrent.ScheduledExecutorService.class);
        assertThat(scheduler).isNotNull();
      });
  }
}
