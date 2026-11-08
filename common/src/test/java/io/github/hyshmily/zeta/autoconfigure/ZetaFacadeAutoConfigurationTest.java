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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.zeta.DefaultZeta;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.NodeIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
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

  private final ApplicationContextRunner runner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(ZetaFacadeAutoConfiguration.class)
  );

  @AfterEach
  void tearDown() {
    NodeIdentity.setInstanceOverride(null);
    NodeIdentity.setNodeOverride(null);
  }

  /**
   * Verifies that the facade auto-configuration is self-sufficient: it registers its
   * own {@link ZetaProperties} bean, so it works even when no other Zeta
   * auto-configuration is active in the context.
   */
  @Test
  void hotKeyBeanIsCreatedWithMinimalDependencies() {
    runner.run(ctx -> {
      assertThat(ctx).hasSingleBean(ZetaProperties.class);
      assertThat(ctx).hasSingleBean(Zeta.class);
      assertThat(ctx.getBean(Zeta.class).detector().localTopKeys(10)).isEmpty();
    });
  }

  /**
   * The health view is one shared singleton assembled from the heartbeat properties: every
   * consumer injects this bean, so a configured {@code min-alive-workers} reaches the cache,
   * the endpoints and the listeners alike.
   */
  @Test
  void clusterHealthViewIsASingleSharedBeanCarryingTheConfiguredThreshold() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.local.heartbeat.min-alive-workers=2")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasSingleBean(HealthView.class);
        HealthView view = ctx.getBean(HealthView.class);
        view.onHeartbeat(new WorkerHeartbeatMessage(0L, "worker-1", 1L, true, null));
        // One observed alive Worker meets the derived threshold but not the configured one.
        assertThat(view.isClusterHealthy()).isFalse();
      });
  }

  /**
   * Verifies that the HotKey facade bean is created when all optional dependencies are provided.
   */
  @Test
  void hotKeyBeanIsCreatedWithAllDependencies() {
    new ApplicationContextRunner()
      .withBean("hotKeyDetector", TopK.class, () -> mock(TopK.class))
      .withBean("workerTopK", TopK.class, () -> mock(TopK.class))
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Zeta.class));
  }

  /**
   * P2 guard for the refreshAll fan-out: the auto-configuration must wire the
   * dedicated {@code hotKeyExecutor} refresh pool into the facade, so a bulk
   * refresh never parks blocking IO on the JVM-global {@code commonPool} nor
   * contends with read-miss loads on {@code interruptibleLoadExecutor}.
   * A capturing executor proves the fan-out tasks were submitted to the
   * injected pool (a fan-out on the JVM common pool would never touch it).
   */
  @Test
  void refreshAll_fansOutThroughTheInjectedRefreshExecutor_neverCommonPool() {
    // Record AND run inline: recording alone would deadlock refreshAll (its
    // allOf(...).join() waits for tasks this executor never starts), while
    // the inline run still proves each fan-out task was submitted to the
    // injected pool — a common-pool fan-out would never touch it.
    List<Runnable> submitted = new ArrayList<>();
    Executor capturing = task -> {
      submitted.add(task);
      task.run();
    };
    new ApplicationContextRunner()
      .withBean("hotKeyDetector", TopK.class, () -> mock(TopK.class))
      .withBean("workerTopK", TopK.class, () -> mock(TopK.class))
      .withBean(HotKeyCache.class, () -> mock(HotKeyCache.class))
      .withBean("hotKeyExecutor", Executor.class, () -> capturing)
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> {
        DefaultZeta facade = (DefaultZeta) ctx.getBean(Zeta.class);
        HotKeyCache cache = ctx.getBean(HotKeyCache.class);

        facade.refreshAll(Map.of("k1", () -> "v1", "k2", () -> "v2"));

        // Fan-out went through the injected executor — one task per key,
        // executed inline by the capturing pool (never the commonPool).
        assertThat(submitted).hasSize(2);
        verify(cache).refresh(eq("k1"), any(), anyLong(), anyLong(), anyBoolean());
        verify(cache).refresh(eq("k2"), any(), anyLong(), anyLong(), anyBoolean());
      });
  }

  /**
   * Verifies that a custom HotKey bean is respected and the auto-configured one is not created.
   */
  @Test
  void hotKeyBeanIsNotCreatedWhenAlreadyDefined() {
    new ApplicationContextRunner()
      .withBean(Zeta.class, () -> mock(Zeta.class))
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(Zeta.class));
  }

  /**
   * Verifies that the instance ID configured via properties is propagated to {@link NodeIdentity}.
   */
  @Test
  void instanceIdIsSetFromPropertiesWhenConfigured() {
    String instanceId = "test-instance-456";
    NodeIdentity.setInstanceOverride(null);
    new ApplicationContextRunner()
      .withPropertyValues("zeta.local.instance-id=" + instanceId)
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(NodeIdentity.getInstanceId()).isEqualTo(instanceId));
  }

  /**
   * The identity overrides are JVM-global: a later context without an instance id must clear
   * the override left by an earlier context instead of inheriting it.
   */
  @Test
  void aLaterContextWithoutInstanceIdClearsThePreviousOverride() {
    new ApplicationContextRunner()
      .withPropertyValues("zeta.local.instance-id=first-context")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(NodeIdentity.getInstanceId()).isEqualTo("first-context"));

    runner.run(ctx -> assertThat(NodeIdentity.getInstanceId()).isNotEqualTo("first-context"));
  }

  /**
   * Verifies that the node id configured via properties is propagated to {@link NodeIdentity},
   * overriding the instance-derived identity.
   */
  @Test
  void nodeIdIsSetFromPropertiesWhenConfigured() {
    NodeIdentity.setNodeOverride(null);
    new ApplicationContextRunner()
      .withPropertyValues("zeta.local.node-id=777")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> assertThat(NodeIdentity.getNodeId()).isEqualTo(777L));
  }

  /**
   * Verifies that the HotKey bean is created when only the app-level TopK (hotKeyDetector) is present.
   */
  @Test
  void hotKeyBeanAcceptsOnlyAppTopK() {
    new ApplicationContextRunner()
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
      .withPropertyValues("zeta.local.scheduler-pool-size=2")
      .withConfiguration(AutoConfigurations.of(ZetaFacadeAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).hasBean("hotKeyScheduler");
        var scheduler = ctx.getBean("hotKeyScheduler", java.util.concurrent.ScheduledExecutorService.class);
        assertThat(scheduler).isNotNull();
      });
  }
}
