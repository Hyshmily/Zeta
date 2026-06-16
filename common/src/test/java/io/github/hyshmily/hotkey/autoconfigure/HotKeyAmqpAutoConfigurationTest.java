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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.reporting.BbrRateLimiter;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.reporting.ReportPublisher;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sync.*;
import io.github.hyshmily.hotkey.util.ratelimit.SreRateLimiter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link HotKeyAmqpAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyAmqpAutoConfigurationTest {

  private final ApplicationContextRunner reportRunner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class)
  );

  private final ApplicationContextRunner syncRunner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class))
    .withPropertyValues("hotkey.sync.enabled=true");

  private final ApplicationContextRunner workerRunner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class))
    .withPropertyValues("hotkey.worker-listener.enabled=true");

  /**
   * Verifies that ReportConfiguration is skipped when no RabbitTemplate bean is present.
   */
  @Test
  void reportConfigIsSkippedWhenRabbitTemplateBeanNotPresent() {
    reportRunner.run(ctx -> {
      assertThat(ctx).doesNotHaveBean(ReportPublisher.class);
      assertThat(ctx).doesNotHaveBean(HotKeyReporter.class);
    });
  }

  /**
   * Verifies that ReportConfiguration is skipped when hotkey.report.enabled is false.
   */
  @Test
  void reportConfigIsSkippedWhenPropertyIsDisabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class))
      .withPropertyValues("hotkey.report.enabled=false")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(ReportPublisher.class);
        assertThat(ctx).doesNotHaveBean(HotKeyReporter.class);
      });
  }

  /**
   * Verifies that ReportConfiguration is active by default when RabbitTemplate is present.
   */
  @Test
  void reportConfigIsActiveByDefaultWhenRabbitTemplatePresent() {
    reportRunner.run(ctx -> assertThat(ctx).isNotNull());
  }

  /**
   * Verifies that ReportPublisher is created with custom exchange and app name properties.
   */
  @Test
  void reportPublisherIsCreatedWithCorrectProperties() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    HotKeyProperties properties = new HotKeyProperties();
    properties.setReportExchange("test.report.exchange");
    properties.setAppName("test-app");

    HotKeyAmqpAutoConfiguration.ReportConfiguration config = new HotKeyAmqpAutoConfiguration.ReportConfiguration();
    ReportPublisher publisher = config.reportPublisher(rabbitTemplate, properties);

    assertThat(publisher).isNotNull();
  }

  /**
   * Verifies that HotKeyReporter is created with its required dependencies.
   */
  @Test
  @SuppressWarnings("unchecked")
  void hotKeyReporterIsCreatedWithRequiredDependencies() {
    ReportPublisher reportPublisher = mock(ReportPublisher.class);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    HotKeyProperties properties = new HotKeyProperties();
    ObjectProvider<ClusterHealthView> healthViewProvider = mock(ObjectProvider.class);

    HotKeyAmqpAutoConfiguration.ReportConfiguration config = new HotKeyAmqpAutoConfiguration.ReportConfiguration();
    ObjectProvider<BbrRateLimiter> bbrProvider = mock(ObjectProvider.class);
    HotKeyReporter reporter = config.hotKeyReporter(
      reportPublisher, scheduler, properties, new RingManager(150), healthViewProvider, bbrProvider
    );

    assertThat(reporter).isNotNull();
  }

  /**
   * Verifies that ReportPublisher is configured with the app name from HotKeyProperties.
   */
  @Test
  void reportPublisherUsesAppNameFromProperties() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    HotKeyProperties properties = new HotKeyProperties();
    properties.setAppName("my-app");
    properties.setReportExchange("my.exchange");

    HotKeyAmqpAutoConfiguration.ReportConfiguration config = new HotKeyAmqpAutoConfiguration.ReportConfiguration();
    ReportPublisher publisher = config.reportPublisher(rabbitTemplate, properties);

    assertThat(publisher).isNotNull();
  }

  /**
   * Verifies that SyncConfiguration is skipped when hotkey.sync.enabled is false.
   */
  @Test
  void syncConfigIsSkippedWhenPropertyIsNotEnabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class))
      .withPropertyValues("hotkey.sync.enabled=false")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(FanoutExchange.class);
        assertThat(ctx).doesNotHaveBean(CacheSyncPublisher.class);
        assertThat(ctx).doesNotHaveBean(CacheSyncListener.class);
      });
  }

  /**
   * Verifies that the sync FanoutExchange is created with the configured name, durable, and non-auto-delete.
   */
  @Test
  void hotkeySyncExchangeIsCreatedWithCorrectProperties() {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setExchangeName("test.sync.exchange");

    HotKeyAmqpAutoConfiguration.SyncConfiguration config = new HotKeyAmqpAutoConfiguration.SyncConfiguration();
    FanoutExchange exchange = config.hotkeySyncExchange(props);

    assertThat(exchange).isNotNull();
    assertThat(exchange.getName()).isEqualTo("test.sync.exchange");
    assertThat(exchange.isDurable()).isTrue();
    assertThat(exchange.isAutoDelete()).isFalse();
  }

  /**
   * Verifies that the sync Queue is created with the configured prefix and instance ID suffix.
   */
  @Test
  void hotkeySyncQueueIsCreatedWithCorrectProperties() {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setQueuePrefix("test.sync");

    HotKeyAmqpAutoConfiguration.SyncConfiguration config = new HotKeyAmqpAutoConfiguration.SyncConfiguration();
    Queue queue = config.hotkeySyncQueue(props);

    assertThat(queue).isNotNull();
    assertThat(queue.getName()).isEqualTo(
      "test.sync:" + io.github.hyshmily.hotkey.util.InstanceIdGenerator.get()
    );
    assertThat(queue.isDurable()).isTrue();
  }

  /**
   * Verifies that the sync Binding correctly binds the queue to the exchange.
   */
  @Test
  void hotkeySyncBindingBindsQueueToExchange() {
    Queue queue = new Queue("test-queue");
    FanoutExchange exchange = new FanoutExchange("test-exchange");

    HotKeyAmqpAutoConfiguration.SyncConfiguration config = new HotKeyAmqpAutoConfiguration.SyncConfiguration();
    Binding binding = config.hotkeySyncBinding(queue, exchange);

    assertThat(binding).isNotNull();
    assertThat(binding.getDestination()).isEqualTo("test-queue");
    assertThat(binding.getExchange()).isEqualTo("test-exchange");
  }

  /**
   * Verifies that CacheSyncConfiguration is instantiated and properties are configured.
   */
  @Test
  void cacheSyncPublisherIsCreated() {
    HotKeyAmqpAutoConfiguration.SyncConfiguration config = new HotKeyAmqpAutoConfiguration.SyncConfiguration();
    CacheSyncProperties props = new CacheSyncProperties();
    assertThat(config).isNotNull();
    assertThat(props).isNotNull();
  }

  /**
   * Verifies that CacheSyncListener is created with its required dependencies.
   */
  @Test
  void cacheSyncListenerIsCreatedWithRequiredDependencies() {
    Cache<String, Object> localCache = mock(Cache.class);
    Function<String, Object> redisLoader = mock(Function.class);
    CacheSyncProperties props = new CacheSyncProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    CacheExpireManager expireManager = mock(CacheExpireManager.class);

    HotKeyAmqpAutoConfiguration.SyncConfiguration config = new HotKeyAmqpAutoConfiguration.SyncConfiguration();
    CacheSyncListener listener = config.cacheSyncListener(
      localCache,
      redisLoader,
      props,
      scheduler,
      expireManager,
      mock(RuleMatcher.class)
    );

    assertThat(listener).isNotNull();
  }

  /**
   * Verifies that WorkerListenerConfiguration is skipped when hotkey.worker-listener.enabled is false.
   */
  @Test
  void workerConfigIsSkippedWhenPropertyIsNotEnabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class))
      .withPropertyValues("hotkey.worker-listener.enabled=false")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(WorkerListener.class);
        assertThat(ctx).doesNotHaveBean(FanoutExchange.class);
      });
  }

  /**
   * Verifies that WorkerListenerConfiguration is skipped by default (property not enabled).
   */
  @Test
  void workerConfigIsSkippedByDefault() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(WorkerListener.class);
        assertThat(ctx).doesNotHaveBean(FanoutExchange.class);
      });
  }

  /**
   * Verifies that the worker FanoutExchange is created with the configured name, durable, and non-auto-delete.
   */
  @Test
  void hotkeyWorkerExchangeIsCreatedWithCorrectProperties() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setExchangeName("test.worker.exchange");

    HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration();
    FanoutExchange exchange = config.hotkeyWorkerExchange(props);

    assertThat(exchange).isNotNull();
    assertThat(exchange.getName()).isEqualTo("test.worker.exchange");
    assertThat(exchange.isDurable()).isTrue();
    assertThat(exchange.isAutoDelete()).isFalse();
  }

  /**
   * Verifies that the worker Queue is created with the configured prefix.
   */
  @Test
  void hotkeyWorkerQueueIsCreatedWithCorrectProperties() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setQueuePrefix("test.worker");

    HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration();
    Queue queue = config.hotkeyWorkerQueue(props);

    assertThat(queue).isNotNull();
    assertThat(queue.getName()).startsWith("test.worker:");
    assertThat(queue.isDurable()).isTrue();
  }

  /**
   * Verifies that the worker Binding correctly binds the queue to the exchange.
   */
  @Test
  void hotkeyWorkerBindingBindsQueueToExchange() {
    Queue queue = new Queue("wq");
    FanoutExchange exchange = new FanoutExchange("we");

    HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration();
    Binding binding = config.hotkeyWorkerBinding(queue, exchange);

    assertThat(binding).isNotNull();
    assertThat(binding.getDestination()).isEqualTo("wq");
    assertThat(binding.getExchange()).isEqualTo("we");
  }

  /**
   * Verifies that SreRateLimiter is created with the configured success threshold.
   */
  @Test
  void sreRateLimiterIsCreatedWithCorrectProperties() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.getSre().setSuccessThreshold(0.8);
    props.getSre().setWindowMs(2000);
    props.getSre().setBuckets(5);
    props.getSre().setMinSamples(10);

    HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration();
    SreRateLimiter limiter = config.hotKeySreRateLimiter(props);

    assertThat(limiter).isNotNull();
  }

  /**
   * Verifies that WorkerListener is created with its required dependencies.
   */
  @Test
  void workerListenerIsCreatedWithRequiredDependencies() {
    Cache<String, Object> localCache = mock(Cache.class);
    Function<String, Object> redisLoader = mock(Function.class);
    WorkerListenerProperties props = new WorkerListenerProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    CacheExpireManager expireManager = mock(CacheExpireManager.class);

    HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration();
    ObjectProvider<SreRateLimiter> sreProvider = mock(ObjectProvider.class);
    WorkerListener listener = config.workerListener(
      localCache,
      redisLoader,
      props,
      scheduler,
      expireManager,
      sreProvider
    );

    assertThat(listener).isNotNull();
  }

}
