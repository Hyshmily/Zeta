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
import io.github.hyshmily.hotkey.sync.*;
import io.github.hyshmily.hotkey.cache.CacheExpireManager;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.monitor.WorkerHealthMonitor;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.reporting.ReportPublisher;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.beans.factory.ObjectProvider;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link HotKeyAmqpAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyAmqpAutoConfigurationTest {

  // ── Context runners ──────────────────────────────────────────────────────

  private final ApplicationContextRunner reportRunner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class)
  );

  private final ApplicationContextRunner syncRunner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class))
    .withPropertyValues("hotkey.sync.enabled=true");

  private final ApplicationContextRunner workerRunner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class))
    .withPropertyValues("hotkey.worker-listener.enabled=true");

  // ── Report tests ─────────────────────────────────────────────────────────

  @Test
  void reportConfigIsSkippedWhenRabbitTemplateBeanNotPresent() {
    reportRunner.run(ctx -> {
      assertThat(ctx).doesNotHaveBean(ReportPublisher.class);
      assertThat(ctx).doesNotHaveBean(HotKeyReporter.class);
    });
  }

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

  @Test
  void reportConfigIsActiveByDefaultWhenRabbitTemplatePresent() {
    reportRunner.run(ctx -> assertThat(ctx).isNotNull());
  }

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

  @Test
  void hotKeyReporterIsCreatedWithRequiredDependencies() {
    ReportPublisher reportPublisher = mock(ReportPublisher.class);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    HotKeyProperties properties = new HotKeyProperties();

    HotKeyAmqpAutoConfiguration.ReportConfiguration config = new HotKeyAmqpAutoConfiguration.ReportConfiguration();
    ObjectProvider<RingManager> ringProvider = mock(ObjectProvider.class);
    HotKeyReporter reporter = config.hotKeyReporter(new WorkerHealthMonitor(), reportPublisher, scheduler, properties, ringProvider);

    assertThat(reporter).isNotNull();
  }

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

  @Test
  void hotKeyReportSchedulerIsCreated() {
    HotKeyAmqpAutoConfiguration.ReportConfiguration config = new HotKeyAmqpAutoConfiguration.ReportConfiguration();
    ScheduledExecutorService scheduler = config.hotKeyReportScheduler();

    assertThat(scheduler).isNotNull();
    assertThat(scheduler.isShutdown()).isFalse();
    scheduler.shutdown();
  }

  @Test
  void hotKeyReportSchedulerThreadNameContainsReport() {
    HotKeyAmqpAutoConfiguration.ReportConfiguration config = new HotKeyAmqpAutoConfiguration.ReportConfiguration();
    ScheduledExecutorService scheduler = config.hotKeyReportScheduler();

    assertThat(scheduler).isNotNull();
    scheduler.execute(() -> {
      String threadName = Thread.currentThread().getName();
      assertThat(threadName).contains("hotkey-report");
    });
    scheduler.shutdown();
  }

  // ── Sync tests ───────────────────────────────────────────────────────────

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

  @Test
  void cacheSyncPublisherIsCreated() {
    HotKeyAmqpAutoConfiguration.SyncConfiguration config = new HotKeyAmqpAutoConfiguration.SyncConfiguration();
    CacheSyncProperties props = new CacheSyncProperties();
    assertThat(config).isNotNull();
    assertThat(props).isNotNull();
  }

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

  @Test
  void hotKeySyncSchedulerIsCreated() {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setSchedulerPoolSize(2);

    HotKeyAmqpAutoConfiguration.SyncConfiguration config = new HotKeyAmqpAutoConfiguration.SyncConfiguration();
    ScheduledExecutorService scheduler = config.hotKeySyncScheduler(props);

    assertThat(scheduler).isNotNull();
    assertThat(scheduler.isShutdown()).isFalse();
    scheduler.shutdown();
  }

  // ── Worker Listener tests ────────────────────────────────────────────────

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

  @Test
  void workerConfigIsSkippedByDefault() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyAmqpAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(WorkerListener.class);
        assertThat(ctx).doesNotHaveBean(FanoutExchange.class);
      });
  }

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

  @Test
  void workerListenerIsCreatedWithRequiredDependencies() {
    Cache<String, Object> localCache = mock(Cache.class);
    Function<String, Object> redisLoader = mock(Function.class);
    WorkerListenerProperties props = new WorkerListenerProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    CacheExpireManager expireManager = mock(CacheExpireManager.class);

    HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration();
    WorkerHealthMonitor healthMonitor = new WorkerHealthMonitor();
    WorkerListener listener = config.workerListener(
      localCache,
      redisLoader,
      props,
      scheduler,
      expireManager,
      healthMonitor
    );

    assertThat(listener).isNotNull();
  }

  @Test
  void hotKeyWorkerSchedulerIsCreated() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setSchedulerPoolSize(2);

    HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new HotKeyAmqpAutoConfiguration.WorkerListenerConfiguration();
    ScheduledExecutorService scheduler = config.hotKeyWorkerScheduler(props);

    assertThat(scheduler).isNotNull();
    assertThat(scheduler.isShutdown()).isFalse();
    scheduler.shutdown();
  }
}
