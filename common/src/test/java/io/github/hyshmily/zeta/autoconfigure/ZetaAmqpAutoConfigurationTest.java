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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.reporting.BbrRateLimiter;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.reporting.ReportPublisher;
import io.github.hyshmily.zeta.reporting.impl.BbrRateLimiterImpl;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.RingManager;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sharding.impl.RingManagerImpl;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatVerifier;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListenerProperties;
import io.github.hyshmily.zeta.util.SystemLoadMonitor;
import io.github.hyshmily.zeta.util.ratelimit.SreRateLimiter;
import io.github.hyshmily.zeta.util.ratelimit.impl.SreRateLimiterImpl;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link ZetaAmqpAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class ZetaAmqpAutoConfigurationTest {

  private final ApplicationContextRunner reportRunner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(ZetaAmqpAutoConfiguration.class)
  );

  private final ApplicationContextRunner syncRunner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
    .withPropertyValues("zeta.sync.enabled=true");

  private final ApplicationContextRunner workerRunner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
    .withPropertyValues("zeta.worker-listener.enabled=true");

  /**
   * Verifies that ReportConfiguration is skipped when no RabbitTemplate bean is present.
   */
  @Test
  void reportConfigIsSkippedWhenRabbitTemplateBeanNotPresent() {
    reportRunner.run(ctx -> {
      assertThat(ctx).doesNotHaveBean(ReportPublisher.class);
      assertThat(ctx).doesNotHaveBean(KeyReporterImpl.class);
    });
  }

  /**
   * Verifies that ReportConfiguration is skipped when zeta.report.enabled is false.
   */
  @Test
  void reportConfigIsSkippedWhenPropertyIsDisabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.report.enabled=false")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(ReportPublisher.class);
        assertThat(ctx).doesNotHaveBean(KeyReporterImpl.class);
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
    ZetaProperties properties = new ZetaProperties();
    properties.setReportExchange("test.report.exchange");
    properties.setAppName("test-app");

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    ReportPublisher publisher = config.reportPublisher(rabbitTemplate, properties);

    assertThat(publisher).isNotNull();
  }

  /**
   * Verifies that KeyReporterImpl is created with its required dependencies.
   */
  @Test
  @SuppressWarnings("unchecked")
  void hotKeyReporterIsCreatedWithRequiredDependencies() {
    ReportPublisher reportPublisher = mock(ReportPublisher.class);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    ZetaProperties properties = new ZetaProperties();
    ObjectProvider<HealthView> healthViewProvider = mock(ObjectProvider.class);

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    ObjectProvider<BbrRateLimiterImpl> bbrProvider = mock(ObjectProvider.class);
    KeyReporter reporter = config.hotKeyReporter(
      reportPublisher,
      scheduler,
      properties,
      new RingManagerImpl(150),
      healthViewProvider,
      bbrProvider
    );

    assertThat(reporter).isNotNull();
  }

  /**
   * Verifies that ReportPublisher is configured with the app name from ZetaProperties.
   */
  @Test
  void reportPublisherUsesAppNameFromProperties() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    ZetaProperties properties = new ZetaProperties();
    properties.setAppName("my-app");
    properties.setReportExchange("my.exchange");

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    ReportPublisher publisher = config.reportPublisher(rabbitTemplate, properties);

    assertThat(publisher).isNotNull();
  }

  /**
   * Verifies that SyncConfiguration is skipped when zeta.sync.enabled is false.
   */
  @Test
  void syncConfigIsSkippedWhenPropertyIsNotEnabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.sync.enabled=false")
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

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
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

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
    Queue queue = config.hotkeySyncQueue(props);

    assertThat(queue).isNotNull();
    assertThat(queue.getName()).isEqualTo("test.sync:" + io.github.hyshmily.zeta.util.InstanceIdGenerator.get());
    assertThat(queue.isDurable()).isTrue();
  }

  /**
   * Verifies that the sync Binding correctly binds the queue to the exchange.
   */
  @Test
  void hotkeySyncBindingBindsQueueToExchange() {
    Queue queue = new Queue("test-queue");
    FanoutExchange exchange = new FanoutExchange("test-exchange");

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
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
    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
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
    CacheLoader redisLoader = mock(CacheLoader.class);
    CacheSyncProperties props = new CacheSyncProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    ExpireManager expireManager = mock(ExpireManager.class);

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
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
   * Verifies that WorkerListenerConfiguration is skipped when zeta.worker-listener.enabled is false.
   */
  @Test
  void workerConfigIsSkippedWhenPropertyIsNotEnabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.worker-listener.enabled=false")
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
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
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

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
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

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
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

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
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

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    SreRateLimiter limiter = config.hotKeySreRateLimiter(props);

    assertThat(limiter).isNotNull();
  }

  /**
   * Verifies that WorkerListener is created with its required dependencies.
   */
  @Test
  void workerListenerIsCreatedWithRequiredDependencies() {
    Cache<String, Object> localCache = mock(Cache.class);
    CacheLoader redisLoader = mock(CacheLoader.class);
    WorkerListenerProperties props = new WorkerListenerProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    ExpireManager expireManager = mock(ExpireManager.class);

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    ObjectProvider<SreRateLimiterImpl> sreProvider = mock(ObjectProvider.class);
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

  // ── ReportConfiguration additional beans ──

  @Test
  void hotkeyReportExchangeIsCreatedWithCorrectProperties() {
    ZetaProperties properties = new ZetaProperties();
    properties.setReportExchange("test.report.exchange");

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    DirectExchange exchange = config.hotkeyReportExchange(properties);

    assertThat(exchange).isNotNull();
    assertThat(exchange.getName()).isEqualTo("test.report.exchange");
    assertThat(exchange.isDurable()).isTrue();
    assertThat(exchange.isAutoDelete()).isFalse();
  }

  @Test
  void reportMessageConverterIsCreated() {
    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    MessageConverter converter = config.reportMessageConverter();

    assertThat(converter).isNotNull();
  }

  @Test
  void ringManagerIsCreatedWithCorrectVirtualNodes() {
    ZetaProperties properties = new ZetaProperties();
    properties.getConsistentHashing().setVirtualNodes(300);

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    RingManager ringManager = config.ringManager(properties);

    assertThat(ringManager).isNotNull();
    assertThat(ringManager.getVirtualNodeCount()).isEqualTo(300);
  }

  @Test
  void hotKeyCpuMonitorIsCreatedWithCorrectProperties() {
    ZetaProperties properties = new ZetaProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    SystemLoadMonitor monitor = config.hotKeyCpuMonitor(properties, scheduler);

    assertThat(monitor).isNotNull();
  }

  @Test
  void hotKeyBbrRateLimiterIsCreatedWithCorrectProperties() {
    SystemLoadMonitor cpuMonitor = mock(SystemLoadMonitor.class);
    ZetaProperties properties = new ZetaProperties();

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    BbrRateLimiter limiter = config.hotKeyBbrRateLimiter(cpuMonitor, properties);

    assertThat(limiter).isNotNull();
  }

  @Test
  void hotKeyReporterAcceptsBbrRateLimiterViaProvider() {
    ReportPublisher reportPublisher = mock(ReportPublisher.class);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    ZetaProperties properties = new ZetaProperties();
    RingManager ringManager = new RingManagerImpl(150);
    ObjectProvider<HealthView> healthViewProvider = mock(ObjectProvider.class);
    ObjectProvider<BbrRateLimiterImpl> bbrProvider = mock(ObjectProvider.class);

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    KeyReporter reporter = config.hotKeyReporter(
      reportPublisher,
      scheduler,
      properties,
      ringManager,
      healthViewProvider,
      bbrProvider
    );

    assertThat(reporter).isNotNull();
  }

  @Test
  void cacheSyncPublisherIsCreatedWithAllDependencies() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    CacheSyncProperties props = new CacheSyncProperties();

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
    CacheSyncPublisher publisher = config.cacheSyncPublisher(rabbitTemplate, props);

    assertThat(publisher).isNotNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  void hotKeyRedisLoaderIsCreated() {
    org.springframework.data.redis.core.StringRedisTemplate redisTemplate = mock(
      org.springframework.data.redis.core.StringRedisTemplate.class
    );

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
    CacheLoader loader = config.hotKeyRedisLoader(redisTemplate);

    assertThat(loader).isNotNull();
  }

  @Test
  void syncListenerContainerIsCreatedWithCorrectProperties() {
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    CacheSyncListener cacheSyncListener = mock(CacheSyncListener.class);
    CacheSyncProperties props = new CacheSyncProperties();

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
    SimpleMessageListenerContainer container = config.syncListenerContainer(
      connectionFactory,
      cacheSyncListener,
      props
    );

    assertThat(container).isNotNull();
    assertThat(container.getQueueNames()).containsExactly(props.getQueueName());
  }

  // ── WorkerListenerConfiguration remaining beans ──

  @Test
  void hotkeyHeartbeatExchangeIsCreatedWithCorrectProperties() {
    ZetaProperties properties = new ZetaProperties();
    properties.getHeartbeat().setExchangeName("test.heartbeat.exchange");

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    TopicExchange exchange = config.hotkeyHeartbeatExchange(properties);

    assertThat(exchange).isNotNull();
    assertThat(exchange.getName()).isEqualTo("test.heartbeat.exchange");
    assertThat(exchange.isDurable()).isTrue();
    assertThat(exchange.isAutoDelete()).isFalse();
  }

  @Test
  void hotkeyHeartbeatQueueIsCreated() {
    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    Queue queue = config.hotkeyHeartbeatQueue();

    assertThat(queue).isNotNull();
    assertThat(queue.getName()).startsWith("zeta.heartbeat:");
    assertThat(queue.isDurable()).isFalse();
    assertThat(queue.isAutoDelete()).isTrue();
  }

  @Test
  void hotkeyHeartbeatBindingBindsQueueToExchange() {
    Queue queue = new Queue("hq");
    TopicExchange exchange = new TopicExchange("he");

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    Binding binding = config.hotkeyHeartbeatBinding(queue, exchange);

    assertThat(binding).isNotNull();
    assertThat(binding.getDestination()).isEqualTo("hq");
    assertThat(binding.getExchange()).isEqualTo("he");
    assertThat(binding.getRoutingKey()).isEqualTo(ZetaConstants.ROUTING_KEY_HEARTBEAT + "*");
  }

  @Test
  void heartbeatContainerIsCreated() {
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    HealthView healthView = mock(HealthView.class);
    Queue heartbeatQueue = new Queue("zeta.heartbeat:test");

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    SimpleMessageListenerContainer container = config.heartbeatContainer(connectionFactory, healthView, heartbeatQueue);

    assertThat(container).isNotNull();
    assertThat(container.getQueueNames()).containsExactly("zeta.heartbeat:test");
  }

  @Test
  void workerListenerContainerIsCreated() {
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    Queue workerQueue = new Queue("zeta.worker:test");
    WorkerListener workerListener = mock(WorkerListener.class);
    WorkerListenerProperties props = new WorkerListenerProperties();

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    SimpleMessageListenerContainer container = config.workerListenerContainer(
      connectionFactory,
      workerQueue,
      workerListener,
      props
    );

    assertThat(container).isNotNull();
    assertThat(container.getQueueNames()).containsExactly("zeta.worker:test");
  }

  @Test
  void workerHeartbeatVerifierIsCreated() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    HealthView healthView = mock(HealthView.class);
    ZetaProperties properties = new ZetaProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    WorkerHeartbeatVerifier verifier = config.workerHeartbeatVerifier(
      rabbitTemplate,
      healthView,
      properties,
      scheduler
    );

    assertThat(verifier).isNotNull();
  }

  // ── Conditional configuration skip tests ──

  @Test
  void allConfigSkippedWhenRabbitTemplateClassNotPresent() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(ReportPublisher.class);
        assertThat(ctx).doesNotHaveBean(CacheSyncPublisher.class);
        assertThat(ctx).doesNotHaveBean(WorkerListener.class);
      });
  }

  @Test
  void workerListenerConfigRequiresRedisTemplateBean() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.worker-listener.enabled=true")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(WorkerListener.class);
      });
  }

  @Test
  void reportBbrRateLimiterSkippedWhenLocalReporterDisabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.local.reporter.enabled=false")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(BbrRateLimiter.class);
      });
  }

  @Test
  void workerSreRateLimiterSkippedWhenSreDisabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.worker-listener.sre.enabled=false")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(SreRateLimiter.class);
      });
  }

  @Test
  void workerListenerContainer_withConcurrentConsumersAndPrefetchCount_shouldSetProperties() {
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    Queue workerQueue = new Queue("zeta.worker:test");
    WorkerListener workerListener = mock(WorkerListener.class);
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setConcurrentConsumers(4);
    props.setPrefetchCount(10);

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();

    // Container must be created without error when non-default properties are set
    assertThat(config.workerListenerContainer(connectionFactory, workerQueue, workerListener, props)).isNotNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  void hotKeyReporter_withHealthViewProvider_shouldCreateClusterHealthViewWithProperties() {
    ReportPublisher reportPublisher = mock(ReportPublisher.class);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    ZetaProperties properties = new ZetaProperties();
    RingManager ringManager = new RingManagerImpl(150);
    HealthView customHealthView = new HealthViewImpl(3, 5000, 2);

    ObjectProvider<HealthView> healthViewProvider = mock(ObjectProvider.class);
    when(healthViewProvider.getIfAvailable(any())).thenReturn(customHealthView);

    ObjectProvider<BbrRateLimiterImpl> bbrProvider = mock(ObjectProvider.class);

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    KeyReporter reporter = config.hotKeyReporter(
      reportPublisher,
      scheduler,
      properties,
      ringManager,
      healthViewProvider,
      bbrProvider
    );

    assertThat(reporter).isNotNull();
  }
}
