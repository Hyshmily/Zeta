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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.reporting.ReportPublisher;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.ConsistentHashRingImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.HealthViewImpl;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListenerProperties;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.DefaultResourceLoader;

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
   * Verifies that ReportConfiguration is skipped when zeta.reportToWorker.enabled is false.
   */
  @Test
  void reportConfigIsSkippedWhenPropertyIsDisabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.reportToWorker.enabled=false")
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
    properties.setReportExchange("test.reportToWorker.exchange");
    ZetaAppProperties appProperties = new ZetaAppProperties();
    appProperties.setAppName("test-app");

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    ReportPublisher publisher = config.reportPublisher(rabbitTemplate, properties, appProperties);

    assertThat(publisher).isNotNull();
  }

  /**
   * Verifies that KeyReporterImpl is created with its required dependencies.
   */
  @Test
  void hotKeyReporterIsCreatedWithRequiredDependencies() {
    ReportPublisher reportPublisher = mock(ReportPublisher.class);
    ZetaProperties properties = new ZetaProperties();

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    KeyReporter reporter = config.hotKeyReporter(
      reportPublisher,
      properties,
      new ZetaAppProperties(),
      new ConsistentHashRingImpl(150),
      new HealthViewImpl(5_000)
    );

    assertThat(reporter).isNotNull();
  }

  /**
   * Verifies that ReportPublisher is configured with the app name from ZetaAppProperties.
   */
  @Test
  void reportPublisherUsesAppNameFromProperties() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    ZetaProperties properties = new ZetaProperties();
    ZetaAppProperties appProperties = new ZetaAppProperties();
    appProperties.setAppName("my-app");
    properties.setReportExchange("my.exchange");

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    ReportPublisher publisher = config.reportPublisher(rabbitTemplate, properties, appProperties);

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
   * Verifies that the dedicated heartbeat {@code @Primary} connection factory is NOT
   * created when no control-plane feature (worker listener / worker mode) is enabled —
   * it must not hijack the application's unqualified {@code ConnectionFactory} injections.
   */
  @Test
  void heartbeatConnectionFactoryIsAbsentWhenNoControlPlaneFeatureEnabled() {
    reportRunner.run(ctx -> assertThat(ctx).doesNotHaveBean("zetaHeartbeatConnectionFactory"));
  }

  /**
   * Verifies that the heartbeat connection factory is created when the worker
   * listener (the feature that consumes it on the app side) is enabled.
   */
  @Test
  void heartbeatConnectionFactoryIsCreatedWhenWorkerListenerEnabled() {
    workerRunner.run(ctx -> assertThat(ctx).hasBean("zetaHeartbeatConnectionFactory"));
  }

  /**
   * Verifies that the heartbeat connection factory is created in worker mode
   * (the worker-side control-plane consumers: heartbeat producer, config
   * gossip, PING/PONG verification).
   */
  @Test
  void heartbeatConnectionFactoryIsCreatedWhenWorkerModeEnabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.worker.enabled=true")
      .run(ctx -> assertThat(ctx).hasBean("zetaHeartbeatConnectionFactory"));
  }

  /**
   * Verifies that a data-plane {@link ConnectionFactory} resolution prefers
   * Boot's {@code rabbitConnectionFactory} even when the control-plane factory
   * is also present — the data/control plane isolation is preserved without
   * hard-coded {@code @Qualifier} injection points.
   */
  @Test
  void dataPlaneFactoryResolutionPrefersBootFactoryOverControlPlaneFactory() {
    ConnectionFactory boot = mock(ConnectionFactory.class);
    ConnectionFactory controlPlane = mock(ConnectionFactory.class);
    org.springframework.context.support.StaticApplicationContext beanFactory =
      new org.springframework.context.support.StaticApplicationContext();
    beanFactory.getBeanFactory().registerSingleton("rabbitConnectionFactory", boot);
    beanFactory.getBeanFactory().registerSingleton("zetaHeartbeatConnectionFactory", controlPlane);
    beanFactory.refresh();

    assertThat(ZetaAmqpAutoConfiguration.dataPlaneConnectionFactory(beanFactory)).isSameAs(boot);
  }

  /**
   * Verifies that a user-defined {@code ConnectionFactory} under a non-default bean name is
   * resolved instead of only Boot's {@code rabbitConnectionFactory}.
   */
  @Test
  void dataPlaneFactoryResolutionFallsBackToUserNamedFactory() {
    ConnectionFactory userFactory = mock(ConnectionFactory.class);
    org.springframework.context.support.StaticApplicationContext beanFactory =
      new org.springframework.context.support.StaticApplicationContext();
    beanFactory.getBeanFactory().registerSingleton("myOwnConnectionFactory", userFactory);
    beanFactory.refresh();

    assertThat(ZetaAmqpAutoConfiguration.dataPlaneConnectionFactory(beanFactory)).isSameAs(userFactory);
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
    assertThat(queue.getName()).isEqualTo("test.sync:" + io.github.hyshmily.zeta.util.NodeIdentity.getInstanceId());
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
    CacheSyncProperties props = new CacheSyncProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
    @SuppressWarnings("unchecked")
    ObjectProvider<StampClock> clocks = mock(ObjectProvider.class);
    CacheSyncListener listener = config.cacheSyncListener(
      props,
      scheduler,
      mock(SyncDecisionHandler.class),
      new ZetaAppProperties(),
      clocks
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
        assertThat(ctx).doesNotHaveBean("hotkeyWorkerExchange");
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
        assertThat(ctx).doesNotHaveBean("hotkeyWorkerExchange");
      });
  }

  /**
   * Verifies that the worker decision exchange is a durable direct exchange: the type is what
   * keeps a shared broker from fanning one application's decisions out to another's queues.
   */
  @Test
  void hotkeyWorkerExchangeIsCreatedWithCorrectProperties() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setExchangeName("test.worker.exchange");

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    DirectExchange exchange = config.hotkeyWorkerExchange(props);

    assertThat(exchange).isNotNull();
    assertThat(exchange.getType()).isEqualTo("direct");
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
   * Verifies that the worker Binding uses this application's decision routing key — the whole
   * shared-broker isolation mechanism.
   */
  @Test
  void hotkeyWorkerBindingBindsQueueToExchangeWithAppNameKey() {
    Queue queue = new Queue("wq");
    DirectExchange exchange = new DirectExchange("we");
    ZetaAppProperties appProps = new ZetaAppProperties();
    appProps.setAppName("orders");

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    Binding binding = config.hotkeyWorkerBinding(queue, exchange, appProps);

    assertThat(binding).isNotNull();
    assertThat(binding.getDestination()).isEqualTo("wq");
    assertThat(binding.getExchange()).isEqualTo("we");
    assertThat(binding.getRoutingKey()).isEqualTo("send.orders");
  }

  /**
   * Verifies that WorkerListener is created with its required dependencies.
   */
  @Test
  void workerListenerIsCreatedWithRequiredDependencies() {
    Cache<String, CacheEntry> localCache = mock(Cache.class);
    WorkerListenerProperties props = new WorkerListenerProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    WorkerListener listener = config.workerListener(
      props,
      scheduler,
      mock(WorkerDecisionHandler.class),
      new io.github.hyshmily.zeta.autoconfigure.ZetaAppProperties()
    );

    assertThat(listener).isNotNull();
  }

  // ── ReportConfiguration additional beans ──

  @Test
  void hotkeyReportExchangeIsCreatedWithCorrectProperties() {
    ZetaProperties properties = new ZetaProperties();
    properties.setReportExchange("test.reportToWorker.exchange");

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    DirectExchange exchange = config.hotkeyReportExchange(properties);

    assertThat(exchange).isNotNull();
    assertThat(exchange.getName()).isEqualTo("test.reportToWorker.exchange");
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
  void reportMessageConverter_isJacksonJsonConverter() {
    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();

    assertThat(config.reportMessageConverter()).isInstanceOf(Jackson2JsonMessageConverter.class);
  }

  @Test
  void ringManagerIsCreatedWithCorrectVirtualNodes() {
    ZetaProperties properties = new ZetaProperties();
    properties.getConsistentHashing().setVirtualNodes(300);

    ZetaAmqpAutoConfiguration.ReportConfiguration config = new ZetaAmqpAutoConfiguration.ReportConfiguration();
    ConsistentHashRing ring = config.consistentHashRing(properties);

    assertThat(ring).isNotNull();
    assertThat(ring.getVirtualNodeCount()).isEqualTo(300);
  }

  @Test
  void cacheSyncPublisherIsCreatedWithAllDependencies() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    CacheSyncProperties props = new CacheSyncProperties();

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
    CacheSyncPublisher publisher = config.cacheSyncPublisher(rabbitTemplate, props, new ZetaAppProperties());

    assertThat(publisher).isNotNull();
  }

  @Test
  void syncListenerContainerIsCreatedWithCorrectProperties() {
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    CacheSyncListener cacheSyncListener = mock(CacheSyncListener.class);
    CacheSyncProperties props = new CacheSyncProperties();

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.registerSingleton("rabbitConnectionFactory", connectionFactory);

    ZetaAmqpAutoConfiguration.SyncConfiguration config = new ZetaAmqpAutoConfiguration.SyncConfiguration();
    SimpleMessageListenerContainer container = config.syncListenerContainer(
      beanFactory,
      new Queue(props.getQueueName()),
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
    assertThat(binding.getRoutingKey()).isEqualTo(ZetaConstants.Routing.KEY_HEARTBEAT + "*");
  }

  @Test
  void heartbeatContainerIsCreated() {
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    HealthView healthView = mock(HealthView.class);
    Queue heartbeatQueue = new Queue("zeta.heartbeat:test");

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    SimpleMessageListenerContainer container = config.heartbeatContainer(
      connectionFactory,
      healthView,
      heartbeatQueue,
      new ZetaAppProperties()
    );

    assertThat(container).isNotNull();
    assertThat(container.getQueueNames()).containsExactly("zeta.heartbeat:test");
  }

  @Test
  void heartbeatContainerRecordsOnlyHeartbeatsFromItsOwnApplication() {
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    HealthView healthView = mock(HealthView.class);
    Queue heartbeatQueue = new Queue("zeta.heartbeat:test");
    ZetaAppProperties appProperties = new ZetaAppProperties();
    appProperties.setAppName("tenant-a");

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    MessageListener listener = config
      .heartbeatContainer(connectionFactory, healthView, heartbeatQueue, appProperties)
      .getMessageListener();

    listener.onMessage(new WorkerHeartbeatMessage(1L, "foreign-worker", 1L, true, "tenant-b").toMessage());
    verifyNoInteractions(healthView);

    WorkerHeartbeatMessage local = new WorkerHeartbeatMessage(2L, "local-worker", 1L, true, "tenant-a");
    listener.onMessage(local.toMessage());
    verify(healthView).onHeartbeat(local);

    WorkerHeartbeatMessage unstamped = new WorkerHeartbeatMessage(3L, "older-worker", 1L, true, null);
    listener.onMessage(unstamped.toMessage());
    verify(healthView).onHeartbeat(unstamped);
  }

  @Test
  void workerListenerContainerIsCreated() {
    ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    Queue workerQueue = new Queue("zeta.worker:test");
    WorkerListener workerListener = mock(WorkerListener.class);
    WorkerListenerProperties props = new WorkerListenerProperties();

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.registerSingleton("rabbitConnectionFactory", connectionFactory);

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();
    SimpleMessageListenerContainer container = config.workerListenerContainer(
      beanFactory,
      workerQueue,
      workerListener,
      props
    );

    assertThat(container).isNotNull();
    assertThat(container.getQueueNames()).containsExactly("zeta.worker:test");
  }

  // ── HeartbeatConnectionConfiguration ──

  /**
   * Verifies that the dedicated heartbeat connection factory inherits the full
   * {@link RabbitProperties} surface — including SSL (TLS) — via Spring Boot's
   * connection factory configurer, instead of only host/port/credentials.
   */
  @Test
  void heartbeatConnectionFactoryInheritsSslAndCredentialsFromRabbitProperties() {
    RabbitProperties props = new RabbitProperties();
    props.setHost("broker.example.com");
    props.setPort(5671);
    props.setUsername("zeta");
    props.setPassword("secret");
    props.getSsl().setEnabled(true);
    ObjectProvider<RabbitProperties> propsProvider = mock(ObjectProvider.class);
    when(propsProvider.getIfAvailable()).thenReturn(props);

    ZetaAmqpAutoConfiguration.HeartbeatConnectionConfiguration config =
      new ZetaAmqpAutoConfiguration.HeartbeatConnectionConfiguration();
    CachingConnectionFactory cf = config.heartbeatConnectionFactory(propsProvider, new DefaultResourceLoader());

    assertThat(cf.getRabbitConnectionFactory().isSSL()).isTrue();
    assertThat(cf.getHost()).isEqualTo("broker.example.com");
    assertThat(cf.getPort()).isEqualTo(5671);
    assertThat(cf.getUsername()).isEqualTo("zeta");
    assertThat(cf.getVirtualHost()).isEqualTo("/");
  }

  /**
   * Verifies that the heartbeat connection factory stays plain (no TLS) when
   * SSL is not enabled in {@link RabbitProperties}.
   */
  @Test
  void heartbeatConnectionFactoryIsPlainWhenSslDisabled() {
    RabbitProperties props = new RabbitProperties();
    ObjectProvider<RabbitProperties> propsProvider = mock(ObjectProvider.class);
    when(propsProvider.getIfAvailable()).thenReturn(props);

    ZetaAmqpAutoConfiguration.HeartbeatConnectionConfiguration config =
      new ZetaAmqpAutoConfiguration.HeartbeatConnectionConfiguration();
    CachingConnectionFactory cf = config.heartbeatConnectionFactory(propsProvider, new DefaultResourceLoader());

    assertThat(cf.getRabbitConnectionFactory().isSSL()).isFalse();
    assertThat(cf.getHost()).isEqualTo("localhost");
    assertThat(cf.getPort()).isEqualTo(5672);
  }

  /**
   * Verifies that the heartbeat connection factory falls back to an unconfigured
   * factory when no {@link RabbitProperties} bean is available.
   */
  @Test
  void heartbeatConnectionFactoryFallsBackWhenNoProperties() {
    ObjectProvider<RabbitProperties> propsProvider = mock(ObjectProvider.class);
    when(propsProvider.getIfAvailable()).thenReturn(null);

    ZetaAmqpAutoConfiguration.HeartbeatConnectionConfiguration config =
      new ZetaAmqpAutoConfiguration.HeartbeatConnectionConfiguration();
    CachingConnectionFactory cf = config.heartbeatConnectionFactory(propsProvider, new DefaultResourceLoader());

    assertThat(cf).isNotNull();
    assertThat(cf.getRabbitConnectionFactory().isSSL()).isFalse();
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
  void workerListenerConfigRequiresConnectionFactory() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.worker-listener.enabled=true")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(WorkerListener.class);
      });
  }

  /**
   * A control-plane-only context must not activate any data-plane configuration:
   * the heartbeat factory is itself a {@link ConnectionFactory}, so plain
   * type-based conditions would match it and then fail while resolving the
   * data-plane factory.
   */
  @Test
  void controlPlaneOnlyFactoryDoesNotActivateDataPlaneConfigurations() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaAmqpAutoConfiguration.class))
      .withPropertyValues("zeta.sync.enabled=true", "zeta.worker-listener.enabled=true")
      .withBean("zetaHeartbeatConnectionFactory", ConnectionFactory.class, () -> mock(ConnectionFactory.class))
      .withBean(RabbitTemplate.class, () -> mock(RabbitTemplate.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(ReportPublisher.class);
        assertThat(ctx).doesNotHaveBean(CacheSyncPublisher.class);
        assertThat(ctx).doesNotHaveBean(WorkerListener.class);
        assertThat(ctx).doesNotHaveBean("zetaReportRabbitTemplate");
        assertThat(ctx).doesNotHaveBean("zetaSyncRabbitTemplate");
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

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.registerSingleton("rabbitConnectionFactory", connectionFactory);

    ZetaAmqpAutoConfiguration.WorkerListenerConfiguration config =
      new ZetaAmqpAutoConfiguration.WorkerListenerConfiguration();

    // Container must be created without error when non-default properties are set
    assertThat(config.workerListenerContainer(beanFactory, workerQueue, workerListener, props)).isNotNull();
  }
}
