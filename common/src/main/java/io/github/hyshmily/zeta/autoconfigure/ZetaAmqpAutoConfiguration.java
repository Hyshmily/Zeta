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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.KEY_HEARTBEAT;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.heat.HeatDirectory;
import io.github.hyshmily.zeta.heat.HeatView;
import io.github.hyshmily.zeta.heat.HotKeyContextFactory;
import io.github.hyshmily.zeta.heat.HotKeyHandler;
import io.github.hyshmily.zeta.heat.HotKeyListener;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.reporting.ReportPublisher;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.reporting.impl.ReportFeedLoop;
import io.github.hyshmily.zeta.scheduler.KeepWarm;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.AmqpConsumerProperties;
import io.github.hyshmily.zeta.sync.local.*;
import io.github.hyshmily.zeta.sync.worker.*;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader.AppIsolationFilter;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.CachingConnectionFactoryConfigurer;
import org.springframework.boot.autoconfigure.amqp.ConnectionFactoryCustomizer;
import org.springframework.boot.autoconfigure.amqp.RabbitConnectionFactoryBeanConfigurer;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.Assert;

/**
 * Unified AMQP auto-configuration for HotKey messaging: app-to-Worker reporting,
 * instance-to-instance cache sync, and Worker decision listening.
 *
 * <p>Conditionally activates when {@link RabbitTemplate} is on the classpath.
 *
 * <p><b>Report</b> ({@code zeta.report.enabled}, default {@code true}):
 * app instance aggregates access counts and sends them to the Worker via
 * {@link DirectExchange}.
 *
 * <p><b>Cache Sync</b> ({@code zeta.sync.enabled=true}):
 * instance-to-instance INVALIDATE / PUT / REFRESH broadcasts via {@link FanoutExchange}.
 *
 * <p><b>Worker Listener</b> ({@code zeta.worker-listener.enabled=true}):
 * receives HOT/COOL decisions from the Worker via a {@link DirectExchange}, each instance's
 * queue bound under the routing key {@code send.<appName>} so the fan-out stays inside this
 * application even when several share the broker.
 */
@Internal
@AutoConfiguration(afterName = "org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration")
@ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
@EnableConfigurationProperties(
  { ZetaProperties.class, ZetaAppProperties.class, CacheSyncProperties.class, WorkerListenerProperties.class }
)
@Slf4j
public class ZetaAmqpAutoConfiguration {

  /** Bean name of Spring Boot's default (data-plane) RabbitMQ connection factory. */
  private static final String DATA_PLANE_CONNECTION_FACTORY_BEAN = "rabbitConnectionFactory";

  /**
   * Queue-name prefix of the per-instance heartbeat queue. Unique per instance, non-durable and
   * auto-delete: the broker drops it when the control-plane consumer disconnects, and the
   * reconnect listener declares it again.
   */
  private static final String HEARTBEAT_QUEUE_PREFIX = "zeta.heartbeat:";

  private final ZetaAppProperties appProperties;

  ZetaAmqpAutoConfiguration(ZetaAppProperties appProperties) {
    this.appProperties = appProperties;
  }

  /**
   * Warn once when the shared broker has no tenant discriminator: report
   * routing, sync filtering and Worker decision routing all key on
   * {@code zeta.app-name}, so two applications that both keep the default
   * silently consider each other "ours".
   */
  @PostConstruct
  void warnIfAppNameIsDefault() {
    if (!appProperties.isDefaultAppName()) {
      return;
    }
    log.warn(
      "zeta.app-name is not set (still '{}'); report routing, cache-sync messages and Worker decisions are NOT " +
        "isolated from other applications that share the same broker. Set zeta.app-name to a unique value per " +
        "application — see design decisions: sync messages.",
      ZetaAppProperties.DEFAULT_APP_NAME
    );
  }

  /**
   * Shared shape of the durable per-instance queues (the sync plane and the
   * Worker-listener plane): 60-second message TTL (bounded queue backlog after a
   * node outage) and 24-hour idle expiry (self-cleanup for decommissioned
   * instances).
   */
  private static Queue durableTtlQueue(String queueName) {
    return QueueBuilder.durable(queueName)
      .withArgument("x-message-ttl", 60_000)
      .withArgument("x-expires", 86_400_000)
      .build();
  }

  /**
   * Shared shape of the dedicated sync-plane / Worker-listener schedulers: the
   * pool is at least {@code concurrentConsumers × 2} so every consumer thread
   * can run a delayed follow-up task without starving the others.
   */
  private static ScheduledExecutorService newJitterScheduler(AmqpConsumerProperties properties, String component) {
    int poolSize = Math.max(properties.getSchedulerPoolSize(), properties.getConcurrentConsumers() * 2);
    ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(
      poolSize,
      new ZetaThreadFactory(ZetaConstants.Threads.PREFIX_SCHEDULER + "-" + component + "-")
    );
    // A delayed follow-up parked before shutdown must not run against a closed container.
    scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    scheduler.setRemoveOnCancelPolicy(true);
    return scheduler;
  }

  /**
   * Shared shape of the MANUAL-ack per-instance listener containers (the sync
   * plane and the Worker-listener plane): bound to one declared queue, manual
   * acknowledgment (the handler performs its own ack/nack), configured
   * concurrency/prefetch/autostartup, an uncaught-exception WARN handler, and
   * the channel-aware message listener.
   */
  private static SimpleMessageListenerContainer newManualAckContainer(
    ConnectionFactory connectionFactory,
    Queue queue,
    AmqpConsumerProperties properties,
    ChannelAwareMessageListener listener,
    String uncaughtWarnMessage
  ) {
    SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
    container.setQueueNames(queue.getName());
    container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
    container.setConcurrentConsumers(properties.getConcurrentConsumers());
    container.setPrefetchCount(properties.getPrefetchCount());
    container.setErrorHandler(t -> log.warn(uncaughtWarnMessage, t));
    container.setAutoStartup(properties.isAutoStartup());
    container.setMessageListener(listener);
    // A broker restart or an operator-deleted queue must not stop the listener permanently; the
    // container retries the declaration on the next connection.
    container.setMissingQueuesFatal(false);
    return container;
  }

  /**
   * Declare the heartbeat topology on every control-plane connection. The queue is non-durable
   * and auto-delete, so the broker removes it when the last consumer disconnects; the data-plane
   * {@code RabbitAdmin} only redeclares on a data-plane connection event and cannot repair a
   * control-plane reconnect. A dedicated connection listener, not a second {@code RabbitAdmin}
   * bean: Boot's admin is conditional on a missing {@code AmqpAdmin}, so exposing another one
   * would disable the data-plane declarations. The listener runs on every control-plane
   * connection and declares the same topology idempotently.
   *
   * @param factory  the control-plane connection factory
   * @param exchange the heartbeat exchange
   * @param queue    the per-instance heartbeat queue
   * @param binding  the heartbeat binding
   */
  private static void declareOnControlPlane(
    CachingConnectionFactory factory,
    TopicExchange exchange,
    Queue queue,
    Binding binding
  ) {
    factory.addConnectionListener(connection -> {
      try (Channel channel = connection.createChannel(false)) {
        declareHeartbeatTopology(channel, exchange, queue, binding);
      } catch (IOException | TimeoutException e) {
        throw new AmqpException("Failed to declare the heartbeat topology on the control-plane connection", e);
      }
    });
  }

  /**
   * Declares the heartbeat exchange, queue and binding on one open channel.
   *
   * @param channel  the channel to declare on
   * @param exchange the heartbeat exchange
   * @param queue    the per-instance heartbeat queue
   * @param binding  the heartbeat binding
   * @throws IOException      if the channel fails
   * @throws TimeoutException if the broker does not answer the declaration
   */
  private static void declareHeartbeatTopology(Channel channel, TopicExchange exchange, Queue queue, Binding binding)
    throws IOException, TimeoutException {
    channel.exchangeDeclare(
      exchange.getName(),
      exchange.getType(),
      exchange.isDurable(),
      exchange.isAutoDelete(),
      exchange.isInternal(),
      exchange.getArguments()
    );
    channel.queueDeclare(
      queue.getName(),
      queue.isDurable(),
      queue.isExclusive(),
      queue.isAutoDelete(),
      queue.getArguments()
    );
    // The heartbeat topology binds the per-instance queue; there is no exchange-to-exchange
    // hop on this plane.
    channel.queueBind(binding.getDestination(), binding.getExchange(), binding.getRoutingKey(), binding.getArguments());
  }

  /**
   * Declares the heartbeat topology on a fresh control-plane connection, used by the listener
   * container's redeclaration hook after a channel-level failure.
   *
   * @param factory  the control-plane connection factory
   * @param exchange the heartbeat exchange
   * @param queue    the per-instance heartbeat queue
   * @param binding  the heartbeat binding
   * @throws IOException      if the connection or channel fails
   * @throws TimeoutException if the broker does not answer
   */
  private static void declareHeartbeatTopology(
    CachingConnectionFactory factory,
    TopicExchange exchange,
    Queue queue,
    Binding binding
  ) throws IOException, TimeoutException {
    Connection connection = factory.createConnection();
    try (connection; Channel channel = connection.createChannel(false)) {
      declareHeartbeatTopology(channel, exchange, queue, binding);
    }
  }

  /**
   * Resolve the data-plane {@link ConnectionFactory}: Spring Boot's default
   * {@code rabbitConnectionFactory} when present (the standard wiring), else the first registered
   * candidate. The control-plane factory is exposed as a {@link ControlPlaneConnectionFactory}
   * holder, not a {@code ConnectionFactory} bean, so it cannot be picked up by this lookup or by
   * any unqualified host-application injection.
   *
   * <p>Every caller is gated by {@code @ConditionalOnBean(ConnectionFactory.class)}, so a
   * candidate always exists when this resolver runs.
   */
  public static ConnectionFactory dataPlaneConnectionFactory(ListableBeanFactory beanFactory) {
    if (beanFactory.containsBean(DATA_PLANE_CONNECTION_FACTORY_BEAN)) {
      return beanFactory.getBean(DATA_PLANE_CONNECTION_FACTORY_BEAN, ConnectionFactory.class);
    }
    String[] names = beanFactory.getBeanNamesForType(ConnectionFactory.class, true, false);
    if (names.length == 0) {
      throw new NoSuchBeanDefinitionException(ConnectionFactory.class);
    }
    return beanFactory.getBean(names[0], ConnectionFactory.class);
  }

  /**
   * Inner configuration for app-to-Worker reportToWorker routing via DirectExchange.
   * Creates the exchange, message converter, dedicated template, publisher, ring,
   * and reporter; the reporter owns its flush timer and consumer pool.
   * Active by default when a data-plane {@link ConnectionFactory} is present.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnBean(ConnectionFactory.class)
  @ConditionalOnProperty(prefix = "zeta.report", name = "enabled", havingValue = "true", matchIfMissing = true)
  @ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "false", matchIfMissing = true)
  static class ReportConfiguration {

    /**
     * Declare the DirectExchange for reportToWorker routing (app → Worker).
     * Routing keys ({@code reportToWorker.<appName>.<nodeId>}) ensure each key's
     * messages land on the correct worker queue.
     */
    @Bean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public DirectExchange hotkeyReportExchange(ZetaProperties properties) {
      return new DirectExchange(properties.getReportExchange(), true, false);
    }

    /**
     * Create the {@link MessageConverter} for serializing report messages.
     * <p>
     * Jackson JSON (cross-version compatible — unknown fields are ignored,
     * see design decisions: sync messages). Report messages are always JSON; no binary encoding.
     */
    @Bean("zetaReportMessageConverter")
    @ConditionalOnMissingBean(name = "zetaReportMessageConverter")
    public MessageConverter reportMessageConverter() {
      ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      return new Jackson2JsonMessageConverter(mapper);
    }

    /**
     * Dedicated {@link RabbitTemplate} for report publishing, so Zeta's JSON
     * serialization is isolated from the application's own message converter.
     *
     * <p>Deliberately NOT {@code @Primary}: a primary template would hijack every
     * unqualified {@code RabbitTemplate} injection in the host application and
     * silently switch its payload serialization to Zeta's converter. Zeta's own
     * call sites resolve this bean by {@code @Qualifier}.
     */
    @Bean("zetaReportRabbitTemplate")
    @ConditionalOnMissingBean(name = "zetaReportRabbitTemplate")
    public RabbitTemplate zetaReportRabbitTemplate(
      ListableBeanFactory beanFactory,
      @Qualifier("zetaReportMessageConverter") MessageConverter converter
    ) {
      RabbitTemplate template = new RabbitTemplate(dataPlaneConnectionFactory(beanFactory));
      template.setMessageConverter(converter);
      return template;
    }

    /**
     * Create the {@link ReportPublisher} for sending batched access-count reports to the Worker.
     */
    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ReportPublisher reportPublisher(
      @Qualifier("zetaReportRabbitTemplate") RabbitTemplate rabbitTemplate,
      ZetaProperties properties,
      ZetaAppProperties appProperties
    ) {
      return new ReportPublisher(rabbitTemplate, properties.getReportExchange(), appProperties.getAppName());
    }

    /**
     * Create the {@link ConsistentHashRing} for consistent-hashing reportToWorker routing.
     */
    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ConsistentHashRing consistentHashRing(ZetaProperties properties) {
      return new HashRing(properties.getConsistentHashing().getVirtualNodes());
    }

    /**
     * Create the {@link KeyReporter} that publishes per-key add batches
     * routed to the Workers. The reporter owns no counting reservoir — the
     * shared {@code AccessCounter} does (design decisions: access counting) —
     * it owns routing, the bounded dispatcher queue and the feed-loop tuner
     * (design decisions: reporting).
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public KeyReporter hotKeyReporter(
      ReportPublisher reportPublisher,
      ZetaProperties properties,
      ZetaAppProperties appProperties,
      ConsistentHashRing ring,
      HealthView clusterHealthView
    ) {
      ReportFeedLoop feedLoop = null;
      // The tuner is wired only when configured; shadow mode computes the trajectory for the
      // gauges and applies nothing.
      if (properties.getReportIntervalTuning() != ZetaProperties.ReportIntervalTuning.OFF) {
        feedLoop = new ReportFeedLoop(
          ReportFeedLoop.Mode.valueOf(properties.getReportIntervalTuning().name()),
          properties.getReportIntervalTargetBatch(),
          properties.getReportIntervalMinMs(),
          properties.getReportIntervalMaxMs(),
          properties.getReportIntervalMs()
        );
      }
      return new KeyReporterImpl(
        reportPublisher,
        appProperties.getAppName(),
        properties.getQueueCapacity(),
        properties.getQueueOfferTimeoutMs(),
        properties.effectiveConsumerCount(),
        ring,
        clusterHealthView,
        feedLoop
      );
    }
  }

  /**
   * Inner configuration for instance-to-instance cache synchronization.
   * Creates a FanoutExchange, per-instance queue with TTL, binding, publisher,
   * sync listener, and a dedicated scheduled executor.
   * Requires {@code zeta.sync.enabled=true}.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnBean(ConnectionFactory.class)
  @ConditionalOnProperty(prefix = "zeta.sync", name = "enabled", havingValue = "true")
  @ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "false", matchIfMissing = true)
  static class SyncConfiguration {

    /**
     * Create the FanoutExchange for broadcasting INVALIDATE/REFRESH messages
     * to all app instances.
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeySyncExchange")
    public FanoutExchange hotkeySyncExchange(CacheSyncProperties properties) {
      return new FanoutExchange(properties.getExchangeName(), true, false);
    }

    /**
     * Create the per-instance sync queue with a 60-second message TTL and 24-hour idle expiry.
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeySyncQueue")
    public Queue hotkeySyncQueue(CacheSyncProperties properties, NodeIdentity identity) {
      return durableTtlQueue(properties.getQueuePrefix() + ":" + identity.instanceId());
    }

    /**
     * Bind the per-instance queue to the sync exchange.
     */
    @Bean
    public Binding hotkeySyncBinding(
      @Qualifier("hotkeySyncQueue") Queue hotkeySyncQueue,
      @Qualifier("hotkeySyncExchange") FanoutExchange hotkeySyncExchange
    ) {
      return BindingBuilder.bind(hotkeySyncQueue).to(hotkeySyncExchange);
    }

    /**
     * Dedicated {@link RabbitTemplate} for cache-sync publishing, isolated from
     * the container-level shared template so message converters cannot
     * cross-contaminate.
     */
    @Bean("zetaSyncRabbitTemplate")
    @ConditionalOnMissingBean(name = "zetaSyncRabbitTemplate")
    public RabbitTemplate zetaSyncRabbitTemplate(ListableBeanFactory beanFactory) {
      return new RabbitTemplate(dataPlaneConnectionFactory(beanFactory));
    }

    /**
     * The PUT payload codec shared by the sync publisher and the receiver context. One
     * mapper owns both directions, so the configured allowlist cannot drift between the
     * writer and the reader.
     */
    @Bean
    @ConditionalOnMissingBean
    public PutPayloadCodec putPayloadCodec(CacheSyncProperties properties) {
      return new PutPayloadCodec(properties);
    }

    /**
     * Create the cache sync publisher for sending INVALIDATE/REFRESH messages.
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncPublisher cacheSyncPublisher(
      @Qualifier("zetaSyncRabbitTemplate") RabbitTemplate rabbitTemplate,
      CacheSyncProperties properties,
      PutPayloadCodec putPayloadCodec,
      ZetaAppProperties appProperties,
      NodeIdentity identity
    ) {
      // The sync plane stamps zeta.app-name (design decisions: sync messages pattern) so peers on a
      // shared broker can drop another application's INVALIDATE / REFRESH /
      // PUT messages instead of acting on them.
      return new CacheSyncPublisher(rabbitTemplate, properties, putPayloadCodec, appProperties.getAppName(), identity);
    }

    /**
     * Dedicated scheduler for the ordered cache-update dispatchers. Isolated from
     * {@code hotKeyScheduler} so peer traffic can never delay the 50 ms report tide. Peer REFRESH
     * loads run on the dedicated peer-refresh executor, so this pool only runs O(1) ordering and
     * control work.
     *
     * <p>Pool size is the larger of {@code zeta.sync.scheduler-pool-size}
     * (default 4) and {@code zeta.sync.concurrent-consumers × 2}.
     */
    @Bean(name = "hotKeySyncScheduler", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "hotKeySyncScheduler")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ScheduledExecutorService hotKeySyncScheduler(CacheSyncProperties properties) {
      return newJitterScheduler(properties, "sync");
    }

    /**
     * Framework-owned {@link DefaultSyncContext}: loader-backed REFRESH, version-guarded
     * INVALIDATE, whole-table clear (design decisions: invalidation, design decisions: cache
     * table) and value-carrying PUT apply. The context is not a user extension point: the
     * policy decides, the context owns every guard and hook. Every optional collaborator
     * degrades to its no-op form rather than failing startup: no cluster loader skips
     * loader-backed reloads, and no SingleFlight leaves the dedup entry in place on removal
     * (design decisions: dedup lifecycle).
     */
    @Bean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public DefaultSyncContext syncContext(
      Cache<String, CacheEntry> hotLocalCache,
      ObjectProvider<CacheLoader<String, Object>> hotKeyClusterLoaderProvider,
      EntryLifecycle entryLifecycle,
      ObjectProvider<SingleFlight> singleFlightProvider,
      ObjectProvider<SyncHook> syncHookProvider,
      ObjectProvider<PutPayloadCodec> putPayloadCodecProvider,
      @Qualifier("hotKeySyncScheduler") ScheduledExecutorService syncScheduler,
      @Qualifier("peerRefreshExecutor") Executor refreshExecutor,
      ObjectProvider<StampClock> stampClockProvider,
      ObjectProvider<HeatView> heatViewProvider,
      ObjectProvider<KeepWarm> warmProvider,
      NamespacePolicyRegistry policies
    ) {
      return new DefaultSyncContext(
        hotLocalCache,
        hotKeyClusterLoaderProvider.getIfAvailable(),
        entryLifecycle,
        syncHookProvider.stream().toList(),
        singleFlightProvider.getIfAvailable(),
        putPayloadCodecProvider.getIfAvailable(),
        syncScheduler,
        refreshExecutor,
        stampClockProvider.getIfAvailable(),
        heatViewProvider.getIfAvailable(),
        warmProvider.getIfAvailable(),
        policies
      );
    }

    /**
     * Default {@link SyncPolicy}: the interface's default methods run the framework's
     * standard sync pipeline through the {@link DefaultSyncContext}.
     */
    @Bean
    @ConditionalOnMissingBean(SyncPolicy.class)
    public SyncPolicy syncPolicy() {
      return new SyncPolicy() {};
    }

    /**
     * Create the sync listener that handles incoming INVALIDATE/REFRESH messages from peers.
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncListener cacheSyncListener(
      CacheSyncProperties properties,
      @Qualifier("hotKeySyncScheduler") ScheduledExecutorService syncScheduler,
      SyncPolicy policy,
      DefaultSyncContext context,
      ZetaAppProperties appProperties,
      ObjectProvider<StampClock> stampClockProvider,
      NodeIdentity identity
    ) {
      // Receiver-side half of the sync-plane filter (design decisions: sync messages): a message stamped with
      // a different application's name is dropped (ack'd) instead of applied.
      return new CacheSyncListener(
        properties,
        syncScheduler,
        policy,
        context,
        appProperties.getAppName(),
        stampClockProvider.getIfAvailable(),
        identity
      );
    }

    /**
     * Create the AMQP message listener container that drives the sync listener.
     */
    @Bean
    @ConditionalOnMissingBean(name = "syncListenerContainer")
    public SimpleMessageListenerContainer syncListenerContainer(
      ListableBeanFactory beanFactory,
      @Qualifier("hotkeySyncQueue") Queue hotkeySyncQueue,
      CacheSyncListener cacheSyncListener,
      CacheSyncProperties properties
    ) {
      return newManualAckContainer(
        dataPlaneConnectionFactory(beanFactory),
        hotkeySyncQueue,
        properties,
        (msg, channel) -> cacheSyncListener.onMessage(channel, msg),
        "Sync listener uncaught exception (message will be requeued by container)"
      );
    }
  }

  /**
   * Inner configuration for receiving Worker HOT/COOL decisions.
   * Creates a DirectExchange, per-instance queue with TTL, binding, worker listener,
   * listener container, and a dedicated scheduled executor.
   * Requires a data-plane {@link ConnectionFactory} and
   * {@code zeta.worker-listener.enabled=true}.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnBean(ConnectionFactory.class)
  @ConditionalOnProperty(prefix = "zeta.worker-listener", name = "enabled", havingValue = "true")
  @ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "false", matchIfMissing = true)
  @Slf4j
  static class WorkerListenerConfiguration {

    /**
     * Create the dedicated notification executor for passive heat-lease endings, so a user
     * listener never runs on a Caffeine write lock or a caller's read thread.
     *
     * <p>It lives here, with the lease table it serves: nothing else in the application can end a
     * lease without a Worker decision, so an app-only deployment with the listener disabled must
     * not pay for either the thread or the table.
     *
     * @return a single-thread daemon executor
     */
    @Bean(name = "heatNotifyExecutor", destroyMethod = "shutdownNow")
    @ConditionalOnMissingBean(name = "heatNotifyExecutor")
    public Executor heatNotifyExecutor() {
      return Executors.newSingleThreadExecutor(new ZetaThreadFactory("zeta-heat"));
    }

    /**
     * Create the cluster heat lease table.
     *
     * @param properties         the live configuration
     * @param heatNotifyExecutor the passive-end notification executor
     * @param healthViewProvider the Worker liveness view, when present
     * @param ringProvider       the consistent-hash ring, when present
     * @return a new lease table
     */
    @Bean
    @ConditionalOnMissingBean
    public HeatDirectory heatDirectory(
      ZetaProperties properties,
      @Qualifier("heatNotifyExecutor") Executor heatNotifyExecutor,
      ObjectProvider<HealthView> healthViewProvider,
      ObjectProvider<ConsistentHashRing> ringProvider
    ) {
      return new HeatDirectory(
        properties.getWarm().getMaxClusterLeases(),
        properties.getWarm().getCoolTombstoneMs(),
        heatNotifyExecutor,
        healthViewProvider.getIfAvailable(),
        ringProvider.getIfAvailable()
      );
    }

    /**
     * Create the direct exchange the Worker publishes HOT/COOL decisions to.
     *
     * <p>Every instance of this application binds its queue under {@code send.<appName>}, so the
     * worker's single publish still reaches all of them, while instances of another application
     * on the same broker receive nothing (design decisions: sync messages).
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyWorkerExchange")
    public DirectExchange hotkeyWorkerExchange(WorkerListenerProperties properties) {
      return new DirectExchange(properties.getExchangeName(), true, false);
    }

    /**
     * Create the per-instance Worker listener queue with a 60-second message TTL and 24-hour idle expiry.
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyWorkerQueue")
    public Queue hotkeyWorkerQueue(WorkerListenerProperties properties, NodeIdentity identity) {
      return durableTtlQueue(properties.getQueuePrefix() + ":" + identity.instanceId());
    }

    /**
     * Bind the per-instance queue to the Worker exchange under this application's decision
     * routing key.
     *
     * <p>The key — {@code send.<appName>} — is the whole isolation mechanism: it is what the
     * Worker publishes under, and it is what keeps another application's decisions out of this
     * queue on a shared broker. {@code zeta.app-name} is validated non-blank at binding time.
     */
    @Bean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public Binding hotkeyWorkerBinding(
      @Qualifier("hotkeyWorkerQueue") Queue hotkeyWorkerQueue,
      @Qualifier("hotkeyWorkerExchange") DirectExchange hotkeyWorkerExchange,
      ZetaAppProperties appProperties
    ) {
      return BindingBuilder.bind(hotkeyWorkerQueue)
        .to(hotkeyWorkerExchange)
        .with(ZetaConstants.Routing.PREFIX_DECISION + appProperties.getAppName());
    }

    /**
     * Dedicated scheduler for jitter-delayed cache-update tasks received from
     * the Worker (HOT/COOL decisions). Isolated from {@code hotKeyScheduler}
     * so that synchronous cluster-loader reads performed by {@code handleHot}
     * can never delay the shared maintenance ticks.
     */
    @Bean(name = "hotKeyWorkerSchedScheduler", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "hotKeyWorkerSchedScheduler")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ScheduledExecutorService hotKeyWorkerSchedScheduler(WorkerListenerProperties properties) {
      return newJitterScheduler(properties, "worker");
    }

    /**
     * Default {@link HotKeyHandler}: prewarm a missing entry on HOT and do nothing on COOL.
     *
     * @return the replaceable handler
     */
    @Bean
    @ConditionalOnMissingBean(HotKeyHandler.class)
    public HotKeyHandler hotKeyHandler() {
      return new HotKeyHandler() {};
    }

    /**
     * Create the listener that processes HOT/COOL decisions sent by the Worker.
     *
     * <p>The app's {@code zeta.app-name} is passed in so foreign-app decisions can be dropped
     * at reception. The mode is the receiver-side boundary rather than the delivery mechanism:
     * a queue bound to the wrong key must not change this application's heat leases.
     *
     * @param properties           the listener configuration
     * @param workerSchedScheduler the ordered dispatcher scheduler
     * @param heatDirectory        the cluster lease table
     * @param handler              the replaceable HOT/COOL action
     * @param listenerProvider     the best-effort observers
     * @param hotKeyCache          the L1 engine used by prewarm
     * @param heatView             the heat query
     * @param policies             the namespace policy registry
     * @param warmProvider         the local renewal scheduler
     * @param entryLifecycle       the entry lifecycle owning the shared TTL arithmetic
     * @param appProperties        this application's identity
     * @return the decision listener
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkerListener workerListener(
      WorkerListenerProperties properties,
      @Qualifier("hotKeyWorkerSchedScheduler") ScheduledExecutorService workerSchedScheduler,
      HeatDirectory heatDirectory,
      HotKeyHandler handler,
      ObjectProvider<HotKeyListener> listenerProvider,
      HotKeyCache hotKeyCache,
      HeatView heatView,
      NamespacePolicyRegistry policies,
      ObjectProvider<KeepWarm> warmProvider,
      EntryLifecycle entryLifecycle,
      ZetaAppProperties appProperties
    ) {
      TtlPolicy ttlPolicy = entryLifecycle.ttlPolicy();
      KeepWarm keepWarm = warmProvider.getIfAvailable();
      HotKeyContextFactory contexts = HotKeyContextFactory.of(hotKeyCache, heatView, policies, keepWarm, ttlPolicy);
      return new WorkerListener(
        properties,
        workerSchedScheduler,
        heatDirectory,
        handler,
        listenerProvider.stream().toList(),
        contexts,
        keepWarm,
        appProperties.getAppName()
      );
    }

    /**
     * Create the AMQP message listener container that processes Worker heartbeat messages.
     *
     * <p>The heartbeat topology (exchange, per-instance auto-delete queue, binding) is built here
     * and declared only on the control-plane connection, on every one of its (re)connects. It is
     * deliberately not a set of {@code @Bean} declarations: the data-plane {@code RabbitAdmin}
     * would declare the same auto-delete queue on the data-plane connection, which belongs to a
     * different connection and therefore cannot keep the control-plane queue alive — and a
     * declaration that only half the wiring knows about is the kind of duplicate that hides which
     * connection actually owns the queue.
     */
    @Bean("hotkeyHeartbeatContainer")
    @ConditionalOnMissingBean(name = "hotkeyHeartbeatContainer")
    public SimpleMessageListenerContainer heartbeatContainer(
      @Qualifier("zetaHeartbeatConnectionFactory") ControlPlaneConnectionFactory controlPlane,
      HealthView healthView,
      ZetaProperties properties,
      NodeIdentity identity,
      ZetaAppProperties appProperties,
      ObjectProvider<ConsistentHashRing> ringProvider
    ) {
      TopicExchange heartbeatExchange = new TopicExchange(properties.getHeartbeat().getExchangeName(), true, false);
      Queue heartbeatQueue = QueueBuilder.nonDurable(HEARTBEAT_QUEUE_PREFIX + identity.instanceId())
        .autoDelete()
        .build();
      Binding heartbeatBinding = BindingBuilder.bind(heartbeatQueue).to(heartbeatExchange).with(KEY_HEARTBEAT + "*");
      declareOnControlPlane(controlPlane.connectionFactory(), heartbeatExchange, heartbeatQueue, heartbeatBinding);
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(controlPlane.connectionFactory()) {
        @Override
        protected void redeclareElementsIfNecessary() {
          // A channel-level failure can drop the auto-delete queue without a reconnect, so the
          // connection listener alone cannot repair it; redeclare on the container's own check.
          try {
            declareHeartbeatTopology(
              controlPlane.connectionFactory(),
              heartbeatExchange,
              heartbeatQueue,
              heartbeatBinding
            );
          } catch (Exception e) {
            log.warn("Failed to redeclare heartbeat topology on the control-plane connection", e);
          }
        }
      };
      container.setQueueNames(heartbeatQueue.getName());
      container.setAcknowledgeMode(AcknowledgeMode.NONE);
      container.setConcurrentConsumers(1);
      container.setPrefetchCount(100);
      container.setMissingQueuesFatal(false);
      container.setErrorHandler(t -> log.warn("Heartbeat listener uncaught exception (message discarded)", t));
      AppIsolationFilter isolation = new AppIsolationFilter(appProperties.getAppName());
      ConsistentHashRing ring = ringProvider.getIfAvailable();
      container.setMessageListener(msg -> {
        // Heartbeats share one topic exchange and bind under heartbeat.*; without the appName
        // header a foreign Worker would enter this tenant's ring and become a routing target.
        if (isolation.dropIfForeign(msg, log, "worker heartbeat")) {
          return;
        }
        WorkerHeartbeatMessage heartbeat = WorkerHeartbeatMessage.from(msg);
        if (heartbeat != null) {
          healthView.onHeartbeat(heartbeat);
          if (ring != null) {
            ring.rebuildIfPossible(healthView);
          }
        }
      });
      return container;
    }

    /**
     * Create the AMQP message listener container that processes Worker HOT/COOL decisions
     * via the {@link WorkerListener}.
     *
     * <p>The container uses {@link AcknowledgeMode#MANUAL} because the
     * {@link WorkerListener#onMessage} performs its own ack/nack
     * (ack-before-update pattern, see design decisions: sync messages).
     */
    @Bean
    @ConditionalOnMissingBean(name = "workerListenerContainer")
    public SimpleMessageListenerContainer workerListenerContainer(
      ListableBeanFactory beanFactory,
      @Qualifier("hotkeyWorkerQueue") Queue hotkeyWorkerQueue,
      WorkerListener workerListener,
      WorkerListenerProperties properties
    ) {
      return newManualAckContainer(
        dataPlaneConnectionFactory(beanFactory),
        hotkeyWorkerQueue,
        properties,
        (msg, channel) -> workerListener.onMessage(channel, msg),
        "Worker listener uncaught exception (message will be requeued by container)"
      );
    }
  }

  /**
   * Dedicated {@link CachingConnectionFactory} for heartbeat traffic, isolated from the
   * data-plane connection (report, sync, broadcast) so control-plane liveliness cannot be
   * affected by data-plane congestion or broker flow control on the shared connection.
   *
   * <p>The factory is built through Spring Boot's
   * {@link RabbitConnectionFactoryBeanConfigurer} bean — the sanctioned extension point for custom
   * connection factories — so the control plane inherits the full {@link RabbitProperties}
   * surface with the same semantics as the Boot-managed data-plane factory: credentials,
   * virtual host, heartbeat, timeouts, {@code spring.rabbitmq.addresses} failover,
   * {@code spring.rabbitmq.ssl.*} (TLS), {@code RabbitConnectionDetails} and
   * {@code spring.rabbitmq.ssl.bundle}. Boot's {@link CachingConnectionFactoryConfigurer} and
   * every {@link ConnectionFactoryCustomizer} are applied as well, so the wrapper's address list,
   * shuffle mode and channel cache match the data plane.
   *
   * <p><b>Holder, not a {@code ConnectionFactory} bean.</b> The factory is exposed through
   * {@link ControlPlaneConnectionFactory}, so it is never a candidate for a host application's
   * unqualified {@code ConnectionFactory}, {@code RabbitTemplate}, {@code AmqpAdmin} or
   * {@code @RabbitListener} container factory. A {@code @Primary} factory would capture all of
   * them and route the host's business traffic over the heartbeat connection; a plain
   * {@code ConnectionFactory} bean without {@code @Primary} would still make Spring Boot's
   * {@code @ConditionalOnSingleCandidate} template back off. Both are avoided by keeping the
   * holder type distinct and resolving it only through its qualifier inside Zeta.
   *
   * <p><b>Channel mapping (design decisions: heartbeat and health):</b> this factory carries
   * app heartbeat consumption and worker heartbeat production; Boot's
   * {@code rabbitConnectionFactory} carries report publish/consume, cache-sync
   * publish/consume, worker decision consume and worker HOT/COOL broadcast.
   *
   * <p><b>Gating:</b> this configuration only activates when a feature that actually consumes
   * the control-plane connection is enabled — the app-side worker listener or worker mode —
   * and a data-plane {@link ConnectionFactory} exists. Without the factory the holder would
   * build an unconfigured connection factory that dials {@code localhost:5672}, so an
   * application that keeps the AMQP classes on the classpath but has no broker would open a
   * second, unintended connection.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnBean(ConnectionFactory.class)
  @Conditional(HeartbeatFeatureEnabled.class)
  static class HeartbeatConnectionConfiguration {

    @Bean("zetaHeartbeatConnectionFactory")
    @ConditionalOnMissingBean(name = "zetaHeartbeatConnectionFactory")
    public ControlPlaneConnectionFactory heartbeatConnectionFactory(
      ObjectProvider<RabbitProperties> propertiesProvider,
      ObjectProvider<RabbitConnectionFactoryBeanConfigurer> configurerProvider,
      ObjectProvider<CachingConnectionFactoryConfigurer> cachingConfigurerProvider,
      ObjectProvider<ConnectionFactoryCustomizer> customizerProvider,
      ResourceLoader resourceLoader
    ) {
      RabbitProperties properties = propertiesProvider.getIfAvailable();
      if (properties == null) {
        // Fallback: an unconfigured factory; the connection then uses the
        // driver defaults (localhost:5672). In a normal Boot application the
        // RabbitProperties bean is always present.
        return new ControlPlaneConnectionFactory(new CachingConnectionFactory());
      }
      RabbitConnectionFactoryBean factoryBean = new RabbitConnectionFactoryBean();
      RabbitConnectionFactoryBeanConfigurer bootConfigurer = configurerProvider.getIfAvailable();
      if (bootConfigurer != null) {
        // Boot's configurer carries RabbitConnectionDetails and SslBundles as well as the
        // properties; constructing a private one dropped both.
        bootConfigurer.configure(factoryBean);
      } else {
        new RabbitConnectionFactoryBeanConfigurer(resourceLoader, properties).configure(factoryBean);
      }
      try {
        factoryBean.afterPropertiesSet();
        com.rabbitmq.client.ConnectionFactory underlying = factoryBean.getObject();
        Assert.state(underlying != null, "RabbitConnectionFactoryBean produced no ConnectionFactory");
        customizerProvider.orderedStream().forEach(customizer -> customizer.customize(underlying));

        CachingConnectionFactory factory = new CachingConnectionFactory(underlying);
        CachingConnectionFactoryConfigurer configurer = cachingConfigurerProvider.getIfAvailable();
        if (configurer != null) {
          // Boot's configurer owns the cluster addresses and shuffle mode shared with the
          // data-plane factory.
          configurer.configure(factory);
        } else {
          List<String> addresses = properties.getAddresses();
          if (addresses != null && !addresses.isEmpty()) {
            factory.setAddresses(addresses);
          }
          factory.setAddressShuffleMode(properties.getAddressShuffleMode());
        }
        // The control plane sends heartbeats and consumes them; publisher confirms and returns
        // would add ACK frames and correlation state for a message that has no caller to settle.
        factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.NONE);
        factory.setPublisherReturns(false);
        factory.setCacheMode(CachingConnectionFactory.CacheMode.CHANNEL);
        return new ControlPlaneConnectionFactory(factory);
      } catch (Exception ex) {
        throw new IllegalStateException("Failed to create RabbitConnectionFactory", ex);
      }
    }
  }

  /**
   * Internal holder for the control-plane {@link CachingConnectionFactory}. The holder type is
   * deliberately not a {@link ConnectionFactory}: a {@code ConnectionFactory} bean would join
   * every unqualified injection point and Boot's {@code @ConditionalOnSingleCandidate} decision,
   * sharing the host application's business MQ traffic with the heartbeat connection. Zeta's
   * heartbeat template and listener container receive this holder by qualifier and unwrap it
   * through {@link #connectionFactory()}.
   *
   * <p>Delegating {@link InitializingBean} and {@link DisposableBean} keeps the wrapped
   * factory's lifecycle intact even though it is not a bean itself: Spring initializes and
   * destroys the holder, and the holder delegates both calls to the connection factory it owns.
   */
  public static final class ControlPlaneConnectionFactory implements InitializingBean, DisposableBean {

    private final CachingConnectionFactory delegate;

    /**
     * @param delegate the heartbeat connection factory this holder owns
     */
    public ControlPlaneConnectionFactory(CachingConnectionFactory delegate) {
      this.delegate = delegate;
    }

    /**
     * The wrapped connection factory.
     *
     * @return the heartbeat {@link CachingConnectionFactory}
     */
    public CachingConnectionFactory connectionFactory() {
      return delegate;
    }

    @Override
    public void afterPropertiesSet() {
      delegate.afterPropertiesSet();
    }

    @Override
    public void destroy() {
      delegate.destroy();
    }
  }

  /**
   * Condition matching when any Zeta feature that consumes the dedicated
   * control-plane connection factory is enabled: the app-side worker listener
   * ({@code zeta.worker-listener.enabled=true}) or worker mode
   * ({@code zeta.worker.enabled=true}). Both run control-plane traffic
   * (heartbeat consumption and production). Report and cache-sync are pure data-plane features and
   * must not drag in the extra control-plane connection holder.
   */
  static class HeartbeatFeatureEnabled extends AnyNestedCondition {

    HeartbeatFeatureEnabled() {
      super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnProperty(prefix = "zeta.worker-listener", name = "enabled", havingValue = "true")
    static class WorkerListenerEnabled {}

    @ConditionalOnProperty(prefix = "zeta.worker", name = "enabled", havingValue = "true")
    static class WorkerEnabled {}
  }
}
