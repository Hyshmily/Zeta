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
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.reporting.ReportPublisher;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.reporting.impl.ReportFeedLoop;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
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

  /** Bean name of Zeta's dedicated control-plane (heartbeat) connection factory. */
  private static final String CONTROL_PLANE_CONNECTION_FACTORY_BEAN = "zetaHeartbeatConnectionFactory";

  /** Bean name of Spring Boot's default (data-plane) RabbitMQ connection factory. */
  private static final String DATA_PLANE_CONNECTION_FACTORY_BEAN = "rabbitConnectionFactory";

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
    return Executors.newScheduledThreadPool(
      poolSize,
      new ZetaThreadFactory(ZetaConstants.Threads.PREFIX_SCHEDULER + "-" + component + "-")
    );
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
    return container;
  }

  /**
   * Resolve the data-plane {@link ConnectionFactory}: Spring Boot's default
   * {@code rabbitConnectionFactory} when present (the standard wiring), else
   * the user's own factory — the first candidate that is not Zeta's reserved
   * control-plane bean name. The control-plane factory is exposed as a
   * {@link ControlPlaneConnectionFactory} holder, not a {@code ConnectionFactory}
   * bean, so it cannot be picked up by this lookup or by any unqualified
   * host-application injection.
   *
   * <p>Every caller is gated by {@link DataPlaneConnectionFactoryPresent}, so a
   * candidate always exists when this resolver runs.
   */
  public static ConnectionFactory dataPlaneConnectionFactory(ListableBeanFactory beanFactory) {
    String beanName = findDataPlaneConnectionFactoryBeanName(beanFactory);
    if (beanName == null) {
      throw new NoSuchBeanDefinitionException(ConnectionFactory.class);
    }
    return beanFactory.getBean(beanName, ConnectionFactory.class);
  }

  /**
   * The bean name shared by the resolver and the condition: Boot's default name
   * first, otherwise the first non-control-plane candidate.
   */
  private static @Nullable String findDataPlaneConnectionFactoryBeanName(ListableBeanFactory beanFactory) {
    if (beanFactory.containsBean(DATA_PLANE_CONNECTION_FACTORY_BEAN)) {
      return DATA_PLANE_CONNECTION_FACTORY_BEAN;
    }
    for (String name : beanFactory.getBeanNamesForType(ConnectionFactory.class)) {
      if (!CONTROL_PLANE_CONNECTION_FACTORY_BEAN.equals(name)) {
        return name;
      }
    }
    return null;
  }

  /**
   * Inner configuration for app-to-Worker reportToWorker routing via DirectExchange.
   * Creates the exchange, message converter, dedicated template, publisher, ring,
   * and reporter; the reporter owns its flush timer and consumer pool.
   * Active by default when a {@link RabbitTemplate} bean is present.
   */
  @Configuration
  @ConditionalOnBean(RabbitTemplate.class)
  @Conditional(DataPlaneConnectionFactoryPresent.class)
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
      KeyReporterImpl reporter = new KeyReporterImpl(
        reportPublisher,
        appProperties.getAppName(),
        properties.getQueueCapacity(),
        properties.getQueueOfferTimeoutMs(),
        properties.effectiveConsumerCount(),
        ring,
        clusterHealthView
      );
      // The tuner is wired only when configured; shadow mode computes the trajectory for the
      // gauges and applies nothing.
      if (properties.getReportIntervalTuning() != ZetaProperties.ReportIntervalTuning.OFF) {
        reporter.setIntervalFeedLoop(
          new ReportFeedLoop(
            ReportFeedLoop.Mode.valueOf(properties.getReportIntervalTuning().name()),
            properties.getReportIntervalTargetBatch(),
            properties.getReportIntervalMinMs(),
            properties.getReportIntervalMaxMs(),
            properties.getReportIntervalMs()
          )
        );
      }
      return reporter;
    }
  }

  /**
   * Inner configuration for instance-to-instance cache synchronization.
   * Creates a FanoutExchange, per-instance queue with TTL, binding, publisher,
   * sync listener, and a dedicated scheduled executor.
   * Requires {@code zeta.sync.enabled=true}.
   */
  @Configuration
  @Conditional(DataPlaneConnectionFactoryPresent.class)
  @ConditionalOnProperty(prefix = "zeta.sync", name = "enabled", havingValue = "true")
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
    public Binding hotkeySyncBinding(Queue hotkeySyncQueue, FanoutExchange hotkeySyncExchange) {
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
     * Dedicated scheduler for jitter-delayed cache-update tasks received from
     * peer instances. Isolated from {@code hotKeyScheduler} so that synchronous
     * cluster-loader reads performed by {@code handleRefresh} can never delay
     * the 50 ms report tide.
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
      ObjectProvider<StampClock> stampClockProvider
    ) {
      return new DefaultSyncContext(
        hotLocalCache,
        hotKeyClusterLoaderProvider.getIfAvailable(),
        entryLifecycle,
        syncHookProvider.stream().toList(),
        singleFlightProvider.getIfAvailable(),
        putPayloadCodecProvider.getIfAvailable(),
        syncScheduler,
        stampClockProvider.getIfAvailable()
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
   * Matches when a DATA-PLANE {@link ConnectionFactory} bean exists — any
   * candidate except the reserved {@code zetaHeartbeatConnectionFactory} name.
   * The control-plane connection is held by {@link ControlPlaneConnectionFactory},
   * not a {@code ConnectionFactory} bean, so it can never satisfy this
   * condition; the name exclusion is the defensive boundary for a user-supplied
   * factory under Zeta's reserved name.
   */
  static class DataPlaneConnectionFactoryPresent implements ConfigurationCondition {

    @Override
    @NonNull
    public ConfigurationPhase getConfigurationPhase() {
      return ConfigurationPhase.REGISTER_BEAN;
    }

    @Override
    public boolean matches(ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
      ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
      return beanFactory != null && findDataPlaneConnectionFactoryBeanName(beanFactory) != null;
    }
  }

  /**
   * Inner configuration for receiving Worker HOT/COOL decisions.
   * Creates a FanoutExchange, per-instance queue with TTL, binding, worker listener,
   * listener container, and a dedicated scheduled executor.
   * Requires a data-plane {@link ConnectionFactory} (see
   * {@link DataPlaneConnectionFactoryPresent}) and
   * {@code zeta.worker-listener.enabled=true}.
   */
  @Configuration
  @Conditional(DataPlaneConnectionFactoryPresent.class)
  @ConditionalOnProperty(prefix = "zeta.worker-listener", name = "enabled", havingValue = "true")
  @Slf4j
  static class WorkerListenerConfiguration {

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
      Queue hotkeyWorkerQueue,
      DirectExchange hotkeyWorkerExchange,
      ZetaAppProperties appProperties
    ) {
      return BindingBuilder.bind(hotkeyWorkerQueue)
        .to(hotkeyWorkerExchange)
        .with(ZetaConstants.Routing.PREFIX_DECISION + appProperties.getAppName());
    }

    /**
     * Create the TopicExchange for Worker heartbeat broadcasts.
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyHeartbeatExchange")
    public TopicExchange hotkeyHeartbeatExchange(ZetaProperties properties) {
      return new TopicExchange(properties.getHeartbeat().getExchangeName(), true, false);
    }

    /**
     * Create the per-instance non-durable heartbeat queue that auto-deletes on disconnect.
     */
    @Bean
    public Queue hotkeyHeartbeatQueue(NodeIdentity identity) {
      return QueueBuilder.nonDurable("zeta.heartbeat:" + identity.instanceId()).autoDelete().build();
    }

    /**
     * Bind the per-instance heartbeat queue to the heartbeat exchange with routing key {@code heartbeat.*}.
     */
    @Bean
    public Binding hotkeyHeartbeatBinding(Queue hotkeyHeartbeatQueue, TopicExchange hotkeyHeartbeatExchange) {
      return BindingBuilder.bind(hotkeyHeartbeatQueue).to(hotkeyHeartbeatExchange).with(KEY_HEARTBEAT + "*");
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
     * Framework-owned {@link DefaultWorkerContext}: the guarded write path for HOT
     * promotion and COOL downgrade. The context is not a user extension point: the policy
     * decides, the context owns every guard and hook.
     *
     * <p>The shard-ownership refinement for stale cross-Worker COOLs (scale re-balance
     * ghost-COOL kill — see {@link DefaultWorkerContext}) is wired from the ring and the
     * shared health view when present; both are optional so a report-disabled deployment
     * still builds (the guard then fails open to the pure version-guard behaviour).
     */
    @Bean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public DefaultWorkerContext workerContext(
      Cache<String, CacheEntry> hotLocalCache,
      ObjectProvider<CacheLoader<String, Object>> hotKeyClusterLoaderProvider,
      EntryLifecycle entryLifecycle,
      StampClock stampClock,
      ObjectProvider<WorkerDecisionHook> workerDecisionHookProvider,
      ObjectProvider<ConsistentHashRing> ringProvider,
      ObjectProvider<HealthView> healthViewProvider
    ) {
      return new DefaultWorkerContext(
        hotLocalCache,
        hotKeyClusterLoaderProvider.getIfAvailable(),
        entryLifecycle,
        stampClock,
        workerDecisionHookProvider.stream().toList(),
        ringProvider.getIfAvailable(),
        healthViewProvider.getIfAvailable()
      );
    }

    /**
     * Default {@link WorkerPolicy}: the interface's default methods run the framework's
     * standard HOT/COOL pipeline through the {@link DefaultWorkerContext}.
     */
    @Bean
    @ConditionalOnMissingBean(WorkerPolicy.class)
    public WorkerPolicy workerPolicy() {
      return new WorkerPolicy() {};
    }

    /**
     * Create the listener that processes HOT/COOL decisions sent by the Worker.
     *
     * <p>The app's {@code zeta.app-name} is passed in so foreign-app decisions can be
     * dropped at reception. Broker routing already keeps them out of this queue (the binding
     * key is {@code send.<appName>}, design decisions: sync messages), so the header check is
     * the receiver-side boundary rather than the delivery mechanism: a queue bound to the wrong
     * key — an operator mistake or a leftover queue — still must not promote keys in this
     * application's cache. The declared application name is validated non-blank at binding time.
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkerListener workerListener(
      WorkerListenerProperties properties,
      @Qualifier("hotKeyWorkerSchedScheduler") ScheduledExecutorService workerSchedScheduler,
      WorkerPolicy policy,
      DefaultWorkerContext context,
      ZetaAppProperties appProperties
    ) {
      return new WorkerListener(properties, workerSchedScheduler, policy, context, appProperties.getAppName());
    }

    /**
     * Create the AMQP message listener container that processes Worker heartbeat messages.
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyHeartbeatContainer")
    public SimpleMessageListenerContainer heartbeatContainer(
      @Qualifier("zetaHeartbeatConnectionFactory") ControlPlaneConnectionFactory controlPlane,
      HealthView healthView,
      Queue hotkeyHeartbeatQueue,
      ZetaAppProperties appProperties,
      ObjectProvider<ConsistentHashRing> ringProvider
    ) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(controlPlane.connectionFactory());
      container.setQueueNames(hotkeyHeartbeatQueue.getName());
      container.setAcknowledgeMode(AcknowledgeMode.NONE);
      container.setConcurrentConsumers(1);
      container.setPrefetchCount(100);
      container.setErrorHandler(t -> log.warn("Heartbeat listener uncaught exception (message discarded)", t));
      AppIsolationFilter isolation = new AppIsolationFilter(appProperties.getAppName());
      container.setMessageListener(msg -> {
        // Heartbeats share one topic exchange and bind under heartbeat.*; without the appName
        // header a foreign Worker would enter this tenant's ring and become a routing target.
        if (isolation.dropIfForeign(msg, log, "worker heartbeat")) {
          return;
        }
        WorkerHeartbeatMessage heartbeat = WorkerHeartbeatMessage.from(msg);
        if (heartbeat != null) {
          healthView.onHeartbeat(heartbeat);
          ConsistentHashRing ring = ringProvider.getIfAvailable();
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
   * {@link RabbitConnectionFactoryBeanConfigurer} — the sanctioned extension point for custom
   * connection factories — so the control plane inherits the full {@link RabbitProperties}
   * surface with the same semantics as the Boot-managed data-plane factory: credentials,
   * virtual host, heartbeat, timeouts and {@code spring.rabbitmq.ssl.*} (TLS).
   * {@code spring.rabbitmq.ssl.bundle} is not supported (it requires a Boot-internal factory
   * bean subtype).
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
   * see {@link HeartbeatFeatureEnabled}.
   */
  @Configuration
  @Conditional(HeartbeatFeatureEnabled.class)
  static class HeartbeatConnectionConfiguration {

    @Bean("zetaHeartbeatConnectionFactory")
    @ConditionalOnMissingBean(name = "zetaHeartbeatConnectionFactory")
    @SuppressWarnings("all")
    public ControlPlaneConnectionFactory heartbeatConnectionFactory(
      ObjectProvider<RabbitProperties> propertiesProvider,
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
      new RabbitConnectionFactoryBeanConfigurer(resourceLoader, properties).configure(factoryBean);
      try {
        factoryBean.afterPropertiesSet();
        com.rabbitmq.client.ConnectionFactory underlying = factoryBean.getObject();
        Assert.state(underlying != null, "RabbitConnectionFactoryBean produced no ConnectionFactory");

        return new ControlPlaneConnectionFactory(new CachingConnectionFactory(underlying));
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
