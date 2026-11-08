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
import io.github.hyshmily.zeta.sharding.ConsistentHashRingImpl;
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
@lombok.extern.slf4j.Slf4j
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
   *
   * @param queueName the per-instance queue name
   * @return a durable {@link Queue} with {@code x-message-ttl} of 60 seconds
   *         and {@code x-expires} of 24 hours
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
   *
   * @param properties the consumer-plane configuration (pool size and consumer count)
   * @param component thread-name component for the factory ({@code "sync"}, {@code "worker"})
   * @return a daemon-thread scheduled executor named {@code zeta-scheduler-<component>-N}
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
   *
   * @param connectionFactory the (data-plane) connection factory
   * @param queue the queue to consume
   * @param properties the consumer-plane configuration
   * @param listener the channel-aware message handler performing its own ack/nack
   * @param uncaughtWarnMessage the WARN text for an uncaught listener exception
   * @return a configured {@link SimpleMessageListenerContainer}
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
   * the user's own factory — the first candidate that is not Zeta's dedicated
   * control-plane factory. Type-based resolution with an explicit exclusion of
   * the control-plane factory keeps Zeta's own heartbeat connection from
   * hijacking the data-plane beans through its {@code @Primary} marker, while
   * still working when the user names their factory something other than
   * {@code rabbitConnectionFactory}.
   *
   * <p>Every caller is gated by {@link DataPlaneConnectionFactoryPresent}, so a
   * candidate always exists when this resolver runs.
   *
   * @param beanFactory the listable bean factory for name-aware lookup
   * @return the data-plane connection factory (never {@code null})
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
   *
   * @param beanFactory the listable bean factory for name-aware lookup
   * @return the data-plane factory's bean name, or {@code null} when absent
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
     *
     * @param properties the HotKey configuration properties
     * @return a durable, non-auto-delete {@link DirectExchange}
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
     *
     * @return a forward-compatible Jackson JSON converter
     */
    @Bean("zetaReportMessageConverter")
    @ConditionalOnMissingBean(name = "zetaReportMessageConverter")
    public MessageConverter reportMessageConverter() {
      ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      return new Jackson2JsonMessageConverter(mapper);
    }

    /**
     * Dedicated {@link RabbitTemplate} for report publishing.
     * Uses a dedicated instance (not the container-level shared template) so that
     * Zeta's JSON serialization is isolated from the application's own message
     * converter.
     *
     * <p>Deliberately NOT {@code @Primary}: a primary marker here hijacks every
     * unqualified {@code RabbitTemplate} injection in the host application and
     * silently switches its payload serialization to Zeta's JSON converter —
     * exactly the failure mode the data-plane connection-isolation design
     * (DataPlaneConnectionFactoryPresent, HeartbeatConnectionConfiguration)
     * exists to prevent. Zeta's own call sites resolve this bean by
     * {@code @Qualifier}; a host injecting {@code RabbitTemplate} by type with
     * no qualifying name resolves its own template (a field named
     * {@code rabbitTemplate} still falls back to Boot's bean by name), which is
     * the isolation contract for this file.
     *
     * @param beanFactory       the bean factory, used to resolve the data-plane connection factory by type
     * @param converter         the Zeta report message converter
     * @return a new {@link RabbitTemplate} with Zeta's report converter
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
     * Create the {@link ReportPublisher} for sending batched access-add reports to the Worker.
     *
     * @param rabbitTemplate the dedicated Zeta report RabbitMQ template
     * @param properties     the HotKey configuration properties
     * @param appProperties  the application identity (report routing key component)
     * @return a new {@link ReportPublisher} instance
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
     *
     * @param properties the HotKey configuration properties
     * @return a new {@link ConsistentHashRing} instance
     */
    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ConsistentHashRing consistentHashRing(ZetaProperties properties) {
      return new ConsistentHashRingImpl(properties.getConsistentHashing().getVirtualNodes());
    }

    /**
     * Create the {@link KeyReporter} that publishes per-key add batches
     * routed to the Workers. The reporter owns no counting reservoir — the
     * shared {@code AccessCounter} does (design decisions: access counting) —
     * it owns routing, the bounded dispatcher queue and the feed-loop tuner
     * (design decisions: reporting).
     *
     * @param reportPublisher       the reportToWorker publisher for sending batches
     * @param properties            the HotKey configuration properties
     * @param appProperties         the application identity (report routing key component)
     * @param ring                  the consistent-hash ring
     * @param clusterHealthView     the shared cluster health view (never {@code null})
     * @return a new {@link KeyReporterImpl} instance
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
      // design decisions: reporting: feed-loop flush-cadence tuner (default shadow — the
      // trajectory is exposed via gauges, nothing is applied until an
      // operator flips report-interval-tuning to on). The interval range is
      // validated on ZetaProperties at binding time.
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
  @lombok.extern.slf4j.Slf4j
  static class SyncConfiguration {

    /**
     * Create the FanoutExchange for broadcasting INVALIDATE/REFRESH messages
     * to all app instances.
     *
     * @param properties the cache sync configuration properties
     * @return a durable, non-auto-delete {@link FanoutExchange}
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeySyncExchange")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public FanoutExchange hotkeySyncExchange(CacheSyncProperties properties) {
      return new FanoutExchange(properties.getExchangeName(), true, false);
    }

    /**
     * Create the per-instance sync queue with a 60-second message TTL and 24-hour idle expiry.
     *
     * @param properties the cache sync configuration properties
     * @return a durable {@link Queue} with {@code x-message-ttl} of 60 seconds
     *         and {@code x-expires} of 24 hours
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeySyncQueue")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public Queue hotkeySyncQueue(CacheSyncProperties properties) {
      return durableTtlQueue(properties.getQueueName());
    }

    /**
     * Bind the per-instance queue to the sync exchange.
     *
     * @param hotkeySyncQueue    the per-instance sync queue
     * @param hotkeySyncExchange the sync FanoutExchange
     * @return a {@link Binding} connecting the queue to the exchange
     */
    @Bean
    public Binding hotkeySyncBinding(Queue hotkeySyncQueue, FanoutExchange hotkeySyncExchange) {
      return BindingBuilder.bind(hotkeySyncQueue).to(hotkeySyncExchange);
    }

    /**
     * Dedicated {@link RabbitTemplate} for cache-sync publishing.
     * Isolated from the container-level shared template to avoid
     * MessageConverter cross-contamination.
     *
     * @param beanFactory the bean factory, used to resolve the data-plane connection factory by type
     * @return a new {@link RabbitTemplate} instance
     */
    @Bean("zetaSyncRabbitTemplate")
    @ConditionalOnMissingBean(name = "zetaSyncRabbitTemplate")
    public RabbitTemplate zetaSyncRabbitTemplate(ListableBeanFactory beanFactory) {
      return new RabbitTemplate(dataPlaneConnectionFactory(beanFactory));
    }

    /**
     * Create the cache sync publisher for sending INVALIDATE/REFRESH messages.
     *
     * @param rabbitTemplate the dedicated Zeta cache-sync RabbitMQ template
     * @param properties     the cache sync configuration properties
     * @param appProperties  the application identity, stamped on every message as the
     *                       receiver-side isolation filter
     * @return a new {@link CacheSyncPublisher} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncPublisher cacheSyncPublisher(
      @Qualifier("zetaSyncRabbitTemplate") RabbitTemplate rabbitTemplate,
      CacheSyncProperties properties,
      ZetaAppProperties appProperties
    ) {
      // The sync plane stamps zeta.app-name (design decisions: sync messages pattern) so peers on a
      // shared broker can drop another application's INVALIDATE / REFRESH /
      // PUT messages instead of acting on them.
      return new CacheSyncPublisher(rabbitTemplate, properties, appProperties.getAppName());
    }

    /**
     * Dedicated scheduler for jitter-delayed cache-update tasks received from
     * peer instances. Isolated from {@code hotKeyScheduler} so that synchronous
     * cluster-loader reads performed by {@code handleRefresh} can never delay
     * the 50 ms report tide.
     *
     * <p>Pool size is the larger of {@code zeta.sync.scheduler-pool-size}
     * (default 4) and {@code zeta.sync.concurrent-consumers × 2}.
     *
     * @param properties the cache sync configuration properties
     * @return a daemon-thread scheduled executor named {@code zeta-scheduler-sync-N}
     */
    @Bean(name = "hotKeySyncScheduler", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "hotKeySyncScheduler")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ScheduledExecutorService hotKeySyncScheduler(CacheSyncProperties properties) {
      return newJitterScheduler(properties, "sync");
    }

    /**
     * Default {@link SyncDecisionHandler} that performs loader-backed REFRESH,
     * version-guarded INVALIDATE, whole-table clear (design decisions: invalidation, design decisions: cache table) and
     * value-carrying PUT apply. The optional SingleFlight collaborator lets
     * applied removals also drop the key's dedup entry (design decisions: dedup lifecycle); when
     * absent, applied removals leave the dedup entry in place. The cluster
     * loader is optional: when the application supplies no
     * {@code CacheLoader<String, Object>} bean, the handler skips loader-backed
     * reloads instead of failing startup. The sync publisher supplies the PUT
     * payload parser carrying this node's configured deserialization whitelist
     * ({@code zeta.sync.put-allowed-packages}, design decisions: invalidation).
     */
    @Bean
    @ConditionalOnMissingBean(SyncDecisionHandler.class)
    public SyncDecisionHandler defaultSyncDecisionHandler(
      Cache<String, CacheEntry> hotLocalCache,
      ObjectProvider<CacheLoader<String, Object>> hotKeyClusterLoaderProvider,
      EntryLifecycle entryLifecycle,
      ObjectProvider<SingleFlight> singleFlightProvider,
      ObjectProvider<SyncHook> syncHookProvider,
      ObjectProvider<CacheSyncPublisher> syncPublisherProvider
    ) {
      return new DefaultSyncDecisionHandler(
        hotLocalCache,
        hotKeyClusterLoaderProvider.getIfAvailable(),
        entryLifecycle,
        syncHookProvider.stream().toList(),
        singleFlightProvider.getIfAvailable(),
        syncPublisherProvider.getIfAvailable()
      );
    }

    /**
     * Create the sync listener that handles incoming INVALIDATE/REFRESH messages from peers.
     *
     * @param properties         the cache sync configuration properties
     * @param syncScheduler      the scheduler for jitter-delayed cache updates
     * @param decisionHandler    the strategy for processing sync messages
     * @param appProperties      the application identity (receiver-side isolation filter)
     * @param stampClockProvider provider for the shared stamp clock (may be absent)
     * @return a new {@link CacheSyncListener} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncListener cacheSyncListener(
      CacheSyncProperties properties,
      @Qualifier("hotKeySyncScheduler") ScheduledExecutorService syncScheduler,
      SyncDecisionHandler decisionHandler,
      ZetaAppProperties appProperties,
      ObjectProvider<StampClock> stampClockProvider
    ) {
      // Receiver-side half of the sync-plane filter (design decisions: sync messages): a message stamped with
      // a different application's name is dropped (ack'd) instead of applied.
      return new CacheSyncListener(
        properties,
        syncScheduler,
        decisionHandler,
        appProperties.getAppName(),
        stampClockProvider.getIfAvailable()
      );
    }

    /**
     * Create the AMQP message listener container that drives the sync listener.
     *
     * @param beanFactory        the bean factory, used to resolve the data-plane connection factory by type
     * @param hotkeySyncQueue    the per-instance sync queue
     * @param cacheSyncListener  the sync message handler
     * @param properties         the cache sync configuration properties
     * @return a configured {@link SimpleMessageListenerContainer}
     */
    @Bean
    @ConditionalOnMissingBean(name = "syncListenerContainer")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
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
        (msg, channel) -> cacheSyncListener.handleSyncMessage(channel, msg),
        "Sync listener uncaught exception (message will be requeued by container)"
      );
    }
  }

  /**
   * Matches when a DATA-PLANE {@link ConnectionFactory} bean exists — any
   * candidate except Zeta's own control-plane {@code zetaHeartbeatConnectionFactory}.
   * A plain {@code @ConditionalOnBean(ConnectionFactory.class)} here would be
   * satisfied by the heartbeat factory itself (it is already registered whenever
   * a control-plane feature is enabled), wrongly activating the Worker listener
   * in contexts that have no data-plane connection at all.
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
  @lombok.extern.slf4j.Slf4j
  static class WorkerListenerConfiguration {

    /**
     * Create the direct exchange the Worker publishes HOT/COOL decisions to.
     *
     * <p>Every instance of this application binds its queue under {@code send.<appName>}, so the
     * worker's single publish still reaches all of them, while instances of another application
     * on the same broker receive nothing (design decisions: sync messages).
     *
     * @param properties the Worker listener configuration properties
     * @return a durable, non-auto-delete {@link DirectExchange}
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyWorkerExchange")
    public DirectExchange hotkeyWorkerExchange(WorkerListenerProperties properties) {
      return new DirectExchange(properties.getExchangeName(), true, false);
    }

    /**
     * Create the per-instance Worker listener queue with a 60-second message TTL and 24-hour idle expiry.
     *
     * @param properties the Worker listener configuration properties
     * @return a durable {@link Queue} with {@code x-message-ttl} of 60 seconds
     *         and {@code x-expires} of 24 hours
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyWorkerQueue")
    public Queue hotkeyWorkerQueue(WorkerListenerProperties properties) {
      return durableTtlQueue(properties.getQueueName());
    }

    /**
     * Bind the per-instance queue to the Worker exchange under this application's decision
     * routing key.
     *
     * <p>The key — {@code send.<appName>} — is the whole isolation mechanism: it is what the
     * Worker publishes under, and it is what keeps another application's decisions out of this
     * queue on a shared broker. {@code zeta.app-name} is validated non-blank at binding time.
     *
     * @param hotkeyWorkerQueue    the per-instance Worker listener queue
     * @param hotkeyWorkerExchange the Worker decision exchange
     * @param appProperties        the application identity (appName source)
     * @return a {@link Binding} connecting the queue to the exchange
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
     *
     * @param properties the HotKey configuration properties
     * @return a durable, non-auto-delete {@link TopicExchange}
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyHeartbeatExchange")
    public TopicExchange hotkeyHeartbeatExchange(ZetaProperties properties) {
      return new TopicExchange(properties.getHeartbeat().getExchangeName(), true, false);
    }

    /**
     * Create the per-instance non-durable heartbeat queue that auto-deletes on disconnect.
     *
     * @return a non-durable, auto-delete {@link Queue}
     */
    @Bean
    public Queue hotkeyHeartbeatQueue() {
      return QueueBuilder.nonDurable("zeta.heartbeat:" + NodeIdentity.getInstanceId()).autoDelete().build();
    }

    /**
     * Bind the per-instance heartbeat queue to the heartbeat exchange with routing key {@code heartbeat.*}.
     *
     * @param hotkeyHeartbeatQueue    the per-instance heartbeat queue
     * @param hotkeyHeartbeatExchange the heartbeat TopicExchange
     * @return a {@link Binding} connecting the queue to the exchange
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
     *
     * @param properties the Worker listener configuration properties
     * @return a daemon-thread scheduled executor named {@code zeta-scheduler-worker-N}
     */
    @Bean(name = "hotKeyWorkerSchedScheduler", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "hotKeyWorkerSchedScheduler")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ScheduledExecutorService hotKeyWorkerSchedScheduler(WorkerListenerProperties properties) {
      return newJitterScheduler(properties, "worker");
    }

    /**
     * Default {@link WorkerDecisionHandler} that performs loader-backed HOT promotion
     * and COOL downgrade with version guarding.
     *
     * <p>The shard-ownership refinement for stale cross-Worker COOLs (scale
     * re-balance ghost-COOL kill — see
     * {@link io.github.hyshmily.zeta.sync.worker.DefaultWorkerDecisionHandler})
     * is wired from the ring and the shared health view when present; both
     * are optional so a report-disabled deployment still builds (the guard then
     * fails open to the pure version-guard behaviour).
     */
    @Bean
    @ConditionalOnMissingBean(WorkerDecisionHandler.class)
    public WorkerDecisionHandler defaultWorkerDecisionHandler(
      Cache<String, CacheEntry> hotLocalCache,
      ObjectProvider<CacheLoader<String, Object>> hotKeyClusterLoaderProvider,
      EntryLifecycle entryLifecycle,
      StampClock stampClock,
      ObjectProvider<WorkerDecisionHook> workerDecisionHookProvider,
      ObjectProvider<ConsistentHashRing> ringProvider,
      ObjectProvider<HealthView> healthViewProvider
    ) {
      return new DefaultWorkerDecisionHandler(
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
     * Create the listener that processes HOT/COOL decisions sent by the Worker.
     *
     * <p>The app's {@code zeta.app-name} is passed in so foreign-app decisions can be
     * dropped at reception. Broker routing already keeps them out of this queue (the binding
     * key is {@code send.<appName>}, design decisions: sync messages), so the header check is
     * the receiver-side boundary rather than the delivery mechanism: a queue bound to the wrong
     * key — an operator mistake or a leftover queue — still must not promote keys in this
     * application's cache. The declared application name is validated non-blank at binding time.
     *
     * @param properties          the Worker listener configuration properties
     * @param decisionHandler     the strategy for processing HOT/COOL decisions
     * @param appProperties       the application-identity properties (appName source)
     * @return a new {@link WorkerListener} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkerListener workerListener(
      WorkerListenerProperties properties,
      @Qualifier("hotKeyWorkerSchedScheduler") ScheduledExecutorService workerSchedScheduler,
      WorkerDecisionHandler decisionHandler,
      ZetaAppProperties appProperties
    ) {
      return new WorkerListener(properties, workerSchedScheduler, decisionHandler, appProperties.getAppName());
    }

    /**
     * Create the AMQP message listener container that processes Worker heartbeat messages.
     *
     * @param connectionFactory    the dedicated control-plane connection factory
     * @param healthView           the cluster health view to update on heartbeat reception
     * @param hotkeyHeartbeatQueue the heartbeat queue
     * @param appProperties        this application's name, used to drop foreign heartbeats
     * @return a configured {@link SimpleMessageListenerContainer}
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyHeartbeatContainer")
    public SimpleMessageListenerContainer heartbeatContainer(
      @Qualifier("zetaHeartbeatConnectionFactory") ConnectionFactory connectionFactory,
      HealthView healthView,
      Queue hotkeyHeartbeatQueue,
      ZetaAppProperties appProperties
    ) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
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
        }
      });
      return container;
    }

    /**
     * Create the AMQP message listener container that processes Worker HOT/COOL decisions
     * via the {@link WorkerListener}.
     *
     * <p>The container uses {@link AcknowledgeMode#MANUAL} because the
     * {@link WorkerListener#handleWorkerMessage} performs its own ack/nack
     * (ack-before-update pattern, see design decisions: sync messages).
     *
     * @param beanFactory       the bean factory, used to resolve the data-plane connection factory by type
     * @param hotkeyWorkerQueue the per-instance Worker listener queue
     * @param workerListener    the Worker decision listener
     * @param properties        the Worker listener configuration properties
     * @return a configured {@link SimpleMessageListenerContainer}
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
        (msg, channel) -> workerListener.handleWorkerMessage(channel, msg),
        "Worker listener uncaught exception (message will be requeued by container)"
      );
    }
  }

  /**
   * Dedicated {@link CachingConnectionFactory} for heartbeat traffic,
   * isolated from the data-plane connection (report, sync, broadcast).  Prevents
   * control-plane liveliness from being affected by data-plane congestion or
   * broker flow control on the shared connection.
   *
   * <p>Shares the same broker host/port/credentials as the default connection
   * factory but maintains a separate TCP connection and channel pool.  The cost
   * is one extra connection per node — negligible for the cross‑circuit survivability
   * gained (standard practice: K8s health endpoint on separate port, Kafka controller
   * listener, etc.).
   *
   * <p>The factory is built through Spring Boot's
   * {@link RabbitConnectionFactoryBeanConfigurer} — the sanctioned extension point
   * for custom connection factories — so the control plane inherits the full
   * {@link RabbitProperties} surface with the same semantics as the Boot-managed
   * data-plane factory: credentials, virtual host, heartbeat, timeouts, and
   * {@code spring.rabbitmq.ssl.*} (TLS). {@code spring.rabbitmq.ssl.bundle} is not
   * supported (it requires a Boot-internal factory bean subtype).
   *
   * <p><b>Channel mapping (design decisions: heartbeat and health):</b>
   * <ul>
   *   <li><b>Control plane</b> (this factory): app heartbeat consumption and
   *       worker heartbeat production.</li>
   *   <li><b>Data plane</b> (Boot {@code rabbitConnectionFactory}): report publish/consume,
   *       cache-sync publish/consume, worker decision consume, worker HOT/COOL broadcast.</li>
   * </ul>
   *
   * <p><b>{@code @Primary} is load-bearing.</b> Without it, two non-primary
   * {@code ConnectionFactory} candidates would make Spring Boot's
   * {@code RabbitTemplate} ({@code @ConditionalOnSingleCandidate}) silently back
   * off; downstream code injecting {@code RabbitTemplate} would then resolve to
   * {@code zetaReportRabbitTemplate} and inherit its JSON message converter — a
   * silent format change for the consumer's own messages. {@code @Primary}
   * preserves single-candidate resolution for unqualified injections, and every
   * Zeta data-plane bean resolves the data-plane factory explicitly through
   * {@link #dataPlaneConnectionFactory}.
   *
   * <p><b>Gating:</b> this configuration only activates when a feature that
   * actually consumes the control-plane connection is enabled — the app-side
   * worker listener ({@code zeta.worker-listener.enabled=true}) or worker mode
   * ({@code zeta.worker.enabled=true}) — see {@link HeartbeatFeatureEnabled}.
   * A plain app with report/sync only (or everything disabled) gets no extra
   * {@code @Primary} factory, so unqualified {@code ConnectionFactory}
   * injections in the host application resolve to the application's own
   * factory.
   */
  @Configuration
  @Conditional(HeartbeatFeatureEnabled.class)
  static class HeartbeatConnectionConfiguration {

    @Primary
    @Bean("zetaHeartbeatConnectionFactory")
    @ConditionalOnMissingBean(name = "zetaHeartbeatConnectionFactory")
    public CachingConnectionFactory heartbeatConnectionFactory(
      ObjectProvider<RabbitProperties> propertiesProvider,
      ResourceLoader resourceLoader
    ) {
      RabbitProperties properties = propertiesProvider.getIfAvailable();
      if (properties == null) {
        // Fallback: an unconfigured factory; the connection then uses the
        // driver defaults (localhost:5672). In a normal Boot application the
        // RabbitProperties bean is always present.
        return new CachingConnectionFactory();
      }
      RabbitConnectionFactoryBean factoryBean = new RabbitConnectionFactoryBean();
      new RabbitConnectionFactoryBeanConfigurer(resourceLoader, properties).configure(factoryBean);
      try {
        factoryBean.afterPropertiesSet();
        com.rabbitmq.client.ConnectionFactory underlying = factoryBean.getObject();
        Assert.state(underlying != null, "RabbitConnectionFactoryBean produced no ConnectionFactory");

        return new CachingConnectionFactory(underlying);
      } catch (Exception ex) {
        throw new IllegalStateException("Failed to create RabbitConnectionFactory", ex);
      }
    }
  }

  /**
   * Condition matching when any Zeta feature that consumes the dedicated
   * control-plane connection factory is enabled: the app-side worker listener
   * ({@code zeta.worker-listener.enabled=true}) or worker mode
   * ({@code zeta.worker.enabled=true}). Both run control-plane traffic
   * (heartbeat consumption and production). Report and cache-sync are pure data-plane features and
   * must not drag in the extra {@code @Primary} connection factory.
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
