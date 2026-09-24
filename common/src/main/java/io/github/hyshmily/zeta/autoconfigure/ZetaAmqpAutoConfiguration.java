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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.loader.PrefixRoutedLoader;
import io.github.hyshmily.zeta.cache.loader.RedisValueLoader;
import io.github.hyshmily.zeta.cache.loader.ZetaLoaderRegistry;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.reporting.*;
import io.github.hyshmily.zeta.reporting.impl.BbrRateLimiterImpl;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.RingManager;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sharding.impl.RingManagerImpl;
import io.github.hyshmily.zeta.sync.local.*;
import io.github.hyshmily.zeta.sync.worker.*;
import io.github.hyshmily.zeta.util.InstanceIdGenerator;
import io.github.hyshmily.zeta.util.SystemLoadMonitor;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.impl.SystemLoadMonitorImpl;
import io.github.hyshmily.zeta.util.ratelimit.SreRateLimiter;
import io.github.hyshmily.zeta.util.ratelimit.impl.SreRateLimiterImpl;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.jspecify.annotations.NonNull;
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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.Assert;

/**
 * Unified AMQP auto-configuration for HotKey messaging: app-to-Worker reporting,
 * instance-to-instance cache sync, and Worker decision listening.
 *
 * <p>Conditionally activates when {@link RabbitTemplate} is on the classpath.
 * Sub-groups for cache sync and Worker listener additionally require Redis.
 *
 * <p><b>Report</b> ({@code zeta.local.reporter.enabled}, default {@code true}):
 * app instance aggregates access counts and sends them to the Worker via
 * {@link DirectExchange}. No Redis dependency.
 *
 * <p><b>Cache Sync</b> ({@code zeta.sync.enabled=true}):
 * instance-to-instance INVALIDATE / REFRESH broadcasts via {@link FanoutExchange}.
 * Requires Redis for version tracking.
 *
 * <p><b>Worker Listener</b> ({@code zeta.worker-listener.enabled=true}):
 * receives HOT/COOL decisions from the Worker via {@link FanoutExchange}.
 * Requires Redis.
 */
@Internal
@AutoConfiguration(
  afterName = {
    "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration",
    "org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration",
  }
)
@ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
@EnableConfigurationProperties({ ZetaProperties.class, CacheSyncProperties.class, WorkerListenerProperties.class })
public class ZetaAmqpAutoConfiguration {

  private ZetaAmqpAutoConfiguration() {}

  /** Bean name of Spring Boot's default (data-plane) RabbitMQ connection factory. */
  private static final String DATA_PLANE_CONNECTION_FACTORY_BEAN = "rabbitConnectionFactory";

  /** Bean name of Zeta's dedicated control-plane (heartbeat) connection factory. */
  private static final String CONTROL_PLANE_CONNECTION_FACTORY_BEAN = "zetaHeartbeatConnectionFactory";

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
   * <p>Every caller is gated by {@code @ConditionalOnBean(ConnectionFactory.class)},
   * so at least one candidate always exists when this resolver runs.
   *
   * @param beanFactory the listable bean factory for name-aware lookup
   * @return the data-plane connection factory (never {@code null})
   */
  static ConnectionFactory dataPlaneConnectionFactory(ListableBeanFactory beanFactory) {
    if (beanFactory.containsBean(DATA_PLANE_CONNECTION_FACTORY_BEAN)) {
      return beanFactory.getBean(DATA_PLANE_CONNECTION_FACTORY_BEAN, ConnectionFactory.class);
    }

    for (String name : beanFactory.getBeanNamesForType(ConnectionFactory.class)) {
      if (!CONTROL_PLANE_CONNECTION_FACTORY_BEAN.equals(name)) {
        return beanFactory.getBean(name, ConnectionFactory.class);
      }
    }
    throw new NoSuchBeanDefinitionException(ConnectionFactory.class);
  }

  /**
   * Inner configuration for app-to-Worker reportToWorker routing via DirectExchange.
   * Creates the exchange, publisher, ring manager (optional), reporter, and reportToWorker scheduler.
   * Active by default when a {@link RabbitTemplate} bean is present.
   */
  @Configuration
  @ConditionalOnBean(RabbitTemplate.class)
  @ConditionalOnProperty(prefix = "zeta.report", name = "enabled", havingValue = "true", matchIfMissing = true)
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
     * Create the {@link MessageConverter} for serializing reportToWorker messages.
     * <p>
     * JSON by default (Jackson, cross-version compatible); when
     * {@code zeta.local.report-encoding=compact}, {@link ReportMessage}
     * payloads are sent in the compact binary varint format (ADR-0074).
     * The decode side of the returned converter always accepts both formats
     * by first-byte sniffing, so the upgrade order is Workers first, then
     * Apps flip to compact.
     *
     * @param properties the HotKey configuration properties
     * @return a {@link CompactAwareReportMessageConverter} over a Jackson delegate
     */
    @Bean("zetaReportMessageConverter")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public MessageConverter reportMessageConverter(ZetaProperties properties) {
      return new CompactAwareReportMessageConverter(
        new Jackson2JsonMessageConverter(),
        properties.getReportEncoding() == ZetaProperties.ReportEncoding.COMPACT
      );
    }

    /**
     * Dedicated {@link RabbitTemplate} for report publishing.
     * Uses a dedicated instance (not the container-level shared template) so that
     * Zeta's JSON serialization is isolated from the application's own message
     * converter — see bidirectional-converter-pollution issue (P1-5.1).
     *
     * <p>Deliberately NOT {@code @Primary}: a primary marker here hijacks every
     * unqualified {@code RabbitTemplate} injection in the host application and
     * silently switches its payload serialization to Zeta's JSON converter —
     * exactly the failure mode the data-plane connection-isolation design
     * (DataPlaneConnectionFactoryPresent, HeartbeatConnectionConfiguration)
     * exists to prevent. Zeta's own call sites resolve this bean by
     * {@code @Qualifier}; a host injecting {@code RabbitTemplate} by type with
     * no qualifying name now resolves its own template again (a field named
     * {@code rabbitTemplate} still falls back to Boot's bean by name), which is
     * the documented isolation contract for this file.
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
      RabbitTemplate t = new RabbitTemplate(dataPlaneConnectionFactory(beanFactory));
      t.setMessageConverter(converter);
      return t;
    }

    /**
     * Create the {@link ReportPublisher} for sending batched access-count reports to the Worker.
     *
     * @param rabbitTemplate the dedicated Zeta report RabbitMQ template
     * @param properties     the HotKey configuration properties
     * @return a new {@link ReportPublisher} instance
     */
    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ReportPublisher reportPublisher(
      @Qualifier("zetaReportRabbitTemplate") RabbitTemplate rabbitTemplate,
      ZetaProperties properties
    ) {
      return new ReportPublisher(rabbitTemplate, properties.getReportExchange(), properties.getAppName());
    }

    /**
     * Create the {@link RingManager} for consistent-hashing reportToWorker routing.
     *
     * @param properties the HotKey configuration properties
     * @return a new {@link RingManager} instance
     */
    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public RingManager ringManager(ZetaProperties properties) {
      return new RingManagerImpl(properties.getConsistentHashing().getVirtualNodes());
    }

    /**
     * Create the system CPU monitor with EMA smoothing.
     * <p>
     * Uses the JDK platform MXBean ({@link com.sun.management.OperatingSystemMXBean})
     * which is already used by the Worker-side heartbeat producer. The monitor
     * starts sampling on creation and stops on context close.
     *
     * @param properties the HotKey configuration properties
     * @return a new {@link SystemLoadMonitor} instance
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public SystemLoadMonitor hotKeyCpuMonitor(ZetaProperties properties) {
      ZetaProperties.ReporterLimiter cfg = properties.getReporter();
      return new SystemLoadMonitorImpl(cfg.getCpuPollIntervalMs(), cfg.getCpuDecay());
    }

    /**
     * Create the BBR adaptive rate limiter for the reportToWorker publisher.
     * <p>
     * Uses the CPU monitor and the configured BBR parameters. When disabled
     * (or when the CPU monitor itself hasn't been fully initialized yet),
     * the limiter falls back to a permissive mode.
     *
     * @param cpuMonitor the system CPU load monitor
     * @param properties the HotKey configuration properties
     * @return a new {@link BbrRateLimiter} instance
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
      prefix = "zeta.local.reporter",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true
    )
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public BbrRateLimiterImpl hotKeyBbrRateLimiter(SystemLoadMonitor cpuMonitor, ZetaProperties properties) {
      ZetaProperties.ReporterLimiter cfg = properties.getReporter();
      return new BbrRateLimiterImpl(
        cpuMonitor,
        cfg.getCpuThreshold(),
        cfg.getBbrWindowMs(),
        cfg.getBbrWindowBuckets(),
        cfg.getBbrCooldownMs(),
        cfg.getBbrMaxInFlightCeiling()
      );
    }

    /**
     * Create the {@link KeyReporter} that aggregates per-key counts and flushes them
     * at the configured interval.
     *
     * @param reportPublisher       the reportToWorker publisher for sending batches
     * @param properties            the HotKey configuration properties
     * @param ringManager           the consistent-hash ring manager
     * @param healthViewProvider    optional provider for the cluster health view
     * @param bbrRateLimiterProvider optional provider for the BBR rate limiter
     * @return a new {@link KeyReporterImpl} instance
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public KeyReporter hotKeyReporter(
      ReportPublisher reportPublisher,
      @Qualifier("hotKeyScheduler") ScheduledExecutorService hotKeyScheduler,
      ZetaProperties properties,
      RingManager ringManager,
      ObjectProvider<HealthView> healthViewProvider,
      ObjectProvider<BbrRateLimiterImpl> bbrRateLimiterProvider,
      SnowflakeIdGenerator snowflakeIdGenerator
    ) {
      KeyReporterImpl reporter = new KeyReporterImpl(
        reportPublisher,
        hotKeyScheduler,
        properties.getReportIntervalMs(),
        properties.getAppName(),
        properties.getQueueCapacity(),
        properties.getQueueOfferTimeoutMs(),
        properties.effectiveConsumerCount(),
        ringManager,
        healthViewProvider.getIfAvailable(() ->
          new HealthViewImpl(
            properties.getHeartbeat().getTimeoutMs(),
            properties.getHeartbeat().getDegradeAfterFailures()
          )
        ),
        snowflakeIdGenerator
      );
      bbrRateLimiterProvider.ifAvailable(reporter::setBbrRateLimiter);
      return reporter;
    }
  }

  /**
   * Inner configuration for instance-to-instance cache synchronization.
   * Creates a FanoutExchange, per-instance queue with TTL, binding, publisher,
   * Redis loader, sync listener, and a dedicated scheduled executor.
   * Requires Redis and {@code zeta.sync.enabled=true}.
   */
  @Configuration
  @ConditionalOnClass(name = "org.springframework.data.redis.core.RedisTemplate")
  @ConditionalOnBean(ConnectionFactory.class)
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
    @ConditionalOnClass(name = "org.springframework.amqp.core.FanoutExchange")
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
    @ConditionalOnClass(name = "org.springframework.amqp.core.Queue")
    public Queue hotkeySyncQueue(CacheSyncProperties properties) {
      return QueueBuilder.durable(properties.getQueueName())
        .withArgument("x-message-ttl", 60_000)
        .withArgument("x-expires", 86_400_000)
        .build();
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
     * MessageConverter cross-contamination (see issue P1-5.1).
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
     * @return a new {@link CacheSyncPublisher} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncPublisher cacheSyncPublisher(
      @Qualifier("zetaSyncRabbitTemplate") RabbitTemplate rabbitTemplate,
      CacheSyncProperties properties,
      SnowflakeIdGenerator snowflakeIdGenerator,
      ZetaProperties zetaProperties
    ) {
      // The sync plane stamps zeta.local.app-name (ADR-0068 pattern) so peers on a
      // shared broker can drop another application's INVALIDATE / REFRESH /
      // INVALIDATE_ALL / RULES_SYNC instead of acting on them.
      return new CacheSyncPublisher(rabbitTemplate, properties, snowflakeIdGenerator, zetaProperties.getAppName());
    }

    /**
     * Dedicated scheduler for jitter-delayed cache-update tasks received from
     * peer instances. Isolated from {@code hotKeyScheduler} so that synchronous
     * Redis GETs performed by {@code handleRefresh} can never starve the
     * 50 ms reporter flush tick.
     *
     * <p>Pool size is {@code zeta.sync.scheduler-pool-size} (default 4) but
     * should be at least {@code zeta.sync.concurrent-consumers × 2}.
     *
     * @param properties the cache sync configuration properties
     * @return a daemon-thread scheduled executor named {@code zeta-sync-sched-N}
     */
    @Bean(name = "hotKeySyncScheduler", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "hotKeySyncScheduler")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ScheduledExecutorService hotKeySyncScheduler(CacheSyncProperties properties) {
      int poolSize = Math.max(properties.getSchedulerPoolSize(), properties.getConcurrentConsumers() * 2);
      return Executors.newScheduledThreadPool(
        poolSize,
        new ZetaThreadFactory(ZetaConstants.Thread.PREFIX_SCHEDULER + "-sync")
      );
    }

    /**
     * Cluster value loader used by the sync listener to refresh cache entries and
     * by the Worker decision handler for HOT warm-up.
     *
     * <p>When a {@link ZetaLoaderRegistry} bean exists (ADR-0070), the returned
     * loader is a {@link PrefixRoutedLoader}: keys matching a registered prefix
     * load through the application's {@code CacheLoader}, so Worker HOT warm-up
     * and peer REFRESH work for data sources without a Redis value channel;
     * unregistered keys fall back to the plain {@link RedisValueLoader} (Redis
     * GET). With no registry the plain {@link RedisValueLoader} is returned,
     * preserving the historical behavior.
     *
     * <p><b>Call-chain contract:</b> {@link ZetaLoaderRegistry#match} is invoked
     * in exactly two places — {@code Zeta#requireRegisteredSpec} (application
     * read path, needs the full spec) and {@link PrefixRoutedLoader#load} (this
     * path, needs the value only). New value-fetching paths must reuse one of the
     * two.
     *
     * @param stringRedisTemplate the String-based Redis template for reading values
     * @param registryProvider    provider for the optional prefix→loader registry
     * @return a {@code CacheLoader<Object>} that routes through the registry or reads Redis
     */
    @Bean
    @ConditionalOnMissingBean(CacheLoader.class)
    public CacheLoader<Object> hotKeyClusterLoader(
      StringRedisTemplate stringRedisTemplate,
      ObjectProvider<ZetaLoaderRegistry> registryProvider
    ) {
      CacheLoader<Object> redisFallback = new RedisValueLoader(stringRedisTemplate);
      ZetaLoaderRegistry registry = registryProvider.getIfAvailable();
      return registry != null ? new PrefixRoutedLoader(registry, redisFallback) : redisFallback;
    }

    /**
     * Default {@link SyncDecisionHandler} that performs loader-backed REFRESH,
     * version-guarded INVALIDATE, batch INVALIDATE_ALL, and RULES_SYNC. The
     * optional SingleFlight collaborator lets applied removals also drop the
     * key's dedup entry (ADR-0067); when absent, the historical
     * no-invalidation behavior is kept.
     */
    @Bean
    @ConditionalOnMissingBean(SyncDecisionHandler.class)
    public SyncDecisionHandler defaultSyncDecisionHandler(
      Cache<String, Object> hotLocalCache,
      CacheLoader<Object> hotKeyClusterLoader,
      ExpireManager expireManager,
      RuleMatcher ruleMatcher,
      ObjectProvider<SingleFlight> singleFlightProvider,
      ObjectProvider<SyncHook> syncHookProvider
    ) {
      return new DefaultSyncDecisionHandler(
        hotLocalCache,
        hotKeyClusterLoader,
        expireManager,
        ruleMatcher,
        syncHookProvider.stream().toList(),
        singleFlightProvider.getIfAvailable()
      );
    }

    /**
     * Create the sync listener that handles incoming INVALIDATE/REFRESH messages from peers.
     *
     * @param properties          the cache sync configuration properties
     * @param decisionHandler     the strategy for processing sync messages
     * @return a new {@link CacheSyncListener} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncListener cacheSyncListener(
      CacheSyncProperties properties,
      @Qualifier("hotKeySyncScheduler") ScheduledExecutorService syncScheduler,
      SyncDecisionHandler decisionHandler,
      ZetaProperties zetaProperties
    ) {
      // Receiver-side half of the ADR-0068 sync-plane filter: a message stamped with
      // a different application's name is dropped (ack'd) instead of applied.
      warnIfAppNameIsDefault(zetaProperties, "sync listener (peer INVALIDATE/REFRESH filtering)");
      return new CacheSyncListener(properties, syncScheduler, decisionHandler, zetaProperties.getAppName());
    }

    /**
     * Warns when application isolation is effectively off because {@code
     * zeta.local.app-name} was left at its default.
     *
     * <p>ADR-0068's filter drops a message only when the sender <em>declared</em> a
     * different application name. Two applications that both leave the default
     * ({@value io.github.hyshmily.zeta.autoconfigure.ZetaProperties#DEFAULT_APP_NAME})
     * therefore consider each other "ours" and keep cross-talking on a shared broker —
     * silently, since nothing is malformed. The default cannot be made fail-closed
     * without breaking single-application deployments, so it is surfaced instead.
     *
     * @param zetaProperties the application properties
     * @param what           the component whose isolation is at stake, for the message
     */
    @SuppressWarnings("all")
    private void warnIfAppNameIsDefault(ZetaProperties zetaProperties, String what) {
      String configured = zetaProperties.getAppName();
      if (configured == null || configured.isBlank() || ZetaProperties.DEFAULT_APP_NAME.equals(configured)) {
        log.warn(
          "zeta.local.app-name is not set (still '{}'); {} will NOT be isolated from other applications that share " +
            "the same broker. Set zeta.local.app-name to a unique value per application — see ADR-0068.",
          ZetaProperties.DEFAULT_APP_NAME,
          what
        );
      }
    }

    /**
     * Create the AMQP message listener container that drives the sync listener.
     *
     * @param beanFactory        the bean factory, used to resolve the data-plane connection factory by type
     * @param cacheSyncListener  the sync message handler
     * @param properties         the cache sync configuration properties
     * @return a configured {@link SimpleMessageListenerContainer}
     */
    @Bean
    @ConditionalOnBean(ConnectionFactory.class)
    public SimpleMessageListenerContainer syncListenerContainer(
      ListableBeanFactory beanFactory,
      CacheSyncListener cacheSyncListener,
      CacheSyncProperties properties
    ) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
        dataPlaneConnectionFactory(beanFactory)
      );
      container.setQueueNames(properties.getQueueName());
      container.setAutoStartup(properties.isAutoStartup());
      container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
      container.setConcurrentConsumers(properties.getConcurrentConsumers());
      container.setPrefetchCount(properties.getPrefetchCount());
      container.setErrorHandler(t ->
        log.warn("Sync listener uncaught exception (message will be requeued by container)", t)
      );
      container.setMessageListener(
        (ChannelAwareMessageListener) (msg, channel) -> cacheSyncListener.handleSyncMessage(channel, msg)
      );
      return container;
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
      if (beanFactory == null) {
        return false;
      }

      for (String name : beanFactory.getBeanNamesForType(ConnectionFactory.class)) {
        if (!"zetaHeartbeatConnectionFactory".equals(name)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Inner configuration for receiving Worker HOT/COOL decisions.
   * Creates a FanoutExchange, per-instance queue with TTL, binding, worker listener,
   * listener container, and a dedicated scheduled executor.
   * Requires a data-plane {@link ConnectionFactory} (see
   * {@link DataPlaneConnectionFactoryPresent}), Redis, and
   * {@code zeta.worker-listener.enabled=true}.
   */
  @Configuration
  @ConditionalOnClass(name = "org.springframework.data.redis.core.RedisTemplate")
  @Conditional(DataPlaneConnectionFactoryPresent.class)
  @ConditionalOnProperty(prefix = "zeta.worker-listener", name = "enabled", havingValue = "true")
  @lombok.extern.slf4j.Slf4j
  static class WorkerListenerConfiguration {

    /**
     * Create the FanoutExchange for broadcasting Worker HOT/COOL decisions
     * to all app instances.
     *
     * @param properties the Worker listener configuration properties
     * @return a durable, non-auto-delete {@link FanoutExchange}
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyWorkerExchange")
    public FanoutExchange hotkeyWorkerExchange(WorkerListenerProperties properties) {
      return new FanoutExchange(properties.getExchangeName(), true, false);
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
      return QueueBuilder.durable(properties.getQueueName())
        .withArgument("x-message-ttl", 60_000)
        .withArgument("x-expires", 86_400_000)
        .build();
    }

    /**
     * Bind the per-instance queue to the Worker exchange.
     *
     * @param hotkeyWorkerQueue    the per-instance Worker listener queue
     * @param hotkeyWorkerExchange the Worker FanoutExchange
     * @return a {@link Binding} connecting the queue to the exchange
     */
    @Bean
    public Binding hotkeyWorkerBinding(Queue hotkeyWorkerQueue, FanoutExchange hotkeyWorkerExchange) {
      return BindingBuilder.bind(hotkeyWorkerQueue).to(hotkeyWorkerExchange);
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
      return QueueBuilder.nonDurable("zeta.heartbeat:" + InstanceIdGenerator.get()).autoDelete().build();
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
     * Create the SRE adaptive rate limiter for WorkerListener HOT-path throttling.
     * <p>
     * Disabled when {@code zeta.worker-listener.sre.enabled=false}.
     *
     * @param properties the Worker listener configuration properties
     * @return a new {@link SreRateLimiter} instance
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
      prefix = "zeta.worker-listener.sre",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true
    )
    public SreRateLimiterImpl hotKeySreRateLimiter(WorkerListenerProperties properties) {
      WorkerListenerProperties.Sre sreConfig = properties.getSre();
      return new SreRateLimiterImpl(
        sreConfig.getWindowMs(),
        sreConfig.getBuckets(),
        1.0 / sreConfig.getSuccessThreshold(),
        sreConfig.getMinSamples()
      );
    }

    /**
     * Dedicated scheduler for jitter-delayed cache-update tasks received from
     * the Worker (HOT/COOL decisions). Isolated from {@code hotKeyScheduler}
     * so that synchronous Redis GETs performed by {@code handleHot} can never
     * starve the reporter flush tick.
     *
     * @param properties the Worker listener configuration properties
     * @return a daemon-thread scheduled executor named {@code zeta-worker-sched-N}
     */
    @Bean(name = "hotKeyWorkerSchedScheduler", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "hotKeyWorkerSchedScheduler")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public ScheduledExecutorService hotKeyWorkerSchedScheduler(WorkerListenerProperties properties) {
      int poolSize = Math.max(properties.getSchedulerPoolSize(), properties.getConcurrentConsumers() * 2);
      return Executors.newScheduledThreadPool(
        poolSize,
        new ZetaThreadFactory(ZetaConstants.Thread.PREFIX_SCHEDULER + "-worker")
      );
    }

    /**
     * Default {@link WorkerDecisionHandler} that performs loader-backed HOT promotion
     * and COOL downgrade with SRE rate limiting and version guarding.
     */
    @Bean
    @ConditionalOnMissingBean(WorkerDecisionHandler.class)
    public WorkerDecisionHandler defaultWorkerDecisionHandler(
      Cache<String, Object> hotLocalCache,
      CacheLoader<Object> hotKeyClusterLoader,
      ExpireManager expireManager,
      ObjectProvider<SreRateLimiterImpl> sreRateLimiterProvider,
      StringRedisTemplate stringRedisTemplate,
      ZetaProperties zetaProperties,
      SnowflakeIdGenerator snowflakeIdGenerator,
      ObjectProvider<WorkerDecisionHook> workerDecisionHookProvider
    ) {
      VersionController vc = new VersionControllerImpl(
        Optional.ofNullable(stringRedisTemplate),
        zetaProperties.getVersionKeyTtlMinutes(),
        snowflakeIdGenerator
      );
      return new DefaultWorkerDecisionHandler(
        hotLocalCache,
        hotKeyClusterLoader,
        expireManager,
        sreRateLimiterProvider.getIfAvailable(),
        vc,
        workerDecisionHookProvider.stream().toList()
      );
    }

    /**
     * Create the listener that processes HOT/COOL decisions send by the Worker.
     *
     * <p>The app's {@code zeta.app-name} is passed in so foreign-app decisions can be
     * dropped at reception (ADR-0068 shared-broker isolation; the fanout exchange
     * ignores the routing key). A blank appName disables the filter.
     *
     * @param properties          the Worker listener configuration properties
     * @param decisionHandler     the strategy for processing HOT/COOL decisions
     * @param zetaProperties      the HotKey configuration properties (appName source)
     * @return a new {@link WorkerListener} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkerListener workerListener(
      WorkerListenerProperties properties,
      @Qualifier("hotKeyWorkerSchedScheduler") ScheduledExecutorService workerSchedScheduler,
      WorkerDecisionHandler decisionHandler,
      ZetaProperties zetaProperties
    ) {
      return new WorkerListener(properties, workerSchedScheduler, decisionHandler, zetaProperties.getAppName());
    }

    /**
     * Create the AMQP message listener container that processes Worker heartbeat messages.
     *
     * @param connectionFactory   the RabbitMQ connection factory
     * @param healthView          the cluster health view to update on heartbeat reception
     * @param hotkeyHeartbeatQueue the heartbeat queue
     * @return a configured {@link SimpleMessageListenerContainer}
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotkeyHeartbeatContainer")
    public SimpleMessageListenerContainer heartbeatContainer(
      @Qualifier("zetaHeartbeatConnectionFactory") ConnectionFactory connectionFactory,
      HealthView healthView,
      Queue hotkeyHeartbeatQueue
    ) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
      container.setQueueNames(hotkeyHeartbeatQueue.getName());
      container.setAcknowledgeMode(AcknowledgeMode.NONE);
      container.setConcurrentConsumers(1);
      container.setPrefetchCount(100);
      container.setErrorHandler(t -> log.warn("Heartbeat listener uncaught exception (message discarded)", t));
      container.setMessageListener(msg -> {
        WorkerHeartbeatMessage hb = WorkerHeartbeatMessage.from(msg);
        if (hb != null) {
          healthView.onHeartbeat(hb);
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
     * (ack-before-update pattern, see ADR-0004).
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
      Queue hotkeyWorkerQueue,
      WorkerListener workerListener,
      WorkerListenerProperties properties
    ) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
        dataPlaneConnectionFactory(beanFactory)
      );
      container.setQueueNames(hotkeyWorkerQueue.getName());
      container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
      container.setMessageListener(
        (ChannelAwareMessageListener) (msg, channel) -> workerListener.handleWorkerMessage(channel, msg)
      );
      container.setConcurrentConsumers(properties.getConcurrentConsumers());
      container.setPrefetchCount(properties.getPrefetchCount());
      container.setErrorHandler(t ->
        log.warn("Worker listener uncaught exception (message will be requeued by container)", t)
      );
      container.setAutoStartup(properties.isAutoStartup());
      return container;
    }

    /**
     * Dedicated {@link RabbitTemplate} for the heartbeat verifier, initialized
     * with a fixed {@code replyTimeout} at construction time and backed by the
     * dedicated heartbeat connection factory.
     *
     * <p>This isolates PING/PONG verification traffic from the data-plane
     * connection used by reporters, sync publishers, and broadcasters — see
     * issue P2-6.8 and {@link HeartbeatConnectionConfiguration}.
     *
     * @param connectionFactory the dedicated heartbeat connection factory
     * @param properties        the HotKey configuration properties
     * @return a new {@link RabbitTemplate} with {@code replyTimeout} set to
     *         {@code properties.heartbeat.pingTimeoutMs}
     */
    @Bean
    @ConditionalOnMissingBean(name = "zetaVerifyRabbitTemplate")
    public RabbitTemplate zetaVerifyRabbitTemplate(
      @Qualifier("zetaHeartbeatConnectionFactory") ConnectionFactory connectionFactory,
      ZetaProperties properties
    ) {
      RabbitTemplate t = new RabbitTemplate(connectionFactory);
      t.setReplyTimeout((int) properties.getHeartbeat().getPingTimeoutMs());
      return t;
    }

    /**
     * Create the {@link WorkerHeartbeatVerifier} that periodically PINGs Workers
     * to verify they are alive.
     *
     * @param verifyRabbitTemplate the dedicated RabbitMQ template with fixed replyTimeout
     * @param healthView           the cluster health view to update on verification results
     * @param properties           the HotKey configuration properties
     * @return a new {@link WorkerHeartbeatVerifier} instance
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    public WorkerHeartbeatVerifier workerHeartbeatVerifier(
      @Qualifier("zetaVerifyRabbitTemplate") RabbitTemplate verifyRabbitTemplate,
      HealthView healthView,
      ZetaProperties properties,
      @Qualifier("hotKeyScheduler") ScheduledExecutorService hotKeyScheduler
    ) {
      return new WorkerHeartbeatVerifier(
        verifyRabbitTemplate,
        healthView,
        properties.getInstanceId(),
        new WorkerHeartbeatVerifier.VerifierConfig(
          properties.getHeartbeat().getVerifyIntervalMs(),
          properties.getHeartbeat().getPingTimeoutMs(),
          properties.getHeartbeat().getVerifyMaxBackoffMs()
        ),
        hotKeyScheduler
      );
    }
  }

  /**
   * Dedicated {@link CachingConnectionFactory} for heartbeat/verification traffic,
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
   * <p><b>Final channel mapping (ADR-0010 addendum, 2026-07):</b>
   * <ul>
   *   <li><b>Control plane</b> (this factory): app heartbeat consumption, verify
   *       PING/PONG, worker heartbeat producer, worker config gossip.</li>
   *   <li><b>Data plane</b> (Boot {@code rabbitConnectionFactory}): report publish/consume,
   *       cache-sync publish/consume, worker decision consume, worker HOT/COOL broadcast.</li>
   * </ul>
   *
   * <p><b>Why {@code @Primary} is kept:</b> removing it would leave two
   * non-primary {@code ConnectionFactory} candidates, causing Spring Boot's
   * {@code RabbitTemplate} ({@code @ConditionalOnSingleCandidate}) to silently
   * back off. Downstream code injecting {@code RabbitTemplate} would then resolve
   * to {@code zetaReportRabbitTemplate} and inherit its JSON message converter —
   * a silent format change for the consumer's own messages. {@code @Primary} here
   * preserves single-candidate resolution for unqualified injections; all Zeta
   * data-plane beans instead resolve the data-plane factory explicitly via
   * {@link #dataPlaneConnectionFactory}.
   *
   * <p><b>Gating:</b> this configuration only activates when a feature that
   * actually consumes the control-plane connection is enabled — the app-side
   * worker listener ({@code zeta.worker-listener.enabled=true}) or worker mode
   * ({@code zeta.worker.enabled=true}) — see {@link HeartbeatFeatureEnabled}.
   * A plain app with report/sync only (or everything disabled) gets no extra
   * {@code @Primary} factory, so unqualified {@code ConnectionFactory}
   * injections in the host application resolve to the application's own
   * factory exactly as before Zeta was added.
   */
  @Configuration
  @Conditional(HeartbeatFeatureEnabled.class)
  static class HeartbeatConnectionConfiguration {

    @Primary
    @Bean("zetaHeartbeatConnectionFactory")
    @ConditionalOnMissingBean(name = "zetaHeartbeatConnectionFactory")
    @SuppressWarnings("all")
    public CachingConnectionFactory heartbeatConnectionFactory(
      ObjectProvider<RabbitProperties> propsProvider,
      ResourceLoader resourceLoader
    ) {
      RabbitProperties props = propsProvider.getIfAvailable();
      if (props == null) {
        // Fallback: create an unconfigured factory; Spring Boot's
        // default connection factory will be used in most environments.
        return new CachingConnectionFactory();
      }
      RabbitConnectionFactoryBean factoryBean = new RabbitConnectionFactoryBean();
      new RabbitConnectionFactoryBeanConfigurer(resourceLoader, props).configure(factoryBean);
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
   * (heartbeat consumption, PING/PONG verification, heartbeat production,
   * config gossip). Report and cache-sync are pure data-plane features and
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
