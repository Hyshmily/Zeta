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
import io.github.hyshmily.zeta.sync.local.DefaultSyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncHook;
import io.github.hyshmily.zeta.sync.worker.DefaultWorkerDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHandler;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionHook;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatVerifier;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.sync.worker.WorkerListenerProperties;
import io.github.hyshmily.zeta.util.InstanceIdGenerator;
import io.github.hyshmily.zeta.util.SystemLoadMonitor;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import java.time.Duration;
import io.github.hyshmily.zeta.util.impl.SystemLoadMonitorImpl;
import io.github.hyshmily.zeta.util.ratelimit.SreRateLimiter;
import io.github.hyshmily.zeta.util.ratelimit.impl.SreRateLimiterImpl;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Unified AMQP auto-configuration for HotKey messaging: app-to-Worker reporting,
 * instance-to-instance cache sync, and Worker decision listening.
 *
 * <p>Conditionally activates when {@link RabbitTemplate} is on the classpath.
 * Sub-groups for cache sync and Worker listener additionally require Redis.
 *
 * <p><b>Report</b> ({@code zeta.reportToWorker.enabled}, default {@code true}):
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
    public DirectExchange hotkeyReportExchange(ZetaProperties properties) {
      return new DirectExchange(properties.getReportExchange(), true, false);
    }

    /**
     * Create the {@link MessageConverter} for serializing reportToWorker messages to JSON.
     * <p>
     * Uses Jackson JSON serialization (not Java serialization) for efficiency and cross-version
     * compatibility.
     *
     * @return a new {@link Jackson2JsonMessageConverter} instance
     */
    @Bean("zetaReportMessageConverter")
    public MessageConverter reportMessageConverter() {
      return new Jackson2JsonMessageConverter();
    }

    /**
     * Dedicated {@link RabbitTemplate} for report publishing.
     * Uses a dedicated instance (not the container-level shared template) so that
     * Zeta's JSON serialization is isolated from the application's own message
     * converter — see bidirectional-converter-pollution issue (P1-5.1).
     *
     * @param connectionFactory the data-plane (Boot default) RabbitMQ connection factory
     * @param converter         the Zeta JSON message converter
     * @return a new {@link RabbitTemplate} with Zeta's JSON converter
     */
    @Primary
    @Bean("zetaReportRabbitTemplate")
    @ConditionalOnMissingBean(name = "zetaReportRabbitTemplate")
    public RabbitTemplate zetaReportRabbitTemplate(
      @Qualifier("rabbitConnectionFactory") ConnectionFactory connectionFactory,
      @Qualifier("zetaReportMessageConverter") MessageConverter converter
    ) {
      RabbitTemplate t = new RabbitTemplate(connectionFactory);
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
    public BbrRateLimiterImpl hotKeyBbrRateLimiter(SystemLoadMonitor cpuMonitor, ZetaProperties properties) {
      ZetaProperties.ReporterLimiter cfg = properties.getReporter();
      return new BbrRateLimiterImpl(
        cpuMonitor,
        cfg.getCpuThreshold(),
        cfg.getBbrWindowMs(),
        cfg.getBbrWindowBuckets(),
        cfg.getBbrCooldownMs()
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
            properties.getExpectedWorkerCount(),
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
     * @param connectionFactory the data-plane (Boot default) RabbitMQ connection factory
     * @return a new {@link RabbitTemplate} instance
     */
    @Bean("zetaSyncRabbitTemplate")
    @ConditionalOnMissingBean(name = "zetaSyncRabbitTemplate")
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public RabbitTemplate zetaSyncRabbitTemplate(
      @Qualifier("rabbitConnectionFactory") ConnectionFactory connectionFactory
    ) {
      return new RabbitTemplate(connectionFactory);
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
      SnowflakeIdGenerator snowflakeIdGenerator
    ) {
      return new CacheSyncPublisher(rabbitTemplate, properties, snowflakeIdGenerator);
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
     * Default Redis loader used by the sync listener to refresh cache entries via {@code GET}.
     *
     * @param stringRedisTemplate the String-based Redis template for reading values
     * @return a {@link CacheLoader} that reads a key from Redis and returns its value
     */
    @Bean
    @ConditionalOnMissingBean(CacheLoader.class)
    public CacheLoader hotKeyRedisLoader(StringRedisTemplate stringRedisTemplate) {
      return new io.github.hyshmily.zeta.cache.loader.RedisCacheLoader(stringRedisTemplate);
    }

    /**
     * Default {@link SyncDecisionHandler} that performs Redis-backed REFRESH,
     * version-guarded INVALIDATE, batch INVALIDATE_ALL, and RULES_SYNC.
     */
    @Bean
    @ConditionalOnMissingBean(SyncDecisionHandler.class)
    public SyncDecisionHandler defaultSyncDecisionHandler(
      Cache<String, Object> hotLocalCache,
      CacheLoader hotKeyRedisLoader,
      ExpireManager expireManager,
      RuleMatcher ruleMatcher,
      ObjectProvider<SyncHook> syncHookProvider
    ) {
      return new DefaultSyncDecisionHandler(
        hotLocalCache,
        hotKeyRedisLoader,
        expireManager,
        ruleMatcher,
        syncHookProvider.stream().toList()
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
      SyncDecisionHandler decisionHandler
    ) {
      return new CacheSyncListener(properties, syncScheduler, decisionHandler);
    }

    /**
     * Create the AMQP message listener container that drives the sync listener.
     *
     * @param connectionFactory  the RabbitMQ connection factory
     * @param cacheSyncListener  the sync message handler
     * @param properties         the cache sync configuration properties
     * @return a configured {@link SimpleMessageListenerContainer}
     */
    @Bean
    @ConditionalOnBean(ConnectionFactory.class)
    public SimpleMessageListenerContainer syncListenerContainer(
      @Qualifier("rabbitConnectionFactory") ConnectionFactory connectionFactory,
      CacheSyncListener cacheSyncListener,
      CacheSyncProperties properties
    ) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
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
   * Inner configuration for receiving Worker HOT/COOL decisions.
   * Creates a FanoutExchange, per-instance queue with TTL, binding, worker listener,
   * listener container, and a dedicated scheduled executor.
   * Requires Redis and {@code zeta.worker-listener.enabled=true}.
   */
  @Configuration
  @ConditionalOnClass(name = "org.springframework.data.redis.core.RedisTemplate")
  @ConditionalOnBean(name = "rabbitConnectionFactory")
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
     * Default {@link WorkerDecisionHandler} that performs Redis-backed HOT promotion
     * and COOL downgrade with SRE rate limiting and version guarding.
     */
    @Bean
    @ConditionalOnMissingBean(WorkerDecisionHandler.class)
    public WorkerDecisionHandler defaultWorkerDecisionHandler(
      Cache<String, Object> hotLocalCache,
      CacheLoader hotKeyRedisLoader,
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
        hotKeyRedisLoader,
        expireManager,
        sreRateLimiterProvider.getIfAvailable(),
        vc,
        workerDecisionHookProvider.stream().toList()
      );
    }

    /**
     * Create the listener that processes HOT/COOL decisions send by the Worker.
     *
     * @param properties          the Worker listener configuration properties
     * @param decisionHandler     the strategy for processing HOT/COOL decisions
     * @return a new {@link WorkerListener} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkerListener workerListener(
      WorkerListenerProperties properties,
      @Qualifier("hotKeyWorkerSchedScheduler") ScheduledExecutorService workerSchedScheduler,
      WorkerDecisionHandler decisionHandler
    ) {
      return new WorkerListener(properties, workerSchedScheduler, decisionHandler);
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
     * @param connectionFactory the RabbitMQ connection factory
     * @param hotkeyWorkerQueue the per-instance Worker listener queue
     * @param workerListener    the Worker decision listener
     * @param properties        the Worker listener configuration properties
     * @return a configured {@link SimpleMessageListenerContainer}
     */
    @Bean
    @ConditionalOnMissingBean(name = "workerListenerContainer")
    public SimpleMessageListenerContainer workerListenerContainer(
      @Qualifier("rabbitConnectionFactory") ConnectionFactory connectionFactory,
      Queue hotkeyWorkerQueue,
      WorkerListener workerListener,
      WorkerListenerProperties properties
    ) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
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
   * data-plane beans instead qualify explicitly for {@code rabbitConnectionFactory}.
   */
  @Configuration
  static class HeartbeatConnectionConfiguration {

    @Primary
    @Bean("zetaHeartbeatConnectionFactory")
    @ConditionalOnMissingBean(name = "zetaHeartbeatConnectionFactory")
    public CachingConnectionFactory heartbeatConnectionFactory(ObjectProvider<RabbitProperties> propsProvider) {
      RabbitProperties props = propsProvider.getIfAvailable();
      if (props == null) {
        // Fallback: create an unconfigured factory; Spring Boot's
        // default connection factory will be used in most environments.
        return new CachingConnectionFactory();
      }
      CachingConnectionFactory cf = new CachingConnectionFactory(props.getHost(), props.getPort());
      cf.setUsername(props.getUsername());
      cf.setPassword(props.getPassword());
      String vh = props.getVirtualHost();
      cf.setVirtualHost(vh != null ? vh : "/");
      Duration hb = props.getRequestedHeartbeat();
      cf.setRequestedHeartBeat(hb != null ? (int) hb.getSeconds() : 60);
      Duration ct = props.getConnectionTimeout();
      cf.setConnectionTimeout(ct != null ? (int) ct.toMillis() : 60000);
      return cf;
    }
  }
}
