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

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.ROUTING_KEY_HEARTBEAT;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.cache.cachesupport.ExpireManager;
import io.github.hyshmily.hotkey.cache.loader.CacheLoader;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.reporting.BbrRateLimiter;
import io.github.hyshmily.hotkey.reporting.KeyReporter;
import io.github.hyshmily.hotkey.reporting.ReportPublisher;
import io.github.hyshmily.hotkey.reporting.impl.BbrRateLimiterImpl;
import io.github.hyshmily.hotkey.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.HealthView;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sharding.impl.HealthViewImpl;
import io.github.hyshmily.hotkey.sharding.impl.RingManagerImpl;
import io.github.hyshmily.hotkey.sync.local.CacheSyncListener;
import io.github.hyshmily.hotkey.sync.local.CacheSyncProperties;
import io.github.hyshmily.hotkey.sync.local.CacheSyncPublisher;
import io.github.hyshmily.hotkey.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.hotkey.sync.worker.WorkerHeartbeatVerifier;
import io.github.hyshmily.hotkey.sync.worker.WorkerListener;
import io.github.hyshmily.hotkey.sync.worker.WorkerListenerProperties;
import io.github.hyshmily.hotkey.util.HotKeyThreadFactory;
import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
import io.github.hyshmily.hotkey.util.SystemLoadMonitor;
import io.github.hyshmily.hotkey.util.impl.SystemLoadMonitorImpl;
import io.github.hyshmily.hotkey.util.ratelimit.SreRateLimiter;
import io.github.hyshmily.hotkey.util.ratelimit.impl.SreRateLimiterImpl;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Unified AMQP auto-configuration for HotKey messaging: app-to-Worker reporting,
 * instance-to-instance cache sync, and Worker decision listening.
 *
 * <p>Conditionally activates when {@link RabbitTemplate} is on the classpath.
 * Sub-groups for cache sync and Worker listener additionally require Redis.
 *
 * <p><b>Report</b> ({@code hotkey.report.enabled}, default {@code true}):
 * app instance aggregates access counts and sends them to the Worker via
 * {@link DirectExchange}. No Redis dependency.
 *
 * <p><b>Cache Sync</b> ({@code hotkey.sync.enabled=true}):
 * instance-to-instance INVALIDATE / REFRESH broadcasts via {@link FanoutExchange}.
 * Requires Redis for version tracking.
 *
 * <p><b>Worker Listener</b> ({@code hotkey.worker-listener.enabled=true}):
 * receives HOT/COOL decisions from the Worker via {@link FanoutExchange}.
 * Requires Redis.
 */
@Internal
@AutoConfiguration(after = { RedisAutoConfiguration.class, RabbitAutoConfiguration.class })
@ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
@EnableConfigurationProperties({ HotKeyProperties.class, CacheSyncProperties.class, WorkerListenerProperties.class })
public class HotKeyAmqpAutoConfiguration {

  /**
   * Inner configuration for app-to-Worker report routing via DirectExchange.
   * Creates the exchange, publisher, ring manager (optional), reporter, and report scheduler.
   * Active by default when a {@link RabbitTemplate} bean is present.
   */
  @Configuration
  @ConditionalOnBean(RabbitTemplate.class)
  @ConditionalOnProperty(prefix = "hotkey.report", name = "enabled", havingValue = "true", matchIfMissing = true)
  static class ReportConfiguration {

    /**
     * Declare the DirectExchange for report routing (app → Worker).
     * Routing keys ({@code report.<appName>.<nodeId>}) ensure each key's
     * messages land on the correct worker queue.
     *
     * @param properties the HotKey configuration properties
     * @return a durable, non-auto-delete {@link DirectExchange}
     */
    @Bean
    public DirectExchange hotkeyReportExchange(HotKeyProperties properties) {
      return new DirectExchange(properties.getReportExchange(), true, false);
    }

    /**
     * Create the {@link MessageConverter} for serializing report messages to JSON.
     * <p>
     * Uses Jackson JSON serialization (not Java serialization) for efficiency and cross-version
     * compatibility.
     *
     * @return a new {@link Jackson2JsonMessageConverter} instance
     */
    @Bean
    @ConditionalOnMissingBean(MessageConverter.class)
    public MessageConverter reportMessageConverter() {
      return new Jackson2JsonMessageConverter();
    }

    /**
     * Create the {@link ReportPublisher} for sending batched access-count reports to the Worker.
     *
     * @param rabbitTemplate the RabbitMQ template for publishing messages
     * @param properties     the HotKey configuration properties
     * @return a new {@link ReportPublisher} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public ReportPublisher reportPublisher(RabbitTemplate rabbitTemplate, HotKeyProperties properties) {
      return new ReportPublisher(rabbitTemplate, properties.getReportExchange(), properties.getAppName());
    }

    /**
     * Create the {@link RingManager} for consistent-hashing report routing.
     *
     * @param properties the HotKey configuration properties
     * @return a new {@link RingManager} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public RingManager ringManager(HotKeyProperties properties) {
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
    public SystemLoadMonitor hotKeyCpuMonitor(
      HotKeyProperties properties,
      @Qualifier("hotKeyScheduler") ScheduledExecutorService hotKeyScheduler
    ) {
      HotKeyProperties.ReporterLimiter cfg = properties.getReporter();
      return new SystemLoadMonitorImpl(hotKeyScheduler, cfg.getCpuPollIntervalMs(), cfg.getCpuDecay());
    }

    /**
     * Create the BBR adaptive rate limiter for the report publisher.
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
      prefix = "hotkey.local.reporter",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true
    )
    public BbrRateLimiterImpl hotKeyBbrRateLimiter(SystemLoadMonitor cpuMonitor, HotKeyProperties properties) {
      HotKeyProperties.ReporterLimiter cfg = properties.getReporter();
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
     * @param reportPublisher       the report publisher for sending batches
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
      HotKeyProperties properties,
      RingManager ringManager,
      ObjectProvider<HealthView> healthViewProvider,
      ObjectProvider<BbrRateLimiterImpl> bbrRateLimiterProvider
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
        )
      );
      bbrRateLimiterProvider.ifAvailable(reporter::setBbrRateLimiter);
      return reporter;
    }
  }

  /**
   * Inner configuration for instance-to-instance cache synchronization.
   * Creates a FanoutExchange, per-instance queue with TTL, binding, publisher,
   * Redis loader, sync listener, and a dedicated scheduled executor.
   * Requires Redis and {@code hotkey.sync.enabled=true}.
   */
  @Configuration
  @ConditionalOnClass(name = "org.springframework.data.redis.core.RedisTemplate")
  @ConditionalOnProperty(prefix = "hotkey.sync", name = "enabled", havingValue = "true")
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
     * Create the cache sync publisher for sending INVALIDATE/REFRESH messages.
     *
     * @param rabbitTemplate the RabbitMQ template for publishing messages
     * @param properties     the cache sync configuration properties
     * @return a new {@link CacheSyncPublisher} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncPublisher cacheSyncPublisher(RabbitTemplate rabbitTemplate, CacheSyncProperties properties) {
      return new CacheSyncPublisher(rabbitTemplate, properties);
    }

    /**
     * Dedicated scheduler for jitter-delayed cache-update tasks received from
     * peer instances. Isolated from {@code hotKeyScheduler} so that synchronous
     * Redis GETs performed by {@code handleRefresh} can never starve the
     * 50 ms reporter flush tick.
     *
     * <p>Pool size is {@code hotkey.sync.scheduler-pool-size} (default 4) but
     * should be at least {@code hotkey.sync.concurrent-consumers × 2}.
     *
     * @param properties the cache sync configuration properties
     * @return a daemon-thread scheduled executor named {@code hotkey-sync-sched-N}
     */
    @Bean(name = "hotKeySyncScheduler", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "hotKeySyncScheduler")
    public ScheduledExecutorService hotKeySyncScheduler(CacheSyncProperties properties) {
      int poolSize = Math.max(properties.getSchedulerPoolSize(), properties.getConcurrentConsumers() * 2);
      return Executors.newScheduledThreadPool(
        poolSize,
        new HotKeyThreadFactory(HotKeyConstants.THREAD_PREFIX_SCHEDULER + "-sync")
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
      return new io.github.hyshmily.hotkey.cache.loader.RedisCacheLoader(stringRedisTemplate);
    }

    /**
     * Create the sync listener that handles incoming INVALIDATE/REFRESH messages from peers.
     *
     * @param hotLocalCache       the L1 Caffeine cache
     * @param hotKeyRedisLoader   the function for loading values from Redis
     * @param properties          the cache sync configuration properties
     * @param expireManager       the cache expiry manager
     * @param ruleMatcher         the rule matcher for key matching
     * @return a new {@link CacheSyncListener} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncListener cacheSyncListener(
      Cache<String, Object> hotLocalCache,
      CacheLoader hotKeyRedisLoader,
      CacheSyncProperties properties,
      @Qualifier("hotKeySyncScheduler") ScheduledExecutorService syncScheduler,
      ExpireManager expireManager,
      RuleMatcher ruleMatcher
    ) {
      return new CacheSyncListener(
        hotLocalCache,
        hotKeyRedisLoader,
        properties,
        syncScheduler,
        expireManager,
        ruleMatcher
      );
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
      ConnectionFactory connectionFactory,
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
   * Requires Redis and {@code hotkey.worker-listener.enabled=true}.
   */
  @Configuration
  @ConditionalOnClass(name = "org.springframework.data.redis.core.RedisTemplate")
  @ConditionalOnBean(RedisTemplate.class)
  @ConditionalOnProperty(prefix = "hotkey.worker-listener", name = "enabled", havingValue = "true")
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
    public TopicExchange hotkeyHeartbeatExchange(HotKeyProperties properties) {
      return new TopicExchange(properties.getHeartbeat().getExchangeName(), true, false);
    }

    /**
     * Create the per-instance non-durable heartbeat queue that auto-deletes on disconnect.
     *
     * @return a non-durable, auto-delete {@link Queue}
     */
    @Bean
    public Queue hotkeyHeartbeatQueue() {
      return QueueBuilder.nonDurable("hotkey.heartbeat:" + InstanceIdGenerator.get()).autoDelete().build();
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
      return BindingBuilder.bind(hotkeyHeartbeatQueue).to(hotkeyHeartbeatExchange).with(ROUTING_KEY_HEARTBEAT + "*");
    }

    /**
     * Create the SRE adaptive rate limiter for WorkerListener HOT-path throttling.
     * <p>
     * Disabled when {@code hotkey.worker-listener.sre.enabled=false}.
     *
     * @param properties the Worker listener configuration properties
     * @return a new {@link SreRateLimiter} instance
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
      prefix = "hotkey.worker-listener.sre",
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
     * @return a daemon-thread scheduled executor named {@code hotkey-worker-sched-N}
     */
    @Bean(name = "hotKeyWorkerSchedScheduler", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "hotKeyWorkerSchedScheduler")
    public ScheduledExecutorService hotKeyWorkerSchedScheduler(WorkerListenerProperties properties) {
      int poolSize = Math.max(properties.getSchedulerPoolSize(), properties.getConcurrentConsumers() * 2);
      return Executors.newScheduledThreadPool(
        poolSize,
        new HotKeyThreadFactory(HotKeyConstants.THREAD_PREFIX_SCHEDULER + "-worker")
      );
    }

    /**
     * Create the listener that processes HOT/COOL decisions send by the Worker.
     *
     * @param hotLocalCache          the L1 Caffeine cache
     * @param hotKeyRedisLoader      the function for loading values from Redis
     * @param properties             the Worker listener configuration properties
     * @param expireManager          the cache expiry manager
     * @param sreRateLimiterProvider optional provider for the SRE rate limiter
     * @return a new {@link WorkerListener} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkerListener workerListener(
      Cache<String, Object> hotLocalCache,
      CacheLoader hotKeyRedisLoader,
      WorkerListenerProperties properties,
      @Qualifier("hotKeyWorkerSchedScheduler") ScheduledExecutorService workerSchedScheduler,
      ExpireManager expireManager,
      ObjectProvider<SreRateLimiterImpl> sreRateLimiterProvider
    ) {
      return new WorkerListener(
        hotLocalCache,
        hotKeyRedisLoader,
        properties,
        workerSchedScheduler,
        expireManager,
        sreRateLimiterProvider.getIfAvailable()
      );
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
      ConnectionFactory connectionFactory,
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
      ConnectionFactory connectionFactory,
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
     * Create the {@link WorkerHeartbeatVerifier} that periodically PINGs Workers
     * to verify they are alive.
     *
     * @param rabbitTemplate the RabbitMQ template for sending PING messages
     * @param healthView     the cluster health view to update on verification results
     * @param properties     the HotKey configuration properties
     * @return a new {@link WorkerHeartbeatVerifier} instance
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    public WorkerHeartbeatVerifier workerHeartbeatVerifier(
      RabbitTemplate rabbitTemplate,
      HealthView healthView,
      HotKeyProperties properties,
      @Qualifier("hotKeyScheduler") ScheduledExecutorService hotKeyScheduler
    ) {
      return new WorkerHeartbeatVerifier(
        rabbitTemplate,
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
}
