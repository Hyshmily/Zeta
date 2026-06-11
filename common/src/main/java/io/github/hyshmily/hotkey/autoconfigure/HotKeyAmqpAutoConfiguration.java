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
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.reporting.ReportPublisher;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sync.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.ObjectProvider;
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
     */
    @Bean
    public DirectExchange hotkeyReportExchange(HotKeyProperties properties) {
      return new DirectExchange(properties.getReportExchange(), true, false);
    }

    /**
     * Create the {@link ReportPublisher} for sending batched access-count reports to the Worker.
     * <p>
     * Uses Jackson JSON serialization (not Java serialization) for efficiency and cross-version
     * compatibility.
     */
    @Bean
    @ConditionalOnMissingBean(MessageConverter.class)
    public MessageConverter reportMessageConverter() {
      return new Jackson2JsonMessageConverter();
    }

    @Bean
    @ConditionalOnMissingBean
    public ReportPublisher reportPublisher(RabbitTemplate rabbitTemplate, HotKeyProperties properties) {
      return new ReportPublisher(rabbitTemplate, properties.getReportExchange(), properties.getAppName());
    }

    /**
     * Create the {@link RingManager} for consistent-hashing report routing.
     */
    @Bean
    @ConditionalOnMissingBean
    public RingManager ringManager(HotKeyProperties properties) {
      return new RingManager(properties.getConsistentHashing().getVirtualNodes());
    }

    /**
     * Create the {@link HotKeyReporter} that aggregates per-key counts and flushes them
     * at the configured interval.
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    public HotKeyReporter hotKeyReporter(
      ReportPublisher reportPublisher,
      ScheduledExecutorService hotKeyReportScheduler,
      HotKeyProperties properties,
      RingManager ringManager
    ) {
      return new HotKeyReporter(
        reportPublisher,
        hotKeyReportScheduler,
        properties.getReportIntervalMs(),
        properties.getAppName(),
        properties.getQueueCapacity(),
        properties.getQueueOfferTimeoutMs(),
        properties.effectiveConsumerCount(),
        ringManager
      );
    }

    /**
     * Dedicated scheduler for periodic report flushing.
     */
    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService hotKeyReportScheduler() {
      return Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, HotKeyConstants.THREAD_PREFIX_REPORT);
        t.setDaemon(true);
        return t;
      });
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
  static class SyncConfiguration {

    /**
     * Create the FanoutExchange for broadcasting INVALIDATE/REFRESH messages
     * to all app instances.
     */
    @Bean
    public FanoutExchange hotkeySyncExchange(CacheSyncProperties properties) {
      return new FanoutExchange(properties.getExchangeName(), true, false);
    }

    /**
     * Create the per-instance sync queue with a 60-second TTL.
     */
    @Bean
    public Queue hotkeySyncQueue(CacheSyncProperties properties) {
      return QueueBuilder.durable(properties.getQueueName()).withArgument("x-message-ttl", 60_000).build();
    }

    /**
     * Bind the per-instance queue to the sync exchange.
     */
    @Bean
    public Binding hotkeySyncBinding(Queue hotkeySyncQueue, FanoutExchange hotkeySyncExchange) {
      return BindingBuilder.bind(hotkeySyncQueue).to(hotkeySyncExchange);
    }

    /**
     * Create the cache sync publisher for sending INVALIDATE/REFRESH messages.
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncPublisher cacheSyncPublisher(RabbitTemplate rabbitTemplate, CacheSyncProperties properties) {
      return new CacheSyncPublisher(rabbitTemplate, properties);
    }

    /**
     * Default Redis loader used by the sync listener to refresh cache entries via {@code GET}.
     */
    @Bean
    @ConditionalOnMissingBean(name = "hotKeyRedisLoader")
    public Function<String, Object> hotKeyRedisLoader(StringRedisTemplate stringRedisTemplate) {
      return key -> stringRedisTemplate.opsForValue().get(key);
    }

    /**
     * Create the sync listener that handles incoming INVALIDATE/REFRESH messages from peers.
     */
    @Bean
    @ConditionalOnMissingBean
    public CacheSyncListener cacheSyncListener(
      Cache<String, Object> hotLocalCache,
      Function<String, Object> hotKeyRedisLoader,
      CacheSyncProperties properties,
      ScheduledExecutorService hotKeySyncScheduler,
      CacheExpireManager expireManager,
      RuleMatcher ruleMatcher
    ) {
      return new CacheSyncListener(
        hotLocalCache,
        hotKeyRedisLoader,
        properties,
        hotKeySyncScheduler,
        expireManager,
        ruleMatcher
      );
    }

    /**
     * Create the AMQP message listener container that drives the sync listener.
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
      container.setMessageListener(
        (ChannelAwareMessageListener) (msg, channel) -> cacheSyncListener.handleSyncMessage(channel, msg)
      );
      return container;
    }

    /**
     * Scheduled executor for deferred Redis reads in the sync handler.
     */
    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService hotKeySyncScheduler(CacheSyncProperties properties) {
      return Executors.newScheduledThreadPool(properties.getSchedulerPoolSize(), r -> {
        Thread t = new Thread(r, HotKeyConstants.THREAD_PREFIX_SYNC);
        t.setDaemon(true);
        return t;
      });
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
  static class WorkerListenerConfiguration {

    /**
     * Create the FanoutExchange for broadcasting Worker HOT/COOL decisions
     * to all app instances.
     */
    @Bean
    public FanoutExchange hotkeyWorkerExchange(WorkerListenerProperties properties) {
      return new FanoutExchange(properties.getExchangeName(), true, false);
    }

    /**
     * Create the per-instance Worker listener queue with a 60-second TTL.
     */
    @Bean
    public Queue hotkeyWorkerQueue(WorkerListenerProperties properties) {
      return QueueBuilder.durable(properties.getQueueName()).withArgument("x-message-ttl", 60_000).build();
    }

    /**
     * Bind the per-instance queue to the Worker exchange.
     */
    @Bean
    public Binding hotkeyWorkerBinding(Queue hotkeyWorkerQueue, FanoutExchange hotkeyWorkerExchange) {
      return BindingBuilder.bind(hotkeyWorkerQueue).to(hotkeyWorkerExchange);
    }

    /**
     * Create the listener that processes HOT/COOL decisions broadcast by the Worker.
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkerListener workerListener(
      Cache<String, Object> hotLocalCache,
      Function<String, Object> hotKeyRedisLoader,
      WorkerListenerProperties properties,
      ScheduledExecutorService hotKeyWorkerScheduler,
      CacheExpireManager expireManager,
      RingManager ringManager
    ) {
      return new WorkerListener(
        hotLocalCache,
        hotKeyRedisLoader,
        properties,
        hotKeyWorkerScheduler,
        expireManager,
        ringManager
      );
    }

    /**
     * Create the AMQP message listener container that drives the Worker listener.
     */
    @Bean
    public SimpleMessageListenerContainer workerListenerContainer(
      ConnectionFactory connectionFactory,
      WorkerListener workerListener,
      WorkerListenerProperties properties
    ) {
      SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
      container.setQueueNames(properties.getQueueName());
      container.setAutoStartup(properties.isAutoStartup());
      container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
      container.setConcurrentConsumers(properties.getConcurrentConsumers());
      container.setPrefetchCount(properties.getPrefetchCount());
      container.setMessageListener(
        (ChannelAwareMessageListener) (msg, channel) -> workerListener.handleWorkerMessage(channel, msg)
      );
      return container;
    }

    /**
     * Scheduled executor for deferred Redis reads in the Worker listener.
     */
    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService hotKeyWorkerScheduler(WorkerListenerProperties properties) {
      return Executors.newScheduledThreadPool(properties.getSchedulerPoolSize(), r -> {
        Thread t = new Thread(r, HotKeyConstants.THREAD_PREFIX_WORKER);
        t.setDaemon(true);
        return t;
      });
    }
  }
}
