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
import io.github.hyshmily.hotkey.broadcast.CacheSyncListener;
import io.github.hyshmily.hotkey.broadcast.CacheSyncProperties;
import io.github.hyshmily.hotkey.broadcast.CacheSyncPublisher;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Autoconfiguration for instance-to-instance cache synchronization (INVALIDATE / REFRESH).
 * <p>
 * Activates when RabbitTemplate + RedisTemplate are present and {@code hotkey.sync.enabled=true}.
 * <p>
 * Exchange: {@code hotkey.sync.exchange} (FanoutExchange)
 * Queue: {@code hotkey.sync:{instanceId}}
 */
@AutoConfiguration
@ConditionalOnClass(
  name = { "org.springframework.amqp.rabbit.core.RabbitTemplate", "org.springframework.data.redis.core.RedisTemplate" }
)
@ConditionalOnProperty(prefix = "hotkey.sync", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(CacheSyncProperties.class)
public class HotKeySyncAutoConfiguration {

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
  public Function<String, Object> hotKeyRedisLoader(RedisTemplate<String, Object> redisTemplate) {
    return key -> redisTemplate.opsForValue().get(key);
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
    CacheExpireManager expireManager
  ) {
    return new CacheSyncListener(hotLocalCache, hotKeyRedisLoader, properties, hotKeySyncScheduler, expireManager);
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
    container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
    container.setConcurrentConsumers(properties.getConcurrentConsumers());
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
