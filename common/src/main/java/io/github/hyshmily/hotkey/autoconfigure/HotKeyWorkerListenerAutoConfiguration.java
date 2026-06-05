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
import io.github.hyshmily.hotkey.broadcast.WorkerListener;
import io.github.hyshmily.hotkey.broadcast.WorkerListenerProperties;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Auto-configuration for listening to Worker hot/cool decisions.
 * <p>
 * Activates when RedisTemplate is present and {@code hotkey.worker-listener.enabled=true}.
 * <p>
 * Exchange: {@code hotkey.broadcast.exchange} (FanoutExchange) — must match
 * {@code hotkey.worker.messaging.broadcast-exchange} on the Worker side.
 * Queue: {@code hotkey.worker:{instanceId}}
 */
@AutoConfiguration(after = { RedisAutoConfiguration.class, RabbitAutoConfiguration.class })
@ConditionalOnClass(name = "org.springframework.amqp.rabbit.core.RabbitTemplate")
@ConditionalOnBean(RedisTemplate.class)
@ConditionalOnProperty(prefix = "hotkey.worker-listener", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(WorkerListenerProperties.class)
public class HotKeyWorkerListenerAutoConfiguration {

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
    CacheExpireManager expireManager
  ) {
    return new WorkerListener(
      hotLocalCache,
      hotKeyRedisLoader,
      properties,
      hotKeyWorkerScheduler,
      expireManager
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
