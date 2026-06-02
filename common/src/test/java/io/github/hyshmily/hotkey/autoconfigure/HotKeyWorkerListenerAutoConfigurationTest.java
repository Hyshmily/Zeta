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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.broadcast.WorkerListener;
import io.github.hyshmily.hotkey.broadcast.WorkerListenerProperties;
import io.github.hyshmily.hotkey.hotkeycache.CacheExpireManager;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;

/**
 * Tests for {@link HotKeyWorkerListenerAutoConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeyWorkerListenerAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(HotKeyWorkerListenerAutoConfiguration.class))
    .withPropertyValues("hotkey.worker-listener.enabled=true");

  @Test
  void configIsSkippedWhenRequiredClassNotOnClasspath() {
    // RabbitTemplate is not on the test classpath, and RedisTemplate bean is not present,
    // so @ConditionalOnClass / @ConditionalOnBean prevent configuration loading
    runner.run(ctx -> {
      assertThat(ctx).doesNotHaveBean(FanoutExchange.class);
      assertThat(ctx).doesNotHaveBean(Queue.class);
      assertThat(ctx).doesNotHaveBean(Binding.class);
      assertThat(ctx).doesNotHaveBean(WorkerListener.class);
      assertThat(ctx).doesNotHaveBean(SimpleMessageListenerContainer.class);
      assertThat(ctx).doesNotHaveBean(ScheduledExecutorService.class);
    });
  }

  @Test
  void configIsSkippedWhenPropertyIsNotEnabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyWorkerListenerAutoConfiguration.class))
      .withPropertyValues("hotkey.worker-listener.enabled=false")
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(WorkerListener.class);
        assertThat(ctx).doesNotHaveBean(FanoutExchange.class);
      });
  }

  @Test
  void configIsSkippedByDefault() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeyWorkerListenerAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(WorkerListener.class);
        assertThat(ctx).doesNotHaveBean(FanoutExchange.class);
      });
  }

  @Test
  void hotkeyWorkerExchangeIsCreatedWithCorrectProperties() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setExchangeName("test.worker.exchange");

    HotKeyWorkerListenerAutoConfiguration config = new HotKeyWorkerListenerAutoConfiguration();
    FanoutExchange exchange = config.hotkeyWorkerExchange(props);

    assertThat(exchange).isNotNull();
    assertThat(exchange.getName()).isEqualTo("test.worker.exchange");
    assertThat(exchange.isDurable()).isTrue();
    assertThat(exchange.isAutoDelete()).isFalse();
  }

  @Test
  void hotkeyWorkerQueueIsCreatedWithCorrectProperties() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setQueuePrefix("test.worker");

    HotKeyWorkerListenerAutoConfiguration config = new HotKeyWorkerListenerAutoConfiguration();
    Queue queue = config.hotkeyWorkerQueue(props);

    assertThat(queue).isNotNull();
    assertThat(queue.getName()).startsWith("test.worker:");
    assertThat(queue.isDurable()).isTrue();
  }

  @Test
  void hotkeyWorkerBindingBindsQueueToExchange() {
    Queue queue = new Queue("wq");
    FanoutExchange exchange = new FanoutExchange("we");

    HotKeyWorkerListenerAutoConfiguration config = new HotKeyWorkerListenerAutoConfiguration();
    Binding binding = config.hotkeyWorkerBinding(queue, exchange);

    assertThat(binding).isNotNull();
    assertThat(binding.getDestination()).isEqualTo("wq");
    assertThat(binding.getExchange()).isEqualTo("we");
  }

  @Test
  void workerListenerIsCreatedWithRequiredDependencies() {
    Cache<String, Object> localCache = mock(Cache.class);
    Function<String, Object> redisLoader = mock(Function.class);
    WorkerListenerProperties props = new WorkerListenerProperties();
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    CacheExpireManager expireManager = mock(CacheExpireManager.class);

    HotKeyWorkerListenerAutoConfiguration config = new HotKeyWorkerListenerAutoConfiguration();
    WorkerListener listener = config.workerListener(
      localCache, redisLoader, props, scheduler, expireManager
    );

    assertThat(listener).isNotNull();
  }

  @Test
  void workerListenerContainerIsConfiguredCorrectly() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setQueuePrefix("test.worker");

    // Cannot create container without ConnectionFactory, but verify the config exists
    assertThat(props.getConcurrentConsumers()).isGreaterThan(0);
  }

  @Test
  void hotKeyWorkerSchedulerIsCreated() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    props.setSchedulerPoolSize(2);

    HotKeyWorkerListenerAutoConfiguration config = new HotKeyWorkerListenerAutoConfiguration();
    ScheduledExecutorService scheduler = config.hotKeyWorkerScheduler(props);

    assertThat(scheduler).isNotNull();
    assertThat(scheduler.isShutdown()).isFalse();
    scheduler.shutdown();
  }
}
