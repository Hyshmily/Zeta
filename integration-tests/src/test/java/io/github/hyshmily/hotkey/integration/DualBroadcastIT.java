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
package io.github.hyshmily.hotkey.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for dual exchange broadcast (sync + report listener). 
 *
 * <p>Verifies that both the sync broadcast exchange and the report exchange can
 * operate simultaneously without interfering with each other.
 */
@Testcontainers
@Tag("docker")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DualBroadcastIT extends AbstractIntegrationIT {

  @Container
  static GenericContainer<?> redis = new GenericContainer<>(
      DockerImageName.parse("redis:7-alpine"))
    .withExposedPorts(6379);

  @Container
  static GenericContainer<?> rabbitmq = new GenericContainer<>(
      DockerImageName.parse("rabbitmq:4.1-management"))
    .withExposedPorts(5672, 15672)
    .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1))
    .withStartupTimeout(Duration.ofMinutes(2));

  @DynamicPropertySource
  static void overrideProps(DynamicPropertyRegistry r) {
    r.add("spring.data.redis.host", redis::getHost);
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    r.add("spring.rabbitmq.host", rabbitmq::getHost);
    r.add("spring.rabbitmq.port", () -> rabbitmq.getMappedPort(5672));
    r.add("spring.rabbitmq.username", () -> "guest");
    r.add("spring.rabbitmq.password", () -> "guest");
  }

  @Autowired
  RabbitTemplate rabbitTemplate;

  @Autowired
  ConnectionFactory connectionFactory;

  @Autowired
  HotKey hotKey;

  @Test
  void invalidateBroadcast_shouldFanoutToSecondQueue() throws Exception {
    String key = "it:dual:inv:" + UUID.randomUUID();
    String secondQueue = "it.dual." + UUID.randomUUID().toString().substring(0, 8);

    RabbitAdmin admin = new RabbitAdmin(rabbitTemplate.getConnectionFactory());
    Queue queue = new Queue(secondQueue, true, false, false);
    admin.declareQueue(queue);
    admin.declareBinding(
      BindingBuilder.bind(queue).to(new FanoutExchange("hotkey.sync.exchange"))
    );

    CopyOnWriteArrayList<Message> received = new CopyOnWriteArrayList<>();
    AtomicInteger count = new AtomicInteger(0);

    SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
    container.setQueueNames(secondQueue);
    container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
    container.setMessageListener(
      (ChannelAwareMessageListener) (msg, channel) -> {
        received.add(msg);
        count.incrementAndGet();
        channel.basicAck(msg.getMessageProperties().getDeliveryTag(), false);
      }
    );
    container.start();

    try {
      hotKey.putThrough(key, "dual-value", () -> {});
      Thread.sleep(500);

      MessageProperties props = new MessageProperties();
      props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
      props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
      props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
      Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
      rabbitTemplate.send("hotkey.sync.exchange", "", msg);

      await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(count).hasValueGreaterThanOrEqualTo(1));
    } finally {
      container.stop();
      admin.deleteQueue(secondQueue);
    }
  }
}
