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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.reporting.ReportMessage;
import io.github.hyshmily.hotkey.reporting.ReportPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Tests the full pipeline: app publishes a report → mock Worker consumes it →
 * mock Worker sends HOT decision → app promotes the key.
 */
@Testcontainers
@Tag("docker")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class EmbeddedWorkerIT extends AbstractIntegrationIT {

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

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired
  HotKey hotKey;

  @Autowired
  RabbitTemplate rabbitTemplate;

  @Autowired
  ConnectionFactory connectionFactory;

  @Autowired
  StringRedisTemplate redisTemplate;

  @Autowired
  ReportPublisher reportPublisher;

  /** Publishes a report via {@link ReportPublisher} and verifies a mock Worker consuming the report sends back a HOT decision. */
  @Test
  void reportPublished_shouldTriggerHotDecisionViaMockWorker() throws Exception {
    String key = "it:pipeline:" + UUID.randomUUID();
    String mockWorkerQueue = "it.mockworker." + UUID.randomUUID().toString().substring(0, 8);

    // --- Arrange ---

    // Write value to Redis (WorkerListener.handleHot reads via redisLoader)
    redisTemplate.opsForValue().set(key, "pipeline-value");
    // Put into L1 so the entry exists to be promoted
    hotKey.putThrough(key, "pipeline-value", () -> {});

    // Declare report exchange idempotently (matches existing type or creates if missing)
    RabbitAdmin admin = new RabbitAdmin(rabbitTemplate.getConnectionFactory());
    rabbitTemplate.execute(channel -> {
      channel.exchangeDeclare("hotkey.report.exchange", "fanout", true);
      return null;
    });
    Queue queue = new Queue(mockWorkerQueue, true, false, false);
    admin.declareQueue(queue);
    admin.declareBinding(
      BindingBuilder.bind(queue)
        .to(new org.springframework.amqp.core.FanoutExchange("hotkey.report.exchange"))
    );

    // Mock Worker: listens on the report queue, sends HOT decisions back
    SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
    container.setQueueNames(mockWorkerQueue);
    container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
    container.setMessageListener(
      (ChannelAwareMessageListener) (msg, channel) -> {
        try {
          ReportMessage report = MAPPER.readValue(
            new String(msg.getBody(), StandardCharsets.UTF_8),
            ReportMessage.class
          );

          for (String cacheKey : report.counts().keySet()) {
            MessageProperties hotProps = new MessageProperties();
            hotProps.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
            hotProps.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 1L);
            Message hotMsg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), hotProps);
            rabbitTemplate.send("hotkey.worker.exchange", "", hotMsg);
          }

          channel.basicAck(msg.getMessageProperties().getDeliveryTag(), false);
        } catch (Exception e) {
          channel.basicNack(msg.getMessageProperties().getDeliveryTag(), false, false);
        }
      }
    );
    container.start();

    try {
      // --- Act ---

      reportPublisher.publish("0", new ReportMessage("integration-test", System.currentTimeMillis(), Map.of(key, 42L)));

      // --- Assert ---

      await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(() -> assertThat(hotKey.isLocalHotKey(key)).isTrue());
    } finally {
      container.stop();
      admin.deleteQueue(mockWorkerQueue);
    }
  }
}
