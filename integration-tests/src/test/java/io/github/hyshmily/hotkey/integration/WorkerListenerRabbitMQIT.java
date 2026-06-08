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
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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

@Testcontainers
@Tag("docker")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
/** Integration test for Worker HOT/COOL decision delivery via RabbitMQ. */
class WorkerListenerRabbitMQIT extends AbstractIntegrationIT {

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
  HotKey hotKey;

  @Autowired
  StringRedisTemplate redisTemplate;

  @Test
  void hotDecision_shouldPromoteToHOT() {
    String key = "it:worker:hot:" + UUID.randomUUID();
    hotKey.putThrough(key, "worker-promoted", () -> {});
    redisTemplate.opsForValue().set(key, "worker-promoted");
    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isPresent());

    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
    props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 1L);
    Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send("hotkey.worker.exchange", "", msg);

    await()
      .atMost(Duration.ofSeconds(10))
      .untilAsserted(() -> assertThat(hotKey.isLocalHotKey(key)).isTrue());
  }

  @Test
  void coolDecision_shouldDowngrade() {
    String key = "it:worker:cool:" + UUID.randomUUID();
    hotKey.putThrough(key, "to-cool", () -> {});
    redisTemplate.opsForValue().set(key, "to-cool");

    MessageProperties hotProps = new MessageProperties();
    hotProps.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
    hotProps.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 1L);
    Message hotMsg = new Message(key.getBytes(StandardCharsets.UTF_8), hotProps);
    rabbitTemplate.send("hotkey.worker.exchange", "", hotMsg);

    await()
      .atMost(Duration.ofSeconds(10))
      .until(() -> hotKey.isLocalHotKey(key));

    MessageProperties coolProps = new MessageProperties();
    coolProps.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "COOL");
    coolProps.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 2L);
    Message coolMsg = new Message(key.getBytes(StandardCharsets.UTF_8), coolProps);
    rabbitTemplate.send("hotkey.worker.exchange", "", coolMsg);

    await()
      .atMost(Duration.ofSeconds(10))
      .untilAsserted(() -> assertThat(hotKey.isLocalHotKey(key)).isFalse());
  }
}
