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

import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test for report message publishing to RabbitMQ.
 *
 * <p>Verifies that a report message sent to the report exchange can be consumed
 * from a bound queue.
 */
@Testcontainers
@Tag("docker")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ReportPublishRabbitMQIT extends AbstractIntegrationIT {

  @Container
  static GenericContainer<?> rabbitmq = new GenericContainer<>(
      org.testcontainers.utility.DockerImageName.parse("rabbitmq:4.1-management"))
    .withExposedPorts(5672, 15672)
    .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1))
    .withStartupTimeout(Duration.ofMinutes(2));

  @DynamicPropertySource
  static void overrideProps(DynamicPropertyRegistry r) {
    r.add("spring.rabbitmq.host", rabbitmq::getHost);
    r.add("spring.rabbitmq.port", () -> rabbitmq.getMappedPort(5672));
    r.add("spring.rabbitmq.username", () -> "guest");
    r.add("spring.rabbitmq.password", () -> "guest");
    r.add("hotkey.worker-listener.enabled", () -> "false");
  }

  @Autowired
  RabbitTemplate rabbitTemplate;

  @Test
  void reportPublisher_shouldSendMessageToExchange() {
    String testQueue = "it.report.verify." + UUID.randomUUID().toString().substring(0, 8);

    RabbitAdmin admin = new RabbitAdmin(rabbitTemplate.getConnectionFactory());
    admin.declareQueue(new org.springframework.amqp.core.Queue(testQueue, true, false, false));
    admin.declareExchange(new org.springframework.amqp.core.FanoutExchange("hotkey.report.exchange", true, false));
    admin.declareBinding(
      org.springframework.amqp.core.BindingBuilder
        .bind(new org.springframework.amqp.core.Queue(testQueue))
        .to(new org.springframework.amqp.core.FanoutExchange("hotkey.report.exchange"))
    );

    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "REFRESH");
    Message msg = new Message("test-message-body".getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send("hotkey.report.exchange", "report.integration-test.0", msg);

    Message received = rabbitTemplate.receive(testQueue, 5000);
    assertThat(received).isNotNull();
    assertThat(received.getBody()).isNotEmpty();

    admin.deleteQueue(testQueue);
  }
}
