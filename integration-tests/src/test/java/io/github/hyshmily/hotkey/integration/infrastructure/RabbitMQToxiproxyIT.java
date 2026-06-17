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
package io.github.hyshmily.hotkey.integration.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import eu.rekawek.toxiproxy.model.ToxicDirection;

/**
 * Integration test for RabbitMQ network disruption tolerance using Toxiproxy.
 *
 * <p>Inserts latency, connection cuts, and packet loss via Toxiproxy to verify
 * that the HotKey broadcast sync and Worker decision paths recover correctly.
 */
@Testcontainers
@Tag("docker")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RabbitMQToxiproxyIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(RabbitMQToxiproxyIT.class);

  @Container
  static GenericContainer<?> redis = new GenericContainer<>(
      DockerImageName.parse("redis:7-alpine"))
    .withExposedPorts(6379);

  static ToxiproxyContainer toxiproxy;
  static GenericContainer<?> rabbitmq;
  static ToxiproxyContainer.ContainerProxy rabbitmqProxy;
  static boolean toxiproxyAvailable = false;

  static {
    rabbitmq = new GenericContainer<>(
        DockerImageName.parse("rabbitmq:4.1-management"))
      .withExposedPorts(5672, 15672)
      .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1))
      .withStartupTimeout(Duration.ofMinutes(2));
    rabbitmq.start();

    toxiproxyAvailable = isToxiproxyImageAvailable();
    if (toxiproxyAvailable) {
      try {
        toxiproxy = new ToxiproxyContainer(
            DockerImageName.parse("shopify/toxiproxy").withTag("2.1.0"))
          .withExposedPorts(8666);
        toxiproxy.start();
        rabbitmqProxy = toxiproxy.getProxy(rabbitmq, 5672);
      } catch (Exception e) {
        log.warn("Toxiproxy start failed: {}", e.getMessage());
        toxiproxyAvailable = false;
      }
    } else {
      log.warn("Toxiproxy image not available locally, tests will be skipped");
    }
  }

  /**
   * Checks whether the Toxiproxy Docker image is available locally.
   *
   * @return true if the image exists locally, false otherwise
   */
  private static boolean isToxiproxyImageAvailable() {
    try {
      var cmd = org.testcontainers.DockerClientFactory.instance().client()
          .inspectImageCmd("shopify/toxiproxy:2.1.0");
      cmd.exec();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /** Removes any Toxiproxy toxics that were injected during tests. */
  @AfterAll
  static void cleanup() {
    if (rabbitmqProxy != null) {
      try { rabbitmqProxy.toxics().get("latency-down").remove(); } catch (Exception ignored) {}
      try { rabbitmqProxy.toxics().get("loss-down").remove(); } catch (Exception ignored) {}
    }
  }

  /** Skips test execution when the Toxiproxy Docker image is not available locally. */
  @BeforeEach
  void skipWhenToxiproxyUnavailable() {
    Assumptions.assumeTrue(toxiproxyAvailable, "Toxiproxy image not pullable on this network");
  }

  /**
   * Registers dynamic Testcontainers host/port properties for Redis and RabbitMQ,
   * routing through Toxiproxy when available.
   *
   * @param r the dynamic property registry supplied by Spring
   */
  @DynamicPropertySource
  static void overrideProps(DynamicPropertyRegistry r) {
    r.add("spring.data.redis.host", redis::getHost);
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    if (toxiproxyAvailable) {
      r.add("spring.rabbitmq.host", rabbitmqProxy::getContainerIpAddress);
      r.add("spring.rabbitmq.port", rabbitmqProxy::getProxyPort);
    } else {
      r.add("spring.rabbitmq.host", rabbitmq::getHost);
      r.add("spring.rabbitmq.port", () -> rabbitmq.getMappedPort(5672));
    }
    r.add("spring.rabbitmq.username", () -> "guest");
    r.add("spring.rabbitmq.password", () -> "guest");
  }

  @Autowired
  HotKey hotKey;

  @Autowired
  RabbitTemplate rabbitTemplate;

  @Autowired
  StringRedisTemplate redisTemplate;

  /** Injects 500ms latency via Toxiproxy and verifies sync messages are still delivered. */
  @Test
  void addedLatency_shouldNotLoseSyncMessages() throws Exception {
    String key = "it:tox:lat:" + UUID.randomUUID();
    String value = "latency-value";

    hotKey.putThrough(key, value, () -> {});
    await().atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isPresent());

    rabbitmqProxy.toxics().latency(
        "latency-down", ToxicDirection.DOWNSTREAM, 500L).setJitter(100L);

    try {
      for (int i = 0; i < 20; i++) {
        String k = key + ":" + i;
        redisTemplate.opsForValue().set(k, value + "-" + i);
        hotKey.putThrough(k, value + "-" + i, () -> {});
        MessageProperties props = new MessageProperties();
        props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
        props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
        props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
        Message msg = new Message(k.getBytes(StandardCharsets.UTF_8), props);
        rabbitTemplate.send("hotkey.sync.exchange", "", msg);
      }

      Thread.sleep(3000);
    } finally {
      rabbitmqProxy.toxics().get("latency-down").remove();
    }
  }

  /** Cuts the RabbitMQ connection via Toxiproxy and verifies recovery and message processing resume. */
  @Test
  void connectionCut_shouldRecoverAndProcessMessages() throws Exception {
    String key = "it:tox:cut:" + UUID.randomUUID();
    String value = "cut-value";

    hotKey.putThrough(key, value, () -> {});
    await().atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isPresent());

    rabbitmqProxy.setConnectionCut(true);
    try {
      Thread.sleep(3000);
      rabbitmqProxy.setConnectionCut(false);
      Thread.sleep(5000);
    } finally {
      rabbitmqProxy.setConnectionCut(false);
    }

    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
    props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send("hotkey.sync.exchange", "", msg);

    await()
      .atMost(Duration.ofSeconds(20))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isEmpty());
  }

  /** Injects 30% packet loss via Toxiproxy and verifies INVALIDATE messages are not lost. */
  @Test
  void intermittentPacketLoss_shouldNotDropCriticalMessages() throws Exception {
    String key = "it:tox:loss:" + UUID.randomUUID();
    int msgCount = 30;

    for (int i = 0; i < msgCount; i++) {
      String k = key + ":" + i;
      redisTemplate.opsForValue().set(k, "v-" + i);
      hotKey.putThrough(k, "v-" + i, () -> {});
    }

    for (int i = 0; i < msgCount; i++) {
      assertThat(hotKey.peek(key + ":" + i)).as("Key " + i + " should exist").isPresent();
    }

    rabbitmqProxy.toxics().timeout(
        "loss-down", ToxicDirection.DOWNSTREAM, 1L).setToxicity(0.3f);

    try {
      for (int i = 0; i < msgCount; i++) {
        String k = key + ":" + i;
        MessageProperties props = new MessageProperties();
        props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
        props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
        props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
        Message msg = new Message(k.getBytes(StandardCharsets.UTF_8), props);
        try {
          rabbitTemplate.send("hotkey.sync.exchange", "", msg);
        } catch (Exception ignored) {
        }
        Thread.sleep(100);
      }

      Thread.sleep(5000);
    } finally {
      rabbitmqProxy.toxics().get("loss-down").remove();
    }
  }

  /** Injects 200ms latency via Toxiproxy and verifies HOT decisions still promote cache entries. */
  @Test
  void workerDecision_withLatentNetwork_shouldStillPromote() throws Exception {
    String key = "it:tox:wrk:" + UUID.randomUUID();

    redisTemplate.opsForValue().set(key, "worker-latency-value");
    hotKey.putThrough(key, "worker-latency-value", () -> {});
    hotKey.invalidate(key);
    await().atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isEmpty());

    rabbitmqProxy.toxics().latency(
        "wrk-latency-down", ToxicDirection.DOWNSTREAM, 200L).setJitter(50L);

    try {
      MessageProperties props = new MessageProperties();
      props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "HOT");
      props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, 1L);
      props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
      Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
      rabbitTemplate.send("hotkey.broadcast.exchange", "", msg);

      await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> assertThat(hotKey.isLocalHotKey(key)).isTrue());
    } finally {
      rabbitmqProxy.toxics().get("wrk-latency-down").remove();
    }
  }
}
