package io.github.hyshmily.hotkey.integration.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
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

  @AfterAll
  static void cleanup() {
    if (rabbitmqProxy != null) {
      try { rabbitmqProxy.toxics().get("latency-down").remove(); } catch (Exception ignored) {}
      try { rabbitmqProxy.toxics().get("loss-down").remove(); } catch (Exception ignored) {}
    }
  }

  @BeforeEach
  void skipWhenToxiproxyUnavailable() {
    Assumptions.assumeTrue(toxiproxyAvailable, "Toxiproxy image not pullable on this network");
  }

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
      rabbitTemplate.send("hotkey.worker.exchange", "", msg);

      await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> assertThat(hotKey.isLocalHotKey(key)).isTrue());
    } finally {
      rabbitmqProxy.toxics().get("wrk-latency-down").remove();
    }
  }
}
