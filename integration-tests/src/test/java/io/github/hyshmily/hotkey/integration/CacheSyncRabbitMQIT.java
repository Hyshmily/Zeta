package io.github.hyshmily.hotkey.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
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
class CacheSyncRabbitMQIT extends AbstractIntegrationIT {

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
  void invalidateMessage_shouldClearLocalCache() {
    String key = "it:sync:inv:" + UUID.randomUUID();
    hotKey.putThrough(key, "sync-value", () -> {});
    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isPresent());

    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
    props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send("hotkey.sync.exchange", "", msg);

    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isEmpty());
  }

  @Test
  void refreshMessage_shouldReloadValue() {
    String key = "it:sync:ref:" + UUID.randomUUID();
    hotKey.putThrough(key, "original-value", () -> {});
    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isPresent());

    redisTemplate.opsForValue().set(key, "refreshed-value");

    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "REFRESH");
    props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
    props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send("hotkey.sync.exchange", "", msg);

    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).hasValue("refreshed-value"));
  }
}
