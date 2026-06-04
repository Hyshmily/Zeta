package io.github.hyshmily.hotkey.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hyshmily.hotkey.HotKey;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
class HotKeyCacheRedisIT extends AbstractIntegrationIT {

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
  HotKey hotKey;

  @Autowired
  StringRedisTemplate redisTemplate;

  @Test
  void cachePutThrough_shouldStoreInL1() {
    String key = "it:putthrough:" + UUID.randomUUID();
    hotKey.putThrough(key, "hello-redis", () -> {});
    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> {
        Optional<Object> cached = hotKey.peek(key);
        assertThat(cached).isPresent();
        assertThat(cached.get()).isEqualTo("hello-redis");
      });
  }

  @Test
  void cacheGet_withSupplier_shouldLoadAndCache() {
    String key = "it:get:" + UUID.randomUUID();
    Optional<Object> result = hotKey.get(key, () -> "supplied-value", 60000, 30000);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo("supplied-value");

    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isPresent());
  }

  @Test
  void invalidate_shouldRemoveFromL1() {
    String key = "it:invalidate:" + UUID.randomUUID();
    hotKey.putThrough(key, "to-remove", () -> {});
    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isPresent());

    hotKey.invalidate(key);
    assertThat(hotKey.peek(key)).isEmpty();
  }

  @Test
  void cacheGet_shouldReturnL1Hit_onSecondAccess() {
    String key = "it:l1hit:" + UUID.randomUUID();
    hotKey.putThrough(key, "cached-value", () -> {});

    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isPresent());

    Optional<Object> second = hotKey.peek(key);
    assertThat(second).isPresent();
  }
}
