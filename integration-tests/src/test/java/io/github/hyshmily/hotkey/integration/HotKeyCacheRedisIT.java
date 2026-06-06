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

/**
 * Integration tests for Redis-backed L2 cache with L1 (Caffeine) read-through.
 *
 * <p>Covers put-through, get-with-supplier, invalidate, and L1 hit scenarios using
 * Testcontainers for Redis 7 and RabbitMQ 4.1.
 */
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
