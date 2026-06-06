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

@Testcontainers
@Tag("docker")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
/** Integration test for L2 (Redis) read-through when L1 misses. */
class RedisL2ReadIT extends AbstractIntegrationIT {

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
  void get_shouldLoadFromRealRedisViaL2() {
    String key = "it:l2read:exists:" + UUID.randomUUID();
    redisTemplate.opsForValue().set(key, "real-redis-value");

    Optional<Object> result = hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 60000, 30000);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo("real-redis-value");

    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> {
        Optional<Object> cached = hotKey.peek(key);
        assertThat(cached).isPresent();
        assertThat(cached.get()).isEqualTo("real-redis-value");
      });
  }

  @Test
  void get_shouldReturnEmpty_whenKeyNotInRedis() {
    String key = "it:l2read:missing:" + UUID.randomUUID();
    Optional<Object> result = hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 60000, 30000);
    assertThat(result).isEmpty();
  }

  @Test
  void putThrough_withWriter_shouldPersistToRedis() {
    String key = "it:l2read:put:" + UUID.randomUUID();
    hotKey.putThrough(key, "persisted-value", () -> redisTemplate.opsForValue().set(key, "persisted-value"));

    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() ->
        assertThat(redisTemplate.opsForValue().get(key)).isEqualTo("persisted-value")
      );
  }
}
