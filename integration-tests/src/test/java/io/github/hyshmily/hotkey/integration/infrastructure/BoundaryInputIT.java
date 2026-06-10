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

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for boundary and edge case inputs (empty keys, null values, extreme TTLs).
 *
 * <p>Covers null key handling, negative/zero/max TTLs, concurrent operations,
 * hard TTL expiry, and supplier reload semantics.
 */
@Testcontainers
@Tag("docker")
class BoundaryInputIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(BoundaryInputIT.class);

  /** Redis container for L2 cache and version tracking. */
  @Container
  static GenericContainer<?> redis = new GenericContainer<>(
      DockerImageName.parse("redis:7-alpine"))
    .withExposedPorts(6379);

  /** RabbitMQ container for broadcast sync and cross-instance messaging. */
  @Container
  static GenericContainer<?> rabbitmq = new GenericContainer<>(
      DockerImageName.parse("rabbitmq:4.1-management"))
    .withExposedPorts(5672, 15672)
    .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1))
    .withStartupTimeout(Duration.ofMinutes(2));

  /**
   * Overrides Spring properties to point Redis and RabbitMQ to Testcontainers endpoints.
   *
   * @param r the dynamic property registry
   */
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

  @Autowired(required = false)
  StringRedisTemplate redisTemplate;

  /** Calls get() with a null key and verifies no exception is thrown. */
  @Test
  void get_withNullKey_shouldNotThrow() {
    Optional<Object> result = hotKey.get(null, () -> "value", 60000, 30000);
    assertThat(result).isNotNull();
  }

  /** Calls peek() with a null key and verifies no exception is thrown. */
  @Test
  void peek_withNullKey_shouldNotThrow() {
    Optional<Object> result = hotKey.peek(null);
    assertThat(result).isNotNull();
  }

  /** Calls invalidate() with a null key and verifies no exception is thrown. */
  @Test
  void invalidate_withNullKey_shouldNotThrow() {
    hotKey.invalidate(null);
  }

  /** Calls invalidateAll() and verifies no exception is thrown. */
  @Test
  void invalidateAll_shouldNotThrow() {
    hotKey.invalidateAll();
  }

  /** Calls isLocalHotKey() with a null key and verifies it returns false. */
  @Test
  void isLocalHotKey_withNullKey_shouldReturnFalse() {
    assertThat(hotKey.isLocalHotKey(null)).isFalse();
  }

  /** Calls isWorkerHotKey() with a null key and verifies it returns false. */
  @Test
  void isWorkerHotKey_withNullKey_shouldReturnFalse() {
    assertThat(hotKey.isWorkerHotKey(null)).isFalse();
  }

  /** Calls get() with negative TTL values and verifies no exception is thrown. */
  @Test
  void get_withNegativeTtl_shouldNotThrow() {
    String key = "it:bound:negttl:" + UUID.randomUUID();
    Optional<Object> result = hotKey.get(key, () -> "neg-ttl-value", -1, -1);
    assertThat(result).isPresent().hasValue("neg-ttl-value");
  }

  /** Calls get() with zero TTL values and verifies no exception is thrown. */
  @Test
  void get_withZeroTtl_shouldNotThrow() {
    String key = "it:bound:zerottl:" + UUID.randomUUID();
    Optional<Object> result = hotKey.get(key, () -> "zero-ttl-value", 0, 0);
    assertThat(result).isPresent().hasValue("zero-ttl-value");
  }

  /** Calls get() with Long.MAX_VALUE TTL and verifies no exception is thrown. */
  @Test
  void get_withMaxTtl_shouldNotThrow() {
    String key = "it:bound:maxttl:" + UUID.randomUUID();
    Optional<Object> result = hotKey.get(key, () -> "max-ttl-value",
        Long.MAX_VALUE, Long.MAX_VALUE);
    assertThat(result).isPresent().hasValue("max-ttl-value");
  }

  /** Calls putThrough() with a null value and verifies no exception is thrown. */
  @Test
  void putThrough_withNullValue_shouldNotThrow() {
    String key = "it:bound:nullval:" + UUID.randomUUID();
    hotKey.putThrough(key, null, () -> {});
  }

  /** Calls putBeforeInvalidate() with a null value and verifies no exception is thrown. */
  @Test
  void putBeforeInvalidate_withNullValue_shouldNotThrow() {
    String key = "it:bound:nullb4:" + UUID.randomUUID();
    hotKey.putBeforeInvalidate(key, () -> {});
  }

  /** Runs concurrent get() and invalidate() calls and verifies no deadlock or exceptions occur. */
  @Test
  void concurrentGetAndInvalidate_shouldNotDeadlock() throws Exception {
    String key = "it:bound:deadlock:" + UUID.randomUUID();
    int threadCount = 10;
    int opsPerThread = 500;
    AtomicInteger errors = new AtomicInteger(0);
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      pool.submit(() -> {
        try {
          for (int i = 0; i < opsPerThread; i++) {
            if (i % 3 == 0) {
              hotKey.invalidate(key);
            } else {
              hotKey.get(key, () -> "v", 60000, 30000);
            }
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Concurrent get/invalidate error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
    assertThat(errors.get()).isZero();
  }

  /** Runs concurrent get, putThrough, invalidate, and peek operations and verifies no exceptions occur. */
  @Test
  void concurrentMixedOperations_shouldNotThrow() throws Exception {
    int threadCount = 6;
    int opsPerThread = 200;
    AtomicInteger errors = new AtomicInteger(0);
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      int tid = t;
      pool.submit(() -> {
        try {
          for (int i = 0; i < opsPerThread; i++) {
            int idx = i;
            String key = "it:bound:concshut:" + tid + ":" + idx;
            switch (tid % 4) {
              case 0 -> hotKey.get(key, () -> "v-" + tid + "-" + idx, 60000, 30000);
              case 1 -> hotKey.putThrough(key, "v-" + tid + "-" + idx, () -> {});
              case 2 -> hotKey.invalidate(key);
              case 3 -> hotKey.peek(key);
            }
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Shutdown stress error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
    assertThat(errors.get()).isZero();
  }

  /** Stores a value with a short hard TTL and verifies it expires from L1 cache. */
  @Test
  void cacheEntry_expiresAfterHardTtl() throws Exception {
    String key = "it:bound:hardttl:" + UUID.randomUUID();
    hotKey.putThrough(key, "exp-value", () -> {}, 200, 100);
    Thread.sleep(50);
    assertThat(hotKey.peek(key)).isPresent().hasValue("exp-value");
    Thread.sleep(500);
    assertThat(hotKey.peek(key)).isEmpty();
  }

  /** Loads a value via get(), waits for hard TTL expiry, and verifies the supplier is re-invoked. */
  @Test
  void get_reloadsAfterHardTtl() throws Exception {
    String key = "it:bound:reload:" + UUID.randomUUID();
    AtomicLong supplierCount = new AtomicLong(0);

    Optional<Object> r1 = hotKey.get(key, () -> "v-" + supplierCount.incrementAndGet(), 200, 100);
    assertThat(r1).isPresent();
    assertThat(supplierCount.get()).isEqualTo(1);

    Thread.sleep(500);

    Optional<Object> r2 = hotKey.get(key, () -> "v-" + supplierCount.incrementAndGet(), 200, 100);
    assertThat(r2).isPresent();
    assertThat(supplierCount.get()).isGreaterThanOrEqualTo(2);
  }
}
