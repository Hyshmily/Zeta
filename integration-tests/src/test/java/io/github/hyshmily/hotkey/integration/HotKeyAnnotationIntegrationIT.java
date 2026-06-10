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
import io.github.hyshmily.hotkey.annotation.HotKey.OperationType;
import io.github.hyshmily.hotkey.annotation.HotKeyCacheTTL;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for the {@link io.github.hyshmily.hotkey.annotation.HotKey @HotKey} annotation AOP aspect.
 * Verifies that {@link io.github.hyshmily.hotkey.annotation.HotKeyAspect} correctly intercepts
 * READ/WRITE/INVALIDATE operations with SpEL key resolution, soft-expire toggle, and TTL overrides.
 */
@Testcontainers
@Tag("docker")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(HotKeyAnnotationIntegrationIT.AnnotatedService.class)
class HotKeyAnnotationIntegrationIT extends AbstractIntegrationIT {

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
  private HotKey hotKey;

  @Autowired
  private AnnotatedService annotatedService;

  /**
   * Evicts all known annotation cache keys before each test to ensure a clean slate.
   */
  @BeforeEach
  void setUp() {
    hotKey.invalidate("anno:read:static");
    hotKey.invalidate("anno:write:static");
    hotKey.invalidate("anno:invalidate:static");
    hotKey.invalidate("anno:read:nosoft");
    hotKey.invalidate("anno:spel:" + TEST_ARG);
    hotKey.invalidate("anno:write:nosoft");
    hotKey.invalidate("anno:invalidate:nosoft");
    hotKey.invalidate("anno:read:ttl0");
  }

  private static final String TEST_ARG = "spelValue";
  private static final String SUPPLIER_RESULT = "from-supplier";

  /**
   * READ operation caches via {@code getWithSoftExpire}; invalidation forces a fresh supplier call.
   */
  @Test
  void readOperation_shouldCacheViaGetWithSoftExpire() {
    String result = annotatedService.readWithStaticKey(SUPPLIER_RESULT);
    assertThat(result).isEqualTo(SUPPLIER_RESULT);

    String cached = annotatedService.readWithStaticKey("should-not-execute");
    assertThat(cached).isEqualTo(SUPPLIER_RESULT);

    hotKey.invalidate("anno:read:static");
    String reloaded = annotatedService.readWithStaticKey("fresh-supplier");
    assertThat(reloaded).isEqualTo("fresh-supplier");
  }

  /**
   * READ with {@code softExpire=false} uses plain {@code get()} instead of {@code getWithSoftExpire}.
   */
  @Test
  void readOperation_withSoftExpireFalse_shouldUseGet() {
    String result = annotatedService.readWithoutSoftExpire(SUPPLIER_RESULT);
    assertThat(result).isEqualTo(SUPPLIER_RESULT);

    AtomicInteger counter = new AtomicInteger(0);
    String second = annotatedService.readWithoutSoftExpire("call-" + counter.incrementAndGet());
    assertThat(second).isEqualTo(SUPPLIER_RESULT);
  }

  /**
   * WRITE operation mutates the value and invalidates the L1 cache entry.
   */
  @Test
  void writeOperation_shouldMutateAndInvalidate() {
    String result = annotatedService.writeWithStaticKey("write-value");
    assertThat(result).isEqualTo("write-value");

    assertThat(hotKey.peek("anno:write:static")).isEmpty();
  }

  /**
   * INVALIDATE operation clears the L1 entry, then the method body executes normally.
   */
  @Test
  void invalidateOperation_shouldClearCacheAndProceed() throws Exception {
    hotKey.putThrough("anno:invalidate:static", "pre-value", () -> {});
    await().atMost(Duration.ofSeconds(5))
      .untilAsserted(() ->
        assertThat(hotKey.peek("anno:invalidate:static")).isPresent());

    String result = annotatedService.invalidateWithStaticKey("post-value");
    assertThat(result).isEqualTo("post-value");

    assertThat(hotKey.peek("anno:invalidate:static")).isEmpty();
  }

  /**
   * SpEL key expression resolves to the correct cache key for READ operations.
   */
  @Test
  void readOperation_shouldUseSpelKey() {
    String result = annotatedService.readWithSpelKey(TEST_ARG, SUPPLIER_RESULT);
    assertThat(result).isEqualTo(SUPPLIER_RESULT);

    String cached = annotatedService.readWithSpelKey(TEST_ARG, "should-not-execute");
    assertThat(cached).isEqualTo(SUPPLIER_RESULT);
  }

  /**
   * Different SpEL arguments produce distinct cache keys and independent entries.
   */
  @Test
  void readOperation_differentSpelArgs_shouldUseDifferentKeys() {
    String r1 = annotatedService.readWithSpelKey("key-a", "value-a");
    assertThat(r1).isEqualTo("value-a");

    String r2 = annotatedService.readWithSpelKey("key-b", "value-b");
    assertThat(r2).isEqualTo("value-b");

    assertThat(hotKey.peek("anno:spel:key-a")).isPresent();
    assertThat(hotKey.peek("anno:spel:key-b")).isPresent();
  }

  /**
   * WRITE with {@code hardTtlMs=0, softTtlMs=0} silently falls back to default TTL values.
   */
  @Test
  void writeOperation_withZeroTtl_shouldUseDefaults() {
    String result = annotatedService.writeWithStaticKey("ttl0-value");
    assertThat(result).isEqualTo("ttl0-value");

    assertThat(hotKey.peek("anno:write:static")).isEmpty();
  }

  /**
   * WRITE silently ignores {@code softExpire=false} since WRITE always uses {@code putBeforeInvalidate}.
   */
  @Test
  void writeOperation_withSoftExpireFalse_shouldIgnore() {
    String result = annotatedService.writeWithoutSoftExpire("nosoft-write");
    assertThat(result).isEqualTo("nosoft-write");

    assertThat(hotKey.peek("anno:write:nosoft")).isEmpty();
  }

  /**
   * INVALIDATE silently ignores {@code softExpire=false}; invalidation always clears the entry.
   */
  @Test
  void invalidateOperation_withSoftExpireFalse_shouldIgnore() {
    hotKey.putThrough("anno:invalidate:nosoft", "pre-value", () -> {});
    await().atMost(Duration.ofSeconds(5))
      .untilAsserted(() ->
        assertThat(hotKey.peek("anno:invalidate:nosoft")).isPresent());

    String result = annotatedService.invalidateWithoutSoftExpire("post-value");
    assertThat(result).isEqualTo("post-value");

    assertThat(hotKey.peek("anno:invalidate:nosoft")).isEmpty();
  }

  /**
   * READ with {@code hardTtlMs=0, softTtlMs=0} falls back to {@link HotKeyProperties} default TTLs.
   */
  @Test
  void readOperation_withZeroTtl_shouldFallbackToDefaults() {
    String result = annotatedService.readWithZeroTtl(SUPPLIER_RESULT);
    assertThat(result).isEqualTo(SUPPLIER_RESULT);

    String cached = annotatedService.readWithZeroTtl("should-not-execute");
    assertThat(cached).isEqualTo(SUPPLIER_RESULT);

    hotKey.invalidate("anno:read:ttl0");
    String reloaded = annotatedService.readWithZeroTtl("fresh-value");
    assertThat(reloaded).isEqualTo("fresh-value");
  }

  /**
   * Spring {@link Service} bean providing annotated methods for each
   * {@link io.github.hyshmily.hotkey.annotation.HotKey @HotKey} operation type and configuration variant
   * (READ, WRITE, INVALIDATE, SpEL keys, soft-expire toggle, zero-TTL fallback).
   */
  @Service
  public static class AnnotatedService {

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:read:static'",
      operation = OperationType.READ
    )
    @HotKeyCacheTTL(hardTtlMs = 60000, softTtlMs = 30000)
    public String readWithStaticKey(String supplierResult) {
      return supplierResult;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:read:nosoft'",
      operation = OperationType.READ,
      softExpire = false
    )
    @HotKeyCacheTTL(hardTtlMs = 60000)
    public String readWithoutSoftExpire(String supplierResult) {
      return supplierResult;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:write:static'",
      operation = OperationType.WRITE
    )
    public String writeWithStaticKey(String value) {
      return value;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:invalidate:static'",
      operation = OperationType.INVALIDATE
    )
    public String invalidateWithStaticKey(String value) {
      return value;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:spel:' + #arg",
      operation = OperationType.READ
    )
    @HotKeyCacheTTL(hardTtlMs = 60000, softTtlMs = 30000)
    public String readWithSpelKey(String arg, String supplierResult) {
      return supplierResult;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:write:nosoft'",
      operation = OperationType.WRITE,
      softExpire = false
    )
    public String writeWithoutSoftExpire(String value) {
      return value;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:invalidate:nosoft'",
      operation = OperationType.INVALIDATE,
      softExpire = false
    )
    public String invalidateWithoutSoftExpire(String value) {
      return value;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:read:ttl0'",
      operation = OperationType.READ
    )
    public String readWithZeroTtl(String supplierResult) {
      return supplierResult;
    }
  }
}
