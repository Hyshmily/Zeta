package io.github.hyshmily.hotkey.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.annotation.HotKey.OperationType;
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

  @BeforeEach
  void setUp() {
    hotKey.invalidate("anno:read:static");
    hotKey.invalidate("anno:write:static");
    hotKey.invalidate("anno:invalidate:static");
    hotKey.invalidate("anno:read:nosoft");
    hotKey.invalidate("anno:spel:" + TEST_ARG);
  }

  private static final String TEST_ARG = "spelValue";
  private static final String SUPPLIER_RESULT = "from-supplier";

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

  @Test
  void readOperation_withSoftExpireFalse_shouldUseGet() {
    String result = annotatedService.readWithoutSoftExpire(SUPPLIER_RESULT);
    assertThat(result).isEqualTo(SUPPLIER_RESULT);

    AtomicInteger counter = new AtomicInteger(0);
    String second = annotatedService.readWithoutSoftExpire("call-" + counter.incrementAndGet());
    assertThat(second).isEqualTo(SUPPLIER_RESULT);
  }

  @Test
  void writeOperation_shouldMutateAndInvalidate() {
    String result = annotatedService.writeWithStaticKey("write-value");
    assertThat(result).isEqualTo("write-value");

    assertThat(hotKey.peek("anno:write:static")).isEmpty();
  }

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

  @Test
  void readOperation_shouldUseSpelKey() {
    String result = annotatedService.readWithSpelKey(TEST_ARG, SUPPLIER_RESULT);
    assertThat(result).isEqualTo(SUPPLIER_RESULT);

    String cached = annotatedService.readWithSpelKey(TEST_ARG, "should-not-execute");
    assertThat(cached).isEqualTo(SUPPLIER_RESULT);
  }

  @Test
  void readOperation_differentSpelArgs_shouldUseDifferentKeys() {
    String r1 = annotatedService.readWithSpelKey("key-a", "value-a");
    assertThat(r1).isEqualTo("value-a");

    String r2 = annotatedService.readWithSpelKey("key-b", "value-b");
    assertThat(r2).isEqualTo("value-b");

    assertThat(hotKey.peek("anno:spel:key-a")).isPresent();
    assertThat(hotKey.peek("anno:spel:key-b")).isPresent();
  }

  @Service
  public static class AnnotatedService {

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:read:static'",
      operation = OperationType.READ,
      hardTtlMs = 60000,
      softTtlMs = 30000
    )
    public String readWithStaticKey(String supplierResult) {
      return supplierResult;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:read:nosoft'",
      operation = OperationType.READ,
      softExpire = false,
      hardTtlMs = 60000,
      softTtlMs = 0
    )
    public String readWithoutSoftExpire(String supplierResult) {
      return supplierResult;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:write:static'",
      operation = OperationType.WRITE,
      hardTtlMs = 60000,
      softTtlMs = 30000
    )
    public String writeWithStaticKey(String value) {
      return value;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:invalidate:static'",
      operation = OperationType.INVALIDATE,
      hardTtlMs = 60000,
      softTtlMs = 30000
    )
    public String invalidateWithStaticKey(String value) {
      return value;
    }

    @io.github.hyshmily.hotkey.annotation.HotKey(
      key = "'anno:spel:' + #arg",
      operation = OperationType.READ,
      hardTtlMs = 60000,
      softTtlMs = 30000
    )
    public String readWithSpelKey(String arg, String supplierResult) {
      return supplierResult;
    }
  }
}
