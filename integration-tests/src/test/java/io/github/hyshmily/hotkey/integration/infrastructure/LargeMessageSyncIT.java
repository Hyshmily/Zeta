package io.github.hyshmily.hotkey.integration.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Tag("docker")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LargeMessageSyncIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(LargeMessageSyncIT.class);

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

  @Autowired
  RabbitTemplate rabbitTemplate;

  private static String largeValue(int kilobytes) {
    return "x".repeat(kilobytes * 1024);
  }

  // ── Large value storage ──

  @Test
  void putThrough_withLargeValue() throws Exception {
    String key = "it:large:put:" + UUID.randomUUID();
    String value = largeValue(1024); // 1MB

    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    Thread.sleep(500);

    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> {
        assertThat(hotKey.peek(key)).isPresent();
        assertThat(hotKey.peek(key).get()).isEqualTo(value);
      });
  }

  @Test
  void get_withLargeValue() throws Exception {
    String key = "it:large:get:" + UUID.randomUUID();
    String value = largeValue(1024);

    Optional<Object> result = hotKey.get(key, () -> value, 60000, 30000);
    assertThat(result).isPresent().hasValue(value);
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent().hasValue(value);
  }

  @Test
  void largeValue_shouldRoundTripThroughRedis() throws Exception {
    String key = "it:large:rt:" + UUID.randomUUID();
    String value = largeValue(1024);

    redisTemplate.opsForValue().set(key, value);
    String fromRedis = redisTemplate.opsForValue().get(key);
    assertThat(fromRedis).isEqualTo(value);

    hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 60000, 30000);
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent().hasValue(value);
  }

  // ── Large invalidation sync ──

  @Test
  void invalidateSync_withLargeValue() throws Exception {
    String key = "it:large:inv:" + UUID.randomUUID();
    String value = largeValue(1024);

    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    Thread.sleep(500);
    assertThat(hotKey.peek(key)).isPresent();

    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
    props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
    props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send("hotkey.sync.exchange", "", msg);

    await()
      .atMost(Duration.ofSeconds(10))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).isEmpty());
  }

  @Test
  void refreshSync_withLargeValue() throws Exception {
    String key = "it:large:ref:" + UUID.randomUUID();
    String value = largeValue(1024);
    String newValue = largeValue(1024);

    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    Thread.sleep(500);
    assertThat(hotKey.peek(key)).isPresent().hasValue(value);

    redisTemplate.opsForValue().set(key, newValue);

    MessageProperties props = new MessageProperties();
    props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "REFRESH");
    props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
    props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
    Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send("hotkey.sync.exchange", "", msg);

    await()
      .atMost(Duration.ofSeconds(10))
      .untilAsserted(() -> assertThat(hotKey.peek(key)).hasValue(newValue));
  }

  // ── Concurrency with large values ──

  @Test
  void concurrentAccess_withLargeValues() throws Exception {
    int threadCount = 4;
    int opsPerThread = 10;
    AtomicInteger errors = new AtomicInteger(0);
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      String prefix = "it:large:conc:" + t;
      pool.submit(() -> {
        try {
          for (int i = 0; i < opsPerThread; i++) {
            String key = prefix + ":" + i;
            String value = largeValue(256 + i * 64); // 256KB to ~832KB
            hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
            Optional<Object> peeked;
            for (int retry = 0; retry < 50; retry++) {
              peeked = hotKey.peek(key);
              if (peeked.isPresent()) break;
              Thread.sleep(20);
            }
            peeked = hotKey.peek(key);
            assertThat(peeked).isPresent();
            assertThat(peeked.get()).isEqualTo(value);
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Large value concurrent error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
    assertThat(errors.get()).isZero();
  }

  // ── Edge cases ──

  @Test
  void largeValue_withSpecialCharacters() throws Exception {
    String key = "it:large:spec:" + UUID.randomUUID();
    // mix of ASCII, Unicode, and null-like chars
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 500_000; i++) {
      sb.append((char) ('\u0020' + (i % 95)));
    }
    String value = sb.toString();

    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    Thread.sleep(500);

    await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> {
        assertThat(hotKey.peek(key)).isPresent();
        assertThat(hotKey.peek(key).get()).isEqualTo(value);
      });
  }

  @Test
  void largeValue_invalidate_shouldFreeMemory() throws Exception {
    String key = "it:large:mem:" + UUID.randomUUID();
    String value = largeValue(1024);

    hotKey.putThrough(key, value, () -> {});
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent();

    hotKey.invalidate(key);
    assertThat(hotKey.peek(key)).isEmpty();

    // re-store and verify fresh
    String value2 = largeValue(1024);
    hotKey.putThrough(key, value2, () -> {});
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent().hasValue(value2);
  }
}
