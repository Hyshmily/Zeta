package io.github.hyshmily.hotkey.integration.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
class RabbitMQRecoveryIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(RabbitMQRecoveryIT.class);

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

  @Autowired(required = false)
  StringRedisTemplate redisTemplate;

  @Autowired
  RabbitTemplate rabbitTemplate;

  @Test
  void invalidateSync_shouldClearL1() throws Exception {
    String key = "it:rr:inv:" + UUID.randomUUID();
    String value = "sync-inv-value";
    hotKey.putThrough(key, value, () -> {});
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent().hasValue(value);

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
  void refreshSync_shouldUpdateL1() throws Exception {
    String key = "it:rr:ref:" + UUID.randomUUID();
    String value = "sync-ref-old";
    String newValue = "sync-ref-new";
    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    Thread.sleep(300);
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

  @Test
  void concurrentAccess_withBroadcastSync() throws Exception {
    int threadCount = 5;
    int opsPerThread = 20;
    AtomicInteger errors = new AtomicInteger(0);
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      int tid = t;
      String prefix = "it:rr:conc:" + tid;
      pool.submit(() -> {
        try {
          for (int i = 0; i < opsPerThread; i++) {
            String key = prefix + ":" + i;
            hotKey.putThrough(key, "v-" + tid + "-" + i, () -> {});

            if (i % 5 == 0) {
              MessageProperties props = new MessageProperties();
              props.setHeader(HotKeyConstants.AMQP_HEADER_TYPE, "INVALIDATE");
              props.setHeader(HotKeyConstants.AMQP_HEADER_VERSION, Long.MAX_VALUE);
              props.setHeader(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED, false);
              Message msg = new Message(key.getBytes(StandardCharsets.UTF_8), props);
              rabbitTemplate.send("hotkey.sync.exchange", "", msg);
            }
            hotKey.peek(key);
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("RabbitMQ concurrent error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
    assertThat(errors.get()).isLessThan(threadCount * opsPerThread);
  }
}
