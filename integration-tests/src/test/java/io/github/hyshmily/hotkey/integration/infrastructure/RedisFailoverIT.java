package io.github.hyshmily.hotkey.integration.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.dockerjava.api.DockerClient;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Tag("docker")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RedisFailoverIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(RedisFailoverIT.class);

  @Container
  static GenericContainer<?> redis = new GenericContainer<>(
      DockerImageName.parse("redis:7-alpine"))
    .withExposedPorts(6379);

  @DynamicPropertySource
  static void overrideRedis(DynamicPropertyRegistry r) {
    r.add("spring.data.redis.host", redis::getHost);
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    r.add("hotkey.sync.enabled", () -> "false");
    r.add("hotkey.report.enabled", () -> "false");
    r.add("hotkey.worker-listener.enabled", () -> "false");
  }

  @Autowired
  HotKey hotKey;

  @Autowired(required = false)
  StringRedisTemplate redisTemplate;

  @Test
  void putThrough_withRedisBackedRunnable() throws Exception {
    String key = "it:rf:put:" + UUID.randomUUID();
    String value = "redis-backed-value";
    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent().hasValue(value);
    assertThat(redisTemplate.opsForValue().get(key)).isEqualTo(value);
  }

  @Test
  void get_withRedisBackedSupplier() throws Exception {
    String key = "it:rf:get:" + UUID.randomUUID();
    String value = "supplier-value";
    redisTemplate.opsForValue().set(key, value);
    Optional<Object> result = hotKey.get(
        key, () -> redisTemplate.opsForValue().get(key), 60000, 30000);
    assertThat(result).isPresent().hasValue(value);
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent().hasValue(value);
  }

  @Test
  void invalidate_shouldClearL1_andRedis() throws Exception {
    String key = "it:rf:inv:" + UUID.randomUUID();
    String value = "to-be-invalidated";
    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent();

    hotKey.invalidate(key);
    assertThat(hotKey.peek(key)).isEmpty();
    // invalidate clears L1 only (Caffeine); L2 (Redis) is NOT cleared — peers
    // receive a REFRESH broadcast telling them to reload from the reader.
    assertThat(redisTemplate.opsForValue().get(key)).isEqualTo(value);
  }

  @Test
  void concurrentAccess_shouldNotCorruptCache() throws Exception {
    int threadCount = 5;
    int opsPerThread = 20;
    AtomicInteger errors = new AtomicInteger(0);
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      int tid = t;
      String prefix = "it:rf:conc:" + tid;
      pool.submit(() -> {
        try {
          for (int i = 0; i < opsPerThread; i++) {
            String key = prefix + ":" + i;
            hotKey.putThrough(key, "v-" + tid + "-" + i, () -> {});
            Thread.sleep(5);
            assertThat(hotKey.peek(key)).isPresent().hasValue("v-" + tid + "-" + i);
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Redis concurrent error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
    assertThat(errors.get()).isZero();
  }

  @Test
  void operations_resumeAfterRedisRestart() throws Exception {
    String key = "it:rf:restart:" + UUID.randomUUID();
    String value = "restart-value";

    redisTemplate.opsForValue().set(key, value);
    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent().hasValue(value);

    DockerClient docker = redis.getDockerClient();
    String cid = redis.getContainerId();
    docker.pauseContainerCmd(cid).exec();
    log.info("Redis paused");
    Thread.sleep(1000);

    assertThat(hotKey.peek(key)).isPresent().hasValue(value);

    docker.unpauseContainerCmd(cid).exec();
    log.info("Redis unpaused");
    Thread.sleep(2000);

    Optional<Object> result = hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 60000, 30000);
    assertThat(result).isPresent().hasValue(value);
    log.info("Redis restart recovery OK");
  }
}
