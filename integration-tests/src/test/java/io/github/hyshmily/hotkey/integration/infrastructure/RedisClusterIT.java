package io.github.hyshmily.hotkey.integration.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.integration.AbstractIntegrationIT;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Disabled;
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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Tag("docker")
@Tag("redis-cluster")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Disabled("bitnami/redis-cluster:7.4 image unavailable on this VM")
class RedisClusterIT extends AbstractIntegrationIT {

  private static final Logger log = LoggerFactory.getLogger(RedisClusterIT.class);

  @Container
  static GenericContainer<?> redisCluster = new GenericContainer<>(
      DockerImageName.parse("bitnami/redis-cluster:7.4"))
    .withExposedPorts(6379)
    .withEnv("REDIS_CLUSTER_REPLICAS", "0")
    .withEnv("REDIS_NODES", "3")
    .withEnv("REDIS_CLUSTER_CREATOR", "yes")
    .withEnv("REDIS_CLUSTER_DYN_NODES", "yes")
    .waitingFor(Wait.forLogMessage(".*Cluster created successfully.*", 1))
    .withStartupTimeout(Duration.ofMinutes(3));

  @DynamicPropertySource
  static void overrideCluster(DynamicPropertyRegistry r) {
    r.add("spring.data.redis.host", () -> "");
    r.add("spring.data.redis.port", () -> "");
    r.add("spring.data.redis.cluster.nodes",
        () -> redisCluster.getHost() + ":" + redisCluster.getMappedPort(6379));
    // shorter timeouts for cluster discovery
    r.add("spring.data.redis.timeout", () -> "5s");
    r.add("spring.data.redis.lettuce.cluster.refresh.period", () -> "5s");
  }

  @Autowired
  HotKey hotKey;

  @Autowired(required = false)
  StringRedisTemplate redisTemplate;

  @Test
  void clusterBasicPutAndPeek() throws Exception {
    String key = "it:cluster:put:" + UUID.randomUUID();
    hotKey.putThrough(key, "cluster-value", () -> {});
    Thread.sleep(500);
    assertThat(hotKey.peek(key)).isPresent().hasValue("cluster-value");
  }

  @Test
  void clusterGet_withSupplier() throws Exception {
    String key = "it:cluster:get:" + UUID.randomUUID();
    Optional<Object> result = hotKey.get(key, () -> "supplied", 60000, 30000);
    assertThat(result).isPresent().hasValue("supplied");
  }

  @Test
  void clusterRedisTemplate_shouldHandleMovedRedirect() {
    assert redisTemplate != null;
    // keys with hash slots on different cluster nodes
    String key1 = "it:cluster:redirect:a:" + UUID.randomUUID();
    String key2 = "it:cluster:redirect:b:" + UUID.randomUUID();

    redisTemplate.opsForValue().set(key1, "node-a");
    redisTemplate.opsForValue().set(key2, "node-b");

    assertThat(redisTemplate.opsForValue().get(key1)).isEqualTo("node-a");
    assertThat(redisTemplate.opsForValue().get(key2)).isEqualTo("node-b");
  }

  @Test
  void hotKeyFlow_withRedisCluster() throws Exception {
    String key = "it:cluster:flow:" + UUID.randomUUID();
    assert redisTemplate != null;
    redisTemplate.opsForValue().set(key, "cluster-db");

    Optional<Object> result = hotKey.get(key, () -> redisTemplate.opsForValue().get(key), 60000, 30000);
    assertThat(result).isPresent().hasValue("cluster-db");
    Thread.sleep(300);
    assertThat(hotKey.peek(key)).isPresent().hasValue("cluster-db");

    hotKey.invalidate(key);
    assertThat(hotKey.peek(key)).isEmpty();
  }

  @Test
  void concurrentAccess_acrossClusterNodes() throws Exception {
    int threadCount = 5;
    int opsPerThread = 20;
    java.util.concurrent.atomic.AtomicInteger errors = new java.util.concurrent.atomic.AtomicInteger(0);
    var pool = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
    var latch = new java.util.concurrent.CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      int tid = t;
      pool.submit(() -> {
        try {
          for (int i = 0; i < opsPerThread; i++) {
            String key = "it:cluster:conc:" + tid + ":" + i;
            hotKey.putThrough(key, "v-" + tid + "-" + i, () -> {});
            Thread.sleep(5);
            assertThat(hotKey.peek(key)).isPresent();
          }
        } catch (Throwable ex) {
          errors.incrementAndGet();
          log.debug("Cluster concurrent error: {}", ex.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(60, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();
    assertThat(errors.get()).isZero();
  }
}
