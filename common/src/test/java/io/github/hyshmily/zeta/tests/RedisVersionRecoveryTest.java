package io.github.hyshmily.zeta.tests;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Real-Redis integration test for the version plane's degraded-mode contract
 * (ADR-0008/0019): normal INCR while Redis is up, Snowflake fallback while it
 * is down, and a return to INCR after recovery.
 *
 * <p>Mock-based tests can only simulate {@code nextVersion} throwing; only a
 * real server exercises the Lua INCR+EXPIRE path, the connection-failure
 * detection, and the pool behavior across a restart. Self-skips when no
 * Docker daemon is available, so plain {@code mvn test} stays green
 * everywhere. Named {@code *Test} (not {@code *IT}) deliberately: the repo
 * has no failsafe setup, and the docker guard makes it safe for surefire.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@Timeout(180)
class RedisVersionRecoveryTest {

  @Container
  private static final GenericContainer<?> REDIS = new GenericContainer<>(
    DockerImageName.parse("redis:7-alpine")
  ).withExposedPorts(6379);

  private static StringRedisTemplate template;
  private static VersionController versions;

  @BeforeAll
  static void setUp() {
    org.junit.jupiter.api.Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker unavailable — skipping container test"
    );
    LettuceConnectionFactory factory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
    factory.afterPropertiesSet();
    template = new StringRedisTemplate(factory);
    template.afterPropertiesSet();
    versions = new VersionControllerImpl(
      Optional.of(template),
      10_080,
      new SnowflakeIdGenerator(0, 1, 5L, false)
    );
  }

  @AfterAll
  static void tearDown() {
    if (template != null && template.getConnectionFactory() instanceof LettuceConnectionFactory factory) {
      factory.destroy();
    }
  }

  @Test
  @DisplayName("healthy Redis allocates monotonic positive versions")
  void healthyRedis_allocatesMonotonicVersions() {
    VersionController.VersionResult v1 = versions.nextVersion("it:key1");
    VersionController.VersionResult v2 = versions.nextVersion("it:key1");

    assertThat(v1.degraded()).isFalse();
    assertThat(v2.degraded()).isFalse();
    assertThat(v2.dataVersion()).isGreaterThan(v1.dataVersion());
    assertThat(v1.dataVersion()).isPositive();
  }

  @Test
  @DisplayName("Redis outage degrades to negative Snowflake versions and counts them")
  void redisDown_degradesToNegativeVersions() {
    long degradedBefore = versions.getDegradedVersionCount();
    REDIS.stop();

    try {
      VersionController.VersionResult v = versions.nextVersion("it:key2");

      assertThat(v.degraded()).isTrue();
      assertThat(v.dataVersion()).isNegative();
      assertThat(versions.getDegradedVersionCount()).isGreaterThan(degradedBefore);
    } finally {
      REDIS.start();
    }
  }

  @Test
  @DisplayName("recovered Redis serves INCR again (no stuck degradation)")
  void redisRecovered_servesIncrAgain() {
    // Force at least one degraded allocation first so the test pins the
    // transition rather than assuming a healthy start.
    REDIS.stop();
    try {
      assertThat(versions.nextVersion("it:key3").degraded()).isTrue();
    } finally {
      REDIS.start();
    }

    // The pool may hold dead connections from the outage window; poll until
    // a call succeeds instead of asserting on the first attempt.
    VersionController.VersionResult recovered = null;
    long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
    while (System.currentTimeMillis() < deadline) {
      VersionController.VersionResult v = versions.nextVersion("it:key3");
      if (!v.degraded()) {
        recovered = v;
        break;
      }
    }

    assertThat(recovered).as("expected a non-degraded version within 30s of recovery").isNotNull();
    assertThat(recovered.dataVersion()).isPositive();
  }
}
