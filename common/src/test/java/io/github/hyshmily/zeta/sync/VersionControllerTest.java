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
package io.github.hyshmily.zeta.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.VersionController.VersionResult;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * Tests for {@link VersionController}, covering Redis INCR Lua execution,
 * Redis failure fallback (degraded negative version space), and empty
 * Redis template path.
 */
class VersionControllerTest {

  private VersionController controller;
  private StringRedisTemplate redisTemplate;
  private SnowflakeIdGenerator snowflake;

  @BeforeEach
  void setUp() {
    snowflake = new SnowflakeIdGenerator(0, 1);
    redisTemplate = mock(StringRedisTemplate.class);
    controller = new VersionControllerImpl(Optional.of(redisTemplate), 10, snowflake);
  }

  /**
   * Verifies that nextVersion returns a positive version without the degraded flag when Redis responds normally.
   */
  @Test
  void nextVersion_withRedis_shouldReturnPositiveVersionNotDegraded() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(42L);
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isEqualTo(42L);
    assertThat(result.degraded()).isFalse();
  }

  /**
   * Verifies that nextVersion correctly handles a zero version from Redis.
   */
  @Test
  void nextVersion_withRedis_shouldHandleZeroVersion() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(0L);
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isZero();
    assertThat(result.degraded()).isFalse();
  }

  /**
   * Verifies that nextVersion correctly handles Long.MAX_VALUE from Redis.
   */
  @Test
  void nextVersion_withRedis_shouldHandleLargeVersion() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(Long.MAX_VALUE);
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isEqualTo(Long.MAX_VALUE);
    assertThat(result.degraded()).isFalse();
  }

  /**
   * Verifies that nextVersion falls back to a negative degraded version when Redis throws a RuntimeException.
   */
  @Test
  void nextVersion_whenRedisFails_shouldFallbackToDegraded() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenThrow(
      new RuntimeException("Redis connection refused")
    );
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isNegative();
    assertThat(result.degraded()).isTrue();
  }

  /**
   * Verifies that nextVersion falls back to a negative degraded version when Redis throws a non-RuntimeException.
   */
  @Test
  void nextVersion_whenRedisThrows_shouldFallbackToDegraded() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenThrow(
      new IllegalStateException("Lua script error")
    );
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isNegative();
    assertThat(result.degraded()).isTrue();
  }

  /**
   * Verifies that nextVersion falls back to a negative degraded version when Redis returns null.
   */
  @Test
  void nextVersion_whenRedisReturnsNull_shouldFallbackToDegraded() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(null);
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isNegative();
    assertThat(result.degraded()).isTrue();
  }

  /**
   * Verifies that nextVersion always returns a degraded version when no Redis template is configured.
   */
  @Test
  void nextVersion_withEmptyRedisTemplate_shouldAlwaysBeDegraded() {
    VersionController noRedis = new VersionControllerImpl(Optional.empty(), 10, snowflake);
    VersionResult r1 = noRedis.nextVersion("key1");
    assertThat(r1.dataVersion()).isNegative();
    assertThat(r1.degraded()).isTrue();
  }

  /**
   * Verifies that fallbackVersion returns a negative version with the degraded flag set.
   */
  @Test
  void fallbackVersion_shouldReturnNegativeVersionDegraded() {
    VersionResult result = controller.fallbackVersion();
    assertThat(result.dataVersion()).isNegative();
    assertThat(result.degraded()).isTrue();
  }

  /**
   * Verifies that consecutive fallbackVersion calls return monotonically increasing negative values.
   */
  @Test
  void fallbackVersion_shouldBeMonotonicAscending() {
    VersionResult r1 = controller.fallbackVersion();
    VersionResult r2 = controller.fallbackVersion();
    assertThat(r1.dataVersion()).isLessThan(0);
    assertThat(r2.dataVersion()).isGreaterThan(r1.dataVersion());
  }

  /**
   * Verifies that all fallback version values stay within the negative long space across many calls.
   */
  @Test
  void fallbackVersion_shouldStayInNegativeLongSpace() {
    for (int i = 0; i < 1000; i++) {
      VersionResult r = controller.fallbackVersion();
      assertThat(r.dataVersion()).isNegative();
    }
  }

  /**
   * Verifies that consecutive nextVersion calls for the same key return increasing version numbers.
   */
  @Test
  void nextVersion_consecutiveCalls_shouldReturnIncreasingVersions() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(1L, 2L, 3L);
    assertThat(controller.nextVersion("key1").dataVersion()).isEqualTo(1L);
    assertThat(controller.nextVersion("key1").dataVersion()).isEqualTo(2L);
    assertThat(controller.nextVersion("key1").dataVersion()).isEqualTo(3L);
  }

  /**
   * Verifies that nextVersion for different keys produces independent version values.
   */
  @Test
  void nextVersion_differentKeys_shouldBeIndependent() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(100L, 200L);
    assertThat(controller.nextVersion("key-a").dataVersion()).isEqualTo(100L);
    assertThat(controller.nextVersion("key-b").dataVersion()).isEqualTo(200L);
  }

  /**
   * Verifies that fallbackVersion does not wrap to positive long even after many calls.
   */
  @Test
  void fallbackVersion_shouldNotOverflowToPositive() {
    VersionController vc = new VersionControllerImpl(Optional.empty(), 10, snowflake);
    for (int i = 0; i < 100_000; i++) {
      VersionResult r = vc.fallbackVersion();
      assertThat(r.dataVersion()).isNegative();
      assertThat(r.degraded()).isTrue();
    }
  }

  /**
   * Verifies that getDegradedVersionCount increments on each fallback call.
   */
  @Test
  void getDegradedVersionCount_shouldIncrementOnEachFallback() {
    assertThat(controller.getDegradedVersionCount()).isZero();
    controller.fallbackVersion();
    assertThat(controller.getDegradedVersionCount()).isEqualTo(1);
    controller.fallbackVersion();
    assertThat(controller.getDegradedVersionCount()).isEqualTo(2);
  }

  /**
   * Verifies isRedisConfigured returns true when Redis template is present.
   */
  @Test
  void isRedisConfigured_withRedis_shouldReturnTrue() {
    assertThat(controller.isRedisConfigured()).isTrue();
  }

  /**
   * Verifies isRedisConfigured returns false when Redis template is absent.
   */
  @Test
  void isRedisConfigured_withoutRedis_shouldReturnFalse() {
    VersionController noRedis = new VersionControllerImpl(Optional.empty(), 10, snowflake);
    assertThat(noRedis.isRedisConfigured()).isFalse();
  }

  /**
   * Verifies that wraparound detection still returns the new (lower) version
   * when a Redis version key expires (simulated by returning a lower version
   * than previous calls). The floor is never lowered by the regression — it
   * keeps the pre-wraparound maximum, so subsequent increments below it take
   * the (rate-limited) alarm path until the counter climbs back.
   */
  @Test
  void nextVersion_whenWraparound_shouldDetectAndRecover() throws Exception {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenReturn(100L, 1L, 2L);
    assertThat(controller.nextVersion("wrap-key").dataVersion()).isEqualTo(100L);
    // Simulate wraparound: version key expired, INCR restarts from 1.
    // Alarm fires (rate-limited); the floor stays at 100 (max-merge).
    assertThat(controller.nextVersion("wrap-key").dataVersion()).isEqualTo(1L);
    // Subsequent increments are still returned normally.
    assertThat(controller.nextVersion("wrap-key").dataVersion()).isEqualTo(2L);
    assertThat(floorCache(controller).getIfPresent("wrap-key")).isEqualTo(100L);
  }

  /**
   * Verifies the floor race fix: a version below an already-observed higher
   * floor must never lower the floor (atomic max-merge in compute). The old
   * read-then-put implementation raised a false wraparound alarm AND regressed
   * the floor to the stale value when a concurrent higher version committed
   * its floor update in between.
   */
  @Test
  void nextVersion_whenVersionBelowFloor_shouldNotRegressFloor() throws Exception {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(5L, 6L, 5L);
    assertThat(controller.nextVersion("race-key").dataVersion()).isEqualTo(5L);
    assertThat(controller.nextVersion("race-key").dataVersion()).isEqualTo(6L);
    // Simulates the interleaving: another thread's 6 is already in the floor
    // cache when this thread's 5 is observed — the alarm path may fire, but
    // the floor must stay at 6.
    assertThat(controller.nextVersion("race-key").dataVersion()).isEqualTo(5L);
    assertThat(floorCache(controller).getIfPresent("race-key")).isEqualTo(6L);
  }

  /**
   * Verifies {@link VersionControllerImpl#nextVersion} under concurrency: N
   * threads allocating versions for the same key must (a) receive distinct
   * positive INCR values, (b) never regress the per-key floor (it ends at the
   * maximum allocated version), and (c) rate-limit the wraparound ERROR — a
   * later INCR committing its floor update before an earlier INCR observes it
   * legitimately produces v &lt; observed-floor, so the alarm must fire at
   * most once per 10s window, never once per allocation.
   */
  @Test
  void nextVersion_concurrentSameKey_floorNeverRegressesAndAlarmRateLimited() throws Exception {
    StubRedisTemplate stub = new StubRedisTemplate();
    VersionControllerImpl impl = new VersionControllerImpl(Optional.of(stub), 10, snowflake);

    CollectingAppender appender = new CollectingAppender();
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    ch.qos.logback.classic.Logger logbackLogger = context.getLogger(VersionControllerImpl.class);
    appender.start();
    logbackLogger.addAppender(appender);
    try {
      int threads = 8;
      int callsPerThread = 100;
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      ConcurrentLinkedQueue<Long> results = new ConcurrentLinkedQueue<>();
      for (int t = 0; t < threads; t++) {
        pool.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < callsPerThread; i++) {
              results.add(impl.nextVersion("hot-key").dataVersion());
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      pool.shutdownNow();

      // Every allocation got a distinct positive value from the atomic stub INCR.
      assertThat(results).hasSize(threads * callsPerThread);
      assertThat(results).allMatch(v -> v > 0);
      assertThat(results.stream().distinct().count()).isEqualTo(results.size());

      // The floor must equal the highest observed version — never regressed.
      long max = results.stream().mapToLong(Long::longValue).max().orElse(0L);
      assertThat(floorCache(impl).getIfPresent("hot-key")).isEqualTo(max);

      // The wraparound ERROR is rate-limited: at most one per 10s window.
      long errors = appender.events.stream().filter(e -> e.getLevel() == Level.ERROR).count();
      assertThat(errors).isLessThanOrEqualTo(1);
    } finally {
      logbackLogger.detachAppender(appender);
    }
  }

  /**
   * Thread-safe Redis stub: {@code execute} simulates an atomic Redis INCR
   * (Mockito's consecutive-return stubbing is not safe for concurrent calls).
   */
  static class StubRedisTemplate extends StringRedisTemplate {

    final AtomicLong counter = new AtomicLong();

    @SuppressWarnings("unchecked")
    @Override
    public <T> T execute(RedisScript<T> script, java.util.List<String> keys, Object... args) {
      return (T) Long.valueOf(counter.incrementAndGet());
    }
  }

  /** Thread-safe log collector for asserting on ERROR emission counts. */
  static class CollectingAppender extends AppenderBase<ILoggingEvent> {

    final ConcurrentLinkedQueue<ILoggingEvent> events = new ConcurrentLinkedQueue<>();

    @Override
    protected void append(ILoggingEvent eventObject) {
      events.add(eventObject);
    }
  }

  /** Reflection accessor for the private per-key floor cache. */
  @SuppressWarnings("unchecked")
  private static Cache<String, Long> floorCache(Object impl) throws Exception {
    Field f = VersionControllerImpl.class.getDeclaredField("versionFloorCache");
    f.setAccessible(true);
    return (Cache<String, Long>) f.get(impl);
  }

  /**
   * Verifies that nextVersion with blank key still produces a result via Redis.
   */
  @Test
  void nextVersion_withBlankKey_shouldStillWork() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(99L);
    VersionResult result = controller.nextVersion("  ");
    assertThat(result.dataVersion()).isEqualTo(99L);
    assertThat(result.degraded()).isFalse();
  }

  /**
   * Verifies that version key TTL of zero skips the EXPIRE call and still increments.
   */
  @Test
  void nextVersion_withZeroTtl_shouldStillIncrement() {
    VersionController zeroTtl = new VersionControllerImpl(Optional.of(redisTemplate), 0, snowflake);
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(7L);
    VersionResult result = zeroTtl.nextVersion("key1");
    assertThat(result.dataVersion()).isEqualTo(7L);
    assertThat(result.degraded()).isFalse();
  }
}
