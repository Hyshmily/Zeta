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
package io.github.hyshmily.hotkey.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.sync.VersionController;
import io.github.hyshmily.hotkey.sync.VersionController.VersionResult;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Tests for {@link VersionController}, covering Redis INCR Lua execution,
 * Redis failure fallback (degraded negative version space), and empty
 * Redis template path.
 */
class VersionControllerTest {

  private VersionController controller;
  private StringRedisTemplate redisTemplate;

  @BeforeEach
  void setUp() {
    redisTemplate = mock(StringRedisTemplate.class);
    controller = new VersionController(Optional.of(redisTemplate), 10);
  }

  /**
   * Verifies that nextVersion returns a positive version without the degraded flag when Redis responds normally.
   */
  @Test
  void nextVersion_withRedis_shouldReturnPositiveVersionNotDegraded() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenReturn(42L);
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isEqualTo(42L);
    assertThat(result.degraded()).isFalse();
  }

  /**
   * Verifies that nextVersion correctly handles a zero version from Redis.
   */
  @Test
  void nextVersion_withRedis_shouldHandleZeroVersion() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenReturn(0L);
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isZero();
    assertThat(result.degraded()).isFalse();
  }

  /**
   * Verifies that nextVersion correctly handles Long.MAX_VALUE from Redis.
   */
  @Test
  void nextVersion_withRedis_shouldHandleLargeVersion() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenReturn(Long.MAX_VALUE);
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isEqualTo(Long.MAX_VALUE);
    assertThat(result.degraded()).isFalse();
  }

  /**
   * Verifies that nextVersion falls back to a negative degraded version when Redis throws a RuntimeException.
   */
  @Test
  void nextVersion_whenRedisFails_shouldFallbackToDegraded() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenThrow(new RuntimeException("Redis connection refused"));
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isNegative();
    assertThat(result.degraded()).isTrue();
  }

  /**
   * Verifies that nextVersion falls back to a negative degraded version when Redis throws a non-RuntimeException.
   */
  @Test
  void nextVersion_whenRedisThrows_shouldFallbackToDegraded() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenThrow(new IllegalStateException("Lua script error"));
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isNegative();
    assertThat(result.degraded()).isTrue();
  }

  /**
   * Verifies that nextVersion falls back to a negative degraded version when Redis returns null.
   */
  @Test
  void nextVersion_whenRedisReturnsNull_shouldFallbackToDegraded() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenReturn(null);
    VersionResult result = controller.nextVersion("key1");
    assertThat(result.dataVersion()).isNegative();
    assertThat(result.degraded()).isTrue();
  }

  /**
   * Verifies that nextVersion always returns a degraded version when no Redis template is configured.
   */
  @Test
  void nextVersion_withEmptyRedisTemplate_shouldAlwaysBeDegraded() {
    VersionController noRedis = new VersionController(Optional.empty(), 10);
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
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenReturn(1L, 2L, 3L);
    assertThat(controller.nextVersion("key1").dataVersion()).isEqualTo(1L);
    assertThat(controller.nextVersion("key1").dataVersion()).isEqualTo(2L);
    assertThat(controller.nextVersion("key1").dataVersion()).isEqualTo(3L);
  }

  /**
   * Verifies that nextVersion for different keys produces independent version values.
   */
  @Test
  void nextVersion_differentKeys_shouldBeIndependent() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenReturn(100L, 200L);
    assertThat(controller.nextVersion("key-a").dataVersion()).isEqualTo(100L);
    assertThat(controller.nextVersion("key-b").dataVersion()).isEqualTo(200L);
  }
}
