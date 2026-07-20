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
package io.github.hyshmily.zeta.cache.annotationsupporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.annotation.annotationsupporter.ZetaCacheContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ZetaCacheContext tests")
class ZetaCacheContextTest {

  @AfterEach
  void tearDown() {
    ZetaCacheContext.get().restore(null);
  }

  @Test
  @DisplayName("get() returns the same singleton instance")
  void get_returnsSingleton() {
    assertThat(ZetaCacheContext.get()).isSameAs(ZetaCacheContext.get());
  }

  @Test
  @DisplayName("apply with non-default hardTtlMs sets context")
  void apply_withHardTtlMs_setsContext() {
    ZetaCacheContext.get().apply(1000L, 0L, false, false, false);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isEqualTo(1000L);
    assertThat(ZetaCacheContext.get().getSoftTtlMs()).isZero();
    assertThat(ZetaCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("apply with non-default softTtlMs sets context")
  void apply_withSoftTtlMs_setsContext() {
    ZetaCacheContext.get().apply(0L, 500L, false, false, false);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isZero();
    assertThat(ZetaCacheContext.get().getSoftTtlMs()).isEqualTo(500L);
    assertThat(ZetaCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("apply with allowNull true sets context")
  void apply_withAllowNull_setsContext() {
    ZetaCacheContext.get().apply(0L, 0L, true, false, false);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isZero();
    assertThat(ZetaCacheContext.get().getSoftTtlMs()).isZero();
    assertThat(ZetaCacheContext.get().isAllowNull()).isTrue();
  }

  @Test
  @DisplayName("apply with all defaults clears context")
  void apply_withAllDefaults_clearsContext() {
    ZetaCacheContext.get().apply(1000L, 500L, true, false, false);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isEqualTo(1000L);

    ZetaCacheContext.get().apply(0L, 0L, false, false, false);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isZero();
    assertThat(ZetaCacheContext.get().getSoftTtlMs()).isZero();
    assertThat(ZetaCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("getHardTtlMs returns 0 when no context set")
  void getHardTtlMs_whenNoContext_returnsZero() {
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isZero();
  }

  @Test
  @DisplayName("getSoftTtlMs returns 0 when no context set")
  void getSoftTtlMs_whenNoContext_returnsZero() {
    assertThat(ZetaCacheContext.get().getSoftTtlMs()).isZero();
  }

  @Test
  @DisplayName("isAllowNull returns false when no context set")
  void isAllowNull_whenNoContext_returnsFalse() {
    assertThat(ZetaCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("snapshot returns current context values")
  void snapshot_returnsCurrentValues() {
    ZetaCacheContext.get().apply(2000L, 1000L, true, false, false);
    ZetaCacheContext.ContextValues snapshot = ZetaCacheContext.get().snapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.hardTtlMs()).isEqualTo(2000L);
    assertThat(snapshot.softTtlMs()).isEqualTo(1000L);
    assertThat(snapshot.allowNull()).isTrue();
  }

  @Test
  @DisplayName("snapshot returns null when no context set")
  void snapshot_whenNoContext_returnsNull() {
    assertThat(ZetaCacheContext.get().snapshot()).isNull();
  }

  @Test
  @DisplayName("restore restores previously captured values")
  void restore_restoresValues() {
    ZetaCacheContext.get().apply(2000L, 1000L, true, false, false);
    ZetaCacheContext.ContextValues snapshot = ZetaCacheContext.get().snapshot();

    ZetaCacheContext.get().apply(0L, 0L, false, false, false);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isZero();

    ZetaCacheContext.get().restore(snapshot);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isEqualTo(2000L);
    assertThat(ZetaCacheContext.get().getSoftTtlMs()).isEqualTo(1000L);
    assertThat(ZetaCacheContext.get().isAllowNull()).isTrue();
  }

  @Test
  @DisplayName("restore null clears context")
  void restore_null_clearsContext() {
    ZetaCacheContext.get().apply(2000L, 1000L, true, false, false);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isPositive();

    ZetaCacheContext.get().restore(null);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isZero();
    assertThat(ZetaCacheContext.get().getSoftTtlMs()).isZero();
    assertThat(ZetaCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("different threads have isolated contexts")
  void threadIsolation() throws Exception {
    ZetaCacheContext.get().apply(100L, 200L, true, false, false);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isEqualTo(100L);

    AtomicReference<Long> otherThreadHardTtl = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    Thread other = new Thread(() -> {
      assertThat(ZetaCacheContext.get().getHardTtlMs()).isZero();
      ZetaCacheContext.get().apply(300L, 400L, false, false, false);
      otherThreadHardTtl.set(ZetaCacheContext.get().getHardTtlMs());
      latch.countDown();
    });
    other.start();
    latch.await();

    assertThat(otherThreadHardTtl.get()).isEqualTo(300L);
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isEqualTo(100L);
  }

  @Test
  @DisplayName("ThreadLocal does not leak across invocations")
  void noThreadLocalLeak() {
    assertThat(ZetaCacheContext.get().getHardTtlMs()).isZero();
    assertThat(ZetaCacheContext.get().snapshot()).isNull();
  }

  @Test
  @DisplayName("ContextValues record accessors work correctly")
  void contextValuesRecordAccessors() {
    var values = new ZetaCacheContext.ContextValues(5000L, 1000L, true, false, false);
    assertThat(values.hardTtlMs()).isEqualTo(5000L);
    assertThat(values.softTtlMs()).isEqualTo(1000L);
    assertThat(values.allowNull()).isTrue();
    assertThat(values.skipBroadcast()).isFalse();
  }

  // ── @Broadcast / skipBroadcast tests ──

  @Test
  @DisplayName("apply with skipBroadcast true sets skipBroadcast")
  void apply_withSkipBroadcast_setsContext() {
    ZetaCacheContext.get().apply(0L, 0L, false, true, false);
    assertThat(ZetaCacheContext.get().isSkipBroadcast()).isTrue();
    assertThat(ZetaCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("isSkipBroadcast returns false when no context set")
  void isSkipBroadcast_whenNoContext_returnsFalse() {
    assertThat(ZetaCacheContext.get().isSkipBroadcast()).isFalse();
  }

  @Test
  @DisplayName("isSkipBroadcast returns false when context set without skipBroadcast")
  void isSkipBroadcast_whenContextWithoutFlag_returnsFalse() {
    ZetaCacheContext.get().apply(100L, 0L, false, false, false);
    assertThat(ZetaCacheContext.get().isSkipBroadcast()).isFalse();
  }

  @Test
  @DisplayName("snapshot preserves skipBroadcast flag")
  void snapshot_preservesSkipBroadcast() {
    ZetaCacheContext.get().apply(0L, 0L, false, true, false);
    ZetaCacheContext.ContextValues snapshot = ZetaCacheContext.get().snapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.skipBroadcast()).isTrue();

    ZetaCacheContext.get().apply(0L, 0L, false, false, false);
    assertThat(ZetaCacheContext.get().isSkipBroadcast()).isFalse();

    ZetaCacheContext.get().restore(snapshot);
    assertThat(ZetaCacheContext.get().isSkipBroadcast()).isTrue();
  }

  @Test
  @DisplayName("apply with skipBroadcast alone does not clear context")
  void apply_withSkipBroadcastOnly_keepsContext() {
    ZetaCacheContext.get().apply(0L, 0L, false, true, false);
    assertThat(ZetaCacheContext.get().isSkipBroadcast()).isTrue();

    ZetaCacheContext.get().apply(0L, 0L, false, false, false);
    assertThat(ZetaCacheContext.get().isSkipBroadcast()).isFalse();
    assertThat(ZetaCacheContext.get().snapshot()).isNull();
  }

  // ── skipDetection tests ──

  @Test
  @DisplayName("isSkipDetection returns false when no context set")
  void isSkipDetection_whenNoContext_returnsFalse() {
    assertThat(ZetaCacheContext.get().isSkipDetection()).isFalse();
  }

  @Test
  @DisplayName("apply with skipDetection true sets context")
  void apply_withSkipDetection_setsContext() {
    ZetaCacheContext.get().apply(0L, 0L, false, false, true);
    assertThat(ZetaCacheContext.get().isSkipDetection()).isTrue();
  }

  @Test
  @DisplayName("isSkipDetection returns false when context set without skipDetection")
  void isSkipDetection_whenContextWithoutFlag_returnsFalse() {
    ZetaCacheContext.get().apply(100L, 0L, false, false, false);
    assertThat(ZetaCacheContext.get().isSkipDetection()).isFalse();
  }

  @Test
  @DisplayName("snapshot preserves skipDetection flag")
  void snapshot_preservesSkipDetection() {
    ZetaCacheContext.get().apply(0L, 0L, false, false, true);
    ZetaCacheContext.ContextValues snapshot = ZetaCacheContext.get().snapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.skipDetection()).isTrue();

    ZetaCacheContext.get().apply(0L, 0L, false, false, false);
    assertThat(ZetaCacheContext.get().isSkipDetection()).isFalse();

    ZetaCacheContext.get().restore(snapshot);
    assertThat(ZetaCacheContext.get().isSkipDetection()).isTrue();
  }

  @Test
  @DisplayName("apply with skipDetection alone does not clear context")
  void apply_withSkipDetectionOnly_keepsContext() {
    ZetaCacheContext.get().apply(0L, 0L, false, false, true);
    assertThat(ZetaCacheContext.get().isSkipDetection()).isTrue();

    ZetaCacheContext.get().apply(0L, 0L, false, false, false);
    assertThat(ZetaCacheContext.get().isSkipDetection()).isFalse();
    assertThat(ZetaCacheContext.get().snapshot()).isNull();
  }

  @Test
  @DisplayName("restore null clears skipDetection context")
  void restore_null_clearsSkipDetection() {
    ZetaCacheContext.get().apply(0L, 0L, false, false, true);
    assertThat(ZetaCacheContext.get().isSkipDetection()).isTrue();

    ZetaCacheContext.get().restore(null);
    assertThat(ZetaCacheContext.get().isSkipDetection()).isFalse();
  }

  @Test
  @DisplayName("skipDetection is thread-isolated")
  void skipDetectionThreadIsolation() throws Exception {
    ZetaCacheContext.get().apply(0L, 0L, false, false, true);
    assertThat(ZetaCacheContext.get().isSkipDetection()).isTrue();

    AtomicReference<Boolean> otherThreadSkipDetection = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    Thread other = new Thread(() -> {
      assertThat(ZetaCacheContext.get().isSkipDetection()).isFalse();
      ZetaCacheContext.get().apply(0L, 0L, false, false, true);
      otherThreadSkipDetection.set(ZetaCacheContext.get().isSkipDetection());
      latch.countDown();
    });
    other.start();
    latch.await();

    assertThat(otherThreadSkipDetection.get()).isTrue();
    assertThat(ZetaCacheContext.get().isSkipDetection()).isTrue();
  }
}
