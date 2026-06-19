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
package io.github.hyshmily.hotkey.cache.annotationsupporter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("HotKeyCacheContext tests")
class HotKeyCacheContextTest {

  @AfterEach
  void tearDown() {
    HotKeyCacheContext.get().restore(null);
  }

  @Test
  @DisplayName("get() returns the same singleton instance")
  void get_returnsSingleton() {
    assertThat(HotKeyCacheContext.get()).isSameAs(HotKeyCacheContext.get());
  }

  @Test
  @DisplayName("apply with non-default hardTtlMs sets context")
  void apply_withHardTtlMs_setsContext() {
    HotKeyCacheContext.get().apply(1000L, 0L, false, false);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isEqualTo(1000L);
    assertThat(HotKeyCacheContext.get().getSoftTtlMs()).isZero();
    assertThat(HotKeyCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("apply with non-default softTtlMs sets context")
  void apply_withSoftTtlMs_setsContext() {
    HotKeyCacheContext.get().apply(0L, 500L, false, false);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isZero();
    assertThat(HotKeyCacheContext.get().getSoftTtlMs()).isEqualTo(500L);
    assertThat(HotKeyCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("apply with allowNull true sets context")
  void apply_withAllowNull_setsContext() {
    HotKeyCacheContext.get().apply(0L, 0L, true, false);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isZero();
    assertThat(HotKeyCacheContext.get().getSoftTtlMs()).isZero();
    assertThat(HotKeyCacheContext.get().isAllowNull()).isTrue();
  }

  @Test
  @DisplayName("apply with all defaults clears context")
  void apply_withAllDefaults_clearsContext() {
    HotKeyCacheContext.get().apply(1000L, 500L, true, false);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isEqualTo(1000L);

    HotKeyCacheContext.get().apply(0L, 0L, false, false);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isZero();
    assertThat(HotKeyCacheContext.get().getSoftTtlMs()).isZero();
    assertThat(HotKeyCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("getHardTtlMs returns 0 when no context set")
  void getHardTtlMs_whenNoContext_returnsZero() {
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isZero();
  }

  @Test
  @DisplayName("getSoftTtlMs returns 0 when no context set")
  void getSoftTtlMs_whenNoContext_returnsZero() {
    assertThat(HotKeyCacheContext.get().getSoftTtlMs()).isZero();
  }

  @Test
  @DisplayName("isAllowNull returns false when no context set")
  void isAllowNull_whenNoContext_returnsFalse() {
    assertThat(HotKeyCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("snapshot returns current context values")
  void snapshot_returnsCurrentValues() {
    HotKeyCacheContext.get().apply(2000L, 1000L, true, false);
    HotKeyCacheContext.ContextValues snapshot = HotKeyCacheContext.get().snapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.hardTtlMs()).isEqualTo(2000L);
    assertThat(snapshot.softTtlMs()).isEqualTo(1000L);
    assertThat(snapshot.allowNull()).isTrue();
  }

  @Test
  @DisplayName("snapshot returns null when no context set")
  void snapshot_whenNoContext_returnsNull() {
    assertThat(HotKeyCacheContext.get().snapshot()).isNull();
  }

  @Test
  @DisplayName("restore restores previously captured values")
  void restore_restoresValues() {
    HotKeyCacheContext.get().apply(2000L, 1000L, true, false);
    HotKeyCacheContext.ContextValues snapshot = HotKeyCacheContext.get().snapshot();

    HotKeyCacheContext.get().apply(0L, 0L, false, false);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isZero();

    HotKeyCacheContext.get().restore(snapshot);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isEqualTo(2000L);
    assertThat(HotKeyCacheContext.get().getSoftTtlMs()).isEqualTo(1000L);
    assertThat(HotKeyCacheContext.get().isAllowNull()).isTrue();
  }

  @Test
  @DisplayName("restore null clears context")
  void restore_null_clearsContext() {
    HotKeyCacheContext.get().apply(2000L, 1000L, true, false);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isPositive();

    HotKeyCacheContext.get().restore(null);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isZero();
    assertThat(HotKeyCacheContext.get().getSoftTtlMs()).isZero();
    assertThat(HotKeyCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("different threads have isolated contexts")
  void threadIsolation() throws Exception {
    HotKeyCacheContext.get().apply(100L, 200L, true, false);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isEqualTo(100L);

    AtomicReference<Long> otherThreadHardTtl = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    Thread other = new Thread(() -> {
      assertThat(HotKeyCacheContext.get().getHardTtlMs()).isZero();
      HotKeyCacheContext.get().apply(300L, 400L, false, false);
      otherThreadHardTtl.set(HotKeyCacheContext.get().getHardTtlMs());
      latch.countDown();
    });
    other.start();
    latch.await();

    assertThat(otherThreadHardTtl.get()).isEqualTo(300L);
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isEqualTo(100L);
  }

  @Test
  @DisplayName("ThreadLocal does not leak across invocations")
  void noThreadLocalLeak() {
    assertThat(HotKeyCacheContext.get().getHardTtlMs()).isZero();
    assertThat(HotKeyCacheContext.get().snapshot()).isNull();
  }

  @Test
  @DisplayName("ContextValues record accessors work correctly")
  void contextValuesRecordAccessors() {
    var values = new HotKeyCacheContext.ContextValues(5000L, 1000L, true, false);
    assertThat(values.hardTtlMs()).isEqualTo(5000L);
    assertThat(values.softTtlMs()).isEqualTo(1000L);
    assertThat(values.allowNull()).isTrue();
    assertThat(values.skipBroadcast()).isFalse();
  }

  // ── @Broadcast / skipBroadcast tests ──

  @Test
  @DisplayName("apply with skipBroadcast true sets skipBroadcast")
  void apply_withSkipBroadcast_setsContext() {
    HotKeyCacheContext.get().apply(0L, 0L, false, true);
    assertThat(HotKeyCacheContext.get().isSkipBroadcast()).isTrue();
    assertThat(HotKeyCacheContext.get().isAllowNull()).isFalse();
  }

  @Test
  @DisplayName("isSkipBroadcast returns false when no context set")
  void isSkipBroadcast_whenNoContext_returnsFalse() {
    assertThat(HotKeyCacheContext.get().isSkipBroadcast()).isFalse();
  }

  @Test
  @DisplayName("isSkipBroadcast returns false when context set without skipBroadcast")
  void isSkipBroadcast_whenContextWithoutFlag_returnsFalse() {
    HotKeyCacheContext.get().apply(100L, 0L, false, false);
    assertThat(HotKeyCacheContext.get().isSkipBroadcast()).isFalse();
  }

  @Test
  @DisplayName("snapshot preserves skipBroadcast flag")
  void snapshot_preservesSkipBroadcast() {
    HotKeyCacheContext.get().apply(0L, 0L, false, true);
    HotKeyCacheContext.ContextValues snapshot = HotKeyCacheContext.get().snapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.skipBroadcast()).isTrue();

    HotKeyCacheContext.get().apply(0L, 0L, false, false);
    assertThat(HotKeyCacheContext.get().isSkipBroadcast()).isFalse();

    HotKeyCacheContext.get().restore(snapshot);
    assertThat(HotKeyCacheContext.get().isSkipBroadcast()).isTrue();
  }

  @Test
  @DisplayName("apply with skipBroadcast alone does not clear context")
  void apply_withSkipBroadcastOnly_keepsContext() {
    HotKeyCacheContext.get().apply(0L, 0L, false, true);
    assertThat(HotKeyCacheContext.get().isSkipBroadcast()).isTrue();

    HotKeyCacheContext.get().apply(0L, 0L, false, false);
    assertThat(HotKeyCacheContext.get().isSkipBroadcast()).isFalse();
    assertThat(HotKeyCacheContext.get().snapshot()).isNull();
  }
}
