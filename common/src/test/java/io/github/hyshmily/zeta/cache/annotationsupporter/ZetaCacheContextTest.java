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
import io.github.hyshmily.zeta.model.CachePolicy;
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
    ZetaCacheContext.get().push(CachePolicy.of(1000L, 0L, false, false));
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isEqualTo(1000L);
    assertThat(ZetaCacheContext.get().current().softTtlMs().getAsLong()).isZero();
    assertThat(ZetaCacheContext.get().current().nullCaching()).isFalse();
  }

  @Test
  @DisplayName("apply with non-default softTtlMs sets context")
  void apply_withSoftTtlMs_setsContext() {
    ZetaCacheContext.get().push(CachePolicy.of(0L, 500L, false, false));
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isZero();
    assertThat(ZetaCacheContext.get().current().softTtlMs().getAsLong()).isEqualTo(500L);
    assertThat(ZetaCacheContext.get().current().nullCaching()).isFalse();
  }

  @Test
  @DisplayName("apply with allowNull true sets context")
  void apply_withAllowNull_setsContext() {
    ZetaCacheContext.get().push(CachePolicy.of(0L, 0L, true, false));
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isZero();
    assertThat(ZetaCacheContext.get().current().softTtlMs().getAsLong()).isZero();
    assertThat(ZetaCacheContext.get().current().nullCaching()).isTrue();
  }

  @Test
  @DisplayName("apply with all defaults clears context")
  void apply_withAllDefaults_clearsContext() {
    ZetaCacheContext.get().push(CachePolicy.of(1000L, 500L, true, false));
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isEqualTo(1000L);

    ZetaCacheContext.get().push(CachePolicy.of(0L, 0L, false, false));
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isZero();
    assertThat(ZetaCacheContext.get().current().softTtlMs().getAsLong()).isZero();
    assertThat(ZetaCacheContext.get().current().nullCaching()).isFalse();
  }

  @Test
  @DisplayName("getHardTtlMs returns 0 when no context set")
  void getHardTtlMs_whenNoContext_returnsZero() {
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isZero();
  }

  @Test
  @DisplayName("getSoftTtlMs returns 0 when no context set")
  void getSoftTtlMs_whenNoContext_returnsZero() {
    assertThat(ZetaCacheContext.get().current().softTtlMs().getAsLong()).isZero();
  }

  @Test
  @DisplayName("nullCaching returns true by default (CachePolicy defaults)")
  void nullCaching_whenNoContext_returnsTrue() {
    assertThat(ZetaCacheContext.get().current().nullCaching()).isTrue();
  }

  @Test
  @DisplayName("snapshot returns current context values")
  void snapshot_returnsCurrentValues() {
    ZetaCacheContext.get().push(CachePolicy.of(2000L, 1000L, true, false));
    CachePolicy snapshot = ZetaCacheContext.get().snapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.hardTtlMs().getAsLong()).isEqualTo(2000L);
    assertThat(snapshot.softTtlMs().getAsLong()).isEqualTo(1000L);
    assertThat(snapshot.nullCaching()).isTrue();
  }

  @Test
  @DisplayName("snapshot returns null when no context set")
  void snapshot_whenNoContext_returnsNull() {
    assertThat(ZetaCacheContext.get().snapshot()).isNull();
  }

  @Test
  @DisplayName("restore restores previously captured values")
  void restore_restoresValues() {
    ZetaCacheContext.get().push(CachePolicy.of(2000L, 1000L, true, false));
    CachePolicy snapshot = ZetaCacheContext.get().snapshot();

    ZetaCacheContext.get().push(CachePolicy.of(0L, 0L, false, false));
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isZero();

    ZetaCacheContext.get().restore(snapshot);
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isEqualTo(2000L);
    assertThat(ZetaCacheContext.get().current().softTtlMs().getAsLong()).isEqualTo(1000L);
    assertThat(ZetaCacheContext.get().current().nullCaching()).isTrue();
  }

  @Test
  @DisplayName("restore null clears context")
  void restore_null_clearsContext() {
    ZetaCacheContext.get().push(CachePolicy.of(2000L, 1000L, true, false));
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isPositive();

    ZetaCacheContext.get().restore(null);
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isZero();
    assertThat(ZetaCacheContext.get().current().softTtlMs().getAsLong()).isZero();
    assertThat(ZetaCacheContext.get().current().nullCaching()).isTrue();
  }

  @Test
  @DisplayName("different threads have isolated contexts")
  void threadIsolation() throws Exception {
    ZetaCacheContext.get().push(CachePolicy.of(100L, 200L, true, false));
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isEqualTo(100L);

    AtomicReference<Long> otherThreadHardTtl = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    Thread other = new Thread(() -> {
      assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isZero();
      ZetaCacheContext.get().push(CachePolicy.of(300L, 400L, false, false));
      otherThreadHardTtl.set(ZetaCacheContext.get().current().hardTtlMs().getAsLong());
      latch.countDown();
    });
    other.start();
    latch.await();

    assertThat(otherThreadHardTtl.get()).isEqualTo(300L);
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isEqualTo(100L);
  }

  @Test
  @DisplayName("ThreadLocal does not leak across invocations")
  void noThreadLocalLeak() {
    assertThat(ZetaCacheContext.get().current().hardTtlMs().getAsLong()).isZero();
    assertThat(ZetaCacheContext.get().snapshot()).isNull();
  }

  @Test
  @DisplayName("CachePolicy record accessors work correctly")
  void contextValuesRecordAccessors() {
    var values = CachePolicy.of(5000L, 1000L, true, false);
    assertThat(values.hardTtlMs().getAsLong()).isEqualTo(5000L);
    assertThat(values.softTtlMs().getAsLong()).isEqualTo(1000L);
    assertThat(values.nullCaching()).isTrue();
    assertThat(values.skipBroadcast()).isFalse();
  }

  // ── @Broadcast / skipBroadcast tests ──

  @Test
  @DisplayName("push with skipBroadcast true sets skipBroadcast")
  void apply_withSkipBroadcast_setsContext() {
    ZetaCacheContext.get().push(CachePolicy.of(0L, 0L, false, true));
    assertThat(ZetaCacheContext.get().current().skipBroadcast()).isTrue();
    assertThat(ZetaCacheContext.get().current().nullCaching()).isFalse();
  }

  @Test
  @DisplayName("skipBroadcast returns false when no context set")
  void isSkipBroadcast_whenNoContext_returnsFalse() {
    assertThat(ZetaCacheContext.get().current().skipBroadcast()).isFalse();
  }

  @Test
  @DisplayName("skipBroadcast returns false when context set without skipBroadcast")
  void isSkipBroadcast_whenContextWithoutFlag_returnsFalse() {
    ZetaCacheContext.get().push(CachePolicy.of(100L, 0L, false, false));
    assertThat(ZetaCacheContext.get().current().skipBroadcast()).isFalse();
  }

  @Test
  @DisplayName("snapshot preserves skipBroadcast flag")
  void snapshot_preservesSkipBroadcast() {
    ZetaCacheContext.get().push(CachePolicy.of(0L, 0L, false, true));
    CachePolicy snapshot = ZetaCacheContext.get().snapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.skipBroadcast()).isTrue();

    ZetaCacheContext.get().push(CachePolicy.of(0L, 0L, false, false));
    assertThat(ZetaCacheContext.get().current().skipBroadcast()).isFalse();

    ZetaCacheContext.get().restore(snapshot);
    assertThat(ZetaCacheContext.get().current().skipBroadcast()).isTrue();
  }

  @Test
  @DisplayName("push with skipBroadcast alone does not clear context")
  void apply_withSkipBroadcastOnly_keepsContext() {
    ZetaCacheContext.get().push(CachePolicy.of(0L, 0L, false, true));
    assertThat(ZetaCacheContext.get().current().skipBroadcast()).isTrue();

    ZetaCacheContext.get().push(CachePolicy.of(0L, 0L, false, false));
    assertThat(ZetaCacheContext.get().current().skipBroadcast()).isFalse();
  }

}
