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
package io.github.hyshmily.zeta.cache.fluentAPI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.model.CachePolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ZetaWriteCommandTest {

  private Zeta zeta;
  private ZetaWriteCommand<String> command;

  @BeforeEach
  void setUp() {
    zeta = mock(Zeta.class);
    command = new ZetaWriteCommand<>(zeta, "test-key");
  }

  /**
   * The facade's write family passes the policy as the last argument; capture
   * it and assert the resolved TTLs (record equality is not reliable for
   * non-zero TTLs — each {@code ttlSupplier(long)} call allocates a fresh
   * lambda).
   */
  private CachePolicy capturedWritePolicy() {
    ArgumentCaptor<CachePolicy> captor = ArgumentCaptor.forClass(CachePolicy.class);
    verify(zeta).putThrough(any(), any(), any(Runnable.class), captor.capture());
    return captor.getValue();
  }

  @Test
  void putThrough_shouldDelegate() {
    Runnable writer = () -> {};
    command.putThrough("value", writer);
    assertThat(capturedWritePolicy().hardTtlMs().getAsLong()).isZero();
    assertThat(capturedWritePolicy().softTtlMs().getAsLong()).isZero();
  }

  @Test
  void putThrough_withHardTtl_shouldDelegateWithTtl() {
    command.withHardTtl(5000L).putThrough("v", () -> {});
    assertThat(capturedWritePolicy().hardTtlMs().getAsLong()).isEqualTo(5000L);
    assertThat(capturedWritePolicy().softTtlMs().getAsLong()).isZero();
  }

  @Test
  void putThrough_withSoftTtl_shouldDelegateWithTtl() {
    command.withSoftTtl(500L).putThrough("v", () -> {});
    assertThat(capturedWritePolicy().hardTtlMs().getAsLong()).isZero();
    assertThat(capturedWritePolicy().softTtlMs().getAsLong()).isEqualTo(500L);
  }

  @Test
  void putThrough_withBothTtls_shouldDelegateWithTtl() {
    command.withHardTtl(30000L).withSoftTtl(3000L).putThrough("v", () -> {});
    assertThat(capturedWritePolicy().hardTtlMs().getAsLong()).isEqualTo(30000L);
    assertThat(capturedWritePolicy().softTtlMs().getAsLong()).isEqualTo(3000L);
  }

  @Test
  void invalidateAfterMutation_shouldDelegate() {
    Runnable mutation = () -> {};
    command.invalidateAfterMutation(mutation);
    verify(zeta).invalidateAfterPut("test-key", mutation);
  }

  @Test
  void invalidate_shouldDelegate() {
    command.invalidate();
    verify(zeta).invalidate("test-key");
  }

  @Test
  void putThrough_shouldThrowWhenExecutedTwice() {
    command.putThrough("v", () -> {});
    assertThatThrownBy(() -> command.putThrough("v", () -> {})).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void invalidate_shouldThrowWhenAfterPutThrough() {
    command.putThrough("v", () -> {});
    assertThatThrownBy(command::invalidate).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void invalidateAfterMutation_shouldThrowWhenExecutedTwice() {
    command.invalidateAfterMutation(() -> {});
    assertThatThrownBy(() -> command.invalidateAfterMutation(() -> {})).isInstanceOf(IllegalStateException.class);
  }
}
