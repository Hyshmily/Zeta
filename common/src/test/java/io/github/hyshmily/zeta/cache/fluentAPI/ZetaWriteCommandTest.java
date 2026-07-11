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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.zeta.Zeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZetaWriteCommandTest {

  private Zeta zeta;
  private ZetaWriteCommand<String> command;

  @BeforeEach
  void setUp() {
    zeta = mock(Zeta.class);
    command = new ZetaWriteCommand<>(zeta, "test-key");
  }

  @Test
  void putThrough_shouldDelegate() {
    Runnable writer = () -> {};
    command.putThrough("value", writer);
    verify(zeta).putThrough("test-key", "value", writer, 0L, 0L, true);
  }

  @Test
  void putThrough_withHardTtl_shouldDelegateWithTtl() {
    Runnable writer = () -> {};
    command.withHardTtl(5000L).putThrough("v", writer);
    verify(zeta).putThrough("test-key", "v", writer, 5000L, 0L, true);
  }

  @Test
  void putThrough_withSoftTtl_shouldDelegateWithTtl() {
    Runnable writer = () -> {};
    command.withSoftTtl(500L).putThrough("v", writer);
    verify(zeta).putThrough("test-key", "v", writer, 0L, 500L, true);
  }

  @Test
  void putThrough_withBothTtls_shouldDelegateWithTtl() {
    Runnable writer = () -> {};
    command.withHardTtl(30000L).withSoftTtl(3000L).putThrough("v", writer);
    verify(zeta).putThrough("test-key", "v", writer, 30000L, 3000L, true);
  }

  @Test
  void invalidateAfterPut_shouldDelegate() {
    Runnable mutation = () -> {};
    command.putBeforeInvalidate(mutation);
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
  void invalidateAfterPut_shouldThrowWhenExecutedTwice() {
    command.putBeforeInvalidate(() -> {});
    assertThatThrownBy(() -> command.putBeforeInvalidate(() -> {})).isInstanceOf(IllegalStateException.class);
  }
}
