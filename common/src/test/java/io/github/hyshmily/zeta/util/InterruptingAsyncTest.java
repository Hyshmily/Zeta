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
package io.github.hyshmily.zeta.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Boundary tests for {@link InterruptingAsync}: a non-positive budget never runs the task. */
class InterruptingAsyncTest {

  @Test
  void supplyAsync_shouldRejectANonPositiveTimeoutBeforeTheSupplierRuns() {
    AtomicBoolean ran = new AtomicBoolean();

    assertThatThrownBy(() ->
      InterruptingAsync.supplyAsync(
        () -> {
          ran.set(true);
          return "v";
        },
        Runnable::run,
        0,
        TimeUnit.SECONDS
      )
    )
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("timeout must be > 0");
    assertThat(ran).isFalse();

    assertThatThrownBy(() ->
      InterruptingAsync.supplyAsync(() -> "v", Runnable::run, -1, TimeUnit.SECONDS)
    ).isInstanceOf(IllegalArgumentException.class);
  }
}
