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
package io.github.hyshmily.zeta.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TimedRefreshCoordinatorTest {

  /**
   * The ×1.1 interval must saturate, not wrap negative: a wrapped value would
   * be rejected by {@code scheduleWithFixedDelay}, and an intermediate-overflow
   * variant would silently produce an interval below the soft TTL.
   */
  @Test
  void intervalFor_shouldSaturateWhenTheProductOverflows() {
    assertThat(TimedRefreshCoordinator.intervalFor(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  /** A registration after shutdown fails with the same message as the lazy-scheduler path. */
  @Test
  void register_afterDestroy_shouldThrowContractException() {
    TimedRefreshCoordinator coordinator = new TimedRefreshCoordinator();
    coordinator.destroy();

    assertThatThrownBy(() -> coordinator.register("key", 1_000L, () -> {}))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("TimedRefreshCoordinator is destroyed");
  }
}
