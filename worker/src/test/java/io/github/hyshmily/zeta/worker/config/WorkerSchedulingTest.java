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
package io.github.hyshmily.zeta.worker.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** A Worker whose required periodic task cannot be scheduled must fail startup, not run degraded. */
class WorkerSchedulingTest {

  @Test
  void aRejectedTaskFailsStartupAndLeavesTheLifecycleStopped() {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
      .thenThrow(new RejectedExecutionException("executor closed"));
    WorkerAutoConfiguration.WorkerScheduling scheduling = new WorkerAutoConfiguration.WorkerScheduling(
      List.of(new WorkerAutoConfiguration.WorkerScheduling.Task(scheduler, () -> {}, 1_000L, 1_000L))
    );

    assertThatThrownBy(scheduling::start)
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("Failed to schedule worker task");
    assertThat(scheduling.isRunning()).isFalse();
  }
}
