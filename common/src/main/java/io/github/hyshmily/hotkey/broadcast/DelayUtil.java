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
package io.github.hyshmily.hotkey.broadcast;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Utility for introducing random jitter before executing a task.
 * Package-private; used internally by broadcast listeners to spread
 * Redis reads evenly across instances.
 */
final class DelayUtil {

  /**
   * Execute the task after a random delay uniformly distributed
   * between {@code 0} and {@code jitterMs}.  If {@code jitterMs} is
   * {@code 0} or negative the task runs immediately.
   *
   * @param task      the task to execute
   * @param jitterMs  maximum jitter delay in milliseconds
   * @param scheduler the scheduler to use for delayed execution
   */
  static void floatTimeDelay(Runnable task, long jitterMs, ScheduledExecutorService scheduler) {
    if (jitterMs > 0) {
      long delay = ThreadLocalRandom.current().nextLong(jitterMs);
      if (delay > 0) {
        scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);
        return;
      }
    }
    task.run();
  }

  private DelayUtil() {}
}
