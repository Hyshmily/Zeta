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
package io.github.hyshmily.hotkey.util;

import io.github.hyshmily.hotkey.Internal;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Utility for introducing random jitter before executing a task
 * or computing TTL-based expiration timestamps.
 * Used internally by send listeners to spread Redis reads
 * evenly across instances. Non-instantiable — private constructor.
 */
@Internal
public final class DelayUtil {

  /**
   * Execute the task after a random delay uniformly distributed
   * between {@code 0} and {@code jitterMs}.  If {@code jitterMs} is
   * {@code 0} or negative the task runs immediately.
   *
   * @param task      the task to execute
   * @param jitterMs  maximum jitter delay in milliseconds
   * @param scheduler the scheduler to use for delayed execution
   */
  public static void floatTimeDelay(Runnable task, long jitterMs, ScheduledExecutorService scheduler) {
    if (jitterMs > 0) {
      long delay = ThreadLocalRandom.current().nextLong(jitterMs);
      if (delay > 0) {
        scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);
        return;
      }
    }
    scheduler.schedule(task, 0, TimeUnit.MILLISECONDS);
  }

  /**
   * Compute a random jitter offset for a TTL duration.
   * Returns a value in the range {@code [-ttlMs * ttlJitterRatio, ttlMs * ttlJitterRatio)}.
   *
   * @param ttlMs          the base TTL duration in milliseconds
   * @param ttlJitterRatio the jitter ratio (0.0–1.0)
   * @return random jitter offset in milliseconds (maybe negative, zero, or positive)
   */
  public static long computeTtlJitter(long ttlMs, double ttlJitterRatio) {
    if (ttlMs <= 0 || ttlJitterRatio <= 0) {
      return 0;
    }

    long baseJitter = (long) (ttlMs * ttlJitterRatio);
    if (baseJitter <= 0) {
      return 0;
    }

    if (baseJitter > Long.MAX_VALUE / 2) {
      baseJitter = Long.MAX_VALUE / 2;
    }

    long randomOffset = ThreadLocalRandom.current().nextLong(0, 2 * baseJitter + 1);
    return randomOffset - baseJitter;
  }

  /** Private constructor to prevent instantiation of this utility class. */
  private DelayUtil() {}
}
