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

import io.github.hyshmily.zeta.Internal;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility for computing TTL-based random jitter offsets.
 * Used internally to spread expirations and delayed work
 * evenly across instances. Non-instantiable — private constructor.
 */
@Internal
public final class DelayUtil {

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

    long randomOffset = ThreadLocalRandom.current().nextLong(0, 2 * baseJitter);
    return randomOffset - baseJitter;
  }

  /** Private constructor to prevent instantiation of this utility class. */
  private DelayUtil() {}
}
