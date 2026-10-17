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
package io.github.hyshmily.zeta.sync.worker;

/**
 * Atomic snapshot of the Worker's state-machine gossip configuration.
 *
 * <p>Bundles the three state-machine counts with the gossip timestamp into a
 * single immutable value so readers and writers replace or observe all four
 * fields at once. The previous layout kept the counts on the state machine
 * (three separate volatiles) and the timestamp in a shared {@code AtomicLong},
 * so a heartbeat producer reading counts plus timestamp could observe a torn
 * combination (new counts with the old timestamp or vice versa).
 *
 * @param confirmCount      consecutive hot windows required to promote COLD to CONFIRMED_HOT
 * @param coolCount         total consecutive cold windows required for a full cool-down
 * @param preCoolGraceCount cold windows marking entry into PRE_COOLING
 * @param configTimestamp   monotonic gossip version; only strictly newer values are adopted
 */
public record SmConfigSnapshot(int confirmCount, int coolCount, int preCoolGraceCount, long configTimestamp) {
  /**
   * Creates the startup snapshot from static configuration.
   *
   * <p>The timestamp is seeded with the current wall clock
   * ({@code max(now, 0)}), not zero, so a stale gossip replay with a small
   * version can never reverse-cover the freshly started Worker's static
   * configuration. Later updates use strictly increasing wall-clock versions.
   *
   * @param confirmCount      the static confirm count
   * @param coolCount         the static cool count
   * @param preCoolGraceCount the static pre-cool grace count
   * @return the initial snapshot carrying the startup timestamp
   */
  public static SmConfigSnapshot initial(int confirmCount, int coolCount, int preCoolGraceCount) {
    return new SmConfigSnapshot(confirmCount, coolCount, preCoolGraceCount, Math.max(System.currentTimeMillis(), 0L));
  }
}
