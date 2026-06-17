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
package io.github.hyshmily.hotkey.model;

/**
 * Immutable snapshot of L1 (Caffeine) cache performance counters and
 * current capacity.
 *
 * <p>This record is returned by {@link io.github.hyshmily.hotkey.HotKey#stats()}
 * for monitoring and observability purposes.
 *
 * <p><b>Availability:</b>
 * <ul>
 *   <li>{@code hitCount}, {@code missCount}, {@code hitRate}, and
 *       {@code evictionCount} are populated <em>only</em> when Caffeine's
 *       {@code recordStats()} is enabled (automatic when Micrometer is on
 *       the classpath). When stats recording is disabled, all four fields
 *       report {@code 0}.</li>
 *   <li>{@code estimatedSize} is always available — it reflects the
 *       underlying Caffeine estimate and does not require stats recording.</li>
 * </ul>
 *
 * <p>The counters are cumulative since the last Caffeine stats reset
 * (typically process lifetime). Use {@code hitRate} as a measure of
 * cache effectiveness: a low hit rate may indicate that the cache is
 * too small, TTLs are too short, or keys are not being reused.
 *
 * @param hitCount       total number of successful cache hits since stats
 *                       recording was enabled (0 if not recorded)
 * @param missCount      total number of cache misses since stats recording
 *                       was enabled (0 if not recorded)
 * @param hitRate        hit rate in {@code [0.0, 1.0]} — {@code hitCount /
 *                       (hitCount + missCount)}; {@code 0.0} if not recorded
 *                       or if no requests have been made
 * @param evictionCount  total number of entries evicted by Caffeine
 *                       (due to size, weight, or TTL expiry); 0 if not
 *                       recorded
 * @param estimatedSize  best-effort estimate of current number of entries
 *                       in the cache; always available
 */
public record HotKeyCacheStats(
  long hitCount,
  long missCount,
  double hitRate,
  long evictionCount,
  long estimatedSize
) {}
