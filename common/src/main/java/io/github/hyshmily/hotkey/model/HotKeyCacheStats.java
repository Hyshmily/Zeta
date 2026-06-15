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
 * Snapshot of L1 cache performance counters and current capacity.
 * <p>
 * {@code hitCount}, {@code missCount}, {@code hitRate}, and
 * {@code evictionCount} are populated only when Caffeine's
 * {@code recordStats()} is enabled (automatic when Micrometer is on the
 * classpath).  {@code estimatedSize} is always available.
 *
 * @param hitCount       total number of cache hits (0 if stats not recorded)
 * @param missCount      total number of cache misses (0 if stats not recorded)
 * @param hitRate        hit rate in {@code [0.0, 1.0]} (0.0 if stats not recorded)
 * @param evictionCount  number of entries evicted by Caffeine (0 if stats not recorded)
 * @param estimatedSize  best-effort estimate of current entry count
 */
public record HotKeyCacheStats(
  long hitCount,
  long missCount,
  double hitRate,
  long evictionCount,
  long estimatedSize
) {}
