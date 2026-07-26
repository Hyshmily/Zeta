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
package io.github.hyshmily.zeta.annotation;

/**
 * Trigger mode for {@link Intercept @Intercept} annotation.
 *
 * <p>Determines under what condition a {@code @Cacheable} read operation is
 * intercepted and the cached (or fallback) value is returned instead of
 * executing the original method body.
 *
 * @see Intercept
 */
public enum InterceptType {
  /**
   * Intercept when the cache key is recognised as a local hot key by the
   * HeavyKeeper TopK detector in L1.
   */
  IS_LOCAL_HOT,

  /**
   * Always intercept — the original method is never executed. Useful for
   * forcing cache-only access or testing the fallback path.
   */
  FORCE,

  /**
   * Intercept when the per-key request rate exceeds the configured
   * {@link Intercept#qps()} threshold.
   *
   * <p>The implementation uses a two-layer check:
   * <ol>
   *   <li><b>Block table</b> (if
   *       {@link Intercept.QpsConfig#blockDurationMs()} > 0) — a Caffeine-backed
   *       in-memory cache stores the absolute unblock timestamp per key.
   *       Requests hitting a still-blocked key are fast-rejected without
   *       consuming tokens.</li>
   *   <li><b>Bucket4j token bucket</b> — a greedy-refill token bucket with
   *       capacity = {@code threshold} and refill rate = {@code threshold}/s.
   *       When tokens are exhausted, the key enters the block table (if
   *       configured) and the request is intercepted.</li>
   * </ol>
   *
   * <p>During the block period no tokens are consumed from the bucket, so the
   * bucket continues to refill. When the block expires the bucket is at (or
   * near) full capacity, preventing post-block thundering-herd.
   */
  QPS,

  /**
   * Intercept when the per-key concurrent thread count exceeds the
   * configured {@link Intercept.ConcurrentConfig#threshold()} threshold.
   * The counter increments before method execution and decrements
   * in a {@code finally} block, ensuring accurate live count.
   */
  CONCURRENT_THREADS,
}
