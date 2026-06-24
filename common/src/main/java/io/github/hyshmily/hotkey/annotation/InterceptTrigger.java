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
package io.github.hyshmily.hotkey.annotation;

/**
 * Trigger mode for {@link Intercept @Intercept} annotation.
 *
 * <p>Determines under what condition a {@code @Cacheable} read operation is
 * intercepted and the cached (or fallback) value is returned instead of
 * executing the original method body.
 *
 * @see Intercept
 */
public enum InterceptTrigger {

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
   * {@link Intercept#QPS()} threshold. Uses a sliding-window counter
   * (10 buckets, 1-second window) per unique key.
   */
  QPS,
}
