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

import io.github.hyshmily.zeta.Internal;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * Background soft-expire refresh executor for the L1 cache — the scheduling
 * half of the entry lifecycle, split from the former {@code ExpireManager}
 * into its own seam.
 *
 * <p>When a read serves a soft-expired (stale-while-revalidate) entry, the
 * read path calls {@link #triggerBackgroundRefresh}: the fresh value is loaded
 * on a bounded executor, version-guarded against superseding writes, and
 * merged into L1 without blocking the caller. Failures keep the stale entry
 * servable through the lease-on-failure extension (ADR-0036).
 *
 * <p>This interface owns the refresh limiter semaphore; monitoring surfaces
 * (actuator endpoint, Micrometer gauges) read it via {@link #getRefreshLimiter()}.
 */
@Internal
public interface BackgroundRefresher {

  /**
   * Triggers an asynchronous background refresh for the given cache key. The
   * caller (the read path) has already returned the stale value to the
   * client, so this method executes entirely in the background without
   * blocking the caller.
   *
   * @param cacheKey  the key whose value should be refreshed
   * @param reader    the data-source supplier
   * @param softTtlMs the soft TTL to set on the refreshed entry (milliseconds)
   */
  void triggerBackgroundRefresh(String cacheKey, Supplier<?> reader, long softTtlMs);

  /** Expose the refresh limiter semaphore for monitoring purposes. */
  Semaphore getRefreshLimiter();
}
