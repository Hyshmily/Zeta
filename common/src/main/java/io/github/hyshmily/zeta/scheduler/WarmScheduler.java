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

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;

/**
 * The cache's outlet for "this entry may deserve a background renewal". The data plane reports
 * the event; the scheduler owns the policy, timing and gate.
 */
public interface WarmScheduler {
  /**
   * Offer one entry to the renewal scheduler.
   *
   * @param cacheKey the physical key
   * @param entry    the resident entry
   * @param signal   why the event was raised
   * @return whether a renewal task is armed for the key after this call; {@code false} when the
   *         namespace has no loader, its keep-warm trigger refuses the key, or the task budget is
   *         exhausted
   */
  boolean scheduleIfEligible(String cacheKey, CacheEntry entry, WarmSignal signal);

  /**
   * Drop any scheduled renewal for the key.
   *
   * @param cacheKey the physical key
   */
  void cancel(String cacheKey);

  /**
   * Drop the key's renewal when the namespace's keep-warm trigger no longer accepts any live heat
   * signal. A key that is still locally hot, or still covered by a live cluster lease the trigger
   * accepts, keeps its task.
   *
   * @param cacheKey the physical key
   */
  void cancelIfCold(String cacheKey);
}
