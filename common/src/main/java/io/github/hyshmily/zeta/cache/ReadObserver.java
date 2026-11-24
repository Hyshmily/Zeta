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
package io.github.hyshmily.zeta.cache;

import java.util.Set;

/**
 * Receives the keys a completed access-counter tide observed as actually read. The counter owns
 * the cadence; a consumer must only read the set and must not retain it.
 */
@FunctionalInterface
public interface ReadObserver {
  /**
   * @param keys       the keys read in the tide
   * @param nowMonoMs  one monotonic sample for the whole tide
   */
  void onReads(Set<String> keys, long nowMonoMs);

  /** A no-op observer for deployments without a warm scheduler. */
  ReadObserver NONE = (keys, nowMonoMs) -> {};
}
