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
package io.github.hyshmily.zeta.cache.codec;

/**
 * Implemented by cached values that know their own retained heap size.
 *
 * <p>When a value implements this interface, {@link DefaultWeigher} uses the reported size directly
 * and skips the reflective object-graph walk — O(1) instead of O(graph). Recommended for values that
 * already track their size: a DTO with a payload-length field, an image as
 * {@code width × height × 4}, a wrapper around a serialized form, and so on (the pattern Caffeine's
 * documentation calls pre-calculated weights).
 *
 * <p><b>Contract.</b> {@link #weighInBytes()} must return the bytes this value retains: its own
 * object size plus everything reachable from it that a deep measurement would count. Subgraphs
 * shared with other cache entries are counted per entry — the same accounting the reflective walk
 * applies (it deduplicates within one value, never across values). This per-entry convention is
 * what makes weights summable across a cache without a deduplication layer.
 *
 * <p>Returning {@code 0} or a negative value means "unknown" — the weigher then falls back to
 * measuring the graph reflectively. Implementations must be cheap and must not throw: they run on
 * the cache write path, under Caffeine's per-bin lock on its {@code computeIfAbsent} path.
 */
public interface Weighable {

  /**
   * Retained size of this value in bytes, or {@code 0} / negative to fall back to measurement.
   *
   * @return the bytes this value retains, including its own object overhead
   */
  long weighInBytes();
}
