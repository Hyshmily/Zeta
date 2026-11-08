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
package io.github.hyshmily.zeta.hotkeydetector.heavykeeper;

/**
 * Result of one {@link TopK#addDirect} operation.
 *
 * <p>{@code isHotKey} is {@code true} when this add was accepted as hot: the
 * key cleared the admission threshold and either already held a TopK seat or
 * won one. {@code expelledKey} names the key that lost its seat to make room,
 * and is {@code null} otherwise.
 *
 * @param expelledKey the key evicted from the TopK set by this operation,
 *                    or {@code null} if no eviction occurred
 * @param isHotKey    whether this add was accepted as hot
 * @param currentKey  the key the count was for; empty on the shared
 *                    {@link #cold()} result, so callers must not treat the
 *                    cold result's key as a real key
 */
public record AddResult(String expelledKey, boolean isHotKey, String currentKey) {
  private static final AddResult COLD = new AddResult(null, false, "");

  /**
   * The shared "nothing happened" result for rejected adds: the key did not
   * enter the TopK set (non-positive delta, below the admission threshold, or
   * unable to displace a member) and no member was evicted. {@code currentKey}
   * is empty — callers must not treat it as a real key.
   *
   * @return the singleton cold result (never {@code null})
   */
  public static AddResult cold() {
    return COLD;
  }
}
