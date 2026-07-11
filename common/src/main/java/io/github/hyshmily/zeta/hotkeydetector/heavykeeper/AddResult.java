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
 * Result of a single {@link TopK#addDirect} operation.
 *
 * <p>Encapsulates three pieces of information:
 * <ol>
 *   <li>Whether the key being added entered the TopK hot set ({@code isHotKey});</li>
 *   <li>If it did and the set was full, which key was evicted to make room
 *       ({@code expelledKey});</li>
 *   <li>The identity of the key that triggered this operation ({@code currentKey}).</li>
 * </ol>
 *
 * <p>When {@code isHotKey} is {@code false}, the key either did not meet
 * the minimum count threshold or could not displace any current member.
 * In that case {@code expelledKey} is always {@code null}.
 *
 * @param expelledKey the key evicted from the TopK set by this operation,
 *                    or {@code null} if no eviction occurred
 * @param isHotKey    whether the input key entered the TopK hot set during
 *                    this operation
 * @param currentKey  the key that was added (never {@code null})
 */
public record AddResult(String expelledKey, boolean isHotKey, String currentKey) {
  private static final AddResult COLD = new AddResult(null, false, "");

  public static AddResult cold() {
    return COLD;
  }
}
