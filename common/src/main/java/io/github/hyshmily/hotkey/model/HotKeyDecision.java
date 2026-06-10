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
 * A decision emitted by the Worker's sliding-window / state-machine pipeline.
 *
 * <p>Indicates whether a key should be treated as {@link DecisionType#HOT},
 * {@link DecisionType#COOL}, or that no action is required
 * ({@link DecisionType#NONE}).
 *
 * @param type     the decision type
 * @param cacheKey the affected key
 */
public record HotKeyDecision(DecisionType type, String cacheKey) {
  /**
   * Possible decision outcomes for a hot-key evaluation.
   * <ul>
   *   <li>{@link #HOT} — key exceeds the frequency threshold and should be cached</li>
   *   <li>{@link #COOL} — key frequency dropped below cooldown threshold</li>
   *   <li>{@link #NONE} — no action is required</li>
   * </ul>
   */
  public enum DecisionType {
    HOT,
    COOL,
    NONE,
  }

  /**
   * Create a HOT decision for the given key.
   *
   * @param cacheKey the affected cache key
   * @return a new {@code HotKeyDecision} with type {@link DecisionType#HOT}
   */
  public static HotKeyDecision hot(String cacheKey) {
    return new HotKeyDecision(DecisionType.HOT, cacheKey);
  }

  /**
   * Create a COOL decision for the given key.
   *
   * @param cacheKey the affected cache key
   * @return a new {@code HotKeyDecision} with type {@link DecisionType#COOL}
   */
  public static HotKeyDecision cool(String cacheKey) {
    return new HotKeyDecision(DecisionType.COOL, cacheKey);
  }

  /**
   * Create a no-op decision for the given key.
   *
   * @param cacheKey the affected cache key
   * @return a new {@code HotKeyDecision} with type {@link DecisionType#NONE}
   */
  public static HotKeyDecision none(String cacheKey) {
    return new HotKeyDecision(DecisionType.NONE, cacheKey);
  }
}
