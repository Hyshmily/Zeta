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

import io.github.hyshmily.hotkey.sync.worker.WorkerMessage;
import java.util.Map;

/**
 * A decision emitted by the Worker's sliding-window / state-machine pipeline,
 * instructing application instances how to treat a specific cache key.
 *
 * <p>Each evaluation cycle of {@link io.github.hyshmily.hotkey.detection.HotKeyStateMachine}
 * produces at most one {@code HotKeyDecision} per key. The decision is then
 * serialized into a {@link WorkerMessage} and
 * send to all application instances via RabbitMQ.
 *
 * <p>Three outcomes are possible:
 * <ul>
 *   <li>{@link DecisionType#HOT} — key should be promoted in L1 with extended TTL</li>
 *   <li>{@link DecisionType#COOL} — key should revert to normal TTL</li>
 *   <li>{@link DecisionType#NONE} — no change needed</li>
 * </ul>
 *
 * <p>Use the static factory methods ({@link #hot}, {@link #cool}, {@link #none})
 * for concise construction.
 *
 * @param type     the decision type (never {@code null})
 * @param cacheKey the affected cache key (never {@code null})
 */
// modified: added snapShot field for failure rollback
public record HotKeyDecision(DecisionType type, String cacheKey, Map<String, Object> snapShot) {
  /**
   * Possible decision outcomes for a hot-key evaluation.
   * <ul>
   *   <li>{@link #HOT} — key exceeds the frequency threshold and should be cached</li>
   *   <li>{@link #COOL} — key frequency dropped below cooldown threshold</li>
   *   <li>{@link #NONE} — no action is required</li>
   * </ul>
   */
  public enum DecisionType {
    /** Key exceeds the frequency threshold and should be promoted to L1 cache. */
    HOT,
    /** Key frequency has dropped below the cooldown threshold and should be evicted from L1. */
    COOL,
    /** No action is required for this key during this evaluation cycle. */
    NONE,
  }

  /**
   * Create a HOT decision for the given key.
   *
   * @param cacheKey the affected cache key
   * @return a new {@code HotKeyDecision} with type {@link DecisionType#HOT}
   */
  public static HotKeyDecision hot(String cacheKey, Map<String, Object> snapShot) {
    return new HotKeyDecision(DecisionType.HOT, cacheKey, snapShot);
  }

  /**
   * Create a COOL decision for the given key.
   *
   * @param cacheKey the affected cache key
   * @return a new {@code HotKeyDecision} with type {@link DecisionType#COOL}
   */
  public static HotKeyDecision cool(String cacheKey, Map<String, Object> snapShot) {
    return new HotKeyDecision(DecisionType.COOL, cacheKey, snapShot);
  }

  /**
   * Create a no-op decision for the given key.
   *
   * @param cacheKey the affected cache key
   * @return a new {@code HotKeyDecision} with type {@link DecisionType#NONE}
   */
  public static HotKeyDecision none(String cacheKey, Map<String, Object> snapShot) {
    return new HotKeyDecision(DecisionType.NONE, cacheKey, snapShot);
  }
}
