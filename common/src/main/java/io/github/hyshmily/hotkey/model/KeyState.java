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
 * Lifecycle state of a key within the L1 cache, governing which TTL
 * policy is applied.
 *
 * <p>A key starts in {@link #NORMAL} with the configured default TTLs.
 * When the Worker broadcasts a HOT decision, the key transitions to
 * {@link #HOT} and its TTLs are extended. When the Worker broadcasts a
 * COOL decision, the key transitions through {@link #PRE_COOL} then
 * {@link #COOL} before returning to {@link #NORMAL} with the original
 * TTLs restored.
 *
 * <p>Transition sequence:
 * <pre>
 *              HOT broadcast
 *   NORMAL ──────────────────► HOT
 *     ◄─────────────────────────┘
 *      COOL (via PRE_COOL → COOL)
 *
 *   Internal detail:
 *   HOT → PRE_COOL → COOL → NORMAL
 * </pre>
 *
 * <p>The {@link #PRE_COOL} intermediate state prevents TTL flapping:
 * if a new HOT decision arrives during the cool-down grace period,
 * the key reverts to {@link #HOT} without a full transition cycle.
 */
public enum KeyState {
  /**
   * Key has been identified as hot by the Worker. The L1 cache entry is
   * kept with extended TTL to serve the high-frequency reads without
   * hitting the backend.
   */
  HOT,
  /**
   * Key frequency has dropped below the cooldown threshold. The L1 TTL
   * reverts from the extended hot TTL back to the normal configured TTL.
   * This is a terminal state in the cool-down sequence before returning
   * to {@link #NORMAL}.
   */
  COOL,
  /**
   * Transient state between {@link #HOT} and {@link #COOL} during the
   * cool-down sequence. The entry is still cached with extended TTL but
   * is awaiting the next evaluation cycle. If a new HOT decision arrives
   * during this state, the key returns to {@link #HOT} without completing
   * the cool-down, preventing TTL oscillation.
   */
  PRE_COOL,
  /**
   * Default state: the key is cached with the standard configured TTL.
   * All keys start in this state and return to it after the cool-down
   * sequence completes.
   */
  NORMAL
}
