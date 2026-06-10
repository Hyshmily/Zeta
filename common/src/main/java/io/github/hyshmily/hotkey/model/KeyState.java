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
 * Lifecycle state of a key within the hot-key cache.
 *
 * <p>Transitions: {@code NORMAL -> HOT} (when frequency exceeds threshold),
 * {@code HOT -> PRE_COOL -> COOL -> NORMAL} (cooldown sequence).
 */
public enum KeyState {
  /** Key frequency exceeds the hot threshold; kept in L1 with extended TTL. */
  HOT,
  /** Key frequency dropped below cooldown threshold; L1 TTL reverts to normal. */
  COOL,
  /** Transient state between HOT and COOL during cooldown sequence. */
  PRE_COOL,
  /** Default state; key is cached with standard TTL. */
  NORMAL
}
