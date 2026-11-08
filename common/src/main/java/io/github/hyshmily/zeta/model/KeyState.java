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
package io.github.hyshmily.zeta.model;

/**
 * Lifecycle state of a key within the L1 cache, governing which TTL
 * policy is applied.
 *
 * <p>A key starts in {@link #NORMAL} with the configured default TTLs.
 * When the Worker broadcasts a HOT decision — or the local TopK promotes the
 * key — it transitions to {@link #HOT} and its TTLs are extended. A Worker
 * COOL broadcast transitions it to {@link #COOL} with the original TTLs
 * restored. A decision issued by a Worker incarnation that has since died or
 * restarted is demoted back to {@link #NORMAL} on the next read
 * (decision-validity demotion, design decisions: hot state); otherwise a COOL
 * entry returns to {@link #NORMAL} when it expires and reloads, or when a
 * successful refresh rewrites it.
 *
 * <p>Transitions:
 * <pre>
 *   NORMAL → HOT      local TopK promotion / Worker HOT
 *   NORMAL → COOL     Worker COOL
 *   HOT    → COOL     Worker COOL
 *   HOT    → NORMAL   expiry + reload / decision-validity demotion
 *   COOL   → HOT      Worker HOT / local TopK promotion (degraded cluster only)
 *   COOL   → NORMAL   expiry + reload / successful refresh
 * </pre>
 */
public enum KeyState {
  /**
   * Key has been identified as hot — by the Worker, or by local TopK promotion.
   * The L1 cache entry is kept with extended TTL to serve the high-frequency
   * reads without hitting the backend.
   */
  HOT,
  /**
   * Key frequency has dropped below the cooldown threshold. The L1 TTL
   * reverts from the extended hot TTL back to the entry's normal TTLs.
   * The entry normally returns to {@link #NORMAL} when it expires or is
   * evicted and reloaded, or when a refresh rewrites it; while the Worker
   * cluster fails the health gate (graceful degradation) a COOL entry is
   * locally promotable to {@link #HOT} again.
   */
  COOL,
  /**
   * Default state: the key is cached with the standard configured TTL.
   * All keys start in this state; entries return to it after a cool-down or a
   * Worker-decision demotion.
   */
  NORMAL,
}
