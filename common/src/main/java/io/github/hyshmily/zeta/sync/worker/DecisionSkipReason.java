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
package io.github.hyshmily.zeta.sync.worker;

/**
 * Why a Worker HOT/COOL decision was not applied — the argument
 * {@link io.github.hyshmily.zeta.heat.HotKeyListener#onHotSkipped} and
 * {@link io.github.hyshmily.zeta.heat.HotKeyListener#onCoolSkipped} report.
 */
public enum DecisionSkipReason {
  /** The current lease stamp already outranks or duplicates the incoming decision. */
  VERSION_STALE,

  /**
   * A COOL is a stale kill from a previous Worker owner: the lease is still an in-force HOT owned
   * by a different, live Worker incarnation, and the ring still routes the key to it.
   */
  GHOST_COOL,

  /** A later decision in the same batch superseded this one before it could apply. */
  COVERED_BY_NEWER_DECISION,

  /** The issuing Worker incarnation is already dead or superseded in the health view. */
  INCARNATION_DEAD,
}
