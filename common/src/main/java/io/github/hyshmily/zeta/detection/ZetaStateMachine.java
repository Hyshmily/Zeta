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
package io.github.hyshmily.zeta.detection;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.model.ZetaDecision;
import java.util.Map;

/**
 * Per-key state machine that governs hot-key lifecycle transitions on the
 * Worker side.
 */
@Internal
public interface ZetaStateMachine {
  /** Key-level lifecycle stages. */
  enum State {
    COLD,
    CONFIRMED_HOT,
    PRE_COOLING,
  }

  /** Number of consecutive hot windows required to promote COLD → CONFIRMED_HOT. */
  int getConfirmCount();

  void setConfirmCount(int confirmCount);

  /** Total number of consecutive cold windows required for a full cool-down. */
  int getCoolCount();

  void setCoolCount(int coolCount);

  /** The number of cold windows that mark the entry into PRE_COOLING. */
  int getPreCoolGraceCount();

  void setPreCoolGraceCount(int preCoolGraceCount);

  /** Evaluate the current sliding-window observation and return a decision. */
  ZetaDecision evaluate(String key, boolean isHotThisWindow);

  /** Remove all tracked state for the given key, resetting it to COLD. */
  void reset(String key);

  /** Approximate number of keys currently tracked. */
  int getTrackedKeys();

  /** Garbage-collect stale keys. */
  void evictStale(long staleAfterMs);

  /** Return a snapshot of the current state for a key. */
  Map<String, Object> getStateSnapshot(String key);

  /** Roll back the per-key state after a send failure. */
  void rollbackToPreviousState(String key, Map<String, Object> previousState);

  // modified: added key-less overload that extracts key from snapShot
  void rollbackToPreviousState(Map<String, Object> previousState);
}
