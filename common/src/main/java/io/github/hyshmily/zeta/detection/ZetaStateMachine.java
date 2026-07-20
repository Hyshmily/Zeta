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
import io.github.hyshmily.zeta.model.EvaluationContext;
import io.github.hyshmily.zeta.model.StateSnapshot;
import io.github.hyshmily.zeta.model.ZetaDecision;

/**
 * Per-key state machine that governs hot-key lifecycle transitions on the
 * Worker side.
 *
 * <p>The state machine converts per-key sliding-window frequency observations
 * into lifecycle transitions — promoting COLD keys to CONFIRMED_HOT when
 * sustained traffic is detected, and demoting them back to COLD after a
 * prolonged cool-down. Every state transition is gated by a Bayesian
 * confidence assessment that suppresses false-positive broadcasts when
 * the accumulated evidence is weak.
 *
 * <p>{@code confirmCount} serves as a minimum data-sufficiency floor
 * (default 1 window ≈ 50 ms). It is <em>not</em> the primary evidence gate
 * — Bayesian confidence is. The streak check exists only to ensure at least
 * one observation before consulting the Bayesian posterior.
 *
 * <h3>States</h3>
 * <pre>
 *   COLD ──hotStreak >= confirmCount──► CANDIDATE_HOT ──HIGH confidence──► CONFIRMED_HOT
 *                           + LOW/MEDIUM ────────────────► COLD (reset)
 *    ▲                                                                        │
 *    │                                  PRE_COOLING ◄──────coolStreak >= grace─┤
 *    │                                   │                                   │
   *    │                                   └──coolStreak >= coolCount──► COLD──┘
   *    │                                        (MEDIUM/LOW confidence)
 *    └──── hotStreak > 0 ───────────────────────────┘
 *                              (silent revive, no broadcast)
 * </pre>
 *
 * <p>Key design properties:
 * <ul>
 *   <li><b>Bayesian-primary gating:</b> {@code confirmCount} is a minimal
 *       floor (1 window by default). Bayesian confidence is the sole
 *       authority for promotion decisions — HOT leverages the full posterior
 *       from the first hot window, COOL broadcasts require MEDIUM or LOW confidence.</li>
 *   <li><b>CANDIDATE_HOT:</b> a holding state for keys that have crossed the
 *       hot-streak threshold but have only MEDIUM Bayesian confidence. These
 *       keys are tracked internally but never broadcast until confidence
 *       reaches HIGH. A single cold window drops them back to COLD.</li>
 *   <li><b>Asymmetric promotion/demotion:</b> promotion is fast (Bayesian
 *       decides from window 1); demotion requires many cold windows
 *       ({@code coolCount}) with an intermediate grace period, providing
 *       hysteresis against oscillation.</li>
 *   <li><b>Silent revive:</b> during PRE_COOLING, a single hot window silently
 *       returns the key to CONFIRMED_HOT without broadcasting, preventing
 *       HOT/COOL oscillation.</li>
 * </ul>
 */
@Internal
public interface ZetaStateMachine {

  /** Key-level lifecycle stages. */
  enum State {
    /** Default state — key is not hot, no tracking state exists. */
    COLD,
    /**
     * Hot-streak threshold met but Bayesian confidence was only MEDIUM.
     *
     * <p>Keys in this state are tracked internally but never broadcast.
     * They upgrade to {@link #CONFIRMED_HOT} on the next hot window with
     * HIGH confidence, or drop back to {@link #COLD} on a cold window.
     */
    CANDIDATE_HOT,
    /** Actively hot — HOT message has been broadcast to application instances. */
    CONFIRMED_HOT,
    /** Cooling down — traffic has dropped below threshold for the grace period. */
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

  /**
   * Evaluate the current sliding-window observation with Bayesian confidence
   * context and return a decision.
   *
   * <p>This is the sole evaluation entry point. It combines the binary
   * hot/cold verdict from the sliding window with the multi-dimensional
   * evidence in {@code ctx} (HeavyKeeper sketch count, window sum,
   * threshold, CV) to produce a confidence-gated decision.
   *
   * @param key             the cache key (must not be {@code null})
   * @param isHotThisWindow {@code true} if the sliding-window frequency sum
   *                        exceeds the hot threshold during this evaluation
   *                        cycle; {@code false} otherwise
   * @param ctx             aggregated observation data for Bayesian evaluation
   *                        (must not be {@code null})
   * @return a non-null {@link ZetaDecision} indicating what action the
   *         caller should take (HOT, COOL, or NONE)
   */
  ZetaDecision evaluate(String key, boolean isHotThisWindow, EvaluationContext ctx);

  /** Remove all tracked state for the given key, resetting it to COLD. */
  void reset(String key);

  /** Approximate number of keys currently tracked. */
  int getTrackedKeys();

  /** Garbage-collect stale keys. */
  void evictStale(long staleAfterMs);

  /**
   * Return a snapshot of the current state for a key.
   *
   * @param key the cache key
   * @return the state snapshot, or {@code null} if the key has no tracked state
   */
  StateSnapshot getStateSnapshot(String key);

  /**
   * Roll back the per-key state to a previous snapshot after a send failure.
   *
   * @param key           the cache key
   * @param previousState the snapshot to restore; if {@code null}, the key is reset
   */
  void rollbackToPreviousState(String key, StateSnapshot previousState);

  /**
   * Roll back the per-key state using a snapshot that carries its own key.
   *
   * @param previousState the snapshot to restore (must not be {@code null})
   */
  void rollbackToPreviousState(StateSnapshot previousState);
}
