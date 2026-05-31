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
package io.github.hyshmily.hotkey.worker;

import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;

/**
 * Per-key state machine that governs hot-key lifecycle transitions.
 *
 * <h3>State diagram</h3>
 * <pre>
 *   COLD
 *    │  hotStreak >= confirmCount  (consecutive hot windows)
 *    ▼
 *   CONFIRMED_HOT  ─────────────────────────────┐
 *    │                                           │
 *    │  coolStreak >= (coolCount - grace)        │ hotStreak > 0 (revive during pre-cool)
 *    ▼                                           │
 *   PRE_COOLING ─────────────────────────────────┘
 *    │
 *    │  coolStreak >= coolCount  (fully cooled)
 *    ▼
 *   COLD
 * </pre>
 *
 * <h3>Design rationale</h3>
 * <ul>
 *   <li><b>Fast detection:</b> only {@code confirmCount} consecutive hot windows are
 *       needed to promote a key to {@code CONFIRMED_HOT}.</li>
 *   <li><b>Slow cool-down:</b> cooling requires many more consecutive cold windows,
 *       with an intermediate {@code PRE_COOLING} grace period.  If traffic resumes
 *       during this period the key silently returns to {@code CONFIRMED_HOT} without
 *       broadcasting (no COOL → HOT oscillation).</li>
 *   <li><b>Asymmetric thresholds:</b> protect aggressively, recover cautiously.
 *       This prevents flapping when traffic is bursty or when the cluster briefly
 *       stops reporting a key.</li>
 * </ul>
 *
 * <p>Instances are thread-safe and designed for single-shard workers; each key is
 * owned by exactly one worker thanks to consistent-hash routing on the client side.</p>
 */
@RequiredArgsConstructor
public class HotKeyStateMachine {

  /** Key-level state: current lifecycle stage plus streak counters. */
  public enum State {
    /** Not currently tracked as hot. */
    COLD,
    /** Confirmed hot — HOT broadcast has been sent. */
    CONFIRMED_HOT,
    /**
     * Transitional stage between HOT and COLD.
     * No broadcast is sent yet; if the key becomes hot again it returns
     * to {@link #CONFIRMED_HOT} silently, avoiding unnecessary COOL/HOT cycles.
     */
    PRE_COOLING,
  }

  /** Number of consecutive hot windows required to promote COLD → CONFIRMED_HOT. */
  private final int confirmCount;

  /**
   * Total number of consecutive cold windows required for a full cool-down
   * (CONFIRMED_HOT → PRE_COOLING → COLD).  Must be greater than
   * {@code preCoolGraceCount}.
   */
  private final int coolCount;

  /**
   * The number of cold windows that mark the entry into PRE_COOLING.
   * The remaining {@code coolCount - preCoolGraceCount} windows are the
   * grace period during which the key can revive without a broadcast.
   */
  private final int preCoolGraceCount;

  /** Current state + streak counters, keyed by cache key. */
  private final ConcurrentHashMap<String, KeyState> states = new ConcurrentHashMap<>();

  /**
   * Last evaluation timestamp for each key.  Used by {@link #evictStale(long)}
   * to garbage-collect keys that have not been reported for an extended period.
   */
  private final ConcurrentHashMap<String, Long> stateTimestamps = new ConcurrentHashMap<>();

  /**
   * Evaluates the current window result for the given key and returns the
   * appropriate decision (HOT, COOL, or NONE).
   *
   * @param key             the cache key
   * @param isHotThisWindow {@code true} if the sliding-window sum exceeds the
   *                        threshold during this evaluation cycle
   * @return a decision that tells the caller whether to broadcast
   */
  public HotKeyDecision evaluate(String key, boolean isHotThisWindow) {
    // Touch timestamp for eviction tracking
    stateTimestamps.put(key, System.currentTimeMillis());

    KeyState state = states.computeIfAbsent(key, _ -> new KeyState());

    if (isHotThisWindow) {
      state.hotStreak++;
      state.coolStreak = 0; // reset cold streak

      // COLD → CONFIRMED_HOT transition
      if (state.currentState == State.COLD && state.hotStreak >= confirmCount) {
        state.currentState = State.CONFIRMED_HOT;
        return HotKeyDecision.hot(key); // broadcast HOT
      }

      // PRE_COOLING → CONFIRMED_HOT (silent revive)
      if (state.currentState == State.PRE_COOLING) {
        state.currentState = State.CONFIRMED_HOT;
        return HotKeyDecision.none(key); // no broadcast — avoid oscillation
      }
    } else {
      state.coolStreak++;
      state.hotStreak = 0; // reset hot streak

      // CONFIRMED_HOT → PRE_COOLING transition (first cool phase)
      if (state.currentState == State.CONFIRMED_HOT && state.coolStreak >= coolCount - preCoolGraceCount) {
        state.currentState = State.PRE_COOLING;
        // no broadcast yet — wait for full cool-down
      }

      // PRE_COOLING → COLD transition (fully cooled)
      if (state.currentState == State.PRE_COOLING && state.coolStreak >= coolCount) {
        state.currentState = State.COLD;
        return HotKeyDecision.cool(key); // broadcast COOL
      }
    }

    return HotKeyDecision.none(key);
  }

  /**
   * Immediately removes all state for the given key, effectively resetting
   * it to {@link State#COLD}.  Called when the Worker fails to obtain a
   * version from Redis and must abort the current HOT decision.
   */
  public void reset(String key) {
    states.remove(key);
    // stateTimestamps is cleaned up lazily by evictStale();
    // keeping the timestamp for a while avoids immediate re-creation churn.
  }

  /**
   * Garbage-collects state for keys that have not been evaluated within
   * {@code staleAfterMs} milliseconds.  Should be invoked periodically
   * (e.g. every 5 seconds) from a scheduled task.
   */
  public void evictStale(long staleAfterMs) {
    long now = System.currentTimeMillis();
    // Remove state entries whose last touch is older than the threshold
    states
      .keySet()
      .removeIf(key -> {
        Long last = stateTimestamps.get(key);
        return last != null && now - last > staleAfterMs;
      });
    // Purge orphaned timestamps
    stateTimestamps.keySet().removeIf(k -> !states.containsKey(k));
  }

  /**
   * Mutable per-key state.  Instances are confined to the {@code states}
   * map and updated under the key-level concurrency guarantees of
   * {@link ConcurrentHashMap}.
   */
  private static class KeyState {

    /** Current lifecycle stage. */
    State currentState = State.COLD;

    /** Number of consecutive windows above the hot threshold. */
    int hotStreak = 0;

    /** Number of consecutive windows below the hot threshold. */
    int coolStreak = 0;
  }
}
