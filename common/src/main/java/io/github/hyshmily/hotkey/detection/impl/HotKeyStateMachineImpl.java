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
package io.github.hyshmily.hotkey.detection.impl;

import static io.github.hyshmily.hotkey.detection.HotKeyStateMachine.State.*;

import com.google.common.util.concurrent.Striped;
import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.model.HotKeyDecision;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Per-key state machine that governs hot-key lifecycle transitions on the
 * Worker side.
 *
 * <p>Each Worker shard owns a subset of keys (determined by
 * {@link io.github.hyshmily.hotkey.sharding.ConsistentHashRing} routing)
 * and runs one {@code HotKeyStateMachine} instance per owned shard. The
 * state machine converts per-key sliding-window frequency observations
 * into lifecycle transitions — promoting COLD keys to CONFIRMED_HOT when
 * sustained traffic is detected, and demoting them back to COLD after a
 * prolonged cool-down.
 *
 * <h3>State diagram</h3>
 * <pre>
 *   COLD
 *    │  hotStreak >= confirmCount  (consecutive hot windows)
 *    ▼
 *   CONFIRMED_HOT   ─────────────────────────────┐
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
 * <p>Thread-safe: per-key state is guarded by a {@link Striped} lock (1024
 * stripes). Evaluations of different keys proceed in parallel; evaluations
 * of the same key are serialized, eliminating the race window between
 * {@code hotStreak++} and the state transition check caused by concurrent
 * delivery of the same key across multiple consumer threads.
 *
 * <p>Designed for single-shard workers; each key is owned by exactly one worker
 * thanks to consistent-hash routing on the client side.</p>
 */
@Internal
@Slf4j
@AllArgsConstructor
public class HotKeyStateMachineImpl implements HotKeyStateMachine {

  /** Number of consecutive hot windows required to promote COLD → CONFIRMED_HOT. */
  @Getter
  @Setter
  private volatile int confirmCount;

  /**
   * Total number of consecutive cold windows required for a full cool-down
   * (CONFIRMED_HOT → PRE_COOLING → COLD).  Must be greater than
   * {@code preCoolGraceCount}.
   */
  @Getter
  @Setter
  private volatile int coolCount;

  /**
   * The number of cold windows that mark the entry into PRE_COOLING.
   * The remaining {@code coolCount - preCoolGraceCount} windows are the
   * grace period during which the key can revive without a broadcast.
   */
  @Getter
  @Setter
  private volatile int preCoolGraceCount;

  /** Current state + streak counters, keyed by cache key. */
  private final ConcurrentHashMap<String, KeyState> states = new ConcurrentHashMap<>();

  /**
   * Last evaluation timestamp for each key.  Used by {@link #evictStale(long)}
   * to garbage-collect keys that have not been reported for an extended period.
   */
  private final ConcurrentHashMap<String, Long> stateTimestamps = new ConcurrentHashMap<>();

  /**
   * Per-key striped lock — serializes evaluations of the same key when
   * multiple consumer threads process overlapping messages, preventing
   * lost increments on {@code hotStreak++} / {@code coolStreak++}.
   *
   * <p>1024 stripes keep collision probability below 0.1% at
   * {@code concurrency=8} while adding negligible memory overhead.
   */
  private final Striped<Lock> keyLocks = Striped.lazyWeakLock(1024);

  /**
   * Evaluates the current sliding-window observation for the given key and
   * returns a decision instructing the caller whether to broadcast a HOT or
   * COOL message.
   *
   * <p>This method implements the state transitions described in the class
   * Javadoc. It updates the per-key streak counters ({@code hotStreak} /
   * {@code coolStreak}) atomically (via {@link ConcurrentHashMap#computeIfAbsent})
   * and returns one of:
   * <ul>
   *   <li>{@link HotKeyDecision.DecisionType#HOT} — key just crossed the
   *       promotion threshold; broadcast HOT to application instances.</li>
   *   <li>{@link HotKeyDecision.DecisionType#COOL} — key has fully cooled
   *       down; broadcast COOL so apps revert to normal TTL.</li>
   *   <li>{@link HotKeyDecision.DecisionType#NONE} — no state transition
   *       occurred; no action required.</li>
   * </ul>
   *
   * <p>Silent revive (PRE_COOLING → CONFIRMED_HOT) returns NONE to
   * suppress unnecessary broadcasts and prevent HOT/COOL oscillation.
   *
   * @param key             the cache key (must not be {@code null})
   * @param isHotThisWindow {@code true} if the sliding-window frequency sum
   *                        exceeds the hot threshold during this evaluation
   *                        cycle; {@code false} otherwise
   * @return a non-null {@link HotKeyDecision} indicating what action the
   *         caller should take (HOT, COOL, or NONE)
   */
  public HotKeyDecision evaluate(String key, boolean isHotThisWindow) {
    // Fast path: never-before-seen key on a cold window — no state to
    // mutate (no accumulated hotStreak to reset), skip striped-lock
    // acquisition entirely.
    if (!isHotThisWindow && !states.containsKey(key)) {
      return HotKeyDecision.none(key);
    }

    Lock lock = keyLocks.get(key);
    lock.lock();
    try {
      // Touch timestamp for eviction tracking
      stateTimestamps.put(key, System.currentTimeMillis());

      KeyState state = states.computeIfAbsent(key, k -> new KeyState());

      if (isHotThisWindow) {
        state.hotStreak++;
        state.coolStreak = 0; // reset cold streak

        // COLD → CONFIRMED_HOT transition
        if (state.currentState == COLD && state.hotStreak >= confirmCount) {
          state.currentState = CONFIRMED_HOT;
          return HotKeyDecision.hot(key); // broadcast HOT
        }

        // PRE_COOLING → CONFIRMED_HOT (silent revive)
        if (state.currentState == PRE_COOLING) {
          state.currentState = CONFIRMED_HOT;
          return HotKeyDecision.none(key); // no broadcast — avoid oscillation
        }
      } else {
        state.coolStreak++;
        state.hotStreak = 0; // reset hot streak

        // CONFIRMED_HOT → PRE_COOLING transition (first cool phase)
        if (state.currentState == CONFIRMED_HOT && state.coolStreak >= Math.max(1, coolCount - preCoolGraceCount)) {
          state.currentState = PRE_COOLING;
          // no broadcast yet — wait for full cool-down
        }

        // PRE_COOLING → COLD transition (fully cooled)
        if (state.currentState == PRE_COOLING && state.coolStreak >= coolCount) {
          state.currentState = COLD;
          return HotKeyDecision.cool(key); // broadcast COOL
        }
      }

      return HotKeyDecision.none(key);
    } catch (Exception e) {
      log.warn("Unexpected StateMachine Exception for key {}", key, e);
      return HotKeyDecision.none(key);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Immediately removes all tracked state for the given key, effectively
   * resetting it to {@link State#COLD}.
   *
   * <p>Called when the Worker fails to obtain a version from Redis and must
   * abort the current HOT decision (e.g. Redis is unreachable or the key
   * was not found in Redis). After reset, the next {@link #evaluate} call
   * for this key will start from scratch with fresh streak counters.
   *
   * <p>The last-touch timestamp is intentionally not removed to prevent
   * immediate re-creation churn in {@link #evictStale} — it will be cleaned
   * up lazily on the next eviction cycle.
   *
   * @param key the cache key to reset
   */
  public void reset(String key) {
    states.remove(key);
    // stateTimestamps is cleaned up lazily by evictStale();
    // keeping the timestamp for a while avoids immediate re-creation churn.
  }

  /**
   * Approximate number of keys currently tracked by the state machine.
   * <p>The returned value is approximate due to the underlying
   * {@link java.util.concurrent.ConcurrentHashMap#size()} semantics
   * — it reflects a snapshot and may not account for concurrent
   * insertions or removals at the exact moment of the call.</p>
   *
   * @return approximate count of keys currently tracked
   */
  public int getTrackedKeys() {
    return states.size();
  }

  /**
   * Garbage-collects state for keys that have not been evaluated within
   * {@code staleAfterMs} milliseconds.  Should be invoked periodically
   * (e.g. every 5 seconds) from a scheduled task.
   *
   * @param staleAfterMs maximum idle time in milliseconds before a key is evicted
   */
  public void evictStale(long staleAfterMs) {
    long now = System.currentTimeMillis();
    // Single pass: remove stale state and its timestamp together
    states
      .keySet()
      .removeIf(key -> {
        Long last = stateTimestamps.get(key);
        if (last == null) {
          return false;
        }
        if (now - last > staleAfterMs) {
          stateTimestamps.remove(key);
          return true;
        }
        return false;
      });
    // Orphaned timestamps from reset() — only scan when needed
    if (states.size() < stateTimestamps.size()) {
      stateTimestamps.keySet().removeIf(k -> !states.containsKey(k));
    }
  }

  public Map<String, Object> getStateSnapshot(String key) {
    Lock lock = keyLocks.get(key);
    lock.lock();
    try {
      KeyState keyState = states.get(key);
      if (keyState == null) {
        return Collections.emptyMap();
      }
      return Map.of(
        "currentState",
        keyState.currentState.name(),
        "hotStreak",
        keyState.hotStreak,
        "coolStreak",
        keyState.coolStreak
      );
    } finally {
      lock.unlock();
    }
  }

  /**
   * Rolls back the per-key state to its previous value after a broadcast failure.
   * This allows the next evaluation window to re-emit the decision.
   *
   * @param key the key whose state machine should be rolled back
   */
  public void rollbackToPreviousState(String key, Map<String, Object> previousState) {
    Lock lock = keyLocks.get(key);
    lock.lock();
    try {
      if (previousState == null) {
        // No previous state; treat as reset
        reset(key);
        return;
      }

      KeyState keyState = states.computeIfAbsent(key, k -> new KeyState());
      keyState.currentState = State.valueOf((String) previousState.get("currentState"));
      keyState.hotStreak = (int) previousState.get("hotStreak");
      keyState.coolStreak = (int) previousState.get("coolStreak");
    } finally {
      lock.unlock();
    }
  }

  private static class KeyState {

    /** Current lifecycle stage. */
    volatile State currentState = COLD;

    /** Number of consecutive windows above the hot threshold. */
    volatile int hotStreak = 0;

    /** Number of consecutive windows below the hot threshold. */
    volatile int coolStreak = 0;
  }
}
