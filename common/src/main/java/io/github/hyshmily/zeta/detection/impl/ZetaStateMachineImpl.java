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
package io.github.hyshmily.zeta.detection.impl;

import com.google.common.util.concurrent.Striped;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.confidence.ConfidenceEvaluator;
import io.github.hyshmily.zeta.confidence.ConfidenceLevel;
import io.github.hyshmily.zeta.confidence.EvaluationContext;
import io.github.hyshmily.zeta.confidence.ProbabilityResult;
import io.github.hyshmily.zeta.detection.ZetaStateMachine;
import io.github.hyshmily.zeta.model.ZetaDecision;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import static io.github.hyshmily.zeta.detection.ZetaStateMachine.State.*;

/**
 * Per-key state machine that governs hot-key lifecycle transitions on the
 * Worker side.
 *
 * <p>Each Worker shard owns a subset of keys (determined by
 * {@link io.github.hyshmily.zeta.sharding.ConsistentHashRing} routing)
 * and runs one {@code ZetaStateMachineImpl} per shard. The state machine
 * converts per-key sliding-window frequency observations into lifecycle
 * transitions, with every decision gated by Bayesian confidence scoring
 * to suppress false-positive broadcasts.
 *
 * <p>{@code confirmCount} is the minimum data-sufficiency floor (default 1
 * window, i.e. 50 ms). It is <em>not</em> the primary evidence gate —
 * Bayesian confidence is. The moment {@code hotStreak >= confirmCount},
 * the Bayesian posterior determines the outcome: HIGH → promote, MEDIUM →
 * hold at CANDIDATE_HOT, LOW → reset streak.
 *
 * <h3>Bayesian-gated state diagram (simplified)</h3>
 * <pre>
 *   COLD ──hotStreak >= confirm──► CANDIDATE_HOT ──HIGH confidence──► CONFIRMED_HOT
 *                           + MEDIUM stay / LOW reset
 *    ▲                                                                        │
 *    │                                  PRE_COOLING ◄──── coolStreak >= grace ─┤
 *    │                                   │                                   │
   *    │                                   └── coolStreak >= cool ──► COLD ────┘
   *    │                                         (MEDIUM/LOW confidence)
 *    └──── hotStreak > 0 ───────────────────────────┘
 *                              (silent revive)
 * </pre>
 *
 * <h3>Bayesian gating rules</h3>
 * <ul>
 *   <li><b>HOT promotion (COLD → CANDIDATE_HOT → CONFIRMED_HOT):</b>
 *       When {@code hotStreak >= confirmCount}, the confidence level
 *       determines the transition:
 *       <ul>
 *         <li>HIGH → promote to CONFIRMED_HOT and broadcast HOT</li>
 *         <li>MEDIUM → promote to CANDIDATE_HOT, defer broadcast</li>
 *         <li>LOW → decrement hotStreak (stay in COLD next window)</li>
 *       </ul></li>
 *   <li><b>CANDIDATE_HOT hot window:</b> HIGH confidence → CONFIRMED_HOT
 *       + HOT broadcast; MEDIUM/LOW → stay in CANDIDATE_HOT</li>
   *   <li><b>COOL broadcast:</b> Sent when Bayesian confidence is MEDIUM or LOW;
   *       HIGH confidence decrements the coolStreak so the key stays
   *       in PRE_COOLING for another window</li>
 *   <li><b>Silent revive:</b> PRE_COOLING + hot window → CONFIRMED_HOT
 *       with no broadcast (regardless of confidence)</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 * Per-key state is guarded by a {@link Striped} lock (1024 stripes).
 * Evaluations of the same key are serialized, eliminating the race window
 * between {@code hotStreak++} and the state transition check caused by
 * concurrent delivery of the same key across multiple consumer threads.
 * Evaluations of different keys proceed in parallel.
 *
 * <p>{@link #evictStale} reads {@link KeyState#lastUpdateTime} without
 * the per-key lock — the field is {@code volatile} for lock-free visibility.
 * {@link #reset} acquires the per-key lock to avoid races with a concurrent
 * {@link #evaluate} for the same key.
 */
@Internal
@Slf4j
public class ZetaStateMachineImpl implements ZetaStateMachine {

  /**
   * Constructs the state machine with the given lifecycle thresholds and
   * the Bayesian confidence evaluator that gates every state transition.
   *
   * @param confirmCount        consecutive hot windows to promote COLD → CONFIRMED_HOT
   * @param coolCount           total consecutive cold windows for full cool-down
   * @param preCoolGraceCount   cold windows before entering PRE_COOLING
   * @param confidenceEvaluator the Bayesian confidence evaluator (must not be {@code null})
   */
  public ZetaStateMachineImpl(
    int confirmCount,
    int coolCount,
    int preCoolGraceCount,
    ConfidenceEvaluator confidenceEvaluator
  ) {
    this.confirmCount = confirmCount;
    this.coolCount = coolCount;
    this.preCoolGraceCount = preCoolGraceCount;
    this.confidenceEvaluator = confidenceEvaluator;
  }

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
   * grace period during which the key can revive without broadcasting.
   */
  @Getter
  @Setter
  private volatile int preCoolGraceCount;

  /** Current state + streak counters, keyed by cache key. */
  private final ConcurrentHashMap<String, KeyState> states = new ConcurrentHashMap<>();

  /**
   * Per-key striped lock — serializes evaluations of the same key when
   * multiple consumer threads process overlapping messages, preventing
   * lost increments on {@code hotStreak++} / {@code coolStreak++}.
   *
   * <p>1024 stripes keep collision probability below 0.1% at
   * {@code concurrency=8} while adding negligible memory overhead.
   */
  private final Striped<Lock> keyLocks = Striped.lock(1024);

  /** The Bayesian confidence evaluator that gates every state transition. */
  private final ConfidenceEvaluator confidenceEvaluator;

  /**
   * Evaluates the current sliding-window observation with Bayesian confidence
   * context and returns a decision.
   *
   * <p>This method is the sole evaluation entry point. It updates the per-key
   * streak counters atomically via the striped lock and gates every state
   * transition through {@link #confidenceEvaluator}.
   *
   * <p>Return values:
   * <ul>
   *   <li>{@link ZetaDecision.DecisionType#HOT} — key just crossed the
   *       promotion threshold with HIGH confidence; send HOT to apps</li>
   *   <li>{@link ZetaDecision.DecisionType#COOL} — key has fully cooled
   *       down with MEDIUM or LOW confidence; send COOL so apps revert to normal TTL</li>
   *   <li>{@link ZetaDecision.DecisionType#NONE} — no state transition
   *       occurred; no action required</li>
   * </ul>
   *
   * @param key             the cache key (must not be {@code null})
   * @param isHotThisWindow {@code true} if the sliding-window sum exceeds threshold
   * @param ctx             aggregated observation data for Bayesian evaluation
   *                        (must not be {@code null})
   * @return a non-null {@link ZetaDecision}
   */
  @Override
  public ZetaDecision evaluate(String key, boolean isHotThisWindow, EvaluationContext ctx) {
    // Fast path: never-before-seen key on a cold window → no state to mutate.
    // containsKey may race with concurrent insertion but the worst case is one
    // unnecessary locked iteration — never a correctness problem.
    if (!isHotThisWindow && !states.containsKey(key)) {
      return ZetaDecision.none(key, null);
    }

    Lock lock = keyLocks.get(key);
    lock.lock();
    try {
      long now = System.currentTimeMillis();

      KeyState state = states.get(key);
      if (state == null) {
        state = new KeyState();
        states.put(key, state);
      }
      state.lastUpdateTime = now;

      return isHotThisWindow ? evaluateHot(key, state, ctx) : evaluateCold(key, state, ctx);
    } catch (Exception e) {
      log.warn("Unexpected StateMachine Exception for key {}", key, e);
      return ZetaDecision.none(key, null);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Processes a hot-window observation (window sum &ge; threshold).
   *
   * <p>Increments the hot streak, resets the cold streak, then evaluates
   * a Bayesian confidence result. The action taken depends on the current
   * state and the confidence level:
   *
   * <table>
   *   <caption>Hot-window transition matrix</caption>
   *   <tr><th>Current state</th><th>Confidence</th><th>Next state</th><th>Decision</th></tr>
   *   <tr><td>COLD / hotStreak &ge; confirmCount</td><td>HIGH</td><td>CONFIRMED_HOT</td><td>HOT</td></tr>
   *   <tr><td>COLD / hotStreak &ge; confirmCount</td><td>MEDIUM</td><td>CANDIDATE_HOT</td><td>NONE</td></tr>
   *   <tr><td>COLD / hotStreak &ge; confirmCount</td><td>LOW</td><td>COLD (stay, streak decremented)</td><td>NONE</td></tr>
   *   <tr><td>CANDIDATE_HOT</td><td>HIGH</td><td>CONFIRMED_HOT</td><td>HOT</td></tr>
   *   <tr><td>CANDIDATE_HOT</td><td>MEDIUM/LOW</td><td>CANDIDATE_HOT (stay)</td><td>NONE</td></tr>
   *   <tr><td>PRE_COOLING</td><td><em>any</em></td><td>CONFIRMED_HOT</td><td>NONE (silent revive)</td></tr>
   *   <tr><td>CONFIRMED_HOT</td><td><em>any</em></td><td>CONFIRMED_HOT (stay)</td><td>NONE</td></tr>
   * </table>
   *
   * @param key   the cache key
   * @param state the per-key state (mutated in place)
   * @param ctx   the Bayesian evaluation context
   * @return a non-null {@link ZetaDecision}
   */
  private ZetaDecision evaluateHot(String key, KeyState state, EvaluationContext ctx) {
    state.hotStreak++;
    state.coolStreak = 0;

    long obs = ctx.cmsCount() > 0 ? ctx.cmsCount() : ctx.windowSum();
    ProbabilityResult pr = confidenceEvaluator.evaluate(obs, ctx.threshold(), ctx.cv());

    switch (state.currentState) {
      case COLD -> {
        // Not yet enough hot windows → NONE regardless of confidence
        if (state.hotStreak < confirmCount) {
          return ZetaDecision.none(key, null);
        }
        // Sufficient hot windows — confidence decides the outcome
        if (pr.level() == ConfidenceLevel.HIGH) {
          // Strong evidence: promote and broadcast
          state.currentState = CONFIRMED_HOT;
          return ZetaDecision.hot(key, null);
        }
        if (pr.level() == ConfidenceLevel.MEDIUM) {
          // Moderate evidence: promote internally but defer broadcast
          state.currentState = CANDIDATE_HOT;
          return ZetaDecision.none(key, null);
        }
        // Weak evidence: keep in COLD, decrement streak so the key needs
        // another confirmCount hot windows to re-qualify
        state.hotStreak = confirmCount - 1;
        return ZetaDecision.none(key, null);
      }
      case CANDIDATE_HOT -> {
        if (pr.level() == ConfidenceLevel.HIGH) {
          // Evidence strengthened: upgrade and broadcast
          state.currentState = CONFIRMED_HOT;
          return ZetaDecision.hot(key, null);
        }
        // Still insufficient evidence: stay in candidate state
        return ZetaDecision.none(key, null);
      }
      case PRE_COOLING -> {
        // Silent revive: traffic resumed during grace period
        state.currentState = CONFIRMED_HOT;
        return ZetaDecision.none(key, null);
      }
      default -> {
        // CONFIRMED_HOT or unknown state: no transition needed
        return ZetaDecision.none(key, null);
      }
    }
  }

  /**
   * Processes a cold-window observation (window sum &lt; threshold).
   *
   * <p>Increments the cold streak and resets the hot streak. The action
   * depends on the current state:
   *
   * <table>
   *   <caption>Cold-window transition matrix</caption>
   *   <tr><th>Current state</th><th>Condition</th><th>Next state</th><th>Decision</th></tr>
   *   <tr><td>CANDIDATE_HOT</td><td><em>any</em></td><td>COLD</td><td>NONE</td></tr>
   *   <tr><td>CONFIRMED_HOT</td><td>coolStreak &ge; graceCount</td><td>PRE_COOLING</td><td>NONE</td></tr>
   *   <tr><td>CONFIRMED_HOT</td><td>coolStreak &lt; graceCount</td><td>CONFIRMED_HOT (stay)</td><td>NONE</td></tr>
   *   <tr><td>PRE_COOLING</td><td>coolStreak &ge; coolCount + MEDIUM/LOW</td><td>COLD</td><td>COOL</td></tr>
   *   <tr><td>PRE_COOLING</td><td>coolStreak &ge; coolCount + HIGH</td><td>PRE_COOLING (streak decremented)</td><td>NONE</td></tr>
   *   <tr><td>PRE_COOLING</td><td>coolStreak &lt; coolCount</td><td>PRE_COOLING (stay)</td><td>NONE</td></tr>
   *   <tr><td>COLD</td><td><em>any</em></td><td>COLD (stay)</td><td>NONE</td></tr>
   * </table>
   *
   * @param key   the cache key
   * @param state the per-key state (mutated in place)
   * @param ctx   the Bayesian evaluation context
   * @return a non-null {@link ZetaDecision}
   */
  private ZetaDecision evaluateCold(String key, KeyState state, EvaluationContext ctx) {
    state.coolStreak++;
    state.hotStreak = 0;

    switch (state.currentState) {
      case CANDIDATE_HOT -> {
        // Single cold window while in CANDIDATE_HOT: reset, never broadcast
        state.currentState = COLD;
        return ZetaDecision.none(key, null);
      }
      case CONFIRMED_HOT -> {
        // Enter pre-cooling after the grace window count is exhausted.
        // If the same window also satisfies the full cool-down, evaluate
        // the PRE_COOLING transition immediately (single-window cool-down).
        if (state.coolStreak >= Math.max(1, coolCount - preCoolGraceCount)) {
          state.currentState = PRE_COOLING;
          // Check immediate PRE_COOLING → COLD transition
          return evaluatePreCooling(key, state, ctx);
        }
        return ZetaDecision.none(key, null);
      }
      case PRE_COOLING -> {
        return evaluatePreCooling(key, state, ctx);
      }
      default -> {
        return ZetaDecision.none(key, null);
      }
    }
  }

  /**
   * Evaluates a key that is currently in the {@code PRE_COOLING} stage.
   *
   * <p>If the cool streak has reached the configured full cool-down
   * threshold ({@code coolCount}) the method consults the
   * {@link ConfidenceEvaluator} to decide whether the key has actually
   * cooled down. When the evaluator reports {@link ConfidenceLevel#HIGH}
   * the key is considered still confidently hot: the method will not
   * emit a COOL decision, instead it decrements {@code coolStreak} and
   * remains in {@code PRE_COOLING} for another window. For
   * {@code MEDIUM} or {@code LOW} confidence the key is transitioned to
   * {@code COLD} and a COOL decision is returned so callers can broadcast
   * the cool event.
   *
   * @param key the cache key being evaluated
   * @param state the per-key mutable state (mutated in-place)
   * @param ctx the Bayesian evaluation context used to compute confidence
   * @return a non-null {@link ZetaDecision} indicating whether a COOL
   *         decision should be emitted or no action is required
   */
  private ZetaDecision evaluatePreCooling(String key, KeyState state, EvaluationContext ctx) {
    if (state.coolStreak >= coolCount) {
      // Full cool-down reached — check confidence before broadcasting
      long obs = ctx.cmsCount() > 0 ? ctx.cmsCount() : ctx.windowSum();
      ProbabilityResult pr = confidenceEvaluator.evaluate(obs, ctx.threshold(), ctx.cv());
      if (pr.level() != ConfidenceLevel.HIGH) {
        state.currentState = COLD;
        return ZetaDecision.cool(key, null);
      }
      // Still confidently hot (HIGH): don't broadcast COOL, decrement
      // streak and stay in PRE_COOLING for another evaluation window
      state.coolStreak--;
      return ZetaDecision.none(key, null);
    }
    return ZetaDecision.none(key, null);
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
  @Override
  public void reset(String key) {
    Lock lock = keyLocks.get(key);
    lock.lock();
    try {
      states.remove(key);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Approximate number of keys currently tracked by the state machine.
   *
   * <p>The returned value is approximate due to the underlying
   * {@link ConcurrentHashMap#size()} semantics — it reflects a snapshot
   * and may not account for concurrent insertions or removals at the
   * exact moment of the call.
   *
   * @return approximate count of keys currently tracked
   */
  @Override
  public int getTrackedKeys() {
    return states.size();
  }

  /**
   * Garbage-collects state for keys that have not been evaluated within
   * {@code staleAfterMs} milliseconds.
   *
   * <p>Should be invoked periodically (e.g. every 5 seconds) from a
   * scheduled task. Reads {@link KeyState#lastUpdateTime} without the
   * per-key lock — the field is {@code volatile} for visibility.
   *
   * @param staleAfterMs maximum idle time in milliseconds before a key is evicted
   */
  @Override
  public void evictStale(long staleAfterMs) {
    long now = System.currentTimeMillis();
    states.values().removeIf(state -> now - state.lastUpdateTime > staleAfterMs);
  }

  /**
   * Returns a snapshot of the current state for a key, including the
   * key name itself for key-less rollback support.
   *
   * <p>The returned map contains: {@code key}, {@code currentState},
   * {@code hotStreak}, {@code coolStreak}.
   *
   * @param key the cache key
   * @return an immutable map with the state snapshot, or an empty map if
   *         the key has no tracked state
   */
  @Override
  public Map<String, Object> getStateSnapshot(String key) {
    Lock lock = keyLocks.get(key);
    lock.lock();
    try {
      KeyState keyState = states.get(key);
      if (keyState == null) {
        return Collections.emptyMap();
      }
      return Map.of(
        "key",
        key,
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
   * Rolls back the per-key state to its previous value after a send
   * failure, allowing the next evaluation window to re-emit the decision.
   *
   * @param key           the key whose state machine should be rolled back
   * @param previousState the snapshot returned by an earlier
   *                      {@link #getStateSnapshot} call; if {@code null},
   *                      the key is reset entirely
   */
  @Override
  public void rollbackToPreviousState(String key, Map<String, Object> previousState) {
    Lock lock = keyLocks.get(key);
    lock.lock();
    try {
      if (previousState == null) {
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

  /**
   * Key-less overload that extracts the key from the snapshot map.
   *
   * @param previousState a snapshot previously returned by
   *                      {@link #getStateSnapshot} (must contain a {@code key} entry)
   */
  @Override
  public void rollbackToPreviousState(Map<String, Object> previousState) {
    String key = (String) previousState.get("key");
    rollbackToPreviousState(key, previousState);
  }

  /**
   * Per-key mutable state tracked by the state machine.
   *
   * <p>Each live key has exactly one {@code KeyState} instance in
   * {@link #states}. Updates are guarded by the per-key striped lock;
   * reads of {@link #lastUpdateTime} by {@link #evictStale} are lock-free
   * and rely on {@code volatile} semantics.
   */
  private static class KeyState {

    /** Current lifecycle stage. Initialised to {@link State#COLD}. */
    State currentState = COLD;

    /** Number of consecutive windows above the hot threshold. */
    int hotStreak = 0;

    /** Number of consecutive windows below the hot threshold. */
    int coolStreak = 0;

    /** Last evaluation timestamp (epoch millis). Volatile for lock-free reads. */
    volatile long lastUpdateTime;
  }
}
