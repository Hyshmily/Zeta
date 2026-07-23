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
package io.github.hyshmily.zeta.worker.detection.impl;

import static io.github.hyshmily.zeta.detection.ZetaBayesianSM.State.*;

import com.google.common.util.concurrent.Striped;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.model.EvaluationContext;
import io.github.hyshmily.zeta.model.StateSnapshot;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceEvaluator;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceLevel;
import io.github.hyshmily.zeta.worker.confidence.ProbabilityResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Per-key state machine that governs hot-key lifecycle transitions on the
 * Worker side.
 *
 * <p>Each Worker shard owns a subset of keys (determined by
 * {@link io.github.hyshmily.zeta.sharding.ConsistentHashRing} routing)
 * and runs one {@code ZetaBayesianSM} per shard. The state machine
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
 * <h3>State diagram with fast-lane</h3>
 * <pre>
 *       fastlane: windowSum >= ruleThreshold ───────┐
 *                                                   |
 *                                                   ▼
 *   COLD ──hotStreak >= confirm──► CANDIDATE_HOT ──HIGH confidence──► CONFIRMED_HOT
 *                           + MEDIUM stay / LOW reset                   │
 *    ▲                                                                  │
 *    │                                  PRE_COOLING ◄── coolStreak >= grace ─┤
 *    │                                   │                                   │
 *    │                                   ├── coolStreak >= cool ──► COLD ────┘
 *    │                                   │        (MEDIUM/LOW confidence)
 *    │                                   │
 *    │                                   └── evictStale (stale) ──► broadcast COOL ──► (removed)
 *    │                                              |                     (CONFIRMED_HOT or PRE_COOLING,
 *    │                                              |                      staleAfterMs = 2 × coolDurationMs)
 *    └──── hotStreak > 0 ───────────────────────────┘
 *                              (silent revive)
 * </pre>
 *
 * <p><b>Fast-lane bypass:</b> When the window sum meets a configured fast-lane
 * rule threshold, {@link io.github.hyshmily.zeta.worker.detection.Evaluator}
 * sets {@code isFastlane=true} and the evaluation short-circuits via
 * {@link #fastlane} — the key is promoted to {@code CONFIRMED_HOT}
 * unconditionally, skipping all Bayesian confidence gating. Below the rule
 * threshold {@code isFastlane=false} and the key falls through to the normal
 * Bayesian path.
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
 *   <li><b>Fast-lane revive:</b> PRE_COOLING + fastlane → CONFIRMED_HOT
 *       with broadcast (unlike silent revive, fastlane always broadcasts)</li>
 *   <li><b>Periodic stale eviction:</b> {@link #evictStale} runs every
 *       {@code evict-interval-ms} (default 30s) and scans for keys whose
 *       {@code lastUpdateTime} exceeds {@code 2 × coolDurationMs}. Any
 *       key in CONFIRMED_HOT or PRE_COOLING state at eviction triggers
 *       the {@code onCoolEvict} callback to broadcast COOL to all app
 *       instances, then removes the key from the state map. This is the
 *       safety net that cleans up keys left in HOT state after the
 *       worker has stopped receiving reports (e.g. the app instance died
 *       or the network partition healed).</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 * Per-key state is guarded by a {@link Striped} lock (4096 stripes).
 * Evaluations of the same key are serialized, eliminating the race window
 * between {@code hotStreak++} and the state transition check caused by
 * concurrent delivery of the same key across multiple consumer threads.
 * Evaluations of different keys proceed in parallel.
 *
 * <h4>Cross-domain lock ordering</h4>
 *
 * This class issues per-key locks ({@link #keyLocks}) that sit at the
 * <b>top</b> of the global lock hierarchy (see ADR-0017):
 * <ol>
 *   <li><b>State machine per-key ({@code keyLocks})</b> — held for the
 *       duration of {@link #evaluate}.</li>
 *   <li>HeavyKeeper sketch stripes — never held inside this class.</li>
 *   <li>HeavyKeeper admission — never held inside this class.</li>
 * </ol>
 *
 * <p>{@link #confidenceEvaluator} performs only reads of pre-computed
 * HeavyKeeper sketch data (carried in {@link EvaluationContext#cmsCount})
 * and never acquires any HeavyKeeper lock. This invariant is critical:
 * acquiring a HeavyKeeper lock while holding a {@code keyLocks} would
 * invert the hierarchy and create a deadlock path with
 * {@code HeavyKeeper.fading()}.
 *
 * <p>{@link #evictStale} uses a two-phase approach: a lock-free scan
 * collects candidates (relying on {@code volatile} semantics of {@link
 * KeyState#lastUpdateTime}), then each candidate is re-checked under the
 * per-key lock before removal. {@link #reset} acquires the per-key lock
 * to avoid races with a concurrent {@link #evaluate} for the same key.
 *
 * <p>{@link KeyState} uses Lombok {@code @Builder(toBuilder = true)} so the
 * fast-lane path can atomically replace state via the builder while the
 * normal Bayesian path mutates fields in-place on the existing object.
 * {@code @Builder.Default} on {@code lastUpdateTime} evaluates to
 * {@link System#currentTimeMillis()} at build time, ensuring freshly
 * created states have a realistic timestamp.
 */
@Internal
@Slf4j
public class ZetaBayesianSM implements io.github.hyshmily.zeta.detection.ZetaBayesianSM {

  /**
   * Constructs the state machine with the given lifecycle thresholds and
   * the Bayesian confidence evaluator that gates every state transition.
   *
   * @param confirmCount        consecutive hot windows to promote COLD → CONFIRMED_HOT
   * @param coolCount           total consecutive cold windows for full cool-down
   * @param preCoolGraceCount   cold windows before entering PRE_COOLING
   * @param confidenceEvaluator the Bayesian confidence evaluator (must not be {@code null})
   */
  public ZetaBayesianSM(
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
   * <p>4096 stripes keep collision probability below 0.006% at
   * {@code concurrency=8} while adding negligible memory overhead
   * (~164 KB).
   */
  private final Striped<Lock> keyLocks = Striped.lock(4096);

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
   * @param isFastlane      {@code true} when a fast-lane rule matched and the
   *                        window sum met the rule threshold — the key is
   *                        promoted unconditionally, skipping all Bayesian
   *                        confidence gating; {@code false} otherwise
   * @param ctx             aggregated observation data for Bayesian evaluation
   *                        (must not be {@code null})
   * @return a non-null {@link ZetaDecision}
   */

  @Override
  public ZetaDecision evaluate(String key, boolean isHotThisWindow, boolean isFastlane, EvaluationContext ctx) {
    Lock lock = keyLocks.get(key);
    lock.lock();

    StateSnapshot snapShot = null;
    try {
      KeyState state = states.get(key);

      if (isFastlane) {
        // Fast-lane path: when the window sum exceeds a configured rule threshold,
        // promote the key to CONFIRMED_HOT immediately — no Bayesian gating.
        // Creates state if absent; promotes from any existing state; always updates
        // lastUpdateTime to prevent stale eviction.
        return fastlane(state, key);
      }
      if (state == null) {
        // Truly never-before-seen key + cold window → nothing to evaluate.
        // Note: we deliberately skip the lock-free fast path here (no
        // `containsKey` check before the lock) because another thread
        // could insert the key between the check and the lock, causing
        // us to miss the evaluation (TOCTOU).  Entering the lock on
        // every evaluation eliminates this race.
        if (!isHotThisWindow) {
          return ZetaDecision.NONE;
        }
        state = KeyState.builder().build();
        states.put(key, state);
      } else {
        snapShot = new StateSnapshot(key, state.currentState.name(), state.hotStreak, state.coolStreak);
      }
      state.lastUpdateTime = System.currentTimeMillis();

      // Re-check isHot inside the lock to close the cross-component gap:
      // isHotThisWindow was determined before the lock was acquired, and
      // a concurrent reportToWorker for the same key may have pushed the window
      // over the threshold in the intervening microseconds.
      // See isHotRecheckInsideLock_shouldRouteToHotWhenCallerSaysCold.
      boolean hot = isHotThisWindow || (ctx.windowSum() >= ctx.threshold());
      return hot ? evaluateHot(key, state, ctx, snapShot) : evaluateCold(key, state, ctx, snapShot);
    } catch (Exception e) {
      log.warn("Unexpected StateMachine Exception for key {}", key, e);
      // Rollback to pre-mutation snapshot to prevent half-modified KeyState.
      // hotStreak/coolStreak may have been incremented before the failure,
      // leaving the key in an inconsistent state for subsequent evaluations.
      if (snapShot != null) {
        rollbackToPreviousState(key, snapShot);
      } else {
        states.remove(key);
      }
      return ZetaDecision.NONE;
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
  @SuppressWarnings("all")
  private ZetaDecision evaluateHot(String key, KeyState state, EvaluationContext ctx, StateSnapshot snapShot) {
    state.hotStreak++;
    state.coolStreak = 0;

    // Compute the observation once and apply trend boost before the switch.
    // An upward trend (trendStrength > 1.0) inflates the observed count so that
    // steadily-rising keys reach HIGH Bayesian confidence sooner without waiting
    // for the raw window sum to cross the threshold by a wide margin.
    long obs = ctx.cmsCount() > 0 ? ctx.cmsCount() : ctx.windowSum();
    if (ctx.trendStrength() > 1.0) {
      obs = (long)(obs * ctx.trendStrength());
    }

    switch (state.currentState) {
      case COLD -> {
        if (state.hotStreak < confirmCount) {
          return ZetaDecision.none(key, snapShot);
        }

        ProbabilityResult pr = confidenceEvaluator.evaluate(obs, ctx.logThreshold(), ctx.cv());
        switch (pr.level()) {
          case HIGH -> {
            state.currentState = CONFIRMED_HOT;
            log.info("State transition: COLD -> CONFIRMED_HOT key={} obs={} pct={}", key, obs, pr.probability());
            return ZetaDecision.hot(key, snapShot);
          }
          case MEDIUM -> {
            state.currentState = CANDIDATE_HOT;
            return ZetaDecision.none(key, snapShot);
          }
          default -> {
            state.hotStreak = confirmCount - 1;
            return ZetaDecision.none(key, snapShot);
          }
        }
      }
      case CANDIDATE_HOT -> {
        ProbabilityResult pr = confidenceEvaluator.evaluate(obs, ctx.logThreshold(), ctx.cv());
        if (pr.level() == ConfidenceLevel.HIGH) {
          state.currentState = CONFIRMED_HOT;
          log.info("State transition: CANDIDATE_HOT -> CONFIRMED_HOT key={} obs={} pct={}", key, obs, pr.probability());
          return ZetaDecision.hot(key, snapShot);
        }
        return ZetaDecision.none(key, snapShot);
      }
      case PRE_COOLING -> {
        state.currentState = CONFIRMED_HOT;
        log.info("State transition: PRE_COOLING -> CONFIRMED_HOT (silent revive) key={}", key);
        return ZetaDecision.none(key, snapShot);
      }
      default -> {
        return ZetaDecision.none(key, snapShot);
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
  private ZetaDecision evaluateCold(String key, KeyState state, EvaluationContext ctx, StateSnapshot snapShot) {
    state.coolStreak++;
    state.hotStreak = 0;

    switch (state.currentState) {
      case CANDIDATE_HOT -> {
        state.currentState = COLD;
        return ZetaDecision.none(key, snapShot);
      }
      case CONFIRMED_HOT -> {
        // Enter pre-cooling after the grace window count is exhausted.
        // If the same window also satisfies the full cool-down, evaluate
        // the PRE_COOLING transition immediately (single-window cool-down).
        // A downward trend (trendStrength < 1.0) reduces the grace window
        // proportionally so that decaying keys shed their HOT state faster.

        int graceNeeded = Math.max(1, coolCount - preCoolGraceCount);
        double ts = ctx.trendStrength();
        if (ts > 0 && ts < 1.0) {
          graceNeeded = (int)(graceNeeded * Math.max(ts, 0.4));
        }
        if (state.coolStreak >= graceNeeded) {
          state.currentState = PRE_COOLING;
          // Check immediate PRE_COOLING → COLD transition

          return evaluatePreCooling(key, state, ctx, snapShot);
        }
        return ZetaDecision.none(key, snapShot);
      }
      case PRE_COOLING -> {
        return evaluatePreCooling(key, state, ctx, snapShot);
      }
      default -> {
        return ZetaDecision.none(key, snapShot);
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
  private ZetaDecision evaluatePreCooling(String key, KeyState state, EvaluationContext ctx, StateSnapshot snapShot) {
    if (state.coolStreak >= coolCount) {
      long obs = ctx.cmsCount() > 0 ? ctx.cmsCount() : ctx.windowSum();
      // A downward trend reduces the effective observation so that
      // decaying keys reach NON-HIGH confidence faster and emit COOL earlier.
      if (ctx.trendStrength() > 0 && ctx.trendStrength() < 1.0) {
        obs = Math.max(1, (long)(obs * ctx.trendStrength()));
      }
      ProbabilityResult pr = confidenceEvaluator.evaluate(obs, ctx.logThreshold(), ctx.cv());
      if (pr.level() != ConfidenceLevel.HIGH) {
        state.currentState = COLD;
        log.info("State transition: PRE_COOLING -> COLD key={} obs={} pct={}", key, obs, pr.probability());
        return ZetaDecision.cool(key, snapShot);
      }
      state.coolStreak--;
      return ZetaDecision.none(key, snapShot);
    }
    return ZetaDecision.none(key, snapShot);
  }

  /**
   * Fast-lane promotion: unconditionally set the key to CONFIRMED_HOT.
   *
   * <p>Called when a key matches a fast-lane rule and its sliding-window sum
   * equals or exceeds the rule threshold. Bypasses all Bayesian confidence
   * gating — the key is promoted immediately and the decision is returned
   * for broadcast.
   *
   * <p>Three cases:
   * <ul>
   *   <li><b>State is null</b> — create a new KeyState, mark CONFIRMED_HOT,
   *       set hotStreak to confirmCount, store in the map.</li>
   *   <li><b>State exists, not CONFIRMED_HOT</b> — use toBuilder to promote
   *       to CONFIRMED_HOT, reset coolStreak, update lastUpdateTime, and
   *       write back to the map.</li>
   *   <li><b>State exists, already CONFIRMED_HOT</b> — simply refresh
   *       lastUpdateTime to keep stale eviction at bay.</li>
   * </ul>
   *
   * @param state the current KeyState for this key (may be {@code null})
   * @param key   the cache key
   * @return a HOT decision with a pre-mutation snapshot for rollback
   */
  private ZetaDecision fastlane(KeyState state, String key) {
    if (state == null) {
      state = KeyState.builder()
        .currentState(CONFIRMED_HOT)
        .hotStreak(confirmCount)
        .lastUpdateTime(System.currentTimeMillis())
        .build();
      states.put(key, state);

      return ZetaDecision.hot(
        key,
        new StateSnapshot(key, state.currentState.name(), state.hotStreak, state.coolStreak)
      );
    }

    if (state.currentState != CONFIRMED_HOT) {
      state = state
        .toBuilder()
        .currentState(CONFIRMED_HOT)
        .hotStreak(Math.max(state.hotStreak, confirmCount))
        .coolStreak(0)
        .lastUpdateTime(System.currentTimeMillis())
        .build();
      states.put(key, state);
    } else {
      state.lastUpdateTime = System.currentTimeMillis();
    }
    StateSnapshot snapShot = new StateSnapshot(key, state.currentState.name(), state.hotStreak, state.coolStreak);
    return ZetaDecision.hot(key, snapShot);
  }

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
   * scheduled task. Uses a two-phase approach to avoid TOCTOU races:
   * phase 1 collects candidate keys via a lock-free scan (relying on
   * {@code volatile} semantics of {@link KeyState#lastUpdateTime});
   * phase 2 acquires the per-key lock via {@code tryLock()} and
   * re-verifies staleness before removing. If the lock is contended
   * the key is preserved until the next cycle.
   *
   * @param staleAfterMs maximum idle time in milliseconds before a key is evicted
   */
  @Override
  @SuppressWarnings("all")
  public void evictStale(long staleAfterMs, Consumer<String> onCoolEvict) {
    long now = System.currentTimeMillis();

    // Phase 1: lock-free scan to collect candidates that appear stale.
    List<String> candidates = new ArrayList<>();
    states.forEach((key, state) -> {
      if (now - state.lastUpdateTime > staleAfterMs) {
        candidates.add(key);
      }
    });

    // Phase 2: for each candidate, acquire per-key lock and re-check before
    // removing.  tryLock() avoids blocking on actively-evaluated keys — if
    // the lock is contended, the key is being accessed right now and should
    // be kept alive until the next eviction cycle.
    if (!candidates.isEmpty()) {
      for (String key : candidates) {
        Lock lock = keyLocks.get(key);
        if (lock.tryLock()) {
          try {
            KeyState state = states.get(key);
            if (state != null && now - state.lastUpdateTime > staleAfterMs) {
              if (state.currentState == CONFIRMED_HOT || state.currentState == PRE_COOLING) {
                // handle the key can not be evicted silently,
                // we need to broadcast COOL to the app if the key is still HOT or PRE_COOLING
                onCoolEvict.accept(key);
                log.info("Stale HOT key evicted, COOL broadcast triggered: key={}", key);
              }
              states.remove(key);
            }
          } finally {
            lock.unlock();
          }
        }
      }
    }
  }

  /**
   * Returns a snapshot of the current state for a key.
   *
   * @param key the cache key
   * @return the state snapshot, or {@code null} if the key has no tracked state
   */
  @Override
  public StateSnapshot getStateSnapshot(String key) {
    Lock lock = keyLocks.get(key);
    lock.lock();
    try {
      KeyState keyState = states.get(key);
      if (keyState == null) {
        return null;
      }
      return new StateSnapshot(key, keyState.currentState.name(), keyState.hotStreak, keyState.coolStreak);
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
  public void rollbackToPreviousState(String key, StateSnapshot previousState) {
    log.warn("Rolling back state for key: {}", key);
    Lock lock = keyLocks.get(key);
    lock.lock();
    try {
      if (previousState == null) {
        reset(key);
        return;
      }

      KeyState keyState = states.computeIfAbsent(key, k -> KeyState.builder().build());
      keyState.currentState = State.valueOf(previousState.currentState());
      keyState.hotStreak = previousState.hotStreak();
      keyState.coolStreak = previousState.coolStreak();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Key-less overload that uses the snapshot's own key field.
   *
   * @param previousState the snapshot to restore (must not be {@code null})
   */
  @Override
  public void rollbackToPreviousState(StateSnapshot previousState) {
    rollbackToPreviousState(previousState.key(), previousState);
  }

  /**
   * Per-key mutable state tracked by the state machine.
   *
   * <p>Each live key has exactly one {@code KeyState} instance in
   * {@link #states}. Updates are guarded by the per-key striped lock;
   * reads of {@link #lastUpdateTime} by {@link #evictStale} are lock-free
   * and rely on {@code volatile} semantics.
   */
  @Builder(toBuilder = true)
  private static class KeyState {

    /** Current lifecycle stage. Initialised to {@link State#COLD}. */
    @Builder.Default
    State currentState = COLD;

    /** Number of consecutive windows above the hot threshold. */
    @Builder.Default
    int hotStreak = 0;

    /** Number of consecutive windows below the hot threshold. */
    @Builder.Default
    int coolStreak = 0;

    /** Last evaluation timestamp (epoch millis). Volatile for lock-free reads. */
    @Builder.Default
    volatile long lastUpdateTime = System.currentTimeMillis();
  }
}
