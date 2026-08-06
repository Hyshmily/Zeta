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
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.confidence.BayesianConfidenceEstimator;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceEvaluator;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceLevel;
import io.github.hyshmily.zeta.worker.confidence.ProbabilityResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
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
 * the accumulated posterior (per-key {@code posteriorMean} and
 * {@code accumulatedPrecision}, capped at κ_max = 5) determines the
 * outcome: HIGH → promote, MEDIUM → hold at CANDIDATE_HOT, LOW → retention
 * or full reset after {@value #MAX_LOW_RESETS} consecutive LOWs.
 *
 * <h3>State diagram with fast-lane</h3>
 * <pre>
 *       fastlane: windowSum >= ruleThreshold ───────────────────┐
 *                                                               │
 *                                                               ▼
 *               ┌── accumulate ────────────────────────-──┐
 *               │  posteriorMean, accumulatedPrecision    │
 *               ▼                                         │
 *   COLD ──hotStreak >= confirm──► CANDIDATE_HOT ──accumulate + HIGH ──► CONFIRMED_HOT
 *              + LOW retention (MAX_LOW_RESETS)            │
 *              + MEDIUM → CANDIDATE_HOT                    │ coolStreak >= grace
 *    ▲                                                     ▼
 *    │                                            ┌───────────────┐
 *    │                                            │  PRE_COOLING  │
 *    │                                            │  posterior    │
 *    │                                            │  reset to     │
 *    │                                            │  global prior │
 *    │                                            └───────┬───────┘
 *    │                                                    │
 *    │                              coolStreak >= cool ───┤
 *    │                              + MEDIUM/LOW ────────► COLD (broadcast COOL)
 *    │                              + HIGH ──────────────► stay (coolStreak--)
 *    │                                                    │
 *    │                              evictStale (stale) ───┤
 *    │                              (CONFIRMED_HOT or     └──► broadcast COOL ──► (removed)
 *    │                               PRE_COOLING,
 *    │                               staleAfterMs =
 *    │                               evictIntervalMs)
 *    │
 *    └──── hotStreak > 0 ──────────────────────────────────┘
 *                     (silent revive, no broadcast)
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
 *   <li><b>Accumulated posterior:</b> Each key's {@code posteriorMean} and
 *       {@code accumulatedPrecision} persist across evaluations and serve as
 *       the prior for the next Bayesian gate. Precision is capped at
 *       κ_max = 5 base-likelihood-equivalent observations. Both COLD and
 *       CANDIDATE_HOT paths use the accumulated prior; the COOL path
 *       (PRE_COOLING) resets to the global prior on regime change.</li>
 *   <li><b>HOT promotion (COLD):</b> When {@code hotStreak >= confirmCount},
 *       the confidence level determines the transition:
 *       <ul>
 *         <li>HIGH → promote to CONFIRMED_HOT and broadcast HOT</li>
 *         <li>MEDIUM → promote to CANDIDATE_HOT, defer broadcast</li>
 *         <li>LOW → retention (hotStreak = confirmCount - 1) for the first
 *             {@value #MAX_LOW_RESETS} times; then full reset (hotStreak = 0)</li>
 *       </ul></li>
 *   <li><b>CANDIDATE_HOT hot window:</b> Uses accumulated prior (same as COLD).
 *       HIGH confidence → CONFIRMED_HOT + HOT broadcast; MEDIUM/LOW → stay
 *       in CANDIDATE_HOT</li>
 *   <li><b>COOL broadcast:</b> Sent when Bayesian confidence is MEDIUM or LOW.
 *       Uses a <b>reset</b> global prior — the accumulated hot-phase posterior
 *       is cleared on entry to PRE_COOLING so that hot history does not delay
 *       regime-change detection. HIGH confidence decrements the coolStreak so
 *       the key stays in PRE_COOLING for another window.</li>
 *   <li><b>Silent revive:</b> PRE_COOLING + hot window → CONFIRMED_HOT
 *       with no broadcast (regardless of confidence)</li>
 *   <li><b>Fast-lane revive:</b> PRE_COOLING + fastlane → CONFIRMED_HOT
 *       with broadcast (unlike silent revive, fastlane always broadcasts)</li>
 *   <li><b>Periodic HOT rebroadcast (ADR-0024):</b> while a key stays in
 *       CONFIRMED_HOT, a HOT decision is re-emitted at most once per
 *       {@code rebroadcastIntervalMs} (default 10 s) — recovering HOT
 *       broadcasts lost in flight (ADR-0007 fire-and-forget) and replacing
 *       the fast-lane every-evaluation emission that caused steady-state
 *       broadcast amplification. Stamps are optimistic ({@code lastBroadcastAt}
 *       at decision time); a failed send rolls the stamp back to 0 via
 *       {@link #rollbackToPreviousState}, so the next evaluation retries
 *       immediately. The rollback is guarded by a per-key {@code mutationSeq}
 *       epoch carried in the snapshot: it applies only while no later
 *       evaluation advanced the state — a concurrent consumer's progression is
 *       never clobbered, and the periodic rebroadcast retries instead. COOL is
 *       never rebroadcast — a lost COOL fails in the lenient direction (bounded
 *       by hard TTL and stale eviction).</li>
 *   <li><b>Periodic stale eviction:</b> {@link #evictStale} runs every
 *       {@code evict-interval-ms} (default 20 min) and scans for keys whose
 *       {@code lastUpdateTime} exceeds the configured stale threshold.
 *       Stale keys are removed from the state map under the per-key lock;
 *       any removed key that was in CONFIRMED_HOT or PRE_COOLING state at
 *       eviction then triggers the {@code onCoolEvict} callback to broadcast
 *       COOL to all app instances. Callbacks run outside the per-key lock
 *       (a slow AMQP publish must not stall eviction) and are isolated from
 *       the removal: a failed callback leaves the key evicted, and the
 *       broadcast is skipped if the key was re-evaluated in the meantime.
 *       This is the safety net that cleans up keys left in HOT state after
 *       the worker has stopped receiving reports (e.g. the app instance died
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
 *       duration of {@link #evaluate} and for the removal section of
 *       {@link #evictStale}. The eviction COOL-broadcast callback runs
 *       <em>outside</em> these locks (I/O must never sit under a lock).</li>
 *   <li>HeavyKeeper sketch stripes — never held inside this class.</li>
 *   <li>HeavyKeeper admission — never held inside this class.</li>
 * </ol>
 *
 * <p>The {@code windowSumSupplier} ({@link java.util.function.LongSupplier})
 * is provided by {@link io.github.hyshmily.zeta.worker.detection.Evaluator}
 * and calls {@code SlidingWindowDetector.getWindowSum(key)} inside the
 * per-key lock. This re-read closes the TOCTOU race between the sliding-window
 * update (called outside the lock) and the Bayesian evaluation (inside the
 * lock). The state machine depends only on {@code LongSupplier}, not on
 * {@code SlidingWindowDetector} itself — zero coupling.
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
 * {@link TimeSource#monotonicMillis()} at build time, ensuring freshly
 * created states have a realistic timestamp.
 */
@Internal
@Slf4j
public class ZetaBayesianSM implements io.github.hyshmily.zeta.detection.ZetaBayesianSM {

  /**
   * Constructs the state machine with the given lifecycle thresholds and
   * the Bayesian confidence evaluator that gates every state transition.
   *
   * <p>Compatibility constructor: uses {@link #DEFAULT_REBROADCAST_INTERVAL_MS}
   * as the periodic HOT rebroadcast interval (ADR-0024).
   *
   * @param confirmCount        consecutive hot windows to promote COLD → CONFIRMED_HOT
   * @param coolCount           total consecutive cold windows for full cool-down
   * @param preCoolGraceCount   cold windows before entering PRE_COOLING
   * @param confidenceEvaluator the Bayesian confidence evaluator (must not be {@code null})
   * @param priorMean           the global prior mean (log scale); used as the initial
   *                            posterior mean for new keys
   */
  public ZetaBayesianSM(
    int confirmCount,
    int coolCount,
    int preCoolGraceCount,
    ConfidenceEvaluator confidenceEvaluator,
    double priorMean
  ) {
    this(confirmCount, coolCount, preCoolGraceCount, confidenceEvaluator, priorMean, DEFAULT_REBROADCAST_INTERVAL_MS);
  }

  /**
   * Constructs the state machine with the given lifecycle thresholds, the
   * Bayesian confidence evaluator, and the periodic HOT rebroadcast interval.
   *
   * @param confirmCount           consecutive hot windows to promote COLD → CONFIRMED_HOT
   * @param coolCount              total consecutive cold windows for full cool-down
   * @param preCoolGraceCount      cold windows before entering PRE_COOLING
   * @param confidenceEvaluator    the Bayesian confidence evaluator (must not be {@code null})
   * @param priorMean              the global prior mean (log scale); used as the initial
   *                               posterior mean for new keys
   * @param rebroadcastIntervalMs  minimum interval between periodic HOT rebroadcasts
   *                               for a key that stays in {@code CONFIRMED_HOT} (ADR-0024)
   */
  public ZetaBayesianSM(
    int confirmCount,
    int coolCount,
    int preCoolGraceCount,
    ConfidenceEvaluator confidenceEvaluator,
    double priorMean,
    long rebroadcastIntervalMs
  ) {
    this.confirmCount = confirmCount;
    this.coolCount = coolCount;
    this.preCoolGraceCount = preCoolGraceCount;
    this.confidenceEvaluator = confidenceEvaluator;
    this.priorMean = priorMean;
    this.rebroadcastIntervalMs = rebroadcastIntervalMs;
  }

  /**
   * Default interval between periodic HOT rebroadcasts for a continuously hot
   * key (10 s). Used by the 5-arg compatibility constructor. See ADR-0024.
   */
  static final long DEFAULT_REBROADCAST_INTERVAL_MS = 10_000L;

  /**
   * Minimum interval between periodic HOT rebroadcasts of the same key.
   * Bounds fast-lane steady-state emission and recovers lost HOT broadcasts.
   */
  private final long rebroadcastIntervalMs;

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

  /**
   * Maximum consecutive LOW-confidence resets before a full streak reset.
   * The first N evaluations use retention (hotStreak = confirmCount - 1) to
   * quickly re-evaluate during burst traffic. After N consecutive LOWs, a
   * full reset (hotStreak = 0) breaks the 2↔3 oscillation for borderline
   * keys that persistently fail Bayesian confidence but stay above threshold.
   *
   * <p>When {@code confirmCount == 1} the retention strategy sets hotStreak = 0,
   * which is functionally identical to a full reset — the "quick re-evaluation"
   * benefit only materialises when {@code confirmCount > 1}.
   */
  private static final int MAX_LOW_RESETS = 2;

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

  /** Global prior mean (log scale) for new key initialization. */
  private final double priorMean;

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
  public ZetaDecision evaluate(
    String key,
    boolean isHotThisWindow,
    boolean isFastlane,
    EvaluationContext ctx,
    LongSupplier windowSumSupplier
  ) {
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

        state = KeyState.builder().posteriorMean(priorMean).accumulatedPrecision(0.0).build();
        states.put(key, state);
      } else {
        // Bump the evaluation epoch BEFORE taking the snapshot: the snapshot
        // then carries this evaluation's epoch, so a rollback of its decision
        // is valid only while no later evaluation has advanced the state.
        state.mutationSeq++;
        snapShot = new StateSnapshot(
          key,
          state.currentState.name(),
          state.hotStreak,
          state.coolStreak,
          state.posteriorMean,
          state.accumulatedPrecision,
          state.lowResetCount,
          state.mutationSeq
        );
      }
      state.lastUpdateTime = TimeSource.monotonicMillis();

      // Re-read the current sliding-window sum inside the lock via the
      // LongSupplier provided by Evaluator.  This closes the TOCTOU race:
      // addCount(key, count) was called outside the lock, and another thread
      // could have updated the window between that call and this evaluation.
      boolean hot = isHotThisWindow || (windowSumSupplier.getAsLong() >= ctx.threshold());
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
   *   <tr><td>COLD / hotStreak &ge; confirmCount</td><td>LOW</td><td>COLD (stay, retention or full reset)</td><td>NONE</td></tr>
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

    // Observation = window sum (exact, hard-window).
    // Momentum-adjusted logThreshold (lowered for sustained keys,
    // raised for first-time spikes) is already baked into
    // ctx.adjustedLogThreshold() by the Evaluator.
    long obs = ctx.windowSum();
    if (ctx.trendStrength() > 1.0) {
      obs = (long) (obs * ctx.trendStrength());
    }

    switch (state.currentState) {
      case COLD -> {
        if (state.hotStreak < confirmCount) {
          return ZetaDecision.none(key, snapShot);
        }

        ProbabilityResult pr = confidenceEvaluator.evaluateWithAccumulatedPrior(
          obs,
          ctx.adjustedLogThreshold(),
          ctx.cv(),
          state.posteriorMean,
          state.accumulatedPrecision
        );
        state.posteriorMean = pr.posteriorMean();
        state.accumulatedPrecision = pr.accumulatedPrecision();

        switch (pr.level()) {
          case HIGH -> {
            state.currentState = CONFIRMED_HOT;
            state.lowResetCount = 0;
            state.lastBroadcastAt = TimeSource.monotonicMillis();
            log.info("State transition: COLD -> CONFIRMED_HOT key={} obs={} pct={}", key, obs, pr.probability());
            return ZetaDecision.hot(key, snapShot);
          }
          case MEDIUM -> {
            state.currentState = CANDIDATE_HOT;
            state.lowResetCount = 0;
            return ZetaDecision.none(key, snapShot);
          }
          default -> {
            if (state.lowResetCount < MAX_LOW_RESETS) {
              // Retention strategy: set hotStreak one below confirmCount so the
              // very next hot window re-enters Bayesian evaluation immediately.
              // This avoids wasting confirmCount windows during burst traffic.
              state.hotStreak = confirmCount - 1;
              state.lowResetCount++;
            } else {
              // Full reset after MAX_LOW_RESETS consecutive LOW evaluations.
              // Prevents the 2↔3 oscillation where a borderline key that
              // stays above threshold but consistently fails Bayesian
              // confidence can never reach CONFIRMED_HOT.
              state.hotStreak = 0;
              state.lowResetCount = 0;
            }
            return ZetaDecision.none(key, snapShot);
          }
        }
      }
      case CANDIDATE_HOT -> {
        ProbabilityResult pr = confidenceEvaluator.evaluateWithAccumulatedPrior(
          obs,
          ctx.adjustedLogThreshold(),
          ctx.cv(),
          state.posteriorMean,
          state.accumulatedPrecision
        );
        state.posteriorMean = pr.posteriorMean();
        state.accumulatedPrecision = pr.accumulatedPrecision();

        if (pr.level() == ConfidenceLevel.HIGH) {
          state.currentState = CONFIRMED_HOT;
          state.lowResetCount = 0;
          state.lastBroadcastAt = TimeSource.monotonicMillis();

          log.info("State transition: CANDIDATE_HOT -> CONFIRMED_HOT key={} obs={} pct={}", key, obs, pr.probability());
          return ZetaDecision.hot(key, snapShot);
        }
        return ZetaDecision.none(key, snapShot);
      }
      case PRE_COOLING -> {
        state.currentState = CONFIRMED_HOT;
        state.lowResetCount = 0;

        log.info("State transition: PRE_COOLING -> CONFIRMED_HOT (silent revive) key={}", key);
        return ZetaDecision.none(key, snapShot);
      }
      default -> {
        // CONFIRMED_HOT + hot window: previously always NONE, so a HOT broadcast
        // lost in flight (ADR-0007 fire-and-forget) was never recovered. Re-emit
        // at most once per rebroadcastIntervalMs (ADR-0024).
        long now = TimeSource.monotonicMillis();
        if (now - state.lastBroadcastAt >= rebroadcastIntervalMs) {
          state.lastBroadcastAt = now;
          log.debug("Periodic HOT rebroadcast: key={}", key);
          return ZetaDecision.hot(key, snapShot);
        }
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
    state.lowResetCount = 0;

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
          graceNeeded = (int) (graceNeeded * Math.max(ts, 0.4));
        }
        if (state.coolStreak >= graceNeeded) {
          state.currentState = PRE_COOLING;
          // Regime change: reset accumulated posterior for COOL detection.
          state.posteriorMean = priorMean;
          state.accumulatedPrecision = 0.0;

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
      long obs = ctx.windowSum();
      // A downward trend reduces the effective observation so that
      // decaying keys reach NON-HIGH confidence faster and emit COOL earlier.
      if (ctx.trendStrength() > 0 && ctx.trendStrength() < 1.0) {
        obs = Math.max(1, (long) (obs * ctx.trendStrength()));
      }
      // Regime change: COOL path evaluates with global prior, not accumulated history.
      // Resetting prevents a previously-hot key's accumulated posterior from
      // delaying COOL broadcast.
      state.posteriorMean = priorMean;
      state.accumulatedPrecision = 0.0;
      ProbabilityResult pr = confidenceEvaluator.evaluate(obs, ctx.adjustedLogThreshold(), ctx.cv());

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
   *       set hotStreak to confirmCount, stamp {@code lastBroadcastAt}, store
   *       in the map, and return a HOT decision.</li>
   *   <li><b>State exists, not CONFIRMED_HOT</b> — use toBuilder to promote
   *       to CONFIRMED_HOT, reset coolStreak, stamp {@code lastBroadcastAt},
   *       and write back to the map; returns a HOT decision.</li>
   *   <li><b>State exists, already CONFIRMED_HOT</b> — refresh
   *       {@code lastUpdateTime} to keep stale eviction at bay, then debounce:
   *       returns a HOT decision at most once per {@code rebroadcastIntervalMs}
   *       (ADR-0024), otherwise NONE. Previously this case returned HOT on
   *       every evaluation, causing one broadcast per report per App —
   *       steady-state amplification absorbed only by the broadcaster's
   *       100 ms debounce cache.</li>
   * </ul>
   *
   * @param state the current KeyState for this key (may be {@code null})
   * @param key   the cache key
   * @return a HOT decision with a pre-mutation snapshot for rollback, or NONE
   *         when the periodic rebroadcast is still within its interval
   */
  private ZetaDecision fastlane(KeyState state, String key) {
    StateSnapshot snapShot;
    long now = TimeSource.monotonicMillis();

    if (state == null) {
      state = KeyState.builder()
        .currentState(CONFIRMED_HOT)
        .hotStreak(confirmCount)
        .lastUpdateTime(now)
        .lastBroadcastAt(now)
        .build();

      states.put(key, state);
      state.mutationSeq++;

      snapShot = new StateSnapshot(
        key,
        state.currentState.name(),
        state.hotStreak,
        state.coolStreak,
        state.posteriorMean,
        state.accumulatedPrecision,
        state.lowResetCount,
        state.mutationSeq
      );

      log.info("Fast-lane promotion: key={} state={}", key, state);
      return ZetaDecision.hot(key, snapShot);
    }

    if (state.currentState != CONFIRMED_HOT) {
      state = state
        .toBuilder()
        .currentState(CONFIRMED_HOT)
        .hotStreak(Math.max(state.hotStreak, confirmCount))
        .coolStreak(0)
        .lowResetCount(0)
        .lastUpdateTime(now)
        .lastBroadcastAt(now)
        .build();

      states.put(key, state);
      state.mutationSeq++;

      snapShot = new StateSnapshot(
        key,
        state.currentState.name(),
        state.hotStreak,
        state.coolStreak,
        state.posteriorMean,
        state.accumulatedPrecision,
        state.lowResetCount,
        state.mutationSeq
      );
      return ZetaDecision.hot(key, snapShot);
    }

    // Already CONFIRMED_HOT — refresh liveness, then apply the rebroadcast
    // interval debounce (ADR-0024).
    state.lastUpdateTime = now;
    state.mutationSeq++;

    snapShot = new StateSnapshot(
      key,
      state.currentState.name(),
      state.hotStreak,
      state.coolStreak,
      state.posteriorMean,
      state.accumulatedPrecision,
      state.lowResetCount,
      state.mutationSeq
    );
    if (now - state.lastBroadcastAt >= rebroadcastIntervalMs) {
      state.lastBroadcastAt = now;

      log.debug("Fast-lane periodic HOT rebroadcast: key={}", key);
      return ZetaDecision.hot(key, snapShot);
    }
    return ZetaDecision.none(key, snapShot);
  }

  /**
   * Removes all tracked state for the given key.
   *
   * <p><b>Reentrancy note:</b> This method acquires {@link #keyLocks}. Callers
   * who already hold the per-key lock (e.g. {@link #rollbackToPreviousState})
   * rely on the lock implementation being reentrant. Switching to a
   * non-reentrant lock (e.g. {@link java.util.concurrent.locks.ReentrantLock}
   * → raw {@link java.util.concurrent.locks.Lock}) will cause immediate
   * deadlock on this call path.
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
   * scheduled task. Uses a two-phase approach to avoid TOCTOU races:
   * phase 1 collects candidate keys via a lock-free scan (relying on
   * {@code volatile} semantics of {@link KeyState#lastUpdateTime});
   * phase 2 acquires the per-key lock via {@code tryLock()} and
   * re-verifies staleness before removing. If the lock is contended
   * the key is preserved until the next cycle.
   *
   * <p>Phase 3 broadcasts COOL for the evicted hot keys <em>outside</em> the
   * per-key lock, so a slow AMQP publish cannot stall eviction of other keys
   * or evaluations sharing the same stripe. Callbacks are per-key isolated:
   * a throwing callback aborts neither the remaining keys nor the enclosing
   * eviction cycle, and state cleanup is unaffected by broadcast failure (a
   * lost COOL fails lenient — Apps hold the hot TTL until hard expiry, see
   * ADR-0024). If a key is re-evaluated between removal and broadcast, the
   * COOL is skipped — its fresh state re-broadcasts HOT on its own.
   *
   * @param staleAfterMs maximum idle time in milliseconds before a key is evicted
   */
  @Override
  @SuppressWarnings("all")
  public void evictStale(long staleAfterMs, Consumer<String> onCoolEvict) {
    long now = TimeSource.monotonicMillis();

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
    List<String> evictedHotKeys = new ArrayList<>();
    if (!candidates.isEmpty()) {
      for (String key : candidates) {
        Lock lock = keyLocks.get(key);
        if (lock.tryLock()) {
          try {
            KeyState state = states.get(key);
            if (
              state != null && now - state.lastUpdateTime > staleAfterMs && now - state.lastBroadcastAt > staleAfterMs
            ) {
              if (state.currentState == CONFIRMED_HOT || state.currentState == PRE_COOLING) {
                // Collected for the COOL broadcast in phase 3 — the send itself
                // runs outside the lock so a slow AMQP publish cannot stall the
                // eviction of other keys or evaluations sharing this stripe.
                evictedHotKeys.add(key);
              }
              states.remove(key);
            }
          } finally {
            lock.unlock();
          }
        }
      }
    }

    // Phase 3: broadcast COOL for the evicted hot keys outside the per-key
    // locks. State cleanup already happened in phase 2, so a failed or throwing
    // callback can neither abort the remaining keys nor the enclosing eviction
    // cycle (a lost COOL fails lenient — Apps hold the hot TTL until hard
    // expiry, see ADR-0024).
    for (String key : evictedHotKeys) {
      if (states.containsKey(key)) {
        // The key was re-evaluated after removal and is live again: skip the
        // COOL — its fresh state will re-broadcast HOT on its own.
        continue;
      }
      try {
        onCoolEvict.accept(key);
        log.info("Stale HOT key evicted, COOL broadcast triggered: key={}", key);
      } catch (Exception e) {
        log.warn("COOL broadcast failed for evicted key={}", key, e);
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

      return new StateSnapshot(
        key,
        keyState.currentState.name(),
        keyState.hotStreak,
        keyState.coolStreak,
        keyState.posteriorMean,
        keyState.accumulatedPrecision,
        keyState.lowResetCount,
        keyState.mutationSeq
      );
    } finally {
      lock.unlock();
    }
  }

  /**
   * Rolls back the per-key state to its previous value after a send
   * failure, allowing the next evaluation window to re-emit the decision.
   *
   * <p>The rollback is applied only when the live state still carries the
   * snapshot's {@code mutationSeq} — i.e. no later evaluation advanced the
   * state since the snapshot was taken. With the Worker's parallel report
   * consumers, a stale rollback would otherwise clobber concurrently-advanced
   * state (e.g. reverting a PRE_COOLING progression back to the pre-HOT
   * snapshot); in that case the rollback is skipped and the periodic HOT
   * rebroadcast (ADR-0024) retries the failed decision instead. A key whose
   * state was evicted or reset in the meantime is left alone (never
   * resurrected), matching the same no-op.
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

      KeyState keyState = states.get(key);
      if (keyState == null) {
        log.debug("rollback skipped: state already evicted for key={}", key);
        return;
      }

      if (keyState.mutationSeq != previousState.mutationSeq()) {
        log.debug(
          "rollback skipped: state advanced since snapshot (seq {} -> {}), key={}",
          previousState.mutationSeq(),
          keyState.mutationSeq,
          key
        );
        return;
      }

      keyState.currentState = State.valueOf(previousState.currentState());
      keyState.hotStreak = previousState.hotStreak();
      keyState.coolStreak = previousState.coolStreak();
      keyState.posteriorMean = previousState.posteriorMean();
      keyState.accumulatedPrecision = previousState.accumulatedPrecision();
      keyState.lowResetCount = previousState.lowResetCount();
      // Clear the broadcast stamp so the next evaluation retries the (failed)
      // broadcast immediately instead of waiting out the rebroadcast interval.
      keyState.lastBroadcastAt = 0L;
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
    if (previousState == null) {
      log.warn(
        "rollbackToPreviousState called with null snapshot — ignoring. Use two-arg overload with explicit key to reset."
      );
      return;
    }
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

    /**
     * Consecutive LOW-confidence resets in COLD state.
     * Incremented on LOW retention, reset on MEDIUM/HIGH/cold/fastlane.
     * When this reaches {@code MAX_LOW_RESETS + 1}, the next LOW forces a
     * full streak reset to break the 2↔3 oscillation.
     */
    @Builder.Default
    int lowResetCount = 0;

    /**
     * Accumulated posterior mean (log scale).
     * Updated after each Bayesian confidence evaluation; initialised to the
     * global {@code priorMean} when the key is first seen (the fastlane path
     * falls back to the {@link BayesianConfidenceEstimator#PRIOR_MEAN} default).
     */
    @Builder.Default
    double posteriorMean = BayesianConfidenceEstimator.PRIOR_MEAN;

    /**
     * Accumulated posterior precision from previous observations.
     * Capped at {@code MAX_EFFECTIVE_COUNT × baseLikelihoodPrecision}.
     * Reset to {@code 0.0} for new keys or after stale eviction.
     */
    @Builder.Default
    double accumulatedPrecision = 0.0;

    /** Last evaluation timestamp (epoch millis). Volatile for lock-free reads. */
    @Builder.Default
    volatile long lastUpdateTime = TimeSource.monotonicMillis();

    /**
     * Last time a HOT broadcast was emitted for this key (epoch millis); 0 = never.
     * Stamped optimistically at decision time and reset to 0 on broadcast-failure
     * rollback so the next evaluation retries immediately (ADR-0024).
     */
    @Builder.Default
    volatile long lastBroadcastAt = 0L;

    /**
     * Evaluation epoch, bumped at the start of every evaluation that mutates
     * this key's state (normal path and fast-lane). Snapshots carry the epoch
     * of the evaluation that produced them; a rollback is applied only while
     * the live state still carries the snapshot's epoch — i.e. no later
     * evaluation advanced the state. Guards against a stale rollback
     * clobbering concurrently-advanced state across the Worker's parallel
     * report consumers. Read/written exclusively under the per-key lock.
     */
    @Builder.Default
    long mutationSeq = 0L;
  }
}
