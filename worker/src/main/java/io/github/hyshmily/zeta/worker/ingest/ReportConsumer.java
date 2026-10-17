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
package io.github.hyshmily.zeta.worker.ingest;

import static io.github.hyshmily.zeta.util.TimeSource.currentTimeMillis;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.model.StateSnapshot;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.util.LogThrottle;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.Evaluator;
import io.github.hyshmily.zeta.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcastBuffer;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

/**
 * Worker‑side message consumer that receives batched per‑key access counts
 * reported by application instances.
 *
 * <p>
 * For every key in the batch the consumer:
 * <ol>
 * <li>Feeds the count into the
 * {@link io.github.hyshmily.zeta.worker.detection.SlidingWindowDetector} to
 * update the
 * sliding‑window sum and obtain a binary hot‑or‑not verdict for the
 * current window.</li>
 * <li>Passes that verdict to the {@link ZetaBayesianSM} which tracks
 * consecutive hot/cold windows and decides whether a state transition
 * (COLD → CONFIRMED_HOT → PRE_COOLING → COLD) has occurred.</li>
 * <li>If the state machine returns a {@code HOT} decision, the consumer
 * broadcasts a {@code HOT} message to all application instances.</li>
 * <li>If the state machine returns a {@code COOL} decision, it broadcasts
 * a {@code COOL} message.</li>
 * </ol>
 *
 * <p>
 * Optional staleness filter: when {@code stalenessThresholdMs > 0}, reports
 * whose age ({@code now - timestamp}) exceeds the threshold are dropped.
 * The filter is disabled by default ({@code 0}): the report queue's
 * {@code x-message-ttl} bounds staleness broker-side, and a cross-host
 * wall-clock comparison cannot distinguish a genuinely delayed report from
 * an App whose clock lags behind the Worker — an App clock skew of more
 * than the threshold would otherwise silently blind that instance.
 *
 * <p>
 * Because clients use consistent‑hash routing, every reportToWorker for a given
 * key always reaches the same worker, guaranteeing correct per‑key state
 * without cross‑worker coordination.
 */
@Slf4j
public class ReportConsumer {

  /** Unified evaluator with integrated fast-lane support. */
  private final Evaluator evaluator;
  /** Publishes HOT and COOL decisions back to all application instances. */
  private final WorkerBroadcaster broadcaster;
  /**
   * Global qps estimator tracking overall throughput for dynamic threshold
   * learning.
   */
  private final GlobalQpsEstimator globalQpsEstimator;
  /** Per-key lifecycle state machine. */
  private final ZetaBayesianSM stateMachine;

  /**
   * Optional staleness filter threshold in milliseconds; {@code 0} disables
   * the filter (default). When enabled, only a positive age exceeding the
   * threshold drops a report — a negative age (reporter clock ahead) always
   * passes, since it cannot be distinguished from a healthy fast report.
   */
  private final long stalenessThresholdMs;

  /**
   * Total count of reports dropped by the staleness filter, used to
   * rate-limit the WARN log so a skewing instance becomes visible without
   * flooding the log on the hot path.
   */
  private final AtomicLong staleDroppedCount = new AtomicLong();

  /**
   * Max keys per processing chunk. Chunks bound only the pending-broadcast
   * queue drained after each chunk (bounded memory per batch); there is no
   * intra-chunk parallelism — keys are evaluated sequentially on the
   * consumer thread, with parallelism provided by the concurrent AMQP
   * consumers alone.
   */
  private static final int CHUNK_SIZE = 1000;

  /**
   * Rate-limits the aggregated broadcast-failure WARN to one per
   * {@value LogThrottle#DEFAULT_WINDOW_MS}ms window (ADR-0037 convention).
   * Admission is strict — {@link LogThrottle} claims the window with a
   * compare-and-set, so exactly one caller per window logs. The atomicity and
   * the monotonic clock are provided by {@link LogThrottle}.
   */
  private final LogThrottle broadcastErrorLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Optional dedicated decision-send buffer (ADR-0061). When present,
   * broadcasts are handed to its bounded single-threaded executor instead of
   * publishing synchronously on the AMQP consumer thread; {@code null} keeps
   * the legacy synchronous drain in {@link #processReport}.
   */
  private final WorkerBroadcastBuffer broadcastBuffer;

  /**
   * Optional detection-plane meters (ADR-0080), used for the per-report-batch
   * evaluation timer {@code zeta.worker.report.eval}. {@code null} — the legacy
   * constructors — leaves batches untimed, matching the ADR-0037/0061
   * "null keeps the legacy path" convention for optional collaborators.
   */
  private final WorkerDetectionMetrics metrics;

  /**
   * Learning period (ms) during which COOL broadcasts are suppressed while HOT
   * warmup still flows. Decouples ring admission from early cooling: the Worker
   * receives traffic and reports HOT from the first batch, but threshold
   * adaptation (in {@code ThresholdLearner}) and COOL emission stay held until
   * the estimator has observed a full period. Suppressed COOL decisions roll
   * their state back so the next evaluation re-emits them after warmup.
   */
  private final long learningPeriodMs;

  /** Worker start time on the monotonic clock; anchors the learning-period check. */
  private final long workerStartTimeMs;

  /**
   * Constructs a consumer with the optional decision-send buffer and the
   * detection-plane meters.
   *
   * @param evaluator            the key evaluator
   * @param broadcaster          the HOT/COOL decision broadcaster
   * @param globalQpsEstimator   the global throughput estimator
   * @param stateMachine         the per-key lifecycle state machine
   * @param stalenessThresholdMs the optional report staleness threshold ({@code 0} disables it)
   * @param broadcastBuffer      the decision-send buffer, or {@code null} for the synchronous drain
   * @param metrics              the detection-plane meters, or {@code null} to leave batches untimed
   */
  public ReportConsumer(
    Evaluator evaluator,
    WorkerBroadcaster broadcaster,
    GlobalQpsEstimator globalQpsEstimator,
    ZetaBayesianSM stateMachine,
    long stalenessThresholdMs,
    WorkerBroadcastBuffer broadcastBuffer,
    WorkerDetectionMetrics metrics
  ) {
    this(evaluator, broadcaster, globalQpsEstimator, stateMachine, stalenessThresholdMs, broadcastBuffer, metrics, 0L);
  }

  /**
   * Constructs a consumer with learning-period COOL suppression.
   *
   * @param evaluator            the key evaluator
   * @param broadcaster          the HOT/COOL decision broadcaster
   * @param globalQpsEstimator   the global throughput estimator
   * @param stateMachine         the per-key lifecycle state machine
   * @param stalenessThresholdMs the optional report staleness threshold ({@code 0} disables it)
   * @param broadcastBuffer      the decision-send buffer, or {@code null} for the synchronous drain
   * @param metrics              the detection-plane meters, or {@code null} to leave batches untimed
   * @param learningPeriodMs     the warmup window (ms) suppressing COOL; {@code 0} disables suppression
   */
  public ReportConsumer(
    Evaluator evaluator,
    WorkerBroadcaster broadcaster,
    GlobalQpsEstimator globalQpsEstimator,
    ZetaBayesianSM stateMachine,
    long stalenessThresholdMs,
    WorkerBroadcastBuffer broadcastBuffer,
    WorkerDetectionMetrics metrics,
    long learningPeriodMs
  ) {
    this.evaluator = evaluator;
    this.broadcaster = broadcaster;
    this.globalQpsEstimator = globalQpsEstimator;
    this.stateMachine = stateMachine;
    this.stalenessThresholdMs = stalenessThresholdMs;
    this.broadcastBuffer = broadcastBuffer;
    this.metrics = metrics;
    this.learningPeriodMs = Math.max(0L, learningPeriodMs);
    this.workerStartTimeMs = TimeSource.monotonicMillis();
  }

  /**
   * Whether the Worker is still inside its learning period.
   *
   * @return {@code true} while warmup suppression applies
   */
  boolean isInLearningPeriod() {
    return learningPeriodMs > 0 && TimeSource.monotonicMillis() - workerStartTimeMs < learningPeriodMs;
  }

  /**
   * Main entry point for batched reportToWorker messages.
   *
   * @param message the deserialized message containing counts for multiple keys
   */
  @RabbitListener(queues = "#{@reportQueue.name}", containerFactory = "reportListenerContainerFactory")
  public void onReport(ReportMessage message) {
    try {
      doOnReport(message);
    } catch (Exception e) {
      log.error(
        "Uncaught exception in onReport, discarding message to prevent poison-message requeue loop: appName={}",
        message != null ? message.appName() : "null",
        e
      );
    }
  }

  @SuppressWarnings("all")
  private void doOnReport(ReportMessage message) {
    long now = currentTimeMillis();
    Map<String, Long> keyCounts = message.counts();

    long totalQps = 0L;

    // Optional staleness filter, disabled by default (threshold 0) — the
    // queue's x-message-ttl bounds staleness broker-side, and a cross-host
    // wall-clock comparison cannot tell a delayed report from an App whose
    // clock lags the Worker. When enabled, only a positive age beyond the
    // threshold is dropped; a negative age (reporter clock ahead) passes.
    if (stalenessThresholdMs > 0) {
      long age = now - message.timestamp();
      if (age > stalenessThresholdMs) {
        long dropped = staleDroppedCount.incrementAndGet();
        // Log roughly once per 100 drops (dropped=1, 101, 201, ...). The
        // counter keeps the real cumulative total for metrics — it is never
        // reset (a previous bitwise check made the log timing random and a
        // set(1) corrupted the total, defeating the throttle entirely).
        if (dropped == 1 || dropped % 100 == 1) {
          log.warn("Stale report dropped: appName={}, age={}ms, totalDropped={}", message.appName(), age, dropped);
        }
        return;
      }
    }

    if (keyCounts.isEmpty()) return;

    // Time-aligned global traffic ratio maintained inside the estimator on
    // fixed time slices: every consumer thread reads the same volatile value,
    // so concurrent batches never interleave per-thread samples into a
    // distorted trend. A non-positive or NaN sample maps to the neutral 1.0.
    double globalRatio = sanitizeGlobalRatio(globalQpsEstimator != null ? globalQpsEstimator.getGlobalRatio() : 1.0);

    // Iterate entrySet directly to avoid the ArrayList copy; reuse a single
    // ArrayDeque across chunks to avoid per-chunk allocation. Chunks still
    // bound the pending-broadcast queue size (bounded memory per batch).
    // Decisions travel as value objects: no per-key lambda or record wrapper.
    ArrayDeque<ZetaDecision> pendingBroadcasts = new ArrayDeque<>();
    int chunkKeyCount = 0;

    // Per-batch evaluation timing (ADR-0080). Monotonic clock only: the stamp is
    // taken here and read again after the drain below, never derived from
    // `now - message.timestamp()` — that crosses hosts and is unusable under App
    // clock skew (the staleness-filter note above), and it would fold AMQP queue
    // wait into what is meant to be the evaluation cost. The staleness filter and
    // the global-ratio sampling above are outside the sample; in the default
    // buffered mode (ADR-0061) the decision sends run on the buffer's drain
    // thread and are outside it too.
    long evalStartNanos = metrics != null ? metrics.startReportEval() : 0L;

    // Process each key sequentially on the consumer thread. 8 concurrent
    // consumers already provide sufficient parallelism; intra-chunk
    // parallelisation would amplify stripe-lock contention for no gain.
    for (Map.Entry<String, Long> entry : keyCounts.entrySet()) {
      try {
        String key = entry.getKey();
        long count = entry.getValue();

        totalQps += count;

        ZetaDecision decision = evaluator.evaluate(key, count, globalRatio);
        if (log.isDebugEnabled() && decision.type() != ZetaDecision.DecisionType.NONE) {
          log.debug(
            "BayesianEvaluator decision: key={}, type={}, snapshot={},time={},delay={}ms",
            key,
            decision.type(),
            decision.snapshot(),
            now,
            now - message.timestamp()
          );
        }

        switch (decision.type()) {
          case HOT ->
            // A new hot key has been confirmed. Enqueue the value object; the
            // actual AMQP send — and the decision-version allocation inside
            // the broadcaster — happens after the per-key loop completes.
            pendingBroadcasts.add(decision);
          case COOL -> {
            // Warmup suppression: inside the learning period COOL is held so
            // fresh HOT entries can preheat, while HOT still flows. The state
            // is rolled back so the decision re-emits after warmup instead of
            // leaking a HOT entry that never cools.
            if (isInLearningPeriod()) {
              stateMachine.rollbackToPreviousState(decision.cacheKey(), decision.snapshot());
            } else {
              pendingBroadcasts.add(decision);
            }
          }
          case NONE -> {
            // No state transition occurred – the key remains in its
            // current lifecycle stage. Nothing to do.
          }
        }
      } catch (Exception e) {
        log.error(
          "Error processing report entry: appName={}, key={}, count={}",
          message.appName(),
          entry.getKey(),
          entry.getValue(),
          e
        );
      }
      if (++chunkKeyCount >= CHUNK_SIZE) {
        dispatchBroadcasts(pendingBroadcasts);
        pendingBroadcasts.clear();
        chunkKeyCount = 0;
      }
    }
    if (!pendingBroadcasts.isEmpty()) {
      dispatchBroadcasts(pendingBroadcasts);
      pendingBroadcasts.clear();
    }

    if (metrics != null) {
      metrics.recordReportEval(evalStartNanos);
    }

    globalQpsEstimator.addTotal(totalQps);
  }

  /**
   * Dispatches the chunk's pending decision broadcasts.
   *
   * <p>
   * With a {@link WorkerBroadcastBuffer} (ADR-0061) each decision value object
   * is handed to its bounded single-threaded executor — the AMQP consumer
   * thread never blocks on AMQP publishes, so a mass-heat event cannot stall
   * report consumption; a send failure applies the state rollback on the drain
   * thread, a saturation drop applies it right here on the caller thread.
   * Without a buffer the legacy synchronous drain ({@link #processReport})
   * runs, consistent with the sequential in-chunk evaluation.
   *
   * @param pendingBroadcasts the chunk's collected decisions (drained)
   */
  private void dispatchBroadcasts(ArrayDeque<ZetaDecision> pendingBroadcasts) {
    if (broadcastBuffer != null) {
      pendingBroadcasts.forEach(decision -> broadcastBuffer.submit(decision, broadcaster, stateMachine));
      return;
    }
    processReport(pendingBroadcasts);
  }

  /**
   * Sanitizes a time-aligned global traffic ratio from the estimator: only a
   * strictly positive finite value is a meaningful trend signal, anything else
   * (estimator absent, cold shard, first window) maps to the neutral
   * {@code 1.0} — callers never divide by zero and the trend is never
   * inflated.
   *
   * @param ratio the raw ratio from {@link GlobalQpsEstimator#getGlobalRatio()}
   * @return the ratio to pass to the evaluator; always strictly positive
   */
  private static double sanitizeGlobalRatio(double ratio) {
    return ratio > 0 && Double.isFinite(ratio) ? ratio : 1.0;
  }

  private void processReport(ArrayDeque<ZetaDecision> pendingBroadcasts) {
    // Drain pending broadcasts serially on the consumer thread, consistent
    // with the sequential in-chunk evaluation above. Per ADR-0007, lost
    // messages are tolerated by the next periodic cycle.
    // sendBroadcast no longer throws — errors are logged and swallowed.
    // Each failure still rolls the key's state back (guarded by the state
    // machine's mutationSeq), but the per-failure WARN is aggregated into
    // at most one log per {@link LogThrottle#DEFAULT_WINDOW_MS} window so a
    // RabbitMQ outage cannot flood the log (ADR-0037 convention). The
    // broadcast failure itself (with full stack) is logged once at the send
    // site in {@link WorkerBroadcaster}.
    ZetaDecision decision;
    int failed = 0;
    String firstFailureKey = null;
    StateSnapshot firstFailureSnapshot = null;

    while ((decision = pendingBroadcasts.poll()) != null) {
      boolean ok;
      try {
        ok = switch (decision.type()) {
          case HOT -> broadcaster.broadcastHot(decision.cacheKey());
          case COOL -> broadcaster.broadcastCool(decision.cacheKey());
          case NONE -> true;
        };
      } catch (Exception e) {
        ok = false;
      }
      if (!ok) {
        failed++;
        if (firstFailureKey == null) {
          firstFailureKey = decision.cacheKey();
          firstFailureSnapshot = decision.snapshot();
        }
        stateMachine.rollbackToPreviousState(decision.cacheKey(), decision.snapshot());
      }
    }

    if (failed > 0 && broadcastErrorLogThrottle.tryAcquire()) {
      log.warn(
        "Failed to broadcast {} decision(s), rolled back to previous state (first key={}, first snapshot={}; " +
          "further failures suppressed for {}s)",
        failed,
        firstFailureKey,
        firstFailureSnapshot,
        LogThrottle.DEFAULT_WINDOW_MS / 1000
      );
    }
  }
}
