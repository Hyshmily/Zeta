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
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.Evaluator;
import io.github.hyshmily.zeta.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcastBuffer;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.Builder;
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
   * Window (ms) for the rate-limited broadcast-failure WARN
   * ({@value #BROADCAST_ERROR_LOG_WINDOW_MS}ms, ADR-0037 convention).
   */
  private static final long BROADCAST_ERROR_LOG_WINDOW_MS = 10_000;

  /**
   * Monotonic timestamp of the last rate-limited broadcast-failure WARN.
   * Volatile: report consumers are concurrent ({@code concurrentConsumers}),
   * so the throttle must be visible across threads.
   */
  private volatile long lastBroadcastErrorLoggedAtMs = -BROADCAST_ERROR_LOG_WINDOW_MS;

  /**
   * Previous batch's per-thread sample of
   * {@link GlobalQpsEstimator#getWindowTotal()},
   * used to compute the batch-sampled global traffic ratio passed to
   * {@link Evaluator#evaluate(String, long, double)}.
   *
   * <p>
   * ThreadLocal rather than a shared field: report consumers are concurrent
   * ({@code concurrentConsumers}), and a shared volatile would let two
   * consumers interleave samples so one thread's ratio compares against
   * <em>another</em> thread's batch — mixing unrelated batches into one trend
   * sample. Each consumer thread tracks its own previous sample; the threads
   * are container-managed and long-lived, so nothing leaks. Written once per
   * batch, not once per message.
   */
  @SuppressWarnings("java:S5164")
  private final ThreadLocal<Long> prevGlobalWindowTotal = ThreadLocal.withInitial(() -> 0L);

  /**
   * Optional dedicated decision-send buffer (ADR-0061). When present,
   * broadcasts are handed to its bounded single-threaded executor instead of
   * publishing synchronously on the AMQP consumer thread; {@code null} keeps
   * the legacy synchronous drain in {@link #processReport}.
   */
  private final WorkerBroadcastBuffer broadcastBuffer;

  public ReportConsumer(
    Evaluator evaluator,
    WorkerBroadcaster broadcaster,
    GlobalQpsEstimator globalQpsEstimator,
    ZetaBayesianSM stateMachine,
    long stalenessThresholdMs
  ) {
    this(evaluator, broadcaster, globalQpsEstimator, stateMachine, stalenessThresholdMs, null);
  }

  public ReportConsumer(
    Evaluator evaluator,
    WorkerBroadcaster broadcaster,
    GlobalQpsEstimator globalQpsEstimator,
    ZetaBayesianSM stateMachine,
    long stalenessThresholdMs,
    WorkerBroadcastBuffer broadcastBuffer
  ) {
    this.evaluator = evaluator;
    this.broadcaster = broadcaster;
    this.globalQpsEstimator = globalQpsEstimator;
    this.stateMachine = stateMachine;
    this.stalenessThresholdMs = stalenessThresholdMs;
    this.broadcastBuffer = broadcastBuffer;
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

    // Sample the global window total ONCE per batch, before the per-key loop.
    // Per-key sampling microseconds apart yields a ratio of ≈1.000 always —
    // the documented global-fluctuation normalisation never engaged and only
    // injected sawtooth noise into trendStrength (plus a volatile write per
    // message). A non-positive sample (estimator absent, cold shard, first
    // batch) maps to the neutral ratio 1.0 — no division, no 10× trend
    // inflation through the 0.1 clamp.
    long globalTotal = globalQpsEstimator != null ? Math.max(0, globalQpsEstimator.getWindowTotal()) : 0L;
    double globalRatio = computeGlobalRatio(globalTotal);
    prevGlobalWindowTotal.set(globalTotal);

    // Iterate entrySet directly to avoid the ArrayList copy; reuse a single
    // ArrayDeque across chunks to avoid per-chunk allocation. Chunks still
    // bound the pending-broadcast queue size (bounded memory per batch).
    ArrayDeque<Report> pendingBroadcasts = new ArrayDeque<>();
    int chunkKeyCount = 0;

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
            decision.snapShot(),
            now,
            now - message.timestamp()
          );
        }

        switch (decision.type()) {
          case HOT ->
            // A new hot key has been confirmed. Enqueue to send; the actual
            // AMQP send — and the decision-version allocation inside the
            // broadcaster — happens on the consumer thread after the per-key
            // loop completes.
            pendingBroadcasts.add(
              Report.builder()
                .key(key)
                .task(() -> broadcaster.broadcastHot(key))
                .snapShot(decision.snapShot())
                .build()
            );
          case COOL -> pendingBroadcasts.add(
            Report.builder()
              .key(key)
              .task(() -> broadcaster.broadcastCool(key))
              .snapShot(decision.snapShot())
              .build()
          );
          case NONE -> {
            // No state transition occurred – the key remains in its
            // current lifecycle stage. Nothing to do.
          }
        }
      } catch (Exception e) {
        log.error(
          "Error processing reportToWorker entry: appName={}, key={}, count={}",
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

    globalQpsEstimator.addTotal(totalQps);
  }

  /**
   * Dispatches the chunk's pending decision broadcasts.
   *
   * <p>
   * With a {@link WorkerBroadcastBuffer} (ADR-0061) each decision is handed
   * to its bounded single-threaded executor — the AMQP consumer thread never
   * blocks on AMQP publishes, so a mass-heat event cannot stall report
   * consumption; a send failure applies the state rollback on the drain
   * thread, a saturation drop applies it right here on the caller thread.
   * Without a buffer the legacy synchronous drain ({@link #processReport})
   * runs, consistent with the sequential in-chunk evaluation.
   *
   * @param pendingBroadcasts the chunk's collected decisions (drained)
   */
  private void dispatchBroadcasts(ArrayDeque<Report> pendingBroadcasts) {
    if (broadcastBuffer != null) {
      pendingBroadcasts.forEach(r ->
        broadcastBuffer.submit(r.task(), () -> stateMachine.rollbackToPreviousState(r.key(), r.snapShot()))
      );
      return;
    }
    processReport(pendingBroadcasts);
  }

  /**
   * Computes the batch-sampled global traffic ratio: this batch's sampled
   * global window total divided by the previous batch's sample. A
   * non-positive sample on either side (estimator absent, cold shard, first
   * processed batch) yields the neutral {@code 1.0} — callers never divide
   * by zero and the trend signal is never inflated.
   *
   * @param globalTotal the global window total sampled for this batch
   *                    ({@code >= 0})
   * @return the ratio to pass to the evaluator; always strictly positive
   */
  private double computeGlobalRatio(long globalTotal) {
    long prev = prevGlobalWindowTotal.get();
    if (globalTotal <= 0 || prev <= 0) {
      return 1.0;
    }
    return (double) globalTotal / prev;
  }

  private void processReport(ArrayDeque<Report> pendingBroadcasts) {
    // Drain pending broadcasts serially on the consumer thread, consistent
    // with the sequential in-chunk evaluation above. Per ADR-0007, lost
    // messages are tolerated by the next periodic cycle.
    // sendBroadcast no longer throws — errors are logged and swallowed.
    // Each failure still rolls the key's state back (guarded by the state
    // machine's mutationSeq), but the per-failure WARN is aggregated into
    // at most one log per {@link #BROADCAST_ERROR_LOG_WINDOW_MS} window so a
    // RabbitMQ outage cannot flood the log (ADR-0037 convention). The
    // broadcast failure itself (with full stack) is logged once at the send
    // site in {@link WorkerBroadcaster}.
    Report r;
    int failed = 0;
    String firstFailureKey = null;
    StateSnapshot firstFailureSnapshot = null;

    while ((r = pendingBroadcasts.poll()) != null) {
      if (!Boolean.TRUE.equals(r.task().get())) {
        failed++;
        if (firstFailureKey == null) {
          firstFailureKey = r.key();
          firstFailureSnapshot = r.snapShot();
        }
        stateMachine.rollbackToPreviousState(r.key(), r.snapShot());
      }
    }

    if (failed > 0 && tryAcquireBroadcastErrorLog()) {
      log.warn(
        "Failed to broadcast {} decision(s), rolled back to previous state (first key={}, first snapshot={}; " +
          "further failures suppressed for {}s)",
        failed,
        firstFailureKey,
        firstFailureSnapshot,
        BROADCAST_ERROR_LOG_WINDOW_MS / 1000
      );
    }
  }

  /**
   * Rate limiter for the aggregated broadcast-failure WARN: at most one log
   * per {@link #BROADCAST_ERROR_LOG_WINDOW_MS}ms window. Thread-safe via
   * {@link #lastBroadcastErrorLoggedAtMs} being volatile.
   *
   * @return {@code true} if the caller may log now
   */
  private boolean tryAcquireBroadcastErrorLog() {
    long now = TimeSource.monotonicMillis();
    if (now - lastBroadcastErrorLoggedAtMs < BROADCAST_ERROR_LOG_WINDOW_MS) {
      return false;
    }
    lastBroadcastErrorLoggedAtMs = now;
    return true;
  }

  @Builder
  record Report(String key, Supplier<Boolean> task, StateSnapshot snapShot) {}
}
