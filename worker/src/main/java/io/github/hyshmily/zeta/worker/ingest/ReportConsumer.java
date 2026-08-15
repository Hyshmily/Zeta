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
import io.github.hyshmily.zeta.worker.detection.Evaluator;
import io.github.hyshmily.zeta.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
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
 * <p>For every key in the batch the consumer:
 * <ol>
 *   <li>Feeds the count into the {@link io.github.hyshmily.zeta.worker.detection.SlidingWindowDetector} to update the
 *       sliding‑window sum and obtain a binary hot‑or‑not verdict for the
 *       current window.</li>
 *   <li>Passes that verdict to the {@link ZetaBayesianSM} which tracks
 *       consecutive hot/cold windows and decides whether a state transition
 *       (COLD → CONFIRMED_HOT → PRE_COOLING → COLD) has occurred.</li>
 *   <li>If the state machine returns a {@code HOT} decision, the consumer
 *       broadcasts a {@code HOT} message to all application instances.</li>
 *   <li>If the state machine returns a {@code COOL} decision, it broadcasts
 *       a {@code COOL} message.</li>
 * </ol>
 *
 * <p>Optional staleness filter: when {@code stalenessThresholdMs > 0}, reports
 * whose age ({@code now - timestamp}) exceeds the threshold are dropped.
 * The filter is disabled by default ({@code 0}): the report queue's
 * {@code x-message-ttl} bounds staleness broker-side, and a cross-host
 * wall-clock comparison cannot distinguish a genuinely delayed report from
 * an App whose clock lags behind the Worker — an App clock skew of more
 * than the threshold would otherwise silently blind that instance.
 *
 * <p>Because clients use consistent‑hash routing, every reportToWorker for a given
 * key always reaches the same worker, guaranteeing correct per‑key state
 * without cross‑worker coordination.
 */
@Slf4j
public class ReportConsumer {

  /** Unified evaluator with integrated fast-lane support. */
  private final Evaluator evaluator;
  /** Publishes HOT and COOL decisions back to all application instances. */
  private final WorkerBroadcaster broadcaster;
  /** Global qps estimator tracking overall throughput for dynamic threshold learning. */
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

  /** Max keys per chunk for parallel processing. Beyond this, keys are split into chunks. */
  private static final int CHUNK_SIZE = 1000;

  /** Log a drain-progress summary when the pending send queue exceeds this threshold. */
  private static final int BATCH_DRAIN_WARN_THRESHOLD = 5000;

  public ReportConsumer(
    Evaluator evaluator,
    WorkerBroadcaster broadcaster,
    GlobalQpsEstimator globalQpsEstimator,
    ZetaBayesianSM stateMachine,
    long stalenessThresholdMs
  ) {
    this.evaluator = evaluator;
    this.broadcaster = broadcaster;
    this.globalQpsEstimator = globalQpsEstimator;
    this.stateMachine = stateMachine;
    this.stalenessThresholdMs = stalenessThresholdMs;
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

    log.debug(
      "Processing report: appName={}, keys={}, age={}ms",
      message.appName(),
      keyCounts.size(),
      now - message.timestamp()
    );

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

    List<Map.Entry<String, Long>> entries = new ArrayList<>(keyCounts.entrySet());
    int totalKeys = entries.size();

    for (int chunkStart = 0; chunkStart < totalKeys; chunkStart += CHUNK_SIZE) {
      int chunkEnd = Math.min(chunkStart + CHUNK_SIZE, totalKeys);
      List<Map.Entry<String, Long>> chunk = entries.subList(chunkStart, chunkEnd);

      // Accumulate broadcasts during processing; drain serially after the
      // per-key evaluation loop to avoid blocking AMQP channel write locks.
      ArrayDeque<Report> pendingBroadcasts = new ArrayDeque<>();

      // Process each key sequentially on the consumer thread.  8 concurrent
      // consumers already provide sufficient parallelism; intra-chunk
      // parallelisation would amplify stripe-lock contention for no gain.
      for (Map.Entry<String, Long> entry : chunk) {
        try {
          String key = entry.getKey();
          long count = entry.getValue();

          totalQps += count;

          ZetaDecision decision = evaluator.evaluate(key, count);
          if (decision.type() != ZetaDecision.DecisionType.NONE) {
            log.debug(
              "BayesianEvaluator decision: key={}, type={}, snapshot={}",
              key,
              decision.type(),
              decision.snapShot()
            );
          }

          switch (decision.type()) {
            case HOT ->
              // A new hot key has been confirmed. Pre-allocate a decision
              // version and enqueue to send; actual AMQP send happens
              // on the consumer thread after the per-key loop completes.
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
              // current lifecycle stage.  Nothing to do.
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
      }
      processReport(pendingBroadcasts, message, chunkStart, totalKeys);
    }

    globalQpsEstimator.addTotal(totalQps);
  }

  private void processReport(
    ArrayDeque<Report> pendingBroadcasts,
    ReportMessage message,
    int chunkStart,
    int totalKeys
  ) {
    // Drain pending broadcasts serially on the consumer thread, consistent
    // with the sequential in-chunk evaluation above.  Per ADR-0007, lost
    // messages are tolerated by the next periodic cycle.
    // sendBroadcast no longer throws — errors are logged and swallowed.
    int drainedCount = 0;
    Report r;
    while ((r = pendingBroadcasts.poll()) != null) {
      if (Boolean.TRUE.equals(r.task().get())) {
        drainedCount++;
      } else {
        stateMachine.rollbackToPreviousState(r.key(), r.snapShot());
      }
    }

    if (drainedCount >= BATCH_DRAIN_WARN_THRESHOLD) {
      log.info(
        "ReportConsumer drained {} broadcasts for chunk {}/{} of {} keys from app={}",
        drainedCount,
        (chunkStart / CHUNK_SIZE) + 1,
        (totalKeys + CHUNK_SIZE - 1) / CHUNK_SIZE,
        totalKeys,
        message.appName()
      );
    }
  }

  @Builder
  record Report(String key, Supplier<Boolean> task, StateSnapshot snapShot) {}
}
