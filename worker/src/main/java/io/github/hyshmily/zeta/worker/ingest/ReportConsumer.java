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
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.StateSnapshot;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.worker.detection.Evaluator;
import io.github.hyshmily.zeta.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
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
 * <p>Messages older than 5 seconds are silently discarded to prevent stale
 * data from distorting the sliding‑window view.
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
  /** Worker-scoped HeavyKeeper sketch for cross-instance frequency estimation. */
  private final TopK workerTopK;
  /** Global qps estimator tracking overall throughput for dynamic threshold learning. */
  private final GlobalQpsEstimator globalQpsEstimator;
  /** Per-key lifecycle state machine. */
  private final ZetaBayesianSM stateMachine;

  /** Staleness threshold in milliseconds. Package-visible for testing. */
  long stalenessThresholdMs = 5000L;

  /** Max keys per chunk for parallel processing. Beyond this, keys are split into chunks. */
  private static final int CHUNK_SIZE = 1000;

  /** Log a drain-progress summary when the pending send queue exceeds this threshold. */
  private static final int BATCH_DRAIN_WARN_THRESHOLD = 5000;

  public ReportConsumer(
    Evaluator evaluator,
    WorkerBroadcaster broadcaster,
    TopK workerTopK,
    GlobalQpsEstimator globalQpsEstimator,
    ZetaBayesianSM stateMachine
  ) {
    this.evaluator = evaluator;
    this.broadcaster = broadcaster;
    this.workerTopK = workerTopK;
    this.globalQpsEstimator = globalQpsEstimator;
    this.stateMachine = stateMachine;
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
    log.debug(
      "Processing report: appName={}, keys={}, counts={}, age={}ms",
      message.appName(),
      message.counts().size(),
      message.counts(),
      now - message.timestamp()
    );
    LongAdder totalQps = new LongAdder();

    // Discard reports that are more than 5 seconds old.
    // Guards against delayed or re‑delivered messages that would
    // feed outdated counts into the sliding window.
    if (now - message.timestamp() > stalenessThresholdMs) {
      log.debug(
        "Stale reportToWorker message, skip: appName={}, age={}ms",
        message.appName(),
        now - message.timestamp()
      );
      return;
    }

    Map<String, Long> keyCounts = message.counts();
    if (keyCounts.isEmpty()) return;

    List<Map.Entry<String, Long>> entries = new ArrayList<>(keyCounts.entrySet());
    int totalKeys = entries.size();

    for (int chunkStart = 0; chunkStart < totalKeys; chunkStart += CHUNK_SIZE) {
      int chunkEnd = Math.min(chunkStart + CHUNK_SIZE, totalKeys);
      List<Map.Entry<String, Long>> chunk = entries.subList(chunkStart, chunkEnd);

      // Accumulate broadcasts during processing; drain serially after the
      // per-key evaluation loop to avoid blocking AMQP channel write locks.
      Queue<Report> pendingBroadcasts = new ConcurrentLinkedQueue<>();

      // Process each key sequentially on the consumer thread.  8 concurrent
      // consumers already provide sufficient parallelism; intra-chunk
      // parallelisation would amplify stripe-lock contention for no gain.
      for (Map.Entry<String, Long> entry : chunk) {
        try {
            String key = entry.getKey();
            long count = entry.getValue();

            totalQps.add(count);

            // Update HeavyKeeper inline (parallel, not serial before the stream).
            // The estimatedCount from this batch will be visible to the Bayesian
            // path starting from the next batch — FastLane is unaffected.
            workerTopK.addDirect(key, count);

            ZetaDecision decision = evaluator.evaluate(key, count);
            if (decision.type() != ZetaDecision.DecisionType.NONE) {
              log.debug(
                "BayesianEvaluator decision: key={}, type={}, snapshot={}",
                key,
                decision.type(),
                decision.snapShot()
              );
            }
            StateSnapshot previousState = decision.snapShot();

            switch (decision.type()) {
              case HOT -> {
                // A new hot key has been confirmed. Pre-allocate a decision
                // version and enqueue to send; actual AMQP send happens
                // on the consumer thread after the per-key loop completes.
                if (
                  pendingBroadcasts.add(
                    Report.builder()
                      .task(() -> broadcaster.broadcastHot(key))
                      .snapShot(decision.snapShot())
                      .build()
                  )
                ) {
                  // markConfirmed handled by TopKValidator (removed — use Baysian state machine as sole authority)
                } else {
                  stateMachine.rollbackToPreviousState(key, previousState);
                  log.warn("Failed to enqueue HOT broadcast for key={}, rolling back state to {}", key, previousState);
                }
              }
              case COOL -> {
                if (
                  pendingBroadcasts.add(
                    Report.builder()
                      .task(() -> broadcaster.broadcastCool(key))
                      .snapShot(decision.snapShot())
                      .build()
                  )
                ) {
                  // markCooled handled by TopKValidator (removed — use Baysian state machine as sole authority)
                } else {
                  stateMachine.rollbackToPreviousState(key, previousState);
                  log.warn("Failed to enqueue COOL broadcast for key={}, rolling back state to {}", key, previousState);
                }
              }
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

    globalQpsEstimator.addTotal(totalQps.sum());
  }

  private void processReport(Queue<Report> pendingBroadcasts, ReportMessage message, int chunkStart, int totalKeys) {
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
        stateMachine.rollbackToPreviousState(r.snapShot());
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
  record Report(Supplier<Boolean> task, StateSnapshot snapShot) {}
}
