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
package io.github.hyshmily.hotkey.worker.ingest;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.SOURCE_SLIDING_WINDOW;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import io.github.hyshmily.hotkey.model.HotKeyDecision;
import io.github.hyshmily.hotkey.reporting.ReportMessage;
import io.github.hyshmily.hotkey.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.hotkey.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.hotkey.worker.detection.TopKValidator;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

/**
 * Worker‑side message consumer that receives batched per‑key access counts
 * reported by application instances.
 *
 * <p>For every key in the batch the consumer:
 * <ol>
 *   <li>Feeds the count into the {@link SlidingWindowDetector} to update the
 *       sliding‑window sum and obtain a binary hot‑or‑not verdict for the
 *       current window.</li>
 *   <li>Passes that verdict to the {@link HotKeyStateMachine} which tracks
 *       consecutive hot/cold windows and decides whether a state transition
 *       (COLD → CONFIRMED_HOT → PRE_COOLING → COLD) has occurred.</li>
 *   <li>If the state machine returns a {@code HOT} decision, the consumer
 *       broadcasts a {@code HOT} message to all application instances and
 *       records the confirmation in the {@link TopKValidator}.</li>
 *   <li>If the state machine returns a {@code COOL} decision, it broadcasts
 *       a {@code COOL} message and marks the key as cooled in the
 *       {@link TopKValidator}.</li>
 * </ol>
 *
 * <p>Messages older than 5 seconds are silently discarded to prevent stale
 * data from distorting the sliding‑window view.
 *
 * <p>Because clients use consistent‑hash routing, every report for a given
 * key always reaches the same worker, guaranteeing correct per‑key state
 * without cross‑worker coordination.
 */
@RequiredArgsConstructor
@Slf4j
public class ReportConsumer {

  /** Sliding-window detector that tracks per-key access counts and returns hot/cold verdicts. */
  private final SlidingWindowDetector detector;
  /** Per-key lifecycle state machine managing COLD / CONFIRMED_HOT / PRE_COOLING transitions. */
  private final HotKeyStateMachine stateMachine;
  /** Publishes HOT and COOL decisions back to all application instances. */
  private final WorkerBroadcaster broadcaster;
  /** TopK pre-warm validator for cross-instance frequency-based confirmation. */
  private final TopKValidator topKValidator;
  /** Worker-scoped HeavyKeeper sketch for cross-instance frequency estimation. */
  private final TopK workerTopK;
  /** Global QPS estimator tracking overall throughput for dynamic threshold learning. */
  private final GlobalQpsEstimator globalQpsEstimator;

  /** Staleness threshold in milliseconds. Package-visible for testing. */
  long stalenessThresholdMs = 5000L;

  /**
   * Main entry point for batched report messages.
   *
   * @param message the deserialized message containing counts for multiple keys
   */
  @RabbitListener(queues = "#{@reportQueue.name}")
  public void onReport(ReportMessage message) {
    try {
      doOnReport(message);
    } catch (Exception e) {
      log.error("Uncaught exception in onReport, discarding message to prevent poison-message requeue loop: appName={}",
          message != null ? message.appName() : "null", e);
    }
  }

  private void doOnReport(ReportMessage message) {
    long now = System.currentTimeMillis();
    long totalQps = 0;

    // Discard reports that are more than 5 seconds old.
    // This guards against delayed or re‑delivered messages that would
    // feed outdated counts into the sliding window.
    if (now - message.timestamp() > stalenessThresholdMs) {
      log.debug("Stale report message, skip: appName={}, age={}ms", message.appName(), now - message.timestamp());
      return;
    }

    // Process each key independently
    for (Map.Entry<String, Long> entry : message.counts().entrySet()) {
      try {
        String key = entry.getKey();
        long count = entry.getValue();

        totalQps += count;
        // Feed the global Worker TopK with the aggregated report count.
        // This populates the Worker-side HeavyKeeper so TopKValidator can
        // pre-warm keys based on cross-instance frequency.
        workerTopK.addDirect(key, (int) Math.min(count, Integer.MAX_VALUE));

        // addCount atomically increments the current time slice and returns
        // true if the sum of the last windowSize slices exceeds the threshold.
        boolean isHot = detector.addCount(key, count);

        Map<String, Object> stateSnapshot = stateMachine.getStateSnapshot(key);

        // The state machine tracks consecutive hot/cold windows and applies
        // hysteresis to decide when to transition between COLD, CONFIRMED_HOT
        // and PRE_COOLING.
        HotKeyDecision decision = stateMachine.evaluate(key, isHot);

        switch (decision.type()) {
          case HOT -> {
            try {
              // A new hot key has been confirmed.
              // Broadcast HOT to all app instances so they can pre‑warm
              // their local caches and enable soft expiration.
              broadcaster.broadcastHot(key, SOURCE_SLIDING_WINDOW);
              // Record the confirmation in TopK for historical ranking.
              topKValidator.markConfirmed(key);
            } catch (WorkerBroadcaster.BroadcastFailedException e) {
              log.warn("Broadcast HOT failed, rolling back state machine for key={}", key, e);
              stateMachine.rollbackToPreviousState(key, stateSnapshot);
            }
          }
          case COOL -> {
            try {
              // The key has fully cooled down.
              // Broadcast COOL so instances can disable soft expiration
              // and let the entry be evicted naturally.
              broadcaster.broadcastCool(key);
              // Update the TopK tracking accordingly.
              topKValidator.markCooled(key);
            } catch (WorkerBroadcaster.BroadcastFailedException e) {
              log.warn("Broadcast COOL failed, rolling back state machine for key={}", key, e);
              stateMachine.rollbackToPreviousState(key, stateSnapshot);
            }
          }
          case NONE -> {
            // No state transition occurred – the key remains in its
            // current lifecycle stage.  Nothing to do.
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
    }
    globalQpsEstimator.addTotal(totalQps);
  }
}
