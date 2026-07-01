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
import static io.github.hyshmily.hotkey.util.TimeSource.currentTimeMillis;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.hotkey.model.HotKeyDecision;
import io.github.hyshmily.hotkey.reporting.ReportMessage;
import io.github.hyshmily.hotkey.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.hotkey.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.hotkey.worker.detection.TopKValidator;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
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

  /** Max keys per chunk for parallel processing. Beyond this, keys are split into chunks. */
  private static final int CHUNK_SIZE = 1000;

  /** Log a drain-progress summary when the pending broadcast queue exceeds this threshold. */
  private static final int BATCH_DRAIN_WARN_THRESHOLD = 5000;

  /**
   * Main entry point for batched report messages.
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

  private void doOnReport(ReportMessage message) {
    long now = currentTimeMillis();
    LongAdder totalQps = new LongAdder();

    // Discard reports that are more than 5 seconds old.
    // Guards against delayed or re‑delivered messages that would
    // feed outdated counts into the sliding window.
    if (now - message.timestamp() > stalenessThresholdMs) {
      log.debug("Stale report message, skip: appName={}, age={}ms", message.appName(), now - message.timestamp());
      return;
    }

    Map<String, Long> keyCounts = message.counts();
    if (keyCounts.isEmpty()) return;

    // Feed all key counts into the Worker's HeavyKeeper in a single
    // batch call.  This executes sketch updates per-key under fine-grained
    // stripe locks, then acquires the global sortedTopK heap
    // lock exactly ONCE for the entire batch — rather than once per
    // key — eliminating the P1-3 lock-contention bottleneck.
    workerTopK.addDirect(keyCounts);

    List<Map.Entry<String, Long>> entries = new ArrayList<>(keyCounts.entrySet());
    int totalKeys = entries.size();

    for (int chunkStart = 0; chunkStart < totalKeys; chunkStart += CHUNK_SIZE) {
      int chunkEnd = Math.min(chunkStart + CHUNK_SIZE, totalKeys);
      List<Map.Entry<String, Long>> chunk = entries.subList(chunkStart, chunkEnd);

      // Accumulate broadcasts during parallel processing; drain serially
      // after the stream completes to avoid ForkJoin threads blocking on
      // AMQP channel write locks.
      Queue<Runnable> pendingBroadcasts = new ConcurrentLinkedQueue<>();

      // Process each key independently
      chunk
        .parallelStream()
        .forEach(entry -> {
          try {
            String key = entry.getKey();
            long count = entry.getValue();

            totalQps.add(count);

            // addCount atomically increments the current time slice and returns
            // true if the sum of the last windowSize slices exceeds the threshold.
            boolean isHot = detector.addCount(key, count);

            // The state machine tracks consecutive hot/cold windows and applies
            // hysteresis to decide when to transition between COLD, CONFIRMED_HOT
            // and PRE_COOLING.
            HotKeyDecision decision = stateMachine.evaluate(key, isHot);

            switch (decision.type()) {
              case HOT -> {
                // A new hot key has been confirmed. Pre-allocate a decision
                // version and enqueue the broadcast; actual AMQP send happens
                // on the consumer thread after parallelStream completes.
                long dv = broadcaster.nextDecisionVersion();
                pendingBroadcasts.add(() -> broadcaster.broadcastHot(key, SOURCE_SLIDING_WINDOW, dv));
                topKValidator.markConfirmed(key);
              }
              case COOL -> {
                long dv = broadcaster.nextDecisionVersion();
                pendingBroadcasts.add(() -> broadcaster.broadcastCool(key, dv));
                topKValidator.markCooled(key);
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
        });

      // Drain pending broadcasts serially on the consumer thread.
      // This avoids ForkJoinPool threads blocking on AMQP channel write
      // locks under high concurrency (8 concurrent consumers).
      // Per ADR-0007, lost messages are tolerated by the next periodic cycle.
      // sendBroadcast no longer throws — errors are logged and swallowed.
      int drainedCount = 0;
      Runnable task;
      while ((task = pendingBroadcasts.poll()) != null) {
        task.run();
        drainedCount++;
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

    globalQpsEstimator.addTotal(totalQps.sum());
  }
}
