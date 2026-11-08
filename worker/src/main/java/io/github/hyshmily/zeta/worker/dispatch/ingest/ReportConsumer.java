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
package io.github.hyshmily.zeta.worker.dispatch.ingest;

import static io.github.hyshmily.zeta.util.TimeSource.currentTimeMillis;

import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.DetectionPipeline;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

/**
 * Consumes batched per-key access counts reported by application instances.
 *
 * <p>The consumer owns transport concerns only: the incarnation fence, the optional staleness
 * filter, malformed-entry rejection, per-batch timing, and one error line per failed entry. Every well-formed {@code (key, count)} pair is handed to {@link DetectionPipeline},
 * which owns the window, the rules and the HOT set.
 *
 * <p>Malformed entries — a null key, a null count, a non-positive count — are dropped before the
 * pipeline: a negative count would push negative mass into the sliding window and corrupt every
 * threshold downstream.
 *
 * <p>Every report is fenced by incarnation: {@link ReportMessage#targetEpoch()} must equal this
 * Worker's epoch. The durable report queue and the App's bounded dispatcher can both outlive a
 * restart, so without the fence a restarted Worker would replay pre-restart windows as current
 * traffic.
 */
@Slf4j
public class ReportConsumer {

  /** Detection-plane meters; a meter-less instance is a no-op. */
  private final WorkerDetectionMetrics metrics;

  /** The detection path: window → rules → registry. */
  private final DetectionPipeline pipeline;

  /**
   * Optional staleness filter threshold in milliseconds; {@code 0} disables it. When enabled, only
   * a positive age exceeding the threshold drops a report — a negative age (reporter clock ahead)
   * always passes, since it cannot be distinguished from a healthy fast report.
   */
  private final long stalenessThresholdMs;

  /**
   * This Worker incarnation's epoch. A report routed to another incarnation must never enter the
   * current sliding window.
   */
  private final long workerEpoch;

  /**
   * Constructs a consumer.
   *
   * @param pipeline             the detection entry point
   * @param workerEpoch          this Worker incarnation's epoch
   * @param stalenessThresholdMs the optional report staleness threshold ({@code 0} disables it)
   * @param metrics              the detection-plane meters (a meter-less instance is a no-op)
   */
  public ReportConsumer(
    DetectionPipeline pipeline,
    long workerEpoch,
    long stalenessThresholdMs,
    WorkerDetectionMetrics metrics
  ) {
    this.metrics = metrics;
    this.pipeline = pipeline;
    this.workerEpoch = workerEpoch;
    this.stalenessThresholdMs = stalenessThresholdMs;
  }

  /**
   * Main entry point for batched report messages.
   *
   * @param message the deserialized message containing counts for multiple keys
   */
  @RabbitListener(queues = "#{@reportQueue.name}", containerFactory = "reportListenerContainerFactory")
  public void onReport(ReportMessage message) {
    try {
      consume(message);
    } catch (Exception e) {
      log.error(
        "Uncaught exception in onReport, discarding message to prevent poison-message requeue loop: appName={}",
        message != null ? message.appName() : "null",
        e
      );
    }
  }

  /**
   * Applies the transport gates in cost order — detection enabled, incarnation fence, optional
   * staleness — then evaluates the batch with one monotonic sample and one window slice. Malformed
   * entries are counted and skipped; a per-entry failure is logged and does not abort the batch.
   *
   * @param message the deserialized report batch
   */
  private void consume(ReportMessage message) {
    if (!pipeline.enabled()) {
      // Nothing downstream reads these counts: drop the batch without iterating it, and without
      // recording an evaluation sample that would dilute zeta.worker.report.eval.
      Map<String, Long> skipped = message.counts();
      metrics.countDisabledReport(skipped == null ? 0L : skipped.size());
      return;
    }

    if (message.targetEpoch() != workerEpoch) {
      metrics.countEpochDroppedReport();
      log.debug(
        "Dropped report for a previous Worker incarnation: appName={}, targetEpoch={}, currentEpoch={}",
        message.appName(),
        message.targetEpoch(),
        workerEpoch
      );
      return;
    }

    if (stalenessThresholdMs > 0) {
      long age = ageMillis(currentTimeMillis(), message.timestamp());
      if (age > stalenessThresholdMs) {
        metrics.countStaleDroppedReport();
        log.debug("Stale report dropped: appName={}, age={}ms", message.appName(), age);
        return;
      }
    }

    Map<String, Long> keyCounts = message.counts();
    // A malformed body can deserialize to a null counts map; an empty one carries nothing.
    if (keyCounts == null || keyCounts.isEmpty()) {
      return;
    }

    long evalStartNanos = metrics.startReportEval();
    // One monotonic sample for the whole batch: the entries share a message and therefore an
    // arrival instant, so the detector and the registry must not re-read the clock per key. The
    // window slice is derived from that sample once, for the same reason.
    long nowMs = TimeSource.monotonicMillis();
    long nowSlice = pipeline.sliceIndex(nowMs);

    // Sequential on the consumer thread: the listener container already runs several consumers,
    // and intra-batch parallelism would only amplify stripe-lock contention for no gain.
    int malformed = 0;
    for (Map.Entry<String, Long> entry : keyCounts.entrySet()) {
      try {
        String key = entry.getKey();
        // Deserialize-safe unboxing: a malformed body such as {"key": null} yields a null
        // value, and a non-positive count would push negative mass into the window.
        Long count = entry.getValue();
        if (key == null || count == null || count <= 0) {
          malformed++;
          continue;
        }

        pipeline.onReport(key, count, nowMs, nowSlice);
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

    if (malformed > 0 && log.isDebugEnabled()) {
      log.debug(
        "Skipped {} malformed report entr(ies): appName={}, batchSize={}",
        malformed,
        message.appName(),
        keyCounts.size()
      );
    }

    metrics.recordReportEval(evalStartNanos);
  }

  /**
   * Age of a report without wrapping at the {@code long} limits: an impossible timestamp
   * ({@code Long.MIN_VALUE}) is treated as maximally stale, while a future timestamp stays
   * negative and passes the filter.
   *
   * <p>A subtraction overflows exactly when the operands' signs differ and the difference's sign
   * differs from the minuend's; that is the test below, so the saturation needs no arithmetic
   * exception — a malformed batch must not allocate a stack trace on the consumer thread.
   *
   * @param now       the current wall-clock millisecond
   * @param timestamp the report's wall-clock millisecond
   * @return the age in milliseconds, or {@link Long#MAX_VALUE} for an impossible timestamp
   */
  private static long ageMillis(long now, long timestamp) {
    long diff = now - timestamp;
    return ((now ^ timestamp) & (now ^ diff)) < 0L ? Long.MAX_VALUE : diff;
  }
}
