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
package io.github.hyshmily.zeta.worker.metrics;

import io.github.hyshmily.zeta.worker.detection.SlidingWindowDetector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;

/**
 * The Worker detection plane's measurement baseline (ADR-0080): the meters a
 * detection-architecture change can be measured against, before a baseline a
 * proposed change can only be argued about.
 *
 * <h2>Why these meters live in the worker module</h2>
 * Every one of them reads a <em>worker</em>-module type —
 * {@link SlidingWindowDetector} (the per-key window map), {@code WorkerBroadcaster}
 * (the decision send funnel) and {@code ReportConsumer} (the per-batch evaluation
 * loop) — while the common module's {@code ZetaMicrometerAutoConfiguration} only
 * sees common-module types (it injects {@code ObjectProvider<ZetaBayesianSM>}, the
 * common interface). Registering them there would require either a common→worker
 * dependency or a new public interface in common exposing worker-private state. The
 * worker's own {@code WorkerAutoConfiguration} registers them instead.
 *
 * <h2>The meters</h2>
 * <table>
 *   <tr><th>Meter</th><th>Type</th><th>Source</th></tr>
 *   <tr><td>{@value #METRIC_DETECTOR_KEYS}</td><td>Gauge</td>
 *       <td>{@link SlidingWindowDetector#getActiveKeyCount()} — keys holding a live
 *           window entry. This is the Worker's dominant per-key memory holder
 *           (~418.5 B/key of the ~678 B/key total, ADR-0080 §3) and, unlike the
 *           pre-existing {@code zeta.worker.tracked.keys} gauge
 *           ({@code ZetaBayesianSM.getTrackedKeys()}, which by construction counts
 *           only keys that have ever been hot), it sees every reported key.</td></tr>
 *   <tr><td>{@value #METRIC_DECISIONS_HOT} / {@value #METRIC_DECISIONS_COOL}</td>
 *       <td>Counters</td>
 *       <td>HOT/COOL decisions actually emitted to the cluster, counted at the
 *           single send funnel — see {@link #countHotDecision()}.</td></tr>
 *   <tr><td>{@value #METRIC_REPORT_EVAL}</td><td>Timer</td>
 *       <td>The per-key evaluation loop of {@code ReportConsumer.doOnReport},
 *           timed on the monotonic clock only — see {@link #startReportEval()}.</td></tr>
 * </table>
 *
 * <p>All three are registered by {@code WorkerAutoConfiguration} only when a
 * {@link MeterRegistry} bean is present; an instance constructed without one keeps
 * every meter unregistered and every recording method a no-op.
 */
public class WorkerDetectionMetrics {

  /** Gauge: keys holding a live entry in the detector's per-key window map. */
  public static final String METRIC_DETECTOR_KEYS = "zeta.worker.detector.keys";

  /** Counter: HOT decisions successfully emitted to the cluster. */
  public static final String METRIC_DECISIONS_HOT = "zeta.worker.decisions.hot";

  /** Counter: COOL decisions successfully emitted to the cluster. */
  public static final String METRIC_DECISIONS_COOL = "zeta.worker.decisions.cool";

  /** Timer: per-report-batch evaluation latency, recorded in nanoseconds. */
  public static final String METRIC_REPORT_EVAL = "zeta.worker.report.eval";

  /**
   * HOT decision counter; {@code null} when no registry was supplied.
   */
  private final Counter hotDecisions;

  /**
   * COOL decision counter; {@code null} when no registry was supplied.
   */
  private final Counter coolDecisions;

  /**
   * Per-report-batch evaluation timer; {@code null} when no registry was supplied.
   */
  private final Timer reportEval;

  /**
   * Registers the Worker detection-plane meters.
   *
   * @param registry the registry to register into, or {@code null} to register
   *                 nothing (micrometer absent, or metrics auto-configuration
   *                 switched off) — every recording method then no-ops
   * @param detector the detector whose live window-map size is gauged; must not be
   *                 {@code null} when a registry is supplied
   */
  public WorkerDetectionMetrics(MeterRegistry registry, SlidingWindowDetector detector) {
    if (registry == null) {
      this.hotDecisions = null;
      this.coolDecisions = null;
      this.reportEval = null;
      return;
    }

    this.hotDecisions = Counter.builder(METRIC_DECISIONS_HOT)
      .description(
        "HOT decisions successfully emitted to the cluster (a failed send is rolled back and re-emitted, so " +
          "attempts are deliberately not counted)"
      )
      .register(registry);
    this.coolDecisions = Counter.builder(METRIC_DECISIONS_COOL)
      .description(
        "COOL decisions successfully emitted to the cluster (a failed send is rolled back and re-emitted, so " +
          "attempts are deliberately not counted)"
      )
      .register(registry);
    this.reportEval = Timer.builder(METRIC_REPORT_EVAL)
      .description(
        "Per-report-batch evaluation latency of ReportConsumer's per-key loop, measured on the monotonic clock " +
          "with nanosecond resolution; excludes AMQP queue wait (never a cross-host wall-clock age) and, in the " +
          "buffered broadcast mode, the decision sends themselves"
      )
      .register(registry);
    Gauge.builder(METRIC_DETECTOR_KEYS, detector, SlidingWindowDetector::getActiveKeyCount)
      .description(
        "Keys holding a live sliding-window entry in the Worker detector — the per-key state that dominates " +
          "Worker memory (zeta.worker.tracked.keys only counts keys that have ever been hot)"
      )
      .register(registry);
  }

  /**
   * Counts one HOT decision successfully emitted to the cluster.
   *
   * <p>Called from {@code WorkerBroadcaster.broadcastHot} immediately after the AMQP
   * send returned, i.e. for emissions only — never for attempts: a failed send makes
   * the broadcaster return {@code false}, the caller rolls the key's state machine
   * back and the decision is re-emitted by the next evaluation or by the periodic
   * HOT rebroadcast (ADR-0024), so counting attempts would count one decision twice.
   * The 100 ms per-key dedup elision emits nothing (that decision was already
   * counted when it was sent) and is deliberately not counted either.
   */
  public void countHotDecision() {
    if (hotDecisions != null) {
      hotDecisions.increment();
    }
  }

  /**
   * Counts one COOL decision successfully emitted to the cluster.
   *
   * <p>Same emission-only semantics as {@link #countHotDecision()}; COOL has no
   * periodic rebroadcast (ADR-0024), so a lost COOL is only re-derived by the state
   * machine's next COOL transition.
   */
  public void countCoolDecision() {
    if (coolDecisions != null) {
      coolDecisions.increment();
    }
  }

  /**
   * Takes the monotonic start stamp for one report batch's evaluation timing.
   *
   * <p>{@link System#nanoTime()} only. The report's wall-clock age
   * ({@code now - message.timestamp()}) must never be used: it crosses hosts, and an
   * App whose clock lags the Worker is indistinguishable from a delayed report —
   * the same reason {@code ReportConsumer}'s optional staleness filter is disabled
   * by default.
   *
   * @return the current monotonic nanosecond stamp
   */
  public long startReportEval() {
    return System.nanoTime();
  }

  /**
   * Records one per-report-batch evaluation sample, with nanosecond resolution
   * ({@code Timer.record(elapsed, NANOSECONDS)}).
   *
   * <p>Like every Micrometer timer, the value is stored and exported in the
   * registry's own base time unit — seconds for the standard registries, hence the
   * {@code _seconds} suffix on a Prometheus export — but the measured quantity is a
   * monotonic nanosecond delta, never a wall-clock timestamp difference.
   *
   * <p>The sample covers the per-key evaluation loop of
   * {@code ReportConsumer.doOnReport} and the decision dispatch that runs inline
   * inside it. It excludes the staleness filter and the global-ratio sampling that
   * precede the loop, and it excludes AMQP queue wait by construction (the clock is
   * read around the loop, not taken from the message). In the buffered broadcast
   * mode (ADR-0061, the default) the sends run on the buffer's drain thread and are
   * therefore outside the sample; only the legacy synchronous mode folds them in.
   *
   * @param startNanos the stamp returned by {@link #startReportEval()} for this batch
   */
  public void recordReportEval(long startNanos) {
    if (reportEval != null) {
      reportEval.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }
  }
}
