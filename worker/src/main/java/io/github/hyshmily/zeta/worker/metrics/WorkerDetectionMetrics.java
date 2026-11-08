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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import io.github.hyshmily.zeta.worker.detection.window.SlidingWindowDetector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.ToDoubleFunction;

/**
 * The detection plane's meters: what the window table holds, what the HOT set holds, what the
 * decision queues are doing, and where keys leave the hot regime.
 *
 * <p>Counters are created in the constructor; gauges and the cumulative function counters are
 * bound afterwards through {@link #bindMeters}, because they read components that themselves need
 * these meters to count a rejection or a promotion. A meter-less instance is a no-op: a null
 * registry is replaced by an empty {@link CompositeMeterRegistry}, so no call site branches on
 * metrics being wired.
 *
 * <p>Each decision type has its own delivery ledger. A HOT decision ends in exactly one of
 * {@code decisions.hot}, {@code dropped{type=hot}}, {@code elided{type=hot}},
 * {@code superseded{type=hot}} or {@code expired{type=hot}}; a COOL decision has the same ledger
 * under {@code decisions.cool} and {@code type=cool}. The seat ledger is separate:
 * {@code promotions == cools + hot.aborted + active} at a quiescent scrape (a promotion is counted
 * when the seat is taken; an undelivered HOT returns it and increments {@code hot.aborted}).
 */
public class WorkerDetectionMetrics {

  /** The three cool-down triggers; one pre-registered counter each keeps the tag off the hot path. */
  public enum CoolReason {
    /** The LOW_QPS dwell elapsed. */
    LOW_QPS("low_qps"),
    /** The EXPIRE deadline passed. */
    EXPIRE("expire"),
    /** The repair sweep found an overdue entry. */
    SWEEP("sweep");

    private final String tag;

    CoolReason(String tag) {
      this.tag = tag;
    }

    /** @return the meter tag value */
    public String tag() {
      return tag;
    }
  }

  /** Gauge: keys holding a live entry in the window table. */
  public static final String METRIC_DETECTOR_KEYS = "zeta.worker.detector.keys";
  /** FunctionCounter: idle windows reclaimed to admit a new key at the tracked-key cap. */
  public static final String METRIC_DETECTOR_CAPACITY_EVICTIONS = "zeta.worker.detector.capacity.evictions";
  /** FunctionCounter: new-key admissions the tracked-key cap rejected. */
  public static final String METRIC_DETECTOR_CAPACITY_REJECTIONS = "zeta.worker.detector.capacity.rejections";
  /** Gauge: keys currently holding a HOT seat. */
  public static final String METRIC_HOT_ACTIVE = "zeta.worker.hot.active";
  /** Gauge: HOT seat capacity. */
  public static final String METRIC_HOT_CAPACITY = "zeta.worker.hot.capacity";
  /** FunctionCounter: promotions refused because every seat was taken. */
  public static final String METRIC_HOT_REJECTED = "zeta.worker.hot.rejected";
  /** Counter: promotions whose HOT decision never reached delivery; the seat was returned. */
  public static final String METRIC_HOT_ABORTED = "zeta.worker.hot.aborted";
  /** Counter: HOT decisions handed to the AMQP client (no publisher confirm). */
  public static final String METRIC_DECISIONS_HOT = "zeta.worker.decisions.hot";
  /** Counter: COOL decisions handed to the AMQP client (no publisher confirm). */
  public static final String METRIC_DECISIONS_COOL = "zeta.worker.decisions.cool";
  /** Counter: decisions that entered delivery and never reached the send call. */
  public static final String METRIC_DECISIONS_DROPPED = "zeta.worker.decisions.dropped";
  /** Counter: decisions elided in memory because the cluster's state would not change. */
  public static final String METRIC_DECISIONS_ELIDED = "zeta.worker.decisions.elided";
  /** Counter: decisions dropped because a newer version for the key was already queued or accepted. */
  public static final String METRIC_DECISIONS_SUPERSEDED = "zeta.worker.decisions.superseded";
  /** Counter: decisions dropped because they waited longer than {@code max-decision-age-ms}. */
  public static final String METRIC_DECISIONS_EXPIRED = "zeta.worker.decisions.expired";
  /** Gauge: pending HOT decisions. */
  public static final String METRIC_BROADCAST_HOT_DEPTH = "zeta.worker.broadcast.hot.depth";
  /** Gauge: pending COOL decisions. */
  public static final String METRIC_BROADCAST_COOL_DEPTH = "zeta.worker.broadcast.cool.depth";
  /** Gauge: checks currently linked in the cool-down wheels. */
  public static final String METRIC_COOLING_SCHEDULED = "zeta.worker.cooling.scheduled";
  /** Timer: how late a wheel-fired due check was processed (the repair sweep is excluded). */
  public static final String METRIC_COOLING_LATENESS = "zeta.worker.cooling.lateness";
  /** Counter: LOW_QPS re-checks whose window was at or above the cool band. */
  public static final String METRIC_COOLING_RECHECK = "zeta.worker.cooling.recheck";
  /** Counter tagged {@code reason}: cool-downs by trigger. */
  public static final String METRIC_COOLING_REASON = "zeta.worker.cooling.reason";
  /** FunctionCounter tagged {@code rule}: reports that cleared the rule's promotion threshold. */
  public static final String METRIC_RULE_MATCHES = "zeta.worker.rule.matches";
  /** FunctionCounter tagged {@code rule}: promotions per rule. */
  public static final String METRIC_RULE_PROMOTIONS = "zeta.worker.rule.promotions";
  /** FunctionCounter tagged {@code rule}: cool-downs per rule. */
  public static final String METRIC_RULE_COOLS = "zeta.worker.rule.cools";
  /** Timer: per-report-batch evaluation latency. */
  public static final String METRIC_REPORT_EVAL = "zeta.worker.report.eval";
  /** Counter: reports discarded because detection is disabled. */
  public static final String METRIC_DETECTION_DISABLED_DROPPED = "zeta.worker.detection.disabled.dropped";
  /** Counter: report batches dropped by the incarnation fence. */
  public static final String METRIC_REPORTS_EPOCH_DROPPED = "zeta.worker.reports.epoch.dropped";
  /** Counter: report batches dropped by the optional staleness filter. */
  public static final String METRIC_REPORTS_STALE_DROPPED = "zeta.worker.reports.stale.dropped";

  private final MeterRegistry registry;

  /** The two series behind one delivery-outcome counter, selected by decision type. */
  private record DecisionCounters(Counter hot, Counter cool) {
    private void increment(ZetaDecision.DecisionType type) {
      switch (type) {
        case HOT -> hot.increment();
        case COOL -> cool.increment();
        case NONE -> throw new IllegalArgumentException("NONE never enters delivery");
      }
    }
  }

  private final DecisionCounters supersededDecisions;
  private final DecisionCounters droppedDecisions;
  private final DecisionCounters expiredDecisions;
  private final DecisionCounters elidedDecisions;
  private final DecisionCounters sentDecisions;
  private final Counter epochDroppedReports;
  private final Counter staleDroppedReports;
  private final Counter abortedPromotions;
  private final Counter disabledReports;
  private final Timer coolingLateness;
  private final Counter lowQpsCools;
  private final Counter expireCools;
  private final Counter sweepCools;
  private final Counter rechecks;
  private final Timer reportEval;

  /** Whether {@link #bindMeters} already registered the gauges; registered once. */
  @GuardedBy("this")
  private boolean metersBound;

  /**
   * Creates the counters.
   *
   * @param registry the meter registry, or {@code null} to keep every meter a no-op
   */
  public WorkerDetectionMetrics(MeterRegistry registry) {
    this.registry = registry != null ? registry : new CompositeMeterRegistry();
    MeterRegistry reg = this.registry;
    this.sentDecisions = new DecisionCounters(
      counter(reg, METRIC_DECISIONS_HOT, "HOT decisions handed to the AMQP client"),
      counter(reg, METRIC_DECISIONS_COOL, "COOL decisions handed to the AMQP client")
    );
    this.droppedDecisions = decisionCounters(
      reg,
      METRIC_DECISIONS_DROPPED,
      "Decisions that entered delivery and never reached the send call (send failure, queue saturation, shutdown)"
    );
    this.elidedDecisions = decisionCounters(
      reg,
      METRIC_DECISIONS_ELIDED,
      "Decisions elided in memory because the cluster's state would not change"
    );
    this.supersededDecisions = decisionCounters(
      reg,
      METRIC_DECISIONS_SUPERSEDED,
      "Decisions dropped because a newer version for the key was already queued or accepted"
    );
    this.expiredDecisions = decisionCounters(
      reg,
      METRIC_DECISIONS_EXPIRED,
      "Decisions dropped because they waited longer than broadcast.max-decision-age-ms"
    );
    this.abortedPromotions = counter(
      reg,
      METRIC_HOT_ABORTED,
      "Promotions whose HOT decision never reached delivery; the seat was returned"
    );
    this.rechecks = counter(
      reg,
      METRIC_COOLING_RECHECK,
      "LOW_QPS re-checks whose window was at or above the cool band"
    );
    this.disabledReports = counter(
      reg,
      METRIC_DETECTION_DISABLED_DROPPED,
      "Reports skipped because detection is disabled"
    );
    this.epochDroppedReports = counter(
      reg,
      METRIC_REPORTS_EPOCH_DROPPED,
      "Report batches dropped by the incarnation fence"
    );
    this.staleDroppedReports = counter(
      reg,
      METRIC_REPORTS_STALE_DROPPED,
      "Report batches dropped by the staleness filter"
    );
    this.reportEval = Timer.builder(METRIC_REPORT_EVAL)
      .description("Per-report-batch evaluation latency, monotonic nanoseconds, excluding AMQP queue wait")
      .register(reg);
    this.coolingLateness = Timer.builder(METRIC_COOLING_LATENESS)
      .description("How late a due cool-down check was processed")
      .register(reg);
    this.lowQpsCools = coolReasonCounter(CoolReason.LOW_QPS);
    this.expireCools = coolReasonCounter(CoolReason.EXPIRE);
    this.sweepCools = coolReasonCounter(CoolReason.SWEEP);
  }

  private Counter coolReasonCounter(CoolReason reason) {
    return Counter.builder(METRIC_COOLING_REASON)
      .tag("reason", reason.tag())
      .description("Cool-downs by trigger")
      .register(registry);
  }

  /**
   * Binds the gauges and the cumulative function counters; called once, after the components they
   * read exist.
   *
   * <p>The depth suppliers are synthesized by the binder rather than owned by a field. Micrometer
   * holds a gauge's state object weakly by default, so these three gauges opt into a strong
   * reference; otherwise the first young collection could clear the supplier and leave the gauge
   * reading {@link Double#NaN}. The captured dispatcher and wheels are application-lifetime beans,
   * so the strong reference does not extend anything that would otherwise be collectible.
   *
   * @param detector         the window table
   * @param hotRegistry      the HOT set
   * @param hotQueueDepth    pending HOT decisions
   * @param coolQueueDepth   pending COOL decisions
   * @param coolingScheduled checks linked in the cool-down wheels
   */
  public synchronized void bindMeters(
    SlidingWindowDetector detector,
    HotKeyRegistry hotRegistry,
    IntSupplier hotQueueDepth,
    IntSupplier coolQueueDepth,
    IntSupplier coolingScheduled
  ) {
    if (metersBound) {
      return;
    }
    metersBound = true;
    Gauge.builder(METRIC_DETECTOR_KEYS, detector, SlidingWindowDetector::activeWindows)
      .description("Keys holding a live sliding-window entry")
      .register(registry);
    FunctionCounter.builder(METRIC_DETECTOR_CAPACITY_EVICTIONS, detector, SlidingWindowDetector::capacityEvictions)
      .description("Idle windows reclaimed to admit a new key at max-tracked-keys")
      .register(registry);
    FunctionCounter.builder(METRIC_DETECTOR_CAPACITY_REJECTIONS, detector, SlidingWindowDetector::rejectedNewKeys)
      .description("New-key admissions rejected because the tracked-key cap was reached")
      .register(registry);
    Gauge.builder(METRIC_HOT_ACTIVE, hotRegistry, HotKeyRegistry::activeCount)
      .description("Keys currently holding a HOT seat")
      .register(registry);
    Gauge.builder(METRIC_HOT_CAPACITY, hotRegistry, HotKeyRegistry::capacity)
      .description("HOT seat capacity (detection.limits.max-active-hot-keys)")
      .register(registry);
    FunctionCounter.builder(METRIC_HOT_REJECTED, hotRegistry, HotKeyRegistry::rejectedCount)
      .description("Promotions refused because every HOT seat was taken")
      .register(registry);
    Gauge.builder(METRIC_BROADCAST_HOT_DEPTH, hotQueueDepth, IntSupplier::getAsInt)
      .description("Pending HOT decisions")
      .strongReference(true)
      .register(registry);
    Gauge.builder(METRIC_BROADCAST_COOL_DEPTH, coolQueueDepth, IntSupplier::getAsInt)
      .description("Pending COOL decisions")
      .strongReference(true)
      .register(registry);
    Gauge.builder(METRIC_COOLING_SCHEDULED, coolingScheduled, IntSupplier::getAsInt)
      .description("Cool-down checks currently linked in the wheels")
      .strongReference(true)
      .register(registry);
  }

  /**
   * Counts one decision handed to the AMQP client. Every other delivery outcome already selects its
   * series by decision type; the send success does too, instead of leaving the caller to branch and
   * call one of two methods.
   *
   * @param type the decision type
   */
  public void countSentDecision(ZetaDecision.DecisionType type) {
    sentDecisions.increment(type);
  }

  /**
   * Counts one decision lost in delivery: a failed publish, a saturation refusal or the shutdown
   * drain.
   *
   * @param type the decision type
   */
  public void countDroppedDecision(ZetaDecision.DecisionType type) {
    droppedDecisions.increment(type);
  }

  /**
   * Counts one decision elided in memory because the cluster's state would not change: a replay of
   * the exact accepted version, a same-type decision inside the debounce window, a COOL for a key
   * the cluster already holds cold, or a COOL whose displaced HOT never reached the funnel.
   *
   * @param type the decision type
   */
  public void countElidedDecision(ZetaDecision.DecisionType type) {
    elidedDecisions.increment(type);
  }

  /**
   * Counts one decision dropped because a newer version for the key was already queued or
   * accepted.
   *
   * @param type the decision type
   */
  public void countSupersededDecision(ZetaDecision.DecisionType type) {
    supersededDecisions.increment(type);
  }

  /**
   * Counts one decision dropped for exceeding the maximum queue residency.
   *
   * @param type the decision type
   */
  public void countExpiredDecision(ZetaDecision.DecisionType type) {
    expiredDecisions.increment(type);
  }

  /** Counts one promotion whose HOT decision never reached delivery. */
  public void countAbortedPromotion() {
    abortedPromotions.increment();
  }

  /**
   * Counts one LOW_QPS re-check whose window was at or above the cool band. A
   * {@code COOL_FOR_EXPIRE} entry cools on its deadline and never re-arms, so it never increments
   * this counter.
   */
  public void countRecheck() {
    rechecks.increment();
  }

  /**
   * Counts reports skipped because detection is disabled, without iterating them.
   *
   * @param reports the number of keys in the dropped batch
   */
  public void countDisabledReport(long reports) {
    if (reports > 0) {
      disabledReports.increment(reports);
    }
  }

  /** Counts one report dropped by the incarnation fence. */
  public void countEpochDroppedReport() {
    epochDroppedReports.increment();
  }

  /** Counts one report dropped by the staleness filter. */
  public void countStaleDroppedReport() {
    staleDroppedReports.increment();
  }

  /**
   * Counts one cool-down by trigger.
   *
   * @param reason the trigger
   */
  public void countCoolReason(CoolReason reason) {
    switch (reason) {
      case LOW_QPS -> lowQpsCools.increment();
      case EXPIRE -> expireCools.increment();
      case SWEEP -> sweepCools.increment();
    }
  }

  /**
   * Registers the {@code rule} function counter for one rule id the first time that id is
   * published; a later publication that keeps the id reuses the same {@code RuleCounters}, so the
   * meter is bound once. The hot path then only increments the rule's counter field, and the
   * scrape reads the same field through this binding.
   *
   * <p>The state object is the counters instance itself and the extraction function is a
   * non-capturing method reference, which the JVM holds as a singleton. Micrometer keeps the
   * <em>state object</em> weakly and has no strong-reference option for a
   * {@code FunctionCounter}, so the state must be the object that is retained for as long as the
   * rule is published — never a lambda created just for the registration. A temporary lambda as
   * the state would be collected after the first young collection and the counter would stop
   * advancing.
   *
   * @param ruleId  the rule id
   * @param state   the rule's counters, retained by the published binding
   * @param matches reads the rule's cumulative eligible-report count from {@code state}
   * @param <T>     the counters type
   * @return the match meter, retained so a retired rule can remove it
   */
  public <T> FunctionCounter ruleMatchCounter(String ruleId, T state, ToDoubleFunction<T> matches) {
    return ruleCounter(
      METRIC_RULE_MATCHES,
      ruleId,
      "Reports that cleared the rule's promotion threshold",
      state,
      matches
    );
  }

  /**
   * Registers the promotion counter for one rule id.
   *
   * @param ruleId     the rule id
   * @param state      the rule's counters, retained by the published binding
   * @param promotions reads the rule's cumulative promotion count from {@code state}
   * @param <T>        the counters type
   * @return the promotion meter, retained so a retired rule can remove it
   */
  public <T> FunctionCounter rulePromotionCounter(String ruleId, T state, ToDoubleFunction<T> promotions) {
    return ruleCounter(METRIC_RULE_PROMOTIONS, ruleId, "Promotions per rule", state, promotions);
  }

  /**
   * Registers the cool-down counter for one rule id.
   *
   * @param ruleId the rule id
   * @param state  the rule's counters, retained by the published binding
   * @param cools  reads the rule's cumulative cool-down count from {@code state}
   * @param <T>    the counters type
   * @return the cool-down meter, retained so a retired rule can remove it
   */
  public <T> FunctionCounter ruleCoolCounter(String ruleId, T state, ToDoubleFunction<T> cools) {
    return ruleCounter(METRIC_RULE_COOLS, ruleId, "Cool-downs per rule", state, cools);
  }

  /**
   * Removes a meter from the registry when its rule is retired.
   *
   * @param meter the retired meter
   */
  public void remove(Meter meter) {
    registry.remove(meter);
  }

  /**
   * Takes the monotonic start stamp for one report batch.
   *
   * @return {@link System#nanoTime()}
   */
  public long startReportEval() {
    return System.nanoTime();
  }

  /**
   * Records one report-batch evaluation sample.
   *
   * @param startNanos the stamp from {@link #startReportEval()}
   */
  public void recordReportEval(long startNanos) {
    reportEval.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Records how late a wheel-fired due cool-down check was processed. The repair sweep's grace is
   * excluded.
   *
   * @param latenessMs non-negative lateness
   */
  public void recordCoolingLateness(long latenessMs) {
    coolingLateness.record(latenessMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Binds one tagged rule counter to the counters object that must stay strongly reachable for the
   * meter's weak reference.
   *
   * @param name        the meter name
   * @param ruleId      the rule tag value
   * @param description the meter description
   * @param state       the rule's counters
   * @param value       reads the rule's cumulative count from {@code state}
   * @param <T>         the counters type
   * @return the registered meter
   */
  private <T> FunctionCounter ruleCounter(
    String name,
    String ruleId,
    String description,
    T state,
    ToDoubleFunction<T> value
  ) {
    return FunctionCounter.builder(name, state, value).tag("rule", ruleId).description(description).register(registry);
  }

  private static Counter counter(MeterRegistry registry, String name, String description) {
    return Counter.builder(name).description(description).register(registry);
  }

  private static DecisionCounters decisionCounters(MeterRegistry registry, String name, String description) {
    return new DecisionCounters(
      Counter.builder(name).tag("type", "hot").description(description).register(registry),
      Counter.builder(name).tag("type", "cool").description(description).register(registry)
    );
  }
}
