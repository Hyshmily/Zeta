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
package io.github.hyshmily.zeta.worker.detection.rule;

import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import io.micrometer.core.instrument.Counter;
import java.util.concurrent.atomic.LongAdder;

/**
 * A published rule paired with the counters that belong to its id.
 *
 * <p>
 * The binding pairs the immutable rule with the counters of its id, so the
 * report path and the
 * cooling check read one object instead of chasing a second lookup through the
 * published rule set
 * and the metrics maps. The binding is never refreshed after publication: an
 * entry keeps the
 * binding that judged it, so a later publication cannot change the bar an
 * already-hot key is
 * measured against.
 *
 * <p>
 * A promotion captures the binding, not the id: the entry keeps the rule that
 * judged it and can
 * advance that rule's counters with field reads, instead of a second lookup
 * through the published
 * rule set and the metrics maps. The counters are shared by every binding of an
 * id, so a
 * publication that replaces a rule with the same id keeps its cumulative
 * statistics.
 *
 * <p>
 * A binding whose id has been removed is retired: a hot entry that cools
 * afterwards finishes
 * against its captured rule, but its late update lands on counters whose meters
 * the publication
 * already removed, so no meter series is recreated.
 */
@Internal
public final class RuleBinding {

  private final HotKeyRule rule;
  private final RuleCounters counters;

  /**
   * Creates a binding over one published rule. The rule is immutable, so every
   * threshold read is
   * a direct final-field access; the counters are supplied by the resolver and
   * may be shared with
   * an older binding of the same rule id.
   *
   * @param rule     the published rule; never {@code null}
   * @param counters the counters of the rule's id; never {@code null}
   */
  RuleBinding(HotKeyRule rule, RuleCounters counters) {
    this.rule = rule;
    this.counters = counters;
  }

  /** @return the rule this binding was published with */
  public HotKeyRule rule() {
    return rule;
  }

  /** @return the counters shared by every binding of this rule id */
  RuleCounters counters() {
    return counters;
  }

  /** @return eligible-report count */
  public long matches() {
    return counters.matches();
  }

  /** @return promotion count */
  public long promotions() {
    return counters.promotions();
  }

  /** @return aborted-promotion count */
  public long aborts() {
    return counters.aborts();
  }

  /** @return cool-out count */
  public long cools() {
    return counters.cools();
  }

  /** Records one resolved key against the rule. */
  public void recordMatch() {
    counters.match();
  }

  /** Records one promotion against the rule. */
  public void recordPromotion() {
    counters.promotion();
  }

  /** Records one promotion whose HOT decision never reached the broker. */
  public void recordAbort() {
    counters.abort();
  }

  /** Records one cool-down against the rule. */
  public void recordCool() {
    counters.cool();
  }

  /** Marks this id's counters retired and removes its meters. */
  void retire(WorkerDetectionMetrics metrics) {
    counters.retire(metrics);
  }
}

/**
 * The counters of one rule id.
 *
 * <p>
 * Every total is a {@link LongAdder} and is the value the endpoint and the
 * startup warning read: Micrometer's {@code Counter.count()} is not a cumulative
 * total on a step registry (OTLP, Datadog, StatsD, ...), and a meter-less
 * registry returns zero forever. The same increments also feed the three
 * Micrometer meters so a scrape still sees the series; the meters are a
 * reporting side channel, never the source of truth.
 */
final class RuleCounters {

  private final LongAdder matches = new LongAdder();
  private final LongAdder promotions = new LongAdder();
  private final LongAdder aborts = new LongAdder();
  private final LongAdder cools = new LongAdder();

  private Counter matchMeter;
  private Counter promotionMeter;
  private Counter coolMeter;

  /**
   * Registers the three counters for this rule id. Called once, before the
   * binding is published.
   *
   * @param metrics the detection meters
   * @param ruleId  the rule id that tags the series
   */
  void bind(WorkerDetectionMetrics metrics, String ruleId) {
    this.matchMeter = metrics.ruleMatches(ruleId);
    this.promotionMeter = metrics.rulePromotions(ruleId);
    this.coolMeter = metrics.ruleCools(ruleId);
  }

  /** @return the cumulative eligible-report count */
  long matches() {
    return matches.sum();
  }

  /** @return the cumulative promotion count */
  long promotions() {
    return promotions.sum();
  }

  /** @return the cumulative aborted-promotion count */
  long aborts() {
    return aborts.sum();
  }

  /** @return the cumulative cool-out count */
  long cools() {
    return cools.sum();
  }

  /** Records one eligible report. */
  void match() {
    matches.increment();
    if (matchMeter != null) {
      matchMeter.increment();
    }
  }

  /** Records one promotion. */
  void promotion() {
    promotions.increment();
    if (promotionMeter != null) {
      promotionMeter.increment();
    }
  }

  /** Records one aborted promotion. */
  void abort() {
    aborts.increment();
  }

  /** Records one cool-out. */
  void cool() {
    cools.increment();
    if (coolMeter != null) {
      coolMeter.increment();
    }
  }

  /**
   * Removes this id's three meters. A late update still lands on the adder and
   * the meter handle, but the removed meter is never scraped again; removal is
   * idempotent, and a partially bound counter skips the handles it never
   * received.
   *
   * @param metrics the detection meters that own the three registrations
   */
  void retire(WorkerDetectionMetrics metrics) {
    if (matchMeter != null) {
      metrics.remove(matchMeter);
    }
    if (promotionMeter != null) {
      metrics.remove(promotionMeter);
    }
    if (coolMeter != null) {
      metrics.remove(coolMeter);
    }
  }
}
