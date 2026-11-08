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
import io.micrometer.core.instrument.FunctionCounter;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.LongAdder;

/**
 * A published rule paired with the counters that belong to its id.
 *
 * <p>The binding pairs the immutable rule with the counters of its id, so the report path and the
 * cooling check read one object instead of chasing a second lookup through the published rule set
 * and the metrics maps. The binding is never refreshed after publication: an entry keeps the
 * binding that judged it, so a later publication cannot change the bar an already-hot key is
 * measured against.
 *
 * <p>A promotion captures the binding, not the id: the entry keeps the rule that judged it and can
 * advance that rule's counters with field reads, instead of a second lookup through the published
 * rule set and the metrics maps. The counters are shared by every binding of an id, so a
 * publication that replaces a rule with the same id keeps its cumulative statistics.
 *
 * <p>A binding whose id has been removed is retired: a hot entry that cools afterwards finishes
 * against its captured rule, but its late update lands on counters whose meters the publication
 * already removed, so no meter series is recreated.
 */
@Internal
public final class RuleBinding {

  private final HotKeyRule rule;
  private final RuleCounters counters;

  /**
   * Creates a binding over one published rule. The rule is immutable, so every threshold read is
   * a direct final-field access; the counters are supplied by the resolver and may be shared with
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
 * <p>{@code matches} is a {@link LongAdder}: every eligible report from any consumer thread
 * increments it, and a default rule concentrates all of that traffic on one counter. The other
 * three are plain {@code volatile long} fields updated through {@link VarHandle}s because their
 * writes happen under a key's stripe lock. The endpoint reads the values directly; the three
 * meters read them with this object as their Micrometer state and a non-capturing method
 * reference as their extraction function, so the meter's weak reference points at the object the
 * published binding retains.
 */
final class RuleCounters {

  private static final VarHandle PROMOTIONS;
  private static final VarHandle ABORTS;
  private static final VarHandle COOLS;

  static {
    // VarHandles are resolved once per class and are the only writers of the three stripe-guarded
    // fields. A missing field is a programming error, so fail class initialization rather than
    // leaving a counter that silently never advances.
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      PROMOTIONS = lookup.findVarHandle(RuleCounters.class, "promotions", long.class);
      ABORTS = lookup.findVarHandle(RuleCounters.class, "aborts", long.class);
      COOLS = lookup.findVarHandle(RuleCounters.class, "cools", long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final LongAdder matches = new LongAdder();

  @SuppressWarnings("unused")
  private volatile long promotions;

  @SuppressWarnings("unused")
  private volatile long aborts;

  @SuppressWarnings("unused")
  private volatile long cools;

  private FunctionCounter promotionMeter;
  private FunctionCounter matchMeter;
  private FunctionCounter coolMeter;

  /**
   * Registers the three function counters that read this rule's fields. Called once, before the
   * binding is published, so the hot path never sees an unbound counter.
   *
   * <p>Each meter's state object is this instance, which the published {@link RuleBinding} keeps
   * reachable, and each extraction function is a non-capturing method reference the JVM holds as a
   * singleton. A lambda created for the registration would be the meter's weak state object:
   * collected after the first young collection, leaving a counter that never advances.
   *
   * @param metrics the detection meters
   * @param ruleId  the rule id that tags the series
   */
  void bind(WorkerDetectionMetrics metrics, String ruleId) {
    this.matchMeter = metrics.ruleMatchCounter(ruleId, this, RuleCounters::matches);
    this.promotionMeter = metrics.rulePromotionCounter(ruleId, this, RuleCounters::promotions);
    this.coolMeter = metrics.ruleCoolCounter(ruleId, this, RuleCounters::cools);
  }

  /** @return the cumulative eligible-report count */
  long matches() {
    return matches.sum();
  }

  /** @return the cumulative promotion count */
  long promotions() {
    return (long) PROMOTIONS.getVolatile(this);
  }

  /** @return the cumulative aborted-promotion count */
  long aborts() {
    return (long) ABORTS.getVolatile(this);
  }

  /** @return the cumulative cool-out count */
  long cools() {
    return (long) COOLS.getVolatile(this);
  }

  /** Records one eligible report. */
  void match() {
    matches.increment();
  }

  /** Records one promotion. */
  void promotion() {
    PROMOTIONS.getAndAdd(this, 1L);
  }

  /** Records one aborted promotion. */
  void abort() {
    ABORTS.getAndAdd(this, 1L);
  }

  /** Records one cool-out. */
  void cool() {
    COOLS.getAndAdd(this, 1L);
  }

  /**
   * Removes this id's three meters. A late update still lands on the fields, but the removed meter
   * is never scraped again; removal is idempotent, so a second retirement is a no-op.
   *
   * @param metrics the detection meters that own the three registrations
   */
  void retire(WorkerDetectionMetrics metrics) {
    metrics.remove(matchMeter);
    metrics.remove(promotionMeter);
    metrics.remove(coolMeter);
  }
}
