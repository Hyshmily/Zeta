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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Every rule type counts the same quantity: one match per report that clears its threshold. */
class RuleResolverTest {

  private SimpleMeterRegistry registry;
  private WorkerDetectionMetrics metrics;
  private RuleResolver resolver;
  private HotKeyRule rule;
  private HotKeyRule defaults;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new WorkerDetectionMetrics(registry);
    rule = new HotKeyRule("r", MatchType.PREFIX, "k", 0, 0, 5, 0L, 0L, 60_000L, null);
    defaults = new HotKeyRule("defaults", MatchType.REGEX, ".*", Integer.MIN_VALUE, -1, 100, 0L, 0L, 60_000L, null);
    resolver = new RuleResolver(RuleSet.of(List.of(rule), defaults), 4, metrics);
  }

  @AfterEach
  void tearDown() {
    registry.close();
  }

  @Test
  void everyEligibleReportCountsOneMatch() {
    assertThat(resolver.resolveIfEligible("k1", 5)).isSameAs(resolver.binding("r"));
    assertThat(resolver.resolveIfEligible("k1", 5)).isSameAs(resolver.binding("r"));

    assertThat(resolver.binding("r").matches()).isEqualTo(2);
    assertThat(
      registry.get(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "r").functionCounter().count()
    ).isEqualTo(2.0d);
  }

  @Test
  void theDefaultRuleCountsEveryEligibleReport() {
    assertThat(resolver.resolveIfEligible("other", 100).rule().id()).isEqualTo("defaults");
    assertThat(resolver.resolveIfEligible("other", 100).rule().id()).isEqualTo("defaults");

    assertThat(resolver.binding("defaults").matches()).isEqualTo(2);
    assertThat(
      registry.get(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "defaults").functionCounter().count()
    ).isEqualTo(2.0d);
  }

  /**
   * A publication retires the removed rule's counters and meters. A key promoted under the old
   * rule can still cool afterwards; that late update must not recreate the retired series.
   */
  @Test
  void publishingARuleSetRetiresTheRemovedRulesCounters() {
    assertThat(resolver.resolveIfEligible("k1", 5)).isSameAs(resolver.binding("r"));
    assertThat(
      registry.find(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "r").functionCounter()
    ).isNotNull();
    RuleBinding retired = resolver.binding("r");

    HotKeyRule replacement = new HotKeyRule("replacement", MatchType.PREFIX, "k", 0, 0, 5, 0L, 0L, 60_000L, null);
    resolver.publish(RuleSet.of(List.of(replacement), defaults));

    assertThat(registry.find(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "r").functionCounter())
      .as("the retired rule's meter is removed from the registry")
      .isNull();

    retired.recordCool();
    retired.recordPromotion();
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_RULE_COOLS).tag("rule", "r").functionCounter())
      .as("a late cool-down does not resurrect the retired meter")
      .isNull();
    assertThat(
      registry.find(WorkerDetectionMetrics.METRIC_RULE_PROMOTIONS).tag("rule", "r").functionCounter()
    ).isNull();
  }

  /**
   * A publication that keeps an id replaces the rule object but not the id's cumulative counters.
   * The entry that captured the old binding and the next promotion share one counter set.
   */
  @Test
  void aPublicationThatKeepsAnIdKeepsItsCumulativeCounters() {
    RuleBinding before = resolver.binding("r");
    before.recordPromotion();

    HotKeyRule replacement = new HotKeyRule("r", MatchType.PREFIX, "k", 0, 0, 5, 0L, 0L, 120_000L, null);
    resolver.publish(RuleSet.of(List.of(replacement), defaults));

    RuleBinding after = resolver.binding("r");
    assertThat(after).isNotSameAs(before);
    assertThat(after.promotions()).isEqualTo(1);

    after.recordPromotion();
    assertThat(before.promotions()).as("the old entry and the new binding share the counters").isEqualTo(2);
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_RULE_PROMOTIONS).tag("rule", "r").functionCounter())
      .as("a retained rule keeps its meter")
      .isNotNull();
  }

  /**
   * An identical fingerprint is a no-op: the snapshot, its bindings and its match cache stay in
   * place, so a configuration refresh that changes nothing does not churn the cache.
   */
  @Test
  void republishingAnIdenticalRuleSetKeepsTheSnapshotAndBindings() {
    RuleBinding before = resolver.binding("r");
    RuleResolver.Snapshot snapshot = resolver.snapshot();

    resolver.publish(RuleSet.of(List.of(rule), defaults));

    assertThat(resolver.snapshot()).isSameAs(snapshot);
    assertThat(resolver.binding("r")).isSameAs(before);
  }

  /**
   * A publication that changes one rule keeps the binding of every unchanged rule, so a threshold
   * edit does not rebuild the rest of the set.
   */
  @Test
  void anUnchangedRuleKeepsItsBindingAcrossAPublication() {
    RuleBinding before = resolver.binding("r");
    HotKeyRule added = new HotKeyRule("added", MatchType.PREFIX, "x", 0, 0, 5, 0L, 0L, 60_000L, null);

    resolver.publish(RuleSet.of(List.of(rule, added), defaults));

    assertThat(resolver.binding("r")).isSameAs(before);
    assertThat(resolver.binding("added")).isNotNull();
  }

  /**
   * A regex rule is unchanged when its expression text is unchanged, even though the configuration
   * gate compiles a new {@code Pattern} on every refresh. Record equality would rebuild the binding
   * for a recompiled pattern; the definition text keeps it.
   */
  @Test
  void anUnchangedRegexRuleKeepsItsBindingAcrossAPublication() {
    defaults = new HotKeyRule("rx-defaults", MatchType.REGEX, ".*", Integer.MIN_VALUE, -1, 100, 0L, 0L, 60_000L, null);
    HotKeyRule regex = new HotKeyRule("rx", MatchType.REGEX, "user:.*", 0, 0, 5, 0L, 0L, 60_000L, null);
    RuleResolver regexResolver = new RuleResolver(RuleSet.of(List.of(regex), defaults), 4, metrics);
    RuleBinding before = regexResolver.binding("rx");

    HotKeyRule recompiled = new HotKeyRule("rx", MatchType.REGEX, "user:.*", 0, 0, 5, 0L, 0L, 60_000L, null);
    HotKeyRule added = new HotKeyRule("added", MatchType.PREFIX, "x", 0, 0, 5, 0L, 0L, 60_000L, null);
    regexResolver.publish(RuleSet.of(List.of(recompiled, added), defaults));

    assertThat(regexResolver.binding("rx")).isSameAs(before);
    assertThat(regexResolver.binding("added")).isNotNull();
  }

  @Test
  void indexesEffectiveExactRulesAndOmitsShadowedOnes() {
    HotKeyRule shadowingPrefix = new HotKeyRule(
      "user-prefix",
      MatchType.PREFIX,
      "user:",
      1,
      0,
      50,
      0L,
      0L,
      60_000L,
      null
    );
    HotKeyRule shadowedExact = new HotKeyRule("shadowed", MatchType.EXACT, "user:42", 0, 1, 5, 0L, 0L, 60_000L, null);
    HotKeyRule effectiveExact = new HotKeyRule("solo", MatchType.EXACT, "solo", 0, 2, 5, 0L, 0L, 60_000L, null);
    defaults = new HotKeyRule("defaults", MatchType.REGEX, ".*", Integer.MIN_VALUE, -1, 100, 0L, 0L, 60_000L, null);
    RuleSet set = RuleSet.of(List.of(shadowingPrefix, shadowedExact, effectiveExact), defaults);
    RuleResolver exactAware = new RuleResolver(set, 4, metrics);

    assertThat(exactAware.resolveIfEligible("solo", 5).rule())
      .as("the indexed exact rule bypasses the floor and meets its own threshold")
      .isSameAs(effectiveExact);
    assertThat(exactAware.resolveIfEligible("user:42", 0))
      .as("the shadowed exact rule does not lift its key over the floor")
      .isNull();
    assertThat(exactAware.resolveIfEligible("user:42", 50).rule()).isSameAs(shadowingPrefix);
  }

  /**
   * A publication that retains an unchanged rule keeps its binding, and that binding still holds the
   * previous {@link HotKeyRule} instance. The exact index is therefore rebuilt from the published
   * set's own rules, not by identity against the retained bindings: an unchanged exact rule must not
   * disappear from the index just because another rule changed.
   */
  @Test
  void anUnchangedExactRuleStaysIndexedAcrossAPublication() {
    HotKeyRule exact = new HotKeyRule("solo", MatchType.EXACT, "solo", 0, 0, 5, 0L, 0L, 60_000L, null);
    RuleResolver exactAware = new RuleResolver(RuleSet.of(List.of(exact), defaults), 4, metrics);
    RuleBinding before = exactAware.binding("solo");
    assertThat(exactAware.resolveIfEligible("solo", 5)).isSameAs(before);

    HotKeyRule added = new HotKeyRule("added", MatchType.PREFIX, "x", 0, 0, 5, 0L, 0L, 60_000L, null);
    HotKeyRule republished = new HotKeyRule("solo", MatchType.EXACT, "solo", 0, 0, 5, 0L, 0L, 60_000L, null);
    exactAware.publish(RuleSet.of(List.of(republished, added), defaults));

    assertThat(exactAware.binding("solo")).as("the unchanged rule keeps its binding").isSameAs(before);
    assertThat(exactAware.resolveIfEligible("solo", 5))
      .as("the retained exact rule is still served from the index at its own threshold")
      .isSameAs(before);
  }

  /**
   * A cold report for an effective exact key must be rejected before the match cache: the rule's
   * own threshold, not the global floor, is the authority, and a below-threshold report must not
   * occupy a cache slot or advance the match counter.
   */
  @Test
  void anExactRuleBelowItsOwnThresholdIsNotCachedOrCounted() {
    HotKeyRule exact = new HotKeyRule("solo", MatchType.EXACT, "solo", 0, 0, 5, 0L, 0L, 60_000L, null);
    defaults = new HotKeyRule("defaults", MatchType.REGEX, ".*", Integer.MIN_VALUE, -1, 100, 0L, 0L, 60_000L, null);
    RuleResolver exactAware = new RuleResolver(RuleSet.of(List.of(exact), defaults), 4, metrics);

    assertThat(exactAware.resolveIfEligible("solo", 4)).isNull();
    assertThat(exactAware.binding("solo").matches()).as("no scan, no match counter").isZero();

    assertThat(exactAware.resolveIfEligible("solo", 5).rule()).isSameAs(exact);
    assertThat(exactAware.binding("solo").matches()).isEqualTo(1);
  }

  /**
   * An eligible exact key is served from the index, not the bounded match cache, and every
   * eligible report counts — the counter is a match count, not a once-per-publication flag.
   */
  @Test
  void everyEligibleExactReportCountsOneMatch() {
    HotKeyRule exact = new HotKeyRule("solo", MatchType.EXACT, "solo", 0, 0, 5, 0L, 0L, 60_000L, null);
    defaults = new HotKeyRule("defaults", MatchType.REGEX, ".*", Integer.MIN_VALUE, -1, 100, 0L, 0L, 60_000L, null);
    RuleResolver exactAware = new RuleResolver(RuleSet.of(List.of(exact), defaults), 4, metrics);

    assertThat(exactAware.resolveIfEligible("solo", 5).rule()).isSameAs(exact);
    assertThat(exactAware.resolveIfEligible("solo", 6).rule()).isSameAs(exact);
    assertThat(exactAware.binding("solo").matches()).isEqualTo(2);
  }

  /**
   * A resolved non-exact rule is gated by its own {@code hotCount}, not only by the global floor:
   * a report that clears the floor but not the rule's threshold must not reach {@code promote}.
   */
  @Test
  void aResolvedRuleBelowItsOwnThresholdIsNotEligible() {
    HotKeyRule prefix = new HotKeyRule("k", MatchType.PREFIX, "k", 0, 0, 100, 0L, 0L, 60_000L, null);
    defaults = new HotKeyRule("defaults", MatchType.REGEX, ".*", Integer.MIN_VALUE, -1, 5, 0L, 0L, 60_000L, null);
    RuleResolver gate = new RuleResolver(RuleSet.of(List.of(prefix), defaults), 4, metrics);

    assertThat(gate.resolveIfEligible("k1", 99)).isNull();
    assertThat(gate.binding("k").matches()).as("a below-threshold report is not a match").isZero();
    assertThat(gate.resolveIfEligible("k1", 100).rule()).isSameAs(prefix);
    assertThat(gate.binding("k").matches()).isEqualTo(1);
  }
}
