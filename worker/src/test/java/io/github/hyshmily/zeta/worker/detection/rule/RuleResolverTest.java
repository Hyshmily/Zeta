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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.regex.Pattern;
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
    assertThat(registry.get(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "r").counter().count()).isEqualTo(
      2.0d
    );
  }

  @Test
  void theDefaultRuleCountsEveryEligibleReport() {
    assertThat(resolver.resolveIfEligible("other", 100).rule().id()).isEqualTo("defaults");
    assertThat(resolver.resolveIfEligible("other", 100).rule().id()).isEqualTo("defaults");

    assertThat(resolver.binding("defaults").matches()).isEqualTo(2);
    assertThat(
      registry.get(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "defaults").counter().count()
    ).isEqualTo(2.0d);
  }

  /**
   * A publication retires the removed rule's counters and meters. A key promoted under the old
   * rule can still cool afterwards; that late update must not recreate the retired series.
   */
  @Test
  void publishingARuleSetRetiresTheRemovedRulesCounters() {
    assertThat(resolver.resolveIfEligible("k1", 5)).isSameAs(resolver.binding("r"));
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "r").counter()).isNotNull();
    RuleBinding retired = resolver.binding("r");

    HotKeyRule replacement = new HotKeyRule("replacement", MatchType.PREFIX, "k", 0, 0, 5, 0L, 0L, 60_000L, null);
    resolver.publish(RuleSet.of(List.of(replacement), defaults));

    assertThat(registry.find(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "r").counter())
      .as("the retired rule's meter is removed from the registry")
      .isNull();

    retired.recordCool();
    retired.recordPromotion();
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_RULE_COOLS).tag("rule", "r").counter())
      .as("a late cool-down does not resurrect the retired meter")
      .isNull();
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_RULE_PROMOTIONS).tag("rule", "r").counter()).isNull();
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
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_RULE_PROMOTIONS).tag("rule", "r").counter())
      .as("a retained rule keeps its meter")
      .isNotNull();
  }

  /**
   * A meter-registration failure while building a publication leaves the previous snapshot, its
   * bindings and its meters in place; the new id is not published and its partially bound series is
   * removed.
   */
  @Test
  void aMeterRegistrationFailureLeavesThePreviousSnapshotPublished() {
    WorkerDetectionMetrics failing = new WorkerDetectionMetrics(registry) {
      @Override
      public Counter ruleMatches(String ruleId) {
        if ("added".equals(ruleId)) {
          throw new IllegalStateException("bind failed");
        }
        return super.ruleMatches(ruleId);
      }
    };
    RuleResolver failingResolver = new RuleResolver(RuleSet.of(List.of(rule), defaults), 4, failing);
    RuleBinding before = failingResolver.binding("r");
    HotKeyRule added = new HotKeyRule("added", MatchType.PREFIX, "x", 0, 0, 5, 0L, 0L, 60_000L, null);

    assertThatThrownBy(() -> failingResolver.publish(RuleSet.of(List.of(rule, added), defaults)))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("bind failed");

    assertThat(failingResolver.binding("r")).isSameAs(before);
    assertThat(failingResolver.binding("added")).isNull();
    assertThat(registry.find(WorkerDetectionMetrics.METRIC_RULE_MATCHES).tag("rule", "added").counter())
      .as("the failed publication removes its partially bound series")
      .isNull();
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
   * A publication rebuilds every binding, even for an unchanged rule, so the binding always holds
   * the rule instance in the published snapshot; the id's cumulative counters survive.
   */
  @Test
  void anUnchangedRuleKeepsItsCountersAcrossAPublication() {
    RuleBinding before = resolver.binding("r");
    before.recordPromotion();
    HotKeyRule added = new HotKeyRule("added", MatchType.PREFIX, "x", 0, 0, 5, 0L, 0L, 60_000L, null);

    resolver.publish(RuleSet.of(List.of(rule, added), defaults));

    RuleBinding after = resolver.binding("r");
    assertThat(after).isNotSameAs(before);
    assertThat(after.rule()).isSameAs(rule);
    assertThat(after.promotions()).isEqualTo(1);
    assertThat(resolver.binding("added")).isNotNull();
  }

  /**
   * A recompiled regex is a new rule instance with the same behaviour. The publication binds it
   * fresh, so the binding carries the new {@link java.util.regex.Pattern}; the counters survive.
   */
  @Test
  void anUnchangedRegexRuleKeepsItsCountersAcrossAPublication() {
    defaults = new HotKeyRule("rx-defaults", MatchType.REGEX, ".*", Integer.MIN_VALUE, -1, 100, 0L, 0L, 60_000L, null);
    HotKeyRule regex = new HotKeyRule("rx", MatchType.REGEX, "user:.*", 0, 0, 5, 0L, 0L, 60_000L, null);
    RuleResolver regexResolver = new RuleResolver(RuleSet.of(List.of(regex), defaults), 4, metrics);
    RuleBinding before = regexResolver.binding("rx");
    before.recordPromotion();

    HotKeyRule recompiled = new HotKeyRule("rx", MatchType.REGEX, "user:.*", 0, 0, 5, 0L, 0L, 60_000L, null);
    HotKeyRule added = new HotKeyRule("added", MatchType.PREFIX, "x", 0, 0, 5, 0L, 0L, 60_000L, null);
    regexResolver.publish(RuleSet.of(List.of(recompiled, added), defaults));

    RuleBinding after = regexResolver.binding("rx");
    assertThat(after).isNotSameAs(before);
    assertThat(after.rule()).isSameAs(recompiled);
    assertThat(after.promotions()).isEqualTo(1);
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
   * A publication rebuilds the exact rule's binding over the new rule instance and keeps its
   * counters. The exact index is rebuilt from the published set's own rules, so an unchanged exact
   * rule stays indexed after another rule changes.
   */
  @Test
  void anUnchangedExactRuleStaysIndexedAcrossAPublication() {
    HotKeyRule exact = new HotKeyRule("solo", MatchType.EXACT, "solo", 0, 0, 5, 0L, 0L, 60_000L, null);
    RuleResolver exactAware = new RuleResolver(RuleSet.of(List.of(exact), defaults), 4, metrics);
    RuleBinding before = exactAware.binding("solo");
    assertThat(exactAware.resolveIfEligible("solo", 5)).isSameAs(before);
    assertThat(before.matches()).isEqualTo(1);

    HotKeyRule added = new HotKeyRule("added", MatchType.PREFIX, "x", 0, 0, 5, 0L, 0L, 60_000L, null);
    HotKeyRule republished = new HotKeyRule("solo", MatchType.EXACT, "solo", 0, 0, 5, 0L, 0L, 60_000L, null);
    exactAware.publish(RuleSet.of(List.of(republished, added), defaults));

    RuleBinding after = exactAware.binding("solo");
    assertThat(after).as("the publication rebuilds the binding").isNotSameAs(before);
    assertThat(after.rule()).as("the binding holds the published rule").isSameAs(republished);
    assertThat(after.matches()).as("the exact rule keeps its counters").isEqualTo(1);
    assertThat(exactAware.resolveIfEligible("solo", 5))
      .as("the published exact rule is still served from the index at its own threshold")
      .isSameAs(after);
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

  /**
   * The global floor can never gate an effective exact rule: an exact rule below
   * {@code minHotCount} still promotes its own key at its own threshold, while every other key keeps
   * the floor.
   */
  @Test
  void anExactRuleBelowTheGlobalFloorStillPromotesAtItsOwnThreshold() {
    HotKeyRule exact = new HotKeyRule("vip", MatchType.EXACT, "vip:1", 0, 0, 3, 0L, 0L, 60_000L, null);
    defaults = new HotKeyRule("defaults", MatchType.REGEX, ".*", Integer.MIN_VALUE, -1, 100, 0L, 0L, 60_000L, null);
    RuleResolver exactAware = new RuleResolver(RuleSet.of(List.of(exact), defaults), 4, metrics);

    assertThat(exactAware.snapshot().minHotCount())
      .as("the floor only covers the rules the scan resolves")
      .isEqualTo(100L);

    assertThat(exactAware.resolveIfEligible("vip:1", 2)).as("below even the exact threshold").isNull();
    assertThat(exactAware.resolveIfEligible("vip:1", 3).rule()).isSameAs(exact);
    assertThat(exactAware.resolveIfEligible("other", 99)).as("the floor still gates every other key").isNull();
  }

  @Test
  void namespaceMatchesTheAnnotationNamespaceAndItsColonForm() {
    HotKeyRule namespace = new HotKeyRule("ns", MatchType.NAMESPACE, "users", 0, 0, 5, 0L, 0L, 60_000L, null);

    assertThat(namespace.matches("users:42")).as("a colon-delimited key is inside the namespace").isTrue();
    assertThat(namespace.matches("users\u0000[J:42]")).as("an annotation key carries U+0000 after the name").isTrue();
    assertThat(namespace.matches("users_profile:42")).as("a sibling namespace is not inside it").isFalse();
    assertThat(namespace.matches("users")).as("the bare namespace is not a key inside it").isFalse();
    assertThat(namespace.matches("usersX")).as("the key needs a delimiter after the name").isFalse();
  }

  @Test
  void exactAndPrefixStillSelectTheirOwnKeys() {
    HotKeyRule exact = new HotKeyRule("exact", MatchType.EXACT, "users:42", 0, 0, 5, 0L, 0L, 60_000L, null);
    HotKeyRule prefix = new HotKeyRule("prefix", MatchType.PREFIX, "users", 0, 1, 5, 0L, 0L, 60_000L, null);

    assertThat(exact.matches("users:42")).isTrue();
    assertThat(exact.matches("users:420")).isFalse();
    assertThat(prefix.matches("users:42")).isTrue();
    assertThat(prefix.matches("users")).isTrue();
    assertThat(prefix.matches("users_profile:42")).as("a bare prefix still spans siblings").isTrue();
  }

  /**
   * At equal priority the namespace sits between the exact key and the prefix: the exact rule owns
   * its key, the namespace owns the rest of the namespace, and the prefix covers the sibling that
   * the namespace deliberately excludes.
   */
  @Test
  void namespaceSitsBetweenExactAndPrefixAtEqualPriority() {
    HotKeyRule exact = new HotKeyRule("exact", MatchType.EXACT, "users:42", 0, 0, 5, 0L, 0L, 60_000L, null);
    HotKeyRule namespace = new HotKeyRule("namespace", MatchType.NAMESPACE, "users", 0, 1, 5, 0L, 0L, 60_000L, null);
    HotKeyRule prefix = new HotKeyRule("prefix", MatchType.PREFIX, "users", 0, 2, 5, 0L, 0L, 60_000L, null);
    HotKeyRule regex = new HotKeyRule("regex", MatchType.REGEX, "users.*", 0, 3, 5, 0L, 0L, 60_000L, null);
    RuleSet set = RuleSet.of(List.of(regex, prefix, namespace, exact), defaults);

    assertThat(set.rules()).extracting(HotKeyRule::id).containsExactly("exact", "namespace", "prefix", "regex");
    assertThat(set.resolve("users:42").id()).isEqualTo("exact");
    assertThat(set.resolve("users:43").id()).isEqualTo("namespace");
    assertThat(set.resolve("users_profile:42").id()).isEqualTo("prefix");
  }

  @Test
  void aNamespacePatternMustBeTheBareCacheName() {
    assertThatThrownBy(() -> new HotKeyRule("ns", MatchType.NAMESPACE, "users:", 0, 0, 5, 0L, 0L, 60_000L, null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("ns")
      .hasMessageContaining("bare cache name");

    assertThatThrownBy(() ->
      new HotKeyRule("ns", MatchType.NAMESPACE, "users\u0000[J]", 0, 0, 5, 0L, 0L, 60_000L, null)
    )
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("bare cache name");

    assertThatThrownBy(() -> new HotKeyRule("ns", MatchType.NAMESPACE, "  ", 0, 0, 5, 0L, 0L, 60_000L, null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("bare cache name");

    assertThatThrownBy(() ->
      new HotKeyRule("ns", MatchType.NAMESPACE, "users", 0, 0, 5, 0L, 0L, 60_000L, Pattern.compile("users"))
    )
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("non-regex");
  }
}
