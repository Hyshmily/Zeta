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
package io.github.hyshmily.hotkey.rule;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RuleMatcher} covering pattern detection, rule evaluation, and rule management.
 */
class RuleMatcherTest {

  private RuleMatcher ruleMatcher;

  @BeforeEach
  void setUp() {
    ruleMatcher = new RuleMatcher(Optional.empty(), Optional.empty());
  }

  // ── of() factory ─────────────────────────────────────────

  @Test
  void of_shouldDetectExactPattern() {
    Rule r = RuleMatcher.of("foo", RuleAction.BLOCK);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.EXACT);
    assertThat(r.getPattern()).isEqualTo("foo");
    assertThat(r.getAction()).isEqualTo(RuleAction.BLOCK);
  }

  @Test
  void of_shouldDetectPrefixPattern() {
    Rule r = RuleMatcher.of("user:*", RuleAction.ALLOW_NO_REPORT);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.PREFIX);
    assertThat(r.getPattern()).isEqualTo("user:");
    assertThat(r.getAction()).isEqualTo(RuleAction.ALLOW_NO_REPORT);
  }

  @Test
  void of_shouldDetectWildcardPattern() {
    Rule r = RuleMatcher.of("abc*def", RuleAction.BLOCK);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.WILDCARD);
    assertThat(r.getPattern()).isEqualTo("abc*def");
  }

  @Test
  void of_shouldDetectWildcardWithQuestionMark() {
    Rule r = RuleMatcher.of("abc?def", RuleAction.BLOCK);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.WILDCARD);
  }

  @Test
  void of_shouldDetectRegexPattern() {
    Rule r = RuleMatcher.of("regex:^foo.*bar$", RuleAction.BLOCK);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.REGEX);
    assertThat(r.getPattern()).isEqualTo("^foo.*bar$");
  }

  // ── addRule / evaluateRule ────────────────────────────────

  @Test
  void evaluateRule_shouldAllowByDefault() {
    assertThat(ruleMatcher.evaluateRule("anything")).isEqualTo(RuleAction.ALLOW);
  }

  @Test
  void evaluateRule_shouldMatchExactBlock() {
    ruleMatcher.addRule(RuleMatcher.of("secret", RuleAction.BLOCK));
    assertThat(ruleMatcher.evaluateRule("secret")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("secret2")).isEqualTo(RuleAction.ALLOW);
  }

  @Test
  void evaluateRule_shouldMatchPrefixBlock() {
    ruleMatcher.addRule(RuleMatcher.of("admin:*", RuleAction.BLOCK));
    assertThat(ruleMatcher.evaluateRule("admin:login")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("user:login")).isEqualTo(RuleAction.ALLOW);
  }

  @Test
  void evaluateRule_shouldMatchWildcardBlock() {
    ruleMatcher.addRule(RuleMatcher.of("a*c", RuleAction.BLOCK));
    assertThat(ruleMatcher.evaluateRule("abc")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("ac")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("abdc")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("ab")).isEqualTo(RuleAction.ALLOW);
  }

  @Test
  void evaluateRule_shouldMatchRegexFind() {
    ruleMatcher.addRule(RuleMatcher.of("regex:error|secret", RuleAction.BLOCK));
    assertThat(ruleMatcher.evaluateRule("error_occurred")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("top_secret_data")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("normal_key")).isEqualTo(RuleAction.ALLOW);
  }

  @Test
  void evaluateRule_shouldRespectFirstMatch() {
    ruleMatcher.addRule(RuleMatcher.of("foo", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("regex:foo", RuleAction.ALLOW_NO_REPORT));
    assertThat(ruleMatcher.evaluateRule("foo")).isEqualTo(RuleAction.BLOCK);
  }

  @Test
  void evaluateRule_shouldAllowNoReport() {
    ruleMatcher.addRule(RuleMatcher.of("audit:*", RuleAction.ALLOW_NO_REPORT));
    assertThat(ruleMatcher.evaluateRule("audit:log")).isEqualTo(RuleAction.ALLOW_NO_REPORT);
  }

  // ── removeRule(pattern, action) ───────────────────────────

  @Test
  void removeRule_shouldRemoveByPatternAndAction() {
    ruleMatcher.addRule(RuleMatcher.of("foo", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("bar", RuleAction.ALLOW_NO_REPORT));

    assertThat(ruleMatcher.removeRule("foo", RuleAction.BLOCK)).isTrue();
    assertThat(ruleMatcher.evaluateRule("foo")).isEqualTo(RuleAction.ALLOW);
    assertThat(ruleMatcher.removeRule("foo", RuleAction.BLOCK)).isFalse();
  }

  @Test
  void removeRule_shouldNotRemoveIfActionDiffers() {
    ruleMatcher.addRule(RuleMatcher.of("foo", RuleAction.BLOCK));
    assertThat(ruleMatcher.removeRule("foo", RuleAction.ALLOW_NO_REPORT)).isFalse();
    assertThat(ruleMatcher.evaluateRule("foo")).isEqualTo(RuleAction.BLOCK);
  }

  @Test
  void removeRule_shouldHandleNonExistent() {
    assertThat(ruleMatcher.removeRule("nonexistent", RuleAction.BLOCK)).isFalse();
  }

  // ── removeRulesByAction ──────────────────────────────────

  @Test
  void removeRulesByAction_shouldRemoveAllMatching() {
    ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("k2", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("k3", RuleAction.ALLOW_NO_REPORT));

    assertThat(ruleMatcher.removeRulesByAction(RuleAction.BLOCK)).isEqualTo(2);
    assertThat(ruleMatcher.evaluateRule("k1")).isEqualTo(RuleAction.ALLOW);
    assertThat(ruleMatcher.evaluateRule("k3")).isEqualTo(RuleAction.ALLOW_NO_REPORT);
  }

  @Test
  void removeRulesByAction_shouldReturnZeroIfNone() {
    assertThat(ruleMatcher.removeRulesByAction(RuleAction.BLOCK)).isZero();
  }

  // ── clearAllRules ───────────────────────────────────────────

  @Test
  void clearRules_shouldRemoveAll() {
    ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("k2", RuleAction.ALLOW_NO_REPORT));
    ruleMatcher.clearRules();
    assertThat(ruleMatcher.getAllRules()).isEmpty();
    assertThat(ruleMatcher.evaluateRule("k1")).isEqualTo(RuleAction.ALLOW);
  }

  // ── getAllRules ──────────────────────────────────────────

  @Test
  void getAllRules_shouldReturnSnapshot() {
    ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
    List<Rule> snapshot = ruleMatcher.getAllRules();
    assertThat(snapshot).hasSize(1);
    snapshot.clear();
    assertThat(ruleMatcher.getAllRules()).hasSize(1);
  }

  // ── replaceRules ─────────────────────────────────────────

  @Test
  void replaceRules_shouldReplaceAll() {
    ruleMatcher.addRule(RuleMatcher.of("old", RuleAction.BLOCK));
    ruleMatcher.replaceRules(List.of(RuleMatcher.of("new1", RuleAction.ALLOW_NO_REPORT)));
    assertThat(ruleMatcher.getAllRules()).hasSize(1);
    assertThat(ruleMatcher.evaluateRule("old")).isEqualTo(RuleAction.ALLOW);
    assertThat(ruleMatcher.evaluateRule("new1")).isEqualTo(RuleAction.ALLOW_NO_REPORT);
  }

  @Test
  void replaceRules_shouldHandleEmpty() {
    ruleMatcher.replaceRules(List.of());
    assertThat(ruleMatcher.getAllRules()).isEmpty();
  }

  // ── removeRule(int) legacy ───────────────────────────────

  @Test
  void removeRuleByIndex_shouldWork() {
    ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("k2", RuleAction.BLOCK));
    ruleMatcher.removeRule(0);
    assertThat(ruleMatcher.evaluateRule("k1")).isEqualTo(RuleAction.ALLOW);
    assertThat(ruleMatcher.evaluateRule("k2")).isEqualTo(RuleAction.BLOCK);
  }

  @Test
  void removeRuleByIndex_shouldHandleOutOfBounds() {
    ruleMatcher.removeRule(99);
    ruleMatcher.removeRule(-1);
  }
}
