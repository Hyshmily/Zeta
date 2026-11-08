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

import java.util.List;
import org.junit.jupiter.api.Test;

/** Rule precedence: priority first, then specificity, then pattern length, then config order. */
class RulePrecedenceTest {

  private static HotKeyRule rule(String id, MatchType type, String pattern, int priority, int order) {
    return new HotKeyRule(id, type, pattern, priority, order, 10, 0L, 0L, 60_000L, null);
  }

  private static final HotKeyRule DEFAULT = rule("defaults", MatchType.REGEX, ".*", Integer.MIN_VALUE, -1);

  private static RuleSet setOf(HotKeyRule... rules) {
    return RuleSet.of(List.of(rules), DEFAULT);
  }

  @Test
  void aHigherPriorityRegexBeatsALowerPriorityExactRule() {
    RuleSet set = setOf(
      rule("exact", MatchType.EXACT, "user:1", 0, 0),
      rule("regex", MatchType.REGEX, "^user:1$", 100, 1)
    );
    assertThat(set.resolve("user:1").id()).isEqualTo("regex");
  }

  @Test
  void atEqualPriorityExactBeatsPrefixBeatsRegex() {
    RuleSet set = setOf(
      rule("regex", MatchType.REGEX, "^user:.*$", 0, 0),
      rule("prefix", MatchType.PREFIX, "user:", 0, 1),
      rule("exact", MatchType.EXACT, "user:1", 0, 2)
    );
    assertThat(set.resolve("user:1").id()).isEqualTo("exact");
    assertThat(set.resolve("user:22").id()).isEqualTo("prefix");
    assertThat(set.resolve("account:1").id()).isEqualTo("defaults");
  }

  @Test
  void atEqualPriorityAndTypeTheLongerPrefixWins() {
    RuleSet set = setOf(
      rule("short", MatchType.PREFIX, "user:", 0, 0),
      rule("long", MatchType.PREFIX, "user:premium:", 0, 1)
    );
    assertThat(set.resolve("user:premium:42").id()).isEqualTo("long");
    assertThat(set.resolve("user:basic:42").id()).isEqualTo("short");
  }

  @Test
  void anUnmatchedKeyFallsBackToTheDefaultRule() {
    RuleSet set = setOf(rule("prefix", MatchType.PREFIX, "user:", 0, 0));
    assertThat(set.resolve("order:42").id()).isEqualTo("defaults");
  }

  @Test
  void configurationOrderBreaksAnOtherwiseCompleteTie() {
    RuleSet set = setOf(
      rule("second", MatchType.PREFIX, "user:", 0, 5),
      rule("first", MatchType.PREFIX, "user:", 0, 3)
    );
    assertThat(set.resolve("user:1").id()).isEqualTo("first");
  }

  @Test
  void theFingerprintIsStableAndSensitiveToEffectiveParameters() {
    RuleSet a = setOf(rule("prefix", MatchType.PREFIX, "user:", 0, 0));
    RuleSet b = setOf(rule("prefix", MatchType.PREFIX, "user:", 0, 0));
    RuleSet different = setOf(rule("prefix", MatchType.PREFIX, "user:", 1, 0));

    assertThat(RuleSet.fingerprint(a.canonical())).isEqualTo(RuleSet.fingerprint(b.canonical()));
    assertThat(RuleSet.fingerprint(a.canonical())).isNotEqualTo(RuleSet.fingerprint(different.canonical()));
  }
}
