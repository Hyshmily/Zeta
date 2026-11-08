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

import java.util.List;
import org.junit.jupiter.api.Test;

/** The flattened cool-down encoding: {@code coolCount == 0} is EXPIRE, positive is LOW_QPS. */
class HotKeyRuleTest {

  /**
   * A compiled {@link java.util.regex.Pattern} has identity equality; two identically configured
   * regex rules must still compare by their definition, or every rebuilt rule set looks changed.
   */
  @Test
  void regexRulesWithTheSameDefinitionAreEqual() {
    HotKeyRule first = new HotKeyRule("regex", MatchType.REGEX, "user-.*", 5, 2, 10L, 2L, 1_000L, 100L, null);
    HotKeyRule second = new HotKeyRule("regex", MatchType.REGEX, "user-.*", 5, 2, 10L, 2L, 1_000L, 100L, null);
    HotKeyRule defaultFirst = new HotKeyRule("default", MatchType.PREFIX, "d", 0, 1, 5L, 1L, 500L, 100L, null);
    HotKeyRule defaultSecond = new HotKeyRule("default", MatchType.PREFIX, "d", 0, 1, 5L, 1L, 500L, 100L, null);

    assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
    assertThat(RuleSet.of(List.of(first), defaultFirst)).isEqualTo(RuleSet.of(List.of(second), defaultSecond));
  }

  /**
   * Configuration order participates in equality (it is the final precedence tiebreak) but not in
   * the canonical definition the rule-set fingerprint consumes.
   */
  @Test
  void configurationOrderDistinguishesRulesButNotTheirDefinition() {
    HotKeyRule first = new HotKeyRule("r", MatchType.PREFIX, "k", 0, 1, 5L, 1L, 500L, 100L, null);
    HotKeyRule second = new HotKeyRule("r", MatchType.PREFIX, "k", 0, 2, 5L, 1L, 500L, 100L, null);

    assertThat(first).isNotEqualTo(second);
    assertThat(first.definition()).isEqualTo(second.definition());
  }

  @Test
  void anExpireRuleRejectsALowQpsDuration() {
    assertThatThrownBy(() -> new HotKeyRule("bad", MatchType.PREFIX, "k", 0, 0, 5, 0L, 1_000L, 60_000L, null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("COOL_FOR_EXPIRE");
  }

  @Test
  void bothStrategiesUseTheSameSaturatedFirstDueArithmetic() {
    HotKeyRule lowQps = new HotKeyRule("low", MatchType.PREFIX, "k", 0, 0, 5, 2L, 1_000L, 100L, null);
    HotKeyRule expire = new HotKeyRule("expire", MatchType.PREFIX, "k", 0, 0, 5, 0L, 0L, 100L, null);

    assertThat(lowQps.isExpire()).isFalse();
    assertThat(expire.isExpire()).isTrue();
    assertThat(lowQps.firstDue(1_000L)).isEqualTo(1_100L);
    assertThat(expire.firstDue(1_000L)).isEqualTo(1_100L);
    assertThat(expire.firstDue(Long.MAX_VALUE - 1)).isEqualTo(Long.MAX_VALUE);
  }
}
