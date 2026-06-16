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
package io.github.hyshmily.hotkey.model;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.model.HotKeyDecision.DecisionType;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyDecision} record covering factory methods and decision type enum.
 */
class HotKeyDecisionTest {

  /**
   * Verifies that the hot factory method creates a decision with HOT type.
   */
  @Test
  void hot_shouldCreateHotDecision() {
    HotKeyDecision decision = HotKeyDecision.hot("key1");
    assertThat(decision.type()).isEqualTo(DecisionType.HOT);
    assertThat(decision.cacheKey()).isEqualTo("key1");
  }

  /**
   * Verifies that the cool factory method creates a decision with COOL type.
   */
  @Test
  void cool_shouldCreateCoolDecision() {
    HotKeyDecision decision = HotKeyDecision.cool("key1");
    assertThat(decision.type()).isEqualTo(DecisionType.COOL);
  }

  /**
   * Verifies that the none factory method creates a decision with NONE type.
   */
  @Test
  void none_shouldCreateNoneDecision() {
    HotKeyDecision decision = HotKeyDecision.none("key1");
    assertThat(decision.type()).isEqualTo(DecisionType.NONE);
  }

  /**
   * Verifies the DecisionType enum contains exactly HOT, COOL, and NONE in that order.
   */
  @Test
  void decisionType_shouldHaveExpectedValues() {
    assertThat(DecisionType.values()).containsExactly(DecisionType.HOT, DecisionType.COOL, DecisionType.NONE);
  }

  @Test
  void hot_shouldAcceptNullKey() {
    HotKeyDecision decision = HotKeyDecision.hot(null);
    assertThat(decision.type()).isEqualTo(DecisionType.HOT);
    assertThat(decision.cacheKey()).isNull();
  }

  @Test
  void cool_shouldAcceptNullKey() {
    HotKeyDecision decision = HotKeyDecision.cool(null);
    assertThat(decision.type()).isEqualTo(DecisionType.COOL);
    assertThat(decision.cacheKey()).isNull();
  }

  @Test
  void none_shouldAcceptNullKey() {
    HotKeyDecision decision = HotKeyDecision.none(null);
    assertThat(decision.type()).isEqualTo(DecisionType.NONE);
    assertThat(decision.cacheKey()).isNull();
  }

  @Test
  void hot_shouldAcceptEmptyKey() {
    HotKeyDecision decision = HotKeyDecision.hot("");
    assertThat(decision.type()).isEqualTo(DecisionType.HOT);
    assertThat(decision.cacheKey()).isEmpty();
  }

  @Test
  void decisionWithNullCacheKey_shouldCreateCorrectly() {
    HotKeyDecision decision = new HotKeyDecision(DecisionType.HOT, null);
    assertThat(decision.type()).isEqualTo(DecisionType.HOT);
    assertThat(decision.cacheKey()).isNull();
  }

  @Test
  void decisionTypeHOT_shouldHaveCorrectName() {
    assertThat(DecisionType.HOT.name()).isEqualTo("HOT");
  }

  @Test
  void decisionTypeCOOL_shouldHaveCorrectName() {
    assertThat(DecisionType.COOL.name()).isEqualTo("COOL");
  }

  @Test
  void decisionTypeNONE_shouldHaveCorrectName() {
    assertThat(DecisionType.NONE.name()).isEqualTo("NONE");
  }

  @Test
  void decisionTypeHOT_shouldHaveOrdinalZero() {
    assertThat(DecisionType.HOT.ordinal()).isZero();
  }

  @Test
  void decisionTypeCOOL_shouldHaveOrdinalOne() {
    assertThat(DecisionType.COOL.ordinal()).isOne();
  }

  @Test
  void decisionTypeNONE_shouldHaveOrdinalTwo() {
    assertThat(DecisionType.NONE.ordinal()).isEqualTo(2);
  }

  @Test
  void toString_shouldNotBeNull() {
    HotKeyDecision decision = HotKeyDecision.hot("testKey");
    assertThat(decision.toString()).isNotNull();
  }

  @Test
  void veryLongKey_shouldBeAccepted() {
    String longKey = "k".repeat(10_000);
    HotKeyDecision decision = HotKeyDecision.hot(longKey);
    assertThat(decision.cacheKey()).isEqualTo(longKey);
  }

  @Test
  void specialCharactersInKey_shouldBeAccepted() {
    String specials = "key:\n\t\r\u0000\u00E9\u4E2D\u6587";
    HotKeyDecision decision = HotKeyDecision.hot(specials);
    assertThat(decision.cacheKey()).isEqualTo(specials);
  }

  @Test
  void equalsAndHashCode_shouldWorkForRecords() {
    HotKeyDecision a = HotKeyDecision.hot("k");
    HotKeyDecision b = HotKeyDecision.hot("k");
    HotKeyDecision c = HotKeyDecision.cool("k");
    assertThat(a).isEqualTo(b);
    assertThat(a).hasSameHashCodeAs(b);
    assertThat(a).isNotEqualTo(c);
  }

  @Test
  void record_shouldBeSerializableViaToString() {
    HotKeyDecision hot = HotKeyDecision.hot("test");
    HotKeyDecision cool = HotKeyDecision.cool("test");
    HotKeyDecision none = HotKeyDecision.none("test");
    assertThat(hot.toString()).contains("HOT").contains("test");
    assertThat(cool.toString()).contains("COOL").contains("test");
    assertThat(none.toString()).contains("NONE").contains("test");
  }

  @Test
  void decisionType_switch_shouldCoverAllCases() {
    for (DecisionType t : DecisionType.values()) {
      boolean covered = switch (t) {
        case HOT -> true;
        case COOL -> true;
        case NONE -> true;
      };
      assertThat(covered).isTrue();
    }
  }
}
