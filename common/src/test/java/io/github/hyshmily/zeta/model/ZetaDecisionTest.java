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
package io.github.hyshmily.zeta.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ZetaDecision} covering factory methods, the versioned shape and the decision
 * type enum.
 */
class ZetaDecisionTest {

  @Test
  void hot_shouldCreateHotDecision() {
    ZetaDecision decision = ZetaDecision.hot("key1");
    assertThat(decision.type()).isEqualTo(DecisionType.HOT);
    assertThat(decision.cacheKey()).isEqualTo("key1");
    assertThat(decision.decisionVersion()).isZero();
  }

  @Test
  void hot_shouldCarryVersion() {
    ZetaDecision decision = ZetaDecision.hot("key1", 42L);
    assertThat(decision.decisionVersion()).isEqualTo(42L);
  }

  @Test
  void cool_shouldCreateCoolDecision() {
    ZetaDecision decision = ZetaDecision.cool("key1");
    assertThat(decision.type()).isEqualTo(DecisionType.COOL);
    assertThat(decision.decisionVersion()).isZero();
  }

  @Test
  void cool_shouldCarryVersion() {
    assertThat(ZetaDecision.cool("key1", 7L).decisionVersion()).isEqualTo(7L);
  }

  @Test
  void none_shouldBeSharedAndCarryNoPayload() {
    assertThat(ZetaDecision.NONE.type()).isEqualTo(DecisionType.NONE);
    assertThat(ZetaDecision.NONE.cacheKey()).isEmpty();
    assertThat(ZetaDecision.NONE.decisionVersion()).isZero();
  }

  @Test
  void decisionType_shouldHaveExpectedValues() {
    assertThat(DecisionType.values()).containsExactly(DecisionType.HOT, DecisionType.COOL, DecisionType.NONE);
  }

  @Test
  void hot_shouldRejectNullKey() {
    assertThatThrownBy(() -> ZetaDecision.hot(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void cool_shouldRejectNullKey() {
    assertThatThrownBy(() -> ZetaDecision.cool(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void hot_shouldAcceptEmptyKey() {
    ZetaDecision decision = ZetaDecision.hot("");
    assertThat(decision.type()).isEqualTo(DecisionType.HOT);
    assertThat(decision.cacheKey()).isEmpty();
  }

  @Test
  void decisionWithNullCacheKey_shouldBeRejected() {
    assertThatThrownBy(() -> new ZetaDecision(DecisionType.HOT, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void decisionWithNullType_shouldBeRejected() {
    assertThatThrownBy(() -> new ZetaDecision(null, "key")).isInstanceOf(NullPointerException.class);
  }

  @Test
  void decisionType_shouldHaveExpectedNames() {
    assertThat(DecisionType.HOT.name()).isEqualTo("HOT");
    assertThat(DecisionType.COOL.name()).isEqualTo("COOL");
    assertThat(DecisionType.NONE.name()).isEqualTo("NONE");
  }

  @Test
  void decisionType_shouldKeepOrdinals() {
    assertThat(DecisionType.HOT.ordinal()).isZero();
    assertThat(DecisionType.COOL.ordinal()).isOne();
    assertThat(DecisionType.NONE.ordinal()).isEqualTo(2);
  }

  @Test
  void toString_shouldNotBeNull() {
    assertThat(ZetaDecision.hot("testKey").toString()).isNotNull();
  }

  @Test
  void veryLongKey_shouldBeAccepted() {
    String longKey = "k".repeat(10_000);
    assertThat(ZetaDecision.hot(longKey).cacheKey()).isEqualTo(longKey);
  }

  @Test
  void specialCharactersInKey_shouldBeAccepted() {
    String specials = "key:\n\t\r\u0000\u00E9\u4E2D\u6587";
    assertThat(ZetaDecision.hot(specials).cacheKey()).isEqualTo(specials);
  }

  @Test
  void equalsAndHashCode_shouldWorkForRecords() {
    ZetaDecision a = ZetaDecision.hot("k");
    ZetaDecision b = ZetaDecision.hot("k");
    ZetaDecision c = ZetaDecision.cool("k");
    assertThat(a).isEqualTo(b);
    assertThat(a).hasSameHashCodeAs(b);
    assertThat(a).isNotEqualTo(c);
  }

  @Test
  void record_shouldBeSerializableViaToString() {
    ZetaDecision hot = ZetaDecision.hot("test");
    ZetaDecision cool = ZetaDecision.cool("test");
    assertThat(hot.toString()).contains("HOT").contains("test");
    assertThat(cool.toString()).contains("COOL").contains("test");
    assertThat(ZetaDecision.NONE.toString()).contains("NONE");
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
