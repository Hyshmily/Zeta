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

  @Test
  void hot_shouldCreateHotDecision() {
    HotKeyDecision decision = HotKeyDecision.hot("key1");
    assertThat(decision.type()).isEqualTo(DecisionType.HOT);
    assertThat(decision.cacheKey()).isEqualTo("key1");
  }

  @Test
  void cool_shouldCreateCoolDecision() {
    HotKeyDecision decision = HotKeyDecision.cool("key1");
    assertThat(decision.type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void none_shouldCreateNoneDecision() {
    HotKeyDecision decision = HotKeyDecision.none("key1");
    assertThat(decision.type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void decisionType_shouldHaveExpectedValues() {
    assertThat(DecisionType.values()).containsExactly(DecisionType.HOT, DecisionType.COOL, DecisionType.PING, DecisionType.NONE);
  }
}
