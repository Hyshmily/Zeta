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
package io.github.hyshmily.zeta.detection;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.detection.impl.ZetaStateMachineImpl;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ZetaStateMachine} covering cold-to-hot, hot-to-cool, pre-cooling revive, reset, and eviction
 * transitions.
 */
class ZetaStateMachineTest {

  private ZetaStateMachine machine;

  @BeforeEach
  void setUp() {
    machine = new ZetaStateMachineImpl(3, 10, 4);
  }

  /**
   * Verifies that enough consecutive hot windows transition a key from cold to HOT.
   */
  @Test
  void coldToHot_requiresConfirmCountConsecutiveHotWindows() {
    assertThat(machine.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
  }

  /**
   * Verifies that enough consecutive cold windows transition a key from HOT through PRE_COOLING to COOL.
   */
  @Test
  void hotToCool_requiresCoolCountConsecutiveColdWindows() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    // coolCount = 10, preCoolGraceCount = 4
    // PRE_COOLING at coolStreak >= 6 (coolCount - grace), COOL at coolStreak >= 10
    for (int i = 0; i < 5; i++) {
      assertThat(machine.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    }
    assertThat(machine.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);

    // coolStreak >= 6 → enters PRE_COOLING
    for (int i = 0; i < 3; i++) {
      assertThat(machine.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    }
    // coolStreak >= 10 → COOL
    assertThat(machine.evaluate("key", false).type()).isEqualTo(DecisionType.COOL);
  }

  /**
   * Verifies that a key in PRE_COOLING reverts to HOT without emitting COOL when a hot window arrives within the grace period.
   */
  @Test
  void preCooling_toHot_shouldReviveWithoutOscillation() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);

    // enter PRE_COOLING
    for (int i = 0; i < 6; i++) {
      machine.evaluate("key", false);
    }

    // revive during grace period → NONE (silent)
    assertThat(machine.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
  }

  /**
   * Verifies that resetting a key clears its state so it starts from COLD on the next evaluation.
   */
  @Test
  void reset_shouldClearState() {
    ZetaDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);
    machine.reset("key");
    assertThat(machine.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
  }

  /**
   * Verifies that stale keys are evicted and subsequent evaluations start from COLD again.
   */
  @Test
  void evictStale_shouldRemoveOldKeys() throws InterruptedException {
    machine.evaluate("staleKey", true);
    Thread.sleep(50);
    machine.evictStale(10);
    assertThat(machine.evaluate("staleKey", false).type()).isEqualTo(DecisionType.NONE);
  }
}
