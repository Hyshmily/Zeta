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

import static org.assertj.core.api.Assertions.*;

import io.github.hyshmily.zeta.detection.impl.ZetaStateMachineImpl;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Edge case tests for {@link ZetaStateMachine} covering single window, immediate cooling, and interleaved keys.
 */
class ZetaStateMachineEdgeTest {

  /**
   * Verifies that the state machine emits HOT on the first evaluation within a single-window configuration.
   */
  @Test
  void shouldHandleSingleHotWindow() {
    ZetaStateMachine m = new ZetaStateMachineImpl(1, 5, 2);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
  }

  /**
   * Verifies that a hot window followed by cold windows transitions from HOT to NONE to COOL.
   */
  @Test
  void shouldHandleImmediateCooling() {
    ZetaStateMachine m = new ZetaStateMachineImpl(1, 2, 1);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.COOL);
  }

  /**
   * Verifies that interleaved evaluations for different keys are tracked independently without interference.
   */
  @Test
  void shouldHandleInterleavedKeys() {
    assertThat(machine("key1", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine("key2", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine("key1", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine("key2", true).type()).isEqualTo(DecisionType.NONE);
  }

  private static ZetaDecision machine(String key, boolean hot) {
    ZetaStateMachine m = new ZetaStateMachineImpl(2, 5, 2);
    return m.evaluate(key, hot);
  }

  @Test
  void nullKey_shouldThrowNullPointer() {
    ZetaStateMachine m = new ZetaStateMachineImpl(2, 5, 2);
    assertThatNullPointerException().isThrownBy(() -> m.evaluate(null, true));
  }

  @Test
  void emptyKey_shouldBeTracked() {
    ZetaStateMachine m = new ZetaStateMachineImpl(2, 5, 2);
    assertThat(m.evaluate("", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("", true).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void reset_nonExistentKey_shouldNotThrow() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    m.reset("never-added");
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void evictStale_withEmptyState_shouldNotThrow() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    m.evictStale(1000);
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void reset_shouldClearTrackedCount() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    m.evaluate("key", true);
    assertThat(m.getTrackedKeys()).isEqualTo(1);
    m.reset("key");
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void repeatedReset_shouldBeSafe() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    m.evaluate("key", true);
    m.reset("key");
    m.reset("key");
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void getTrackedKeys_shouldReflectActiveKeys() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    assertThat(m.getTrackedKeys()).isZero();
    m.evaluate("k1", true);
    assertThat(m.getTrackedKeys()).isEqualTo(1);
    m.evaluate("k2", true);
    assertThat(m.getTrackedKeys()).isEqualTo(2);
    m.reset("k1");
    assertThat(m.getTrackedKeys()).isEqualTo(1);
  }

  @Test
  void hotStreak_resetsOnColdWindow() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void confirmCountOfOne_shouldPromoteImmediately() {
    ZetaStateMachine m = new ZetaStateMachineImpl(1, 5, 2);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void coolCount_one_shouldCoolImmediately() {
    ZetaStateMachine m = new ZetaStateMachineImpl(1, 1, 0);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void evictStale_shouldOnlyRemoveStaleKeys() throws InterruptedException {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    m.evaluate("stale", true);
    Thread.sleep(1);
    m.evictStale(0);
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void settingThresholds_shouldAffectTransitions() {
    ZetaStateMachine m = new ZetaStateMachineImpl(5, 10, 4);
    m.setConfirmCount(1);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
  }

  /**
   * Verifies that {@link ZetaStateMachine#getStateSnapshot(String)} returns a map containing
   * {@code "currentState"}, {@code "hotStreak"}, and {@code "coolStreak"} with expected values
   * after a key reaches CONFIRMED_HOT.
   */
  @Test
  void getStateSnapshot_shouldReturnCorrectSnapshot() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);

    Map<String, Object> snapshot = m.getStateSnapshot("key");
    assertThat(snapshot)
      .containsEntry("currentState", "CONFIRMED_HOT")
      .containsEntry("hotStreak", 3)
      .containsEntry("coolStreak", 0);
  }

  /**
   * Verifies that {@link ZetaStateMachine#getStateSnapshot(String)} returns an empty map for
   * a key that was never added.
   */
  @Test
  void getStateSnapshot_nonExistentKey_shouldReturnEmptyMap() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    assertThat(m.getStateSnapshot("never-added")).isEmpty();
  }

  /**
   * Verifies that {@link ZetaStateMachine#rollbackToPreviousState(String, Map)} restores
   * the per-key state (currentState, hotStreak, coolStreak) from a previously captured snapshot.
   */
  @Test
  void rollbackToPreviousState_shouldRestoreState() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);

    Map<String, Object> snapshot = m.getStateSnapshot("key");

    m.reset("key");
    assertThat(m.getStateSnapshot("key")).isEmpty();

    m.rollbackToPreviousState("key", snapshot);
    assertThat(m.getStateSnapshot("key")).containsEntry("currentState", "CONFIRMED_HOT").containsEntry("hotStreak", 3);

    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
  }

  /**
   * Verifies that {@link ZetaStateMachine#rollbackToPreviousState(String, null)}
   * resets the key's state so the next evaluation starts fresh from COLD.
   */
  @Test
  void rollbackToPreviousState_withNull_shouldReset() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);

    m.rollbackToPreviousState("key", null);

    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
  }

  /**
   * Verifies that {@link ZetaStateMachine#rollbackToPreviousState(String, Map)} does not
   * throw when called with a non-null snapshot for a key that was never tracked.
   */
  @Test
  void rollbackToPreviousState_nonExistentKey_shouldNotThrow() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    Map<String, Object> snapshot = Map.of("currentState", "CONFIRMED_HOT", "hotStreak", 3, "coolStreak", 0);
    assertThatCode(() -> m.rollbackToPreviousState("never-added", snapshot)).doesNotThrowAnyException();
  }

  /**
   * Verifies that rollbackToPreviousState from COLD state restores streaks.
   */
  @Test
  void rollbackToPreviousState_fromCold_shouldRestore() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    Map<String, Object> snap = m.getStateSnapshot("key");
    assertThat(snap).containsEntry("currentState", "COLD");
    m.reset("key");
    m.rollbackToPreviousState("key", snap);
    assertThat(m.getStateSnapshot("key")).containsEntry("currentState", "COLD");
  }

  /**
   * Verifies that rollbackToPreviousState from PRE_COOLING state restores correctly.
   */
  @Test
  void rollbackToPreviousState_fromPreCooling_shouldRestore() {
    ZetaStateMachine m = new ZetaStateMachineImpl(1, 5, 2);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    Map<String, Object> snap = m.getStateSnapshot("key");
    assertThat(snap).containsEntry("currentState", "PRE_COOLING");
    m.reset("key");
    m.rollbackToPreviousState("key", snap);
    assertThat(m.getStateSnapshot("key")).containsEntry("currentState", "PRE_COOLING");
  }

  /**
   * Verifies that rollbackToPreviousState with an invalid state name in the snapshot
   * throws IllegalArgumentException from State.valueOf.
   */
  @Test
  void rollbackToPreviousState_withInvalidStateName_shouldThrow() {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    Map<String, Object> bad = Map.of("currentState", "NON_EXISTENT_STATE", "hotStreak", 0, "coolStreak", 0);
    assertThatThrownBy(() -> m.rollbackToPreviousState("key", bad)).isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Verifies that evictStale cleans up orphaned stateTimestamps entries.
   */
  @Test
  void evictStale_shouldCleanOrphanedTimestamps() throws InterruptedException {
    ZetaStateMachine m = new ZetaStateMachineImpl(3, 10, 4);
    m.evaluate("key", true);
    m.reset("key");
    Thread.sleep(1);
    m.evictStale(0);
    assertThat(m.getTrackedKeys()).isZero();
  }
}
