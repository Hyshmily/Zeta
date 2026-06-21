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
package io.github.hyshmily.hotkey.detection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.github.hyshmily.hotkey.model.HotKeyDecision;
import io.github.hyshmily.hotkey.model.HotKeyDecision.DecisionType;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Edge case tests for {@link HotKeyStateMachine} covering single window, immediate cooling, and interleaved keys.
 */
class HotKeyStateMachineEdgeTest {

  /**
   * Verifies that the state machine emits HOT on the first evaluation within a single-window configuration.
   */
  @Test
  void shouldHandleSingleHotWindow() {
    HotKeyStateMachine m = new HotKeyStateMachine(1, 5, 2);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
  }

  /**
   * Verifies that a hot window followed by cold windows transitions from HOT to NONE to COOL.
   */
  @Test
  void shouldHandleImmediateCooling() {
    HotKeyStateMachine m = new HotKeyStateMachine(1, 2, 1);
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

  private static HotKeyDecision machine(String key, boolean hot) {
    HotKeyStateMachine m = new HotKeyStateMachine(2, 5, 2);
    return m.evaluate(key, hot);
  }

  @Test
  void nullKey_shouldThrowNullPointer() {
    HotKeyStateMachine m = new HotKeyStateMachine(2, 5, 2);
    assertThatNullPointerException().isThrownBy(() -> m.evaluate(null, true));
  }

  @Test
  void emptyKey_shouldBeTracked() {
    HotKeyStateMachine m = new HotKeyStateMachine(2, 5, 2);
    assertThat(m.evaluate("", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("", true).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void reset_nonExistentKey_shouldNotThrow() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    m.reset("never-added");
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void evictStale_withEmptyState_shouldNotThrow() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    m.evictStale(1000);
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void reset_shouldClearTrackedCount() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    m.evaluate("key", true);
    assertThat(m.getTrackedKeys()).isEqualTo(1);
    m.reset("key");
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void repeatedReset_shouldBeSafe() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    m.evaluate("key", true);
    m.reset("key");
    m.reset("key");
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void getTrackedKeys_shouldReflectActiveKeys() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
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
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void confirmCountOfOne_shouldPromoteImmediately() {
    HotKeyStateMachine m = new HotKeyStateMachine(1, 5, 2);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void coolCount_one_shouldCoolImmediately() {
    HotKeyStateMachine m = new HotKeyStateMachine(1, 1, 0);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.COOL);
  }

  @Test
  void evictStale_shouldOnlyRemoveStaleKeys() throws InterruptedException {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    m.evaluate("stale", true);
    Thread.sleep(1);
    m.evictStale(0);
    assertThat(m.getTrackedKeys()).isZero();
  }

  @Test
  void settingThresholds_shouldAffectTransitions() {
    HotKeyStateMachine m = new HotKeyStateMachine(5, 10, 4);
    m.setConfirmCount(1);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
  }

  /**
   * Verifies that {@link HotKeyStateMachine#getStateSnapshot(String)} returns a map containing
   * {@code "currentState"}, {@code "hotStreak"}, and {@code "coolStreak"} with expected values
   * after a key reaches CONFIRMED_HOT.
   */
  @Test
  void getStateSnapshot_shouldReturnCorrectSnapshot() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
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
   * Verifies that {@link HotKeyStateMachine#getStateSnapshot(String)} returns an empty map for
   * a key that was never added.
   */
  @Test
  void getStateSnapshot_nonExistentKey_shouldReturnEmptyMap() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    assertThat(m.getStateSnapshot("never-added")).isEmpty();
  }

  /**
   * Verifies that {@link HotKeyStateMachine#rollbackToPreviousState(String, Map)} restores
   * the per-key state (currentState, hotStreak, coolStreak) from a previously captured snapshot.
   */
  @Test
  void rollbackToPreviousState_shouldRestoreState() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);

    Map<String, Object> snapshot = m.getStateSnapshot("key");

    m.reset("key");
    assertThat(m.getStateSnapshot("key")).isEmpty();

    m.rollbackToPreviousState("key", snapshot);
    assertThat(m.getStateSnapshot("key"))
      .containsEntry("currentState", "CONFIRMED_HOT")
      .containsEntry("hotStreak", 3);

    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
  }

  /**
   * Verifies that {@link HotKeyStateMachine#rollbackToPreviousState(String, null)}
   * resets the key's state so the next evaluation starts fresh from COLD.
   */
  @Test
  void rollbackToPreviousState_withNull_shouldReset() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);

    m.rollbackToPreviousState("key", null);

    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
  }

  /**
   * Verifies that {@link HotKeyStateMachine#rollbackToPreviousState(String, Map)} does not
   * throw when called with a non-null snapshot for a key that was never tracked.
   */
  @Test
  void rollbackToPreviousState_nonExistentKey_shouldNotThrow() {
    HotKeyStateMachine m = new HotKeyStateMachine(3, 10, 4);
    Map<String, Object> snapshot = Map.of(
      "currentState", "CONFIRMED_HOT",
      "hotStreak", 3,
      "coolStreak", 0
    );
    assertThatCode(() -> m.rollbackToPreviousState("never-added", snapshot))
      .doesNotThrowAnyException();
  }
}
