package io.github.hyshmily.hotkey.detection;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.entity.HotKeyDecision;
import io.github.hyshmily.hotkey.entity.HotKeyDecision.DecisionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HotKeyStateMachineTest {

  private HotKeyStateMachine machine;

  @BeforeEach
  void setUp() {
    machine = new HotKeyStateMachine(3, 10, 4);
  }

  @Test
  void coldToHot_requiresConfirmCountConsecutiveHotWindows() {
    assertThat(machine.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true).type()).isEqualTo(DecisionType.NONE);
    assertThat(machine.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void hotToCool_requiresCoolCountConsecutiveColdWindows() {
    HotKeyDecision last = null;
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

  @Test
  void preCooling_toHot_shouldReviveWithoutOscillation() {
    HotKeyDecision last = null;
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

  @Test
  void reset_shouldClearState() {
    HotKeyDecision last = null;
    for (int i = 0; i < 3; i++) {
      last = machine.evaluate("key", true);
    }
    assertThat(last.type()).isEqualTo(DecisionType.HOT);
    machine.reset("key");
    assertThat(machine.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
  }

  @Test
  void evictStale_shouldRemoveOldKeys() throws InterruptedException {
    machine.evaluate("staleKey", true);
    Thread.sleep(50);
    machine.evictStale(10);
    assertThat(machine.evaluate("staleKey", false).type()).isEqualTo(DecisionType.NONE);
  }
}
