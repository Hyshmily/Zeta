package io.github.hyshmily.hotkey.detection;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.entity.HotKeyDecision;
import io.github.hyshmily.hotkey.entity.HotKeyDecision.DecisionType;
import org.junit.jupiter.api.Test;

class HotKeyStateMachineEdgeTest {

  @Test
  void shouldHandleSingleHotWindow() {
    HotKeyStateMachine m = new HotKeyStateMachine(1, 5, 2);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
  }

  @Test
  void shouldHandleImmediateCooling() {
    HotKeyStateMachine m = new HotKeyStateMachine(1, 2, 1);
    assertThat(m.evaluate("key", true).type()).isEqualTo(DecisionType.HOT);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.NONE);
    assertThat(m.evaluate("key", false).type()).isEqualTo(DecisionType.COOL);
  }

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
}
