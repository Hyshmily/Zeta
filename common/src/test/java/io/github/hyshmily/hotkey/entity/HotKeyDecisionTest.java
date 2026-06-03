package io.github.hyshmily.hotkey.entity;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.entity.HotKeyDecision.DecisionType;
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
    assertThat(DecisionType.values()).containsExactly(DecisionType.HOT, DecisionType.COOL, DecisionType.NONE);
  }
}
