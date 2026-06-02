package io.github.hyshmily.hotkey.entity;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KeyStateTest {

  @Test
  void shouldHaveExpectedEnumValues() {
    assertThat(KeyState.values()).containsExactly(KeyState.HOT, KeyState.COOL, KeyState.PRE_COOL, KeyState.NORMAL);
  }
}
