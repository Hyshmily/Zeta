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

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link KeyState} enum verifying its expected values.
 */
class KeyStateTest {

  /**
   * Verifies the KeyState enum contains exactly HOT, COOL, and NORMAL in that order.
   */
  @Test
  void shouldHaveExpectedEnumValues() {
    assertThat(KeyState.values()).containsExactly(KeyState.HOT, KeyState.COOL, KeyState.NORMAL);
  }

  @Test
  void hot_shouldHaveCorrectName() {
    assertThat(KeyState.HOT.name()).isEqualTo("HOT");
  }

  @Test
  void cool_shouldHaveCorrectName() {
    assertThat(KeyState.COOL.name()).isEqualTo("COOL");
  }

  @Test
  void normal_shouldHaveCorrectName() {
    assertThat(KeyState.NORMAL.name()).isEqualTo("NORMAL");
  }

  @Test
  void hot_shouldHaveOrdinalZero() {
    assertThat(KeyState.HOT.ordinal()).isZero();
  }

  @Test
  void cool_shouldHaveOrdinalOne() {
    assertThat(KeyState.COOL.ordinal()).isOne();
  }

  @Test
  void normal_shouldHaveOrdinalTwo() {
    assertThat(KeyState.NORMAL.ordinal()).isEqualTo(2);
  }

  @Test
  void valueOf_shouldResolveHOT() {
    assertThat(KeyState.valueOf("HOT")).isEqualTo(KeyState.HOT);
  }

  @Test
  void valueOf_shouldResolveCOOL() {
    assertThat(KeyState.valueOf("COOL")).isEqualTo(KeyState.COOL);
  }

  @Test
  void valueOf_shouldResolveNORMAL() {
    assertThat(KeyState.valueOf("NORMAL")).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void switch_shouldCoverAllCases() {
    for (KeyState state : KeyState.values()) {
      boolean covered = switch (state) {
        case HOT    -> true;
        case COOL   -> true;
        case NORMAL -> true;
      };
      assertThat(covered).isTrue();
    }
  }
}
