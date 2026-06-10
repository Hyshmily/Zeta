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
package io.github.hyshmily.hotkey.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.annotation.HotKey.OperationType;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKey} annotation {@link HotKey.OperationType} enum values.
 */
class HotKeyAnnotationTest {

  /**
   * Verifies that the {@link OperationType} enum contains all three expected values: READ, WRITE, INVALIDATE.
   */
  @Test
  void operationType_shouldHaveExpectedValues() {
    assertThat(OperationType.values()).containsExactly(
      OperationType.READ,
      OperationType.WRITE,
      OperationType.INVALIDATE
    );
  }

  /**
   * Verifies that the default operation type is READ.
   */
  @Test
  void operationType_defaultShouldBeRead() {
    assertThat(OperationType.READ).isEqualTo(OperationType.READ);
  }
}
