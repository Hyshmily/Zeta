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
package io.github.hyshmily.hotkey.hotkeydetector;

import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.AddResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AddResult}, the result record returned by HeavyKeeper.add() covering hot-key,
 * non-hot, and expelled-key scenarios.
 */
class AddResultTest {

  /**
   * Verifies that a hot-key result has {@code isHotKey() == true}, the correct key, and no expelled key.
   */
  @Test
  void shouldCreateHotKeyResult() {
    AddResult result = new AddResult(null, true, "key1");
    assertThat(result.isHotKey()).isTrue();
    assertThat(result.currentKey()).isEqualTo("key1");
    assertThat(result.expelledKey()).isNull();
  }

  /**
   * Verifies that a result created with an expelled key retains it.
   */
  @Test
  void shouldCreateResultWithExpelledKey() {
    AddResult result = new AddResult("expelledKey", true, "key1");
    assertThat(result.expelledKey()).isEqualTo("expelledKey");
  }

  /**
   * Verifies that a non-hot result has {@code isHotKey() == false}.
   */
  @Test
  void shouldCreateNonHotResult() {
    AddResult result = new AddResult(null, false, "key1");
    assertThat(result.isHotKey()).isFalse();
  }
}
