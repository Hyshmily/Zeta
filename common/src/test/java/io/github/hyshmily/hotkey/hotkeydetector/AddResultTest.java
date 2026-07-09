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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.AddResult;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AddResult}, the result recordReport returned by HeavyKeeper.addDirect() covering hot-key,
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

  @Test
  void shouldHandleNullCurrentKey() {
    AddResult result = new AddResult(null, true, null);
    assertThat(result.currentKey()).isNull();
    assertThat(result.isHotKey()).isTrue();
  }

  @Test
  void shouldVerifyEqualsAndHashCode() {
    AddResult r1 = new AddResult("expelled", true, "current");
    AddResult r2 = new AddResult("expelled", true, "current");
    AddResult r3 = new AddResult("expelled", false, "current");
    assertThat(r1).isEqualTo(r2);
    assertThat(r1).hasSameHashCodeAs(r2);
    assertThat(r1).isNotEqualTo(r3);
  }

  @Test
  void shouldVerifyToString() {
    AddResult result = new AddResult("expelled", true, "current");
    assertThat(result.toString()).contains("expelled", "true", "current");
  }

  @Test
  void cold_shouldReturnSingleton() {
    AddResult cold1 = AddResult.cold();
    AddResult cold2 = AddResult.cold();
    assertThat(cold1).isSameAs(cold2);
    assertThat(cold1.isHotKey()).isFalse();
    assertThat(cold1.expelledKey()).isNull();
  }

  @Test
  void hot_withExpelledKey_shouldHaveExpelledKey() {
    AddResult result = AddResult.cold();
    assertThat(result.expelledKey()).isNull();
    result = new AddResult("expelledKey", true, "currentKey");
    assertThat(result.expelledKey()).isEqualTo("expelledKey");
    assertThat(result.isHotKey()).isTrue();
    assertThat(result.currentKey()).isEqualTo("currentKey");
  }
}
