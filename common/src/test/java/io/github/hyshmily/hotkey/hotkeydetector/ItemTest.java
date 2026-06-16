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

import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.Item;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link Item}, the immutable key-count pair record used by the HeavyKeeper algorithm.
 */
class ItemTest {

  /**
   * Verifies that an Item record is created with the correct key and count fields.
   */
  @Test
  void shouldCreateItemWithKeyAndCount() {
    Item item = new Item("key1", 42);
    assertThat(item.key()).isEqualTo("key1");
    assertThat(item.count()).isEqualTo(42);
  }

  @Test
  void shouldHandleNullKey() {
    Item item = new Item(null, 5);
    assertThat(item.key()).isNull();
    assertThat(item.count()).isEqualTo(5);
  }

  @Test
  void shouldHandleZeroCount() {
    Item item = new Item("key1", 0);
    assertThat(item.key()).isEqualTo("key1");
    assertThat(item.count()).isEqualTo(0);
  }

  @Test
  void shouldHandleNegativeCount() {
    Item item = new Item("key1", -1);
    assertThat(item.key()).isEqualTo("key1");
    assertThat(item.count()).isEqualTo(-1);
  }

  @Test
  void shouldVerifyEqualsAndHashCode() {
    Item i1 = new Item("key1", 42);
    Item i2 = new Item("key1", 42);
    Item i3 = new Item("key1", 99);
    assertThat(i1).isEqualTo(i2);
    assertThat(i1).hasSameHashCodeAs(i2);
    assertThat(i1).isNotEqualTo(i3);
  }

  @Test
  void shouldVerifyToString() {
    Item item = new Item("key1", 42);
    assertThat(item.toString()).contains("key1", "42");
  }
}
