package io.github.hyshmily.hotkey.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Item}, the immutable key-count pair record used by the HeavyKeeper algorithm.
 */
class ItemTest {

  @Test
  void shouldCreateItemWithKeyAndCount() {
    Item item = new Item("key1", 42);
    assertThat(item.key()).isEqualTo("key1");
    assertThat(item.count()).isEqualTo(42);
  }
}
