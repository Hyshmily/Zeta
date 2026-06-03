package io.github.hyshmily.hotkey.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AddResult}, the result record returned by HeavyKeeper.add() covering hot-key,
 * non-hot, and expelled-key scenarios.
 */
class AddResultTest {

  @Test
  void shouldCreateHotKeyResult() {
    AddResult result = new AddResult(null, true, "key1");
    assertThat(result.isHotKey()).isTrue();
    assertThat(result.currentKey()).isEqualTo("key1");
    assertThat(result.expelledKey()).isNull();
  }

  @Test
  void shouldCreateResultWithExpelledKey() {
    AddResult result = new AddResult("expelledKey", true, "key1");
    assertThat(result.expelledKey()).isEqualTo("expelledKey");
  }

  @Test
  void shouldCreateNonHotResult() {
    AddResult result = new AddResult(null, false, "key1");
    assertThat(result.isHotKey()).isFalse();
  }
}
