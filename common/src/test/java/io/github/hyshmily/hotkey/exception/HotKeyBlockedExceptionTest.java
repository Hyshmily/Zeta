package io.github.hyshmily.hotkey.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HotKeyBlockedExceptionTest {

  @Test
  void shouldContainCacheKey() {
    var ex = new HotKeyBlockedException("TestClass", "blocked-key-123");
    assertThat(ex.getCacheKey()).isEqualTo("blocked-key-123");
  }

  @Test
  void shouldContainSourceClass() {
    var ex = new HotKeyBlockedException("MyDetector", "mykey");
    assertThat(ex.getSourceClass()).isEqualTo("MyDetector");
  }

  @Test
  void messageShouldContainCacheKey() {
    var ex = new HotKeyBlockedException("TestClass", "secret-key");
    assertThat(ex.getMessage()).contains("secret-key");
    assertThat(ex.getMessage()).contains("blocked by rule");
  }

  @Test
  void logMessageShouldContainTimestampAndSource() {
    var ex = new HotKeyBlockedException("Blocker", "test-key");
    assertThat(ex.getMessage()).contains("Blocker");
    assertThat(ex.getMessage()).contains("test-key");
  }

  @Test
  void shouldBeInstanceOfHotKeyContextException() {
    var ex = new HotKeyBlockedException("Test", "k");
    assertThat(ex).isInstanceOf(HotKeyContextException.class);
    assertThat(ex).isInstanceOf(RuntimeException.class);
  }

  @Test
  void shouldSupportCauseChaining() {
    // HotKeyBlockedException has no cause constructor; getCause() must be null
    var ex = new HotKeyBlockedException("Test", "k");
    assertThat(ex.getCause()).isNull();
    // The parent HotKeyContextException supports 3-arg constructor with cause
    var ctxEx = new io.github.hyshmily.hotkey.exception.HotKeyContextException(
      "Test", "ctx msg", new IllegalArgumentException("root cause"));
    assertThat(ctxEx.getCause()).isNotNull();
    assertThat(ctxEx.getCause().getMessage()).isEqualTo("root cause");
  }
}
