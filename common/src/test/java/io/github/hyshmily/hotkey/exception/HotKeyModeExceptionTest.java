package io.github.hyshmily.hotkey.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("HotKeyModeException tests")
class HotKeyModeExceptionTest {

  @Test
  @DisplayName("constructor sets message")
  void constructor_setsMessage() {
    HotKeyModeException ex = new HotKeyModeException("get", "Worker-only mode", "App-mode cache");
    assertThat(ex.getMessage())
      .contains("get")
      .contains("Worker-only mode")
      .contains("App-mode cache");
  }

  @Test
  @DisplayName("getOperation returns the operation name")
  void getOperation_returnsOperation() {
    HotKeyModeException ex = new HotKeyModeException("putThrough", "Worker-only mode", "App-mode cache");
    assertThat(ex.getOperation()).isEqualTo("putThrough");
  }

  @Test
  @DisplayName("getCurrentMode returns the current mode label")
  void getCurrentMode_returnsCurrentMode() {
    HotKeyModeException ex = new HotKeyModeException("get", "Worker-only mode", "App-mode cache");
    assertThat(ex.getCurrentMode()).isEqualTo("Worker-only mode");
  }

  @Test
  @DisplayName("getRequiredMode returns the required mode label")
  void getRequiredMode_returnsRequiredMode() {
    HotKeyModeException ex = new HotKeyModeException("get", "Worker-only mode", "App-mode cache");
    assertThat(ex.getRequiredMode()).isEqualTo("App-mode cache");
  }
}
