package io.github.hyshmily.zeta.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ZetaModeException tests")
class ZetaModeExceptionTest {

  @Test
  @DisplayName("constructor sets message")
  void constructor_setsMessage() {
    ZetaModeException ex = new ZetaModeException("get", "Worker-only mode", "App-mode cache");
    assertThat(ex.getMessage()).contains("get").contains("Worker-only mode").contains("App-mode cache");
  }

  @Test
  @DisplayName("getOperation returns the operation name")
  void getOperation_returnsOperation() {
    ZetaModeException ex = new ZetaModeException("putThrough", "Worker-only mode", "App-mode cache");
    assertThat(ex.getOperation()).isEqualTo("putThrough");
  }

  @Test
  @DisplayName("getCurrentMode returns the current mode label")
  void getCurrentMode_returnsCurrentMode() {
    ZetaModeException ex = new ZetaModeException("get", "Worker-only mode", "App-mode cache");
    assertThat(ex.getCurrentMode()).isEqualTo("Worker-only mode");
  }

  @Test
  @DisplayName("getRequiredMode returns the required mode label")
  void getRequiredMode_returnsRequiredMode() {
    ZetaModeException ex = new ZetaModeException("get", "Worker-only mode", "App-mode cache");
    assertThat(ex.getRequiredMode()).isEqualTo("App-mode cache");
  }
}
