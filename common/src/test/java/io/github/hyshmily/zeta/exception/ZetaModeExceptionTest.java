package io.github.hyshmily.zeta.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ZetaModeExceptionTest {

  @Test
  void messageShouldNameTheOperationAndBothModes() {
    ZetaModeException ex = new ZetaModeException("get", "Worker-only mode", "App-mode cache");

    assertThat(ex.getMessage()).contains("get").contains("Worker-only mode").contains("App-mode cache");
    assertThat(ex.getOperation()).isEqualTo("get");
    assertThat(ex.getCurrentMode()).isEqualTo("Worker-only mode");
    assertThat(ex.getRequiredMode()).isEqualTo("App-mode cache");
  }
}
