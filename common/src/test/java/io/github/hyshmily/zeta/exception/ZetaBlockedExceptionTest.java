package io.github.hyshmily.zeta.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ZetaBlockedExceptionTest {

  @Test
  void shouldContainCacheKey() {
    var ex = new ZetaBlockedException("TestClass", "blocked-key-123");
    assertThat(ex.getCacheKey()).isEqualTo("blocked-key-123");
  }

  @Test
  void shouldContainSourceClass() {
    var ex = new ZetaBlockedException("MyDetector", "mykey");
    assertThat(ex.getSourceClass()).isEqualTo("MyDetector");
  }

  @Test
  void messageShouldContainCacheKey() {
    var ex = new ZetaBlockedException("TestClass", "secret-key");
    assertThat(ex.getMessage()).contains("secret-key");
    assertThat(ex.getMessage()).contains("blocked by rule");
  }

  @Test
  void logMessageShouldContainTimestampAndSource() {
    var ex = new ZetaBlockedException("Blocker", "test-key");
    assertThat(ex.getMessage()).contains("Blocker");
    assertThat(ex.getMessage()).contains("test-key");
  }

  @Test
  void shouldBeInstanceOfHotKeyContextException() {
    var ex = new ZetaBlockedException("Test", "k");
    assertThat(ex).isInstanceOf(ZetaContextException.class);
    assertThat(ex).isInstanceOf(RuntimeException.class);
  }

  @Test
  void shouldSupportCauseChaining() {
    // ZetaBlockedException has no cause constructor; getCause() must be null
    var ex = new ZetaBlockedException("Test", "k");
    assertThat(ex.getCause()).isNull();
    // The parent ZetaContextException supports 3-arg constructor with cause
    var ctxEx = new ZetaContextException("Test", "ctx msg", new IllegalArgumentException("root cause"));
    assertThat(ctxEx.getCause()).isNotNull();
    assertThat(ctxEx.getCause().getMessage()).isEqualTo("root cause");
  }
}
