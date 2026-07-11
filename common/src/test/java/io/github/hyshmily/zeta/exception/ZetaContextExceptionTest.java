package io.github.hyshmily.zeta.exception;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class ZetaContextExceptionTest {

  @Test
  void shouldContainSourceClass() {
    var ex = new ZetaContextException("MySource", "something broke");
    assertThat(ex.getSourceClass()).isEqualTo("MySource");
  }

  @Test
  void shouldRecordTimestamp() {
    var before = Instant.now();
    var ex = new ZetaContextException("Src", "msg");
    var after = Instant.now();
    assertThat(ex.getTimestamp()).isBetween(before, after);
  }

  @Test
  void logMessageShouldContainFormattedTimestamp() {
    var ex = new ZetaContextException("Src", "hello");
    assertThat(ex.getMessage()).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\[Src\\] hello");
  }

  @Test
  void getMessageShouldReturnLogMessage() {
    var ex = new ZetaContextException("Src", "detail");
    assertThat(ex.getMessage()).endsWith("[Src] detail");
  }

  @Test
  void defaultDetailMessageShouldBeNullWhenNotProvided() {
    var ex = new ZetaContextException("Src", null);
    assertThat(ex.getMessage()).contains("[Src] null");
  }

  @Test
  void shouldSupportCause() {
    var cause = new IllegalArgumentException("inner");
    var ex = new ZetaContextException("Src", "outer", cause);
    assertThat(ex.getCause()).isSameAs(cause);
    assertThat(ex.getMessage()).contains("[Src] outer");
  }

  @Test
  void logMessageWithCauseShouldNotContainCauseString() {
    var cause = new RuntimeException("inner");
    var ex = new ZetaContextException("Src", "outer", cause);
    assertThat(ex.getMessage()).contains("[Src] outer");
    assertThat(ex.getMessage()).doesNotContain("inner");
  }

  @Test
  void getMessageReturnsFormattedLogMessage() {
    var ex = new ZetaContextException("MyClass", "my message");
    assertThat(ex.getMessage()).isEqualTo(ex.getLogMessage());
  }
}
