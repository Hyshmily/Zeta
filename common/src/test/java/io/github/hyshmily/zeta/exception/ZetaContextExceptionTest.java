package io.github.hyshmily.zeta.exception;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class ZetaContextExceptionTest {

  @Test
  void shouldContainSourceClass() {
    var ex = new ZetaContextException("MySource", "something broke", null);
    assertThat(ex.getSourceClass()).isEqualTo("MySource");
  }

  @Test
  void shouldRecordTimestamp() {
    var before = Instant.now();
    var ex = new ZetaContextException("Src", "msg", null);
    var after = Instant.now();
    assertThat(ex.getTimestamp()).isBetween(before, after);
  }

  @Test
  void logMessageShouldContainFormattedTimestamp() {
    var ex = new ZetaContextException("Src", "hello", null);
    assertThat(ex.getLogMessage()).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\[Src\\] hello");
  }

  @Test
  void getMessageShouldReturnStableDetailMessage() {
    // getMessage() is the raw detail (stable for monitoring matching);
    // the timestamp-prefixed form lives in getLogMessage() only.
    var ex = new ZetaContextException("Src", "detail", null);
    assertThat(ex.getMessage()).isEqualTo("detail");
    assertThat(ex.getLogMessage()).endsWith("[Src] detail");
  }

  @Test
  void defaultDetailMessageShouldBeNullWhenNotProvided() {
    var ex = new ZetaContextException("Src", null, null);
    assertThat(ex.getMessage()).isNull();
    assertThat(ex.getLogMessage()).contains("[Src] null");
  }

  @Test
  void shouldSupportCause() {
    var cause = new IllegalArgumentException("inner");
    var ex = new ZetaContextException("Src", "outer", cause);
    assertThat(ex.getCause()).isSameAs(cause);
    assertThat(ex.getMessage()).isEqualTo("outer");
    assertThat(ex.getLogMessage()).contains("[Src] outer");
  }

  @Test
  void logMessageWithCauseShouldNotContainCauseString() {
    var cause = new RuntimeException("inner");
    var ex = new ZetaContextException("Src", "outer", cause);
    assertThat(ex.getLogMessage()).contains("[Src] outer");
    assertThat(ex.getLogMessage()).doesNotContain("inner");
  }

  @Test
  void getMessageReturnsRawMessageNotLogMessage() {
    var ex = new ZetaContextException("MyClass", "my message", null);
    assertThat(ex.getMessage()).isEqualTo("my message");
    assertThat(ex.getLogMessage()).endsWith("[MyClass] my message");
  }

  @Test
  void logMessageShouldBeMemoized() {
    // The log message is formatted lazily on first access (ADR-0063); repeated
    // access must return the same memoized instance.
    var ex = new ZetaContextException("Src", "hello", null);
    var first = ex.getLogMessage();
    assertThat(ex.getLogMessage()).isSameAs(first);
    assertThat(ex.getMessage()).isEqualTo("hello");
  }

  @Test
  void defaultConstructorShouldCarryStackTrace() {
    // Only control-flow subclasses (ZetaBlockedException) opt out of stack filling;
    // the base class keeps the standard Throwable behaviour.
    var ex = new ZetaContextException("Src", "hello", null);
    assertThat(ex.getStackTrace()).isNotEmpty();
  }
}
