package io.github.hyshmily.hotkey.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.annotation.HotKey.OperationType;
import org.junit.jupiter.api.Test;

class HotKeyAnnotationTest {

  @Test
  void operationType_shouldHaveExpectedValues() {
    assertThat(OperationType.values()).containsExactly(
      OperationType.READ,
      OperationType.WRITE,
      OperationType.INVALIDATE
    );
  }

  @Test
  void operationType_defaultShouldBeRead() {
    assertThat(OperationType.READ).isEqualTo(OperationType.READ);
  }
}
