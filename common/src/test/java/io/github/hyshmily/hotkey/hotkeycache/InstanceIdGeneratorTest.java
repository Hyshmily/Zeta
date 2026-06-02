package io.github.hyshmily.hotkey.hotkeycache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class InstanceIdGeneratorTest {

  @AfterEach
  void tearDown() {
    InstanceIdGenerator.setOverride("");
  }

  @Test
  void getNodeId_shouldReturnNonNegative() {
    assertThat(InstanceIdGenerator.getNodeId()).isNotNegative();
  }

  @Test
  void get_shouldReturnCachedValueOnSecondCall() {
    String first = InstanceIdGenerator.get();
    String second = InstanceIdGenerator.get();
    assertThat(second).isEqualTo(first);
  }

  @Test
  void setOverride_shouldTakePrecedence() {
    InstanceIdGenerator.setOverride("custom-instance");
    assertThat(InstanceIdGenerator.get()).isEqualTo("custom-instance");
  }
}
