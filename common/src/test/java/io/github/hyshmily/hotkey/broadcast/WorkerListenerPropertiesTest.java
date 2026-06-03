package io.github.hyshmily.hotkey.broadcast;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WorkerListenerProperties} verifying default configuration values and queue name
 * prefix behavior.
 */
class WorkerListenerPropertiesTest {

  @Test
  void shouldHaveDefaultValues() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    assertThat(props.isEnabled()).isFalse();
    assertThat(props.getExchangeName()).isEqualTo("hotkey.worker.exchange");
    assertThat(props.getQueuePrefix()).isEqualTo("hotkey.worker");
    assertThat(props.getWarmupJitterMs()).isEqualTo(100);
  }

  @Test
  void getQueueName_shouldReturnPrefixedName() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    assertThat(props.getQueueName()).startsWith("hotkey.worker:");
  }
}
