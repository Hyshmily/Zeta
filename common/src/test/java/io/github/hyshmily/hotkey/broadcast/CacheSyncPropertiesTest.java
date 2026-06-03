package io.github.hyshmily.hotkey.broadcast;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CacheSyncProperties} verifying default configuration values and queue name
 * prefix behavior.
 */
class CacheSyncPropertiesTest {

  @Test
  void shouldHaveDefaultValues() {
    CacheSyncProperties props = new CacheSyncProperties();
    assertThat(props.isEnabled()).isFalse();
    assertThat(props.getExchangeName()).isEqualTo("hotkey.sync.exchange");
    assertThat(props.getQueuePrefix()).isEqualTo("hotkey.sync");
    assertThat(props.getDedupWindowSeconds()).isEqualTo(10);
    assertThat(props.getDedupMaxSize()).isEqualTo(10_000);
    assertThat(props.getWarmupJitterMs()).isEqualTo(100);
  }

  @Test
  void getQueueName_shouldReturnPrefixedName() {
    CacheSyncProperties props = new CacheSyncProperties();
    assertThat(props.getQueueName()).startsWith("hotkey.sync:");
  }
}
