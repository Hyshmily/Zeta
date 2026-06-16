/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.hyshmily.hotkey.sync;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CacheSyncProperties} verifying default configuration values and queue name
 * prefix behavior.
 */
class CacheSyncPropertiesTest {

  /**
   * Verifies that CacheSyncProperties initializes with expected default values.
   */
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

  /**
   * Verifies that getQueueName returns a name starting with the configured queue prefix followed by colon.
   */
  @Test
  void getQueueName_shouldReturnPrefixedName() {
    CacheSyncProperties props = new CacheSyncProperties();
    assertThat(props.getQueueName()).startsWith("hotkey.sync:");
  }

  /**
   * Verifies all default values are properly initialized.
   */
  @Test
  void shouldHaveAllDefaultValues() {
    CacheSyncProperties props = new CacheSyncProperties();
    assertThat(props.getConcurrentConsumers()).isEqualTo(3);
    assertThat(props.getSchedulerPoolSize()).isEqualTo(4);
    assertThat(props.getPrefetchCount()).isEqualTo(5);
    assertThat(props.isAutoStartup()).isTrue();
  }

  /**
   * Verifies that setters are functional.
   */
  @Test
  void shouldAllowSettingValues() {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setEnabled(true);
    props.setDedupWindowSeconds(30);
    props.setDedupMaxSize(50_000);
    props.setWarmupJitterMs(200);
    assertThat(props.isEnabled()).isTrue();
    assertThat(props.getDedupWindowSeconds()).isEqualTo(30);
    assertThat(props.getDedupMaxSize()).isEqualTo(50_000);
    assertThat(props.getWarmupJitterMs()).isEqualTo(200);
  }
}
