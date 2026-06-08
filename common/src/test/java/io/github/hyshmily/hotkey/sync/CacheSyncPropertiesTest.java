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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.sync.CacheSyncProperties;
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
