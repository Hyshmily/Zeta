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
package io.github.hyshmily.zeta.sync;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.sync.worker.WorkerListenerProperties;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WorkerListenerProperties} verifying default configuration values and queue name
 * prefix behavior.
 */
class WorkerListenerPropertiesTest {

  /**
   * Verifies that WorkerListenerProperties initializes with expected default values.
   */
  @Test
  void shouldHaveDefaultValues() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    assertThat(props.isEnabled()).isFalse();
    assertThat(props.getExchangeName()).isEqualTo(ZetaConstants.EXCHANGE_BROADCAST);
    assertThat(props.getQueuePrefix()).isEqualTo("zeta.worker");
    assertThat(props.getWarmupJitterMs()).isEqualTo(50);
  }

  /**
   * Verifies that getQueueName returns a name starting with the configured worker queue prefix.
   */
  @Test
  void getQueueName_shouldReturnPrefixedName() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    assertThat(props.getQueueName()).startsWith("zeta.worker:");
  }

  /**
   * Verifies SRE sub-properties have expected defaults.
   */
  @Test
  void sreProperties_shouldHaveDefaults() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    assertThat(props.getSre()).isNotNull();
    assertThat(props.getSre().isEnabled()).isTrue();
    assertThat(props.getSre().getWindowMs()).isEqualTo(3000);
    assertThat(props.getSre().getBuckets()).isEqualTo(10);
    assertThat(props.getSre().getMinSamples()).isEqualTo(20);
    assertThat(props.getSre().getSuccessThreshold()).isEqualTo(0.6);
  }

  /**
   * Verifies all remaining default values.
   */
  @Test
  void shouldHaveAllDefaultValues() {
    WorkerListenerProperties props = new WorkerListenerProperties();
    assertThat(props.getConcurrentConsumers()).isEqualTo(2);
    assertThat(props.getSchedulerPoolSize()).isEqualTo(2);
    assertThat(props.getPrefetchCount()).isEqualTo(5);
    assertThat(props.isAutoStartup()).isTrue();
  }
}
