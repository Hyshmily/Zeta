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
package io.github.hyshmily.hotkey.constants;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyConstants} verifying constant values for AMQP, threading, and routing.
 */
class HotKeyConstantsTest {

  @Test
  void shouldHaveExpectedAmqpHeaders() {
    assertThat(HotKeyConstants.AMQP_HEADER_TYPE).isEqualTo("type");
    assertThat(HotKeyConstants.AMQP_HEADER_VERSION).isEqualTo("version");
    assertThat(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED).isEqualTo("isVersionDegraded");
  }

  @Test
  void shouldHaveExpectedThreadPrefixes() {
    assertThat(HotKeyConstants.THREAD_PREFIX_HOTKEY).isEqualTo("hotkey-");
    assertThat(HotKeyConstants.THREAD_PREFIX_SYNC).isEqualTo("hotkey-sync");
    assertThat(HotKeyConstants.THREAD_PREFIX_WORKER).isEqualTo("hotkey-worker");
    assertThat(HotKeyConstants.THREAD_PREFIX_REPORT).isEqualTo("hotkey-report");
  }

  @Test
  void shouldHaveExpectedRoutingKeys() {
    assertThat(HotKeyConstants.ROUTING_KEY_BROADCAST).isEqualTo("broadcast.");
    assertThat(HotKeyConstants.ROUTING_KEY_REPORT).isEqualTo("report.");
  }

  @Test
  void shouldHaveExpectedQueuePrefix() {
    assertThat(HotKeyConstants.QUEUE_PREFIX_REPORT).isEqualTo("hotkey.report.");
  }

  @Test
  void shouldHaveExpectedSources() {
    assertThat(HotKeyConstants.SOURCE_SLIDING_WINDOW).isEqualTo("sliding_window");
    assertThat(HotKeyConstants.SOURCE_TOPK_PRE_WARM).isEqualTo("topk_pre_warm");
  }

  @Test
  void shouldHaveExpectedRedisKeyPrefix() {
    assertThat(HotKeyConstants.REDIS_VERSION_KEY_PREFIX).isEqualTo("hotkey:ver:");
  }

  @Test
  void shouldHaveExpectedDefaultVersion() {
    assertThat(HotKeyConstants.VERSION_DEFAULT).isZero();
  }

  @Test
  void shouldHaveExpectedTopKIncrement() {
    assertThat(HotKeyConstants.TOPK_INCR).isEqualTo(1);
  }
}
