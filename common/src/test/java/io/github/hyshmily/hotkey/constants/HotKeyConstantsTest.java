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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link HotKeyConstants} verifying constant values for AMQP, threading, and routing.
 */
class HotKeyConstantsTest {

  /**
   * Verifies the AMQP message header constant values.
   */
  @Test
  void shouldHaveExpectedAmqpHeaders() {
    assertThat(HotKeyConstants.AMQP_HEADER_TYPE).isEqualTo("type");
    assertThat(HotKeyConstants.AMQP_HEADER_VERSION).isEqualTo("version");
    assertThat(HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED).isEqualTo("isVersionDegraded");
  }

  /**
   * Verifies the thread name prefix constant values.
   */
  @Test
  void shouldHaveExpectedThreadPrefixes() {
    assertThat(HotKeyConstants.THREAD_PREFIX_HOTKEY).isEqualTo("hotkeydetector-");
    assertThat(HotKeyConstants.THREAD_PREFIX_SYNC).isEqualTo("hotkeydetector-sync");
    assertThat(HotKeyConstants.THREAD_PREFIX_WORKER).isEqualTo("hotkeydetector-worker");
    assertThat(HotKeyConstants.THREAD_PREFIX_REPORT).isEqualTo("hotkeydetector-report");
  }

  /**
   * Verifies the AMQP routing key constant values.
   */
  @Test
  void shouldHaveExpectedRoutingKeys() {
    assertThat(HotKeyConstants.ROUTING_KEY_BROADCAST).isEqualTo("broadcast.");
    assertThat(HotKeyConstants.ROUTING_KEY_REPORT).isEqualTo("report.");
  }

  /**
   * Verifies the report queue prefix constant value.
   */
  @Test
  void shouldHaveExpectedQueuePrefix() {
    assertThat(HotKeyConstants.QUEUE_PREFIX_REPORT).isEqualTo("hotkey.report.");
  }

  /**
   * Verifies the source identifier constant values.
   */
  @Test
  void shouldHaveExpectedSources() {
    assertThat(HotKeyConstants.SOURCE_SLIDING_WINDOW).isEqualTo("sliding_window");
    assertThat(HotKeyConstants.SOURCE_TOPK_PRE_WARM).isEqualTo("topk_pre_warm");
  }

  /**
   * Verifies the Redis version key prefix constant value.
   */
  @Test
  void shouldHaveExpectedRedisKeyPrefix() {
    assertThat(HotKeyConstants.REDIS_VERSION_KEY_PREFIX).isEqualTo("hotkeydetector:ver:");
  }

  /**
   * Verifies that the default version constant is zero.
   */
  @Test
  void shouldHaveExpectedDefaultVersion() {
    assertThat(HotKeyConstants.VERSION_DEFAULT).isZero();
  }

  /**
   * Verifies that the default TopK increment constant is one.
   */
  @Test
  void shouldHaveExpectedTopKIncrement() {
    assertThat(HotKeyConstants.TOPK_INCR).isEqualTo(1);
  }
}
