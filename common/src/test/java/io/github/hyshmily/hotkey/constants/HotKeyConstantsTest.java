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
    assertThat(HotKeyConstants.THREAD_PREFIX_HOTKEY).isEqualTo("hotkey-");
    assertThat(HotKeyConstants.THREAD_PREFIX_SYNC).isEqualTo("hotkey-sync");
    assertThat(HotKeyConstants.THREAD_PREFIX_WORKER).isEqualTo("hotkey-worker");
    assertThat(HotKeyConstants.THREAD_PREFIX_REPORT).isEqualTo("hotkey-report");
  }

  /**
   * Verifies the AMQP routing key constant values.
   */
  @Test
  void shouldHaveExpectedRoutingKeys() {
    assertThat(HotKeyConstants.ROUTING_KEY_BROADCAST).isEqualTo("send.");
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
    assertThat(HotKeyConstants.REDIS_VERSION_KEY_PREFIX).isEqualTo("hotkey:ver:");
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

  /**
   * Verifies the AMQP header constants for node ID and timestamp.
   */
  @Test
  void shouldHaveExpectedNodeAndTimestampHeaders() {
    assertThat(HotKeyConstants.AMQP_HEADER_NODE_ID).isEqualTo("nodeId");
    assertThat(HotKeyConstants.AMQP_HEADER_TIMESTAMP).isEqualTo("timestamp");
  }

  /**
   * Verifies the AMQP header constants for heartbeat fields.
   */
  @Test
  void shouldHaveExpectedHeartbeatHeaders() {
    assertThat(HotKeyConstants.AMQP_HEADER_HEARTBEAT_EPOCH).isEqualTo("hbEpoch");
    assertThat(HotKeyConstants.AMQP_HEADER_HEARTBEAT_LOAD).isEqualTo("hbLoad");
    assertThat(HotKeyConstants.AMQP_HEADER_HEARTBEAT_READY).isEqualTo("hbReady");
    assertThat(HotKeyConstants.AMQP_HEADER_HEARTBEAT_CONFIG_FP).isEqualTo("hbConfigFp");
    assertThat(HotKeyConstants.AMQP_HEADER_HEARTBEAT_DV_HWM).isEqualTo("hbDvHwm");
  }

  /**
   * Verifies the AMQP header constants for verify/ping-pong fields.
   */
  @Test
  void shouldHaveExpectedVerifyHeaders() {
    assertThat(HotKeyConstants.AMQP_HEADER_VERIFY_TYPE).isEqualTo("verifyType");
    assertThat(HotKeyConstants.AMQP_HEADER_VERIFY_APP_INSTANCE).isEqualTo("verifyAppInstance");
    assertThat(HotKeyConstants.AMQP_HEADER_VERIFY_WORKER_ID).isEqualTo("verifyWorkerId");
    assertThat(HotKeyConstants.AMQP_HEADER_VERIFY_PING).isEqualTo("PING");
    assertThat(HotKeyConstants.AMQP_HEADER_VERIFY_PONG).isEqualTo("PONG");
  }

  /**
   * Verifies the scheduler thread name prefix constant.
   */
  @Test
  void shouldHaveExpectedSchedulerThreadPrefix() {
    assertThat(HotKeyConstants.THREAD_PREFIX_SCHEDULER).isEqualTo("hotkey-scheduler");
  }

  /**
   * Verifies the NO_SYNC_PUBLISHER warning message constant.
   */
  @Test
  void shouldHaveExpectedNoSyncPublisherMessage() {
    assertThat(HotKeyConstants.NO_SYNC_PUBLISHER).isEqualTo("No sync publisher found, please enable hotkey.sync");
  }

  /**
   * Verifies the rules version AMQP header constant.
   */
  @Test
  void shouldHaveExpectedRulesVersionHeader() {
    assertThat(HotKeyConstants.AMQP_HEADER_RULES_VERSION).isEqualTo("rulesVersion");
  }

  /**
   * Verifies the Redis key for dynamic rules.
   */
  @Test
  void shouldHaveExpectedRedisKeyRules() {
    assertThat(HotKeyConstants.REDIS_KEY_RULES).isEqualTo("hotkey:rules");
  }

  /**
   * Verifies that the utility class has a private constructor for coverage.
   */
  @Test
  void privateConstructor_shouldBeAccessibleViaReflection() throws Exception {
    var ctor = HotKeyConstants.class.getDeclaredConstructor();
    ctor.setAccessible(true);
    var instance = ctor.newInstance();
    assertThat(instance).isNotNull();
  }

  /**
   * Verifies the epoch AMQP header constant value.
   */
  @Test
  void shouldHaveExpectedEpochHeader() {
    assertThat(HotKeyConstants.AMQP_HEADER_EPOCH).isEqualTo("epoch");
  }
}
