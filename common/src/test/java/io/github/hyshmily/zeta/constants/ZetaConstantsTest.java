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
package io.github.hyshmily.zeta.constants;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ZetaConstants} verifying constant values for AMQP, threading, and routing.
 */
class ZetaConstantsTest {

  /**
   * Verifies the AMQP message header constant values.
   */
  @Test
  void shouldHaveExpectedAmqpHeaders() {
    assertThat(ZetaConstants.AMQP_HEADER_TYPE).isEqualTo("type");
    assertThat(ZetaConstants.AMQP_HEADER_VERSION).isEqualTo("version");
    assertThat(ZetaConstants.AMQP_HEADER_IS_VERSION_DEGRADED).isEqualTo("isVersionDegraded");
  }

  /**
   * Verifies the thread name prefix constant values.
   */
  @Test
  void shouldHaveExpectedThreadPrefixes() {
    assertThat(ZetaConstants.THREAD_PREFIX_HOTKEY).isEqualTo("zeta-");
    assertThat(ZetaConstants.THREAD_PREFIX_SYNC).isEqualTo("zeta-sync");
    assertThat(ZetaConstants.THREAD_PREFIX_WORKER).isEqualTo("zeta-worker");
    assertThat(ZetaConstants.THREAD_PREFIX_REPORT).isEqualTo("zeta-report");
  }

  /**
   * Verifies the AMQP routing key constant values.
   */
  @Test
  void shouldHaveExpectedRoutingKeys() {
    assertThat(ZetaConstants.ROUTING_KEY_BROADCAST).isEqualTo("send.");
    assertThat(ZetaConstants.ROUTING_KEY_REPORT).isEqualTo("report.");
  }

  /**
   * Verifies the report queue prefix constant value.
   */
  @Test
  void shouldHaveExpectedQueuePrefix() {
    assertThat(ZetaConstants.QUEUE_PREFIX_REPORT).isEqualTo("zeta.report.");
  }

  /**
   * Verifies the source identifier constant values.
   */
  @Test
  void shouldHaveExpectedSources() {
    assertThat(ZetaConstants.SOURCE_SLIDING_WINDOW).isEqualTo("sliding_window");
    assertThat(ZetaConstants.SOURCE_TOPK_PRE_WARM).isEqualTo("topk_pre_warm");
  }

  /**
   * Verifies the Redis version key prefix constant value.
   */
  @Test
  void shouldHaveExpectedRedisKeyPrefix() {
    assertThat(ZetaConstants.REDIS_VERSION_KEY_PREFIX).isEqualTo("zeta:ver:");
  }

  /**
   * Verifies that the default version constant is zero.
   */
  @Test
  void shouldHaveExpectedDefaultVersion() {
    assertThat(ZetaConstants.VERSION_DEFAULT).isZero();
  }

  /**
   * Verifies that the default TopK increment constant is one.
   */
  @Test
  void shouldHaveExpectedTopKIncrement() {
    assertThat(ZetaConstants.TOPK_INCR).isEqualTo(1);
  }

  /**
   * Verifies the AMQP header constants for node ID and timestamp.
   */
  @Test
  void shouldHaveExpectedNodeAndTimestampHeaders() {
    assertThat(ZetaConstants.AMQP_HEADER_NODE_ID).isEqualTo("nodeId");
    assertThat(ZetaConstants.AMQP_HEADER_TIMESTAMP).isEqualTo("timestamp");
  }

  /**
   * Verifies the AMQP header constants for heartbeat fields.
   */
  @Test
  void shouldHaveExpectedHeartbeatHeaders() {
    assertThat(ZetaConstants.AMQP_HEADER_HEARTBEAT_EPOCH).isEqualTo("hbEpoch");
    assertThat(ZetaConstants.AMQP_HEADER_HEARTBEAT_LOAD).isEqualTo("hbLoad");
    assertThat(ZetaConstants.AMQP_HEADER_HEARTBEAT_READY).isEqualTo("hbReady");
    assertThat(ZetaConstants.AMQP_HEADER_HEARTBEAT_CONFIG_FP).isEqualTo("hbConfigFp");
    assertThat(ZetaConstants.AMQP_HEADER_HEARTBEAT_DV_HWM).isEqualTo("hbDvHwm");
  }

  /**
   * Verifies the AMQP header constants for verify/ping-pong fields.
   */
  @Test
  void shouldHaveExpectedVerifyHeaders() {
    assertThat(ZetaConstants.AMQP_HEADER_VERIFY_TYPE).isEqualTo("verifyType");
    assertThat(ZetaConstants.AMQP_HEADER_VERIFY_APP_INSTANCE).isEqualTo("verifyAppInstance");
    assertThat(ZetaConstants.AMQP_HEADER_VERIFY_WORKER_ID).isEqualTo("verifyWorkerId");
    assertThat(ZetaConstants.AMQP_HEADER_VERIFY_PING).isEqualTo("PING");
    assertThat(ZetaConstants.AMQP_HEADER_VERIFY_PONG).isEqualTo("PONG");
  }

  /**
   * Verifies the scheduler thread name prefix constant.
   */
  @Test
  void shouldHaveExpectedSchedulerThreadPrefix() {
    assertThat(ZetaConstants.THREAD_PREFIX_SCHEDULER).isEqualTo("zeta-scheduler");
  }

  /**
   * Verifies the NO_SYNC_PUBLISHER warning message constant.
   */
  @Test
  void shouldHaveExpectedNoSyncPublisherMessage() {
    assertThat(ZetaConstants.NO_SYNC_PUBLISHER).isEqualTo("No sync publisher found, please enable zeta.sync");
  }

  /**
   * Verifies the rules version AMQP header constant.
   */
  @Test
  void shouldHaveExpectedRulesVersionHeader() {
    assertThat(ZetaConstants.AMQP_HEADER_RULES_VERSION).isEqualTo("rulesVersion");
  }

  /**
   * Verifies the Redis key for dynamic rules.
   */
  @Test
  void shouldHaveExpectedRedisKeyRules() {
    assertThat(ZetaConstants.REDIS_KEY_RULES).isEqualTo("zeta:rules");
  }

  /**
   * Verifies that the utility class has a private constructor for coverage.
   */
  @Test
  void privateConstructor_shouldBeAccessibleViaReflection() throws Exception {
    var ctor = ZetaConstants.class.getDeclaredConstructor();
    ctor.setAccessible(true);
    var instance = ctor.newInstance();
    assertThat(instance).isNotNull();
  }

  /**
   * Verifies the epoch AMQP header constant value.
   */
  @Test
  void shouldHaveExpectedEpochHeader() {
    assertThat(ZetaConstants.AMQP_HEADER_EPOCH).isEqualTo("epoch");
  }
}
