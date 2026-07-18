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

  @Test
  void shouldHaveExpectedAmqpHeaders() {
    assertThat(ZetaConstants.Amqp.HEADER_TYPE).isEqualTo("type");
    assertThat(ZetaConstants.Amqp.HEADER_VERSION).isEqualTo("version");
    assertThat(ZetaConstants.Amqp.HEADER_IS_VERSION_DEGRADED).isEqualTo("isVersionDegraded");
  }

  @Test
  void shouldHaveExpectedThreadPrefixes() {
    assertThat(ZetaConstants.Thread.PREFIX_HOTKEY).isEqualTo("zeta-");
    assertThat(ZetaConstants.Thread.PREFIX_SYNC).isEqualTo("zeta-sync");
    assertThat(ZetaConstants.Thread.PREFIX_WORKER).isEqualTo("zeta-worker");
    assertThat(ZetaConstants.Thread.PREFIX_REPORT).isEqualTo("zeta-reportToWorker");
  }

  @Test
  void shouldHaveExpectedRoutingKeys() {
    assertThat(ZetaConstants.Routing.KEY_BROADCAST).isEqualTo("send.");
    assertThat(ZetaConstants.Routing.KEY_REPORT).isEqualTo("reportToWorker.");
  }

  @Test
  void shouldHaveExpectedQueuePrefix() {
    assertThat(ZetaConstants.Routing.QUEUE_PREFIX_REPORT).isEqualTo("zeta.reportToWorker.");
  }

  @Test
  void shouldHaveExpectedSources() {
    assertThat(ZetaConstants.Source.SLIDING_WINDOW).isEqualTo("sliding_window");
    assertThat(ZetaConstants.Source.TOPK_PRE_WARM).isEqualTo("topk_pre_warm");
  }

  @Test
  void shouldHaveExpectedRedisKeyPrefix() {
    assertThat(ZetaConstants.Redis.VERSION_KEY_PREFIX).isEqualTo("zeta:ver:");
  }

  @Test
  void shouldHaveExpectedDefaultVersion() {
    assertThat(ZetaConstants.Version.VERSION_DEFAULT).isZero();
  }

  @Test
  void shouldHaveExpectedTopKIncrement() {
    assertThat(ZetaConstants.TOPK_INCR).isEqualTo(1);
  }

  @Test
  void shouldHaveExpectedNodeAndTimestampHeaders() {
    assertThat(ZetaConstants.Amqp.HEADER_NODE_ID).isEqualTo("nodeId");
    assertThat(ZetaConstants.Amqp.HEADER_TIMESTAMP).isEqualTo("timestamp");
  }

  @Test
  void shouldHaveExpectedHeartbeatHeaders() {
    assertThat(ZetaConstants.Amqp.HEADER_HEARTBEAT_EPOCH).isEqualTo("hbEpoch");
    assertThat(ZetaConstants.Amqp.HEADER_HEARTBEAT_LOAD).isEqualTo("hbLoad");
    assertThat(ZetaConstants.Amqp.HEADER_HEARTBEAT_READY).isEqualTo("hbReady");
    assertThat(ZetaConstants.Amqp.HEADER_HEARTBEAT_CONFIG_FP).isEqualTo("hbConfigFp");
    assertThat(ZetaConstants.Amqp.HEADER_HEARTBEAT_DV_HWM).isEqualTo("hbDvHwm");
  }

  @Test
  void shouldHaveExpectedVerifyHeaders() {
    assertThat(ZetaConstants.Amqp.HEADER_VERIFY_TYPE).isEqualTo("verifyType");
    assertThat(ZetaConstants.Amqp.HEADER_VERIFY_APP_INSTANCE).isEqualTo("verifyAppInstance");
    assertThat(ZetaConstants.Amqp.HEADER_VERIFY_WORKER_ID).isEqualTo("verifyWorkerId");
    assertThat(ZetaConstants.Amqp.HEADER_VERIFY_PING).isEqualTo("PING");
    assertThat(ZetaConstants.Amqp.HEADER_VERIFY_PONG).isEqualTo("PONG");
  }

  @Test
  void shouldHaveExpectedSchedulerThreadPrefix() {
    assertThat(ZetaConstants.Thread.PREFIX_SCHEDULER).isEqualTo("zeta-scheduler");
  }

  @Test
  void shouldHaveExpectedNoSyncPublisherMessage() {
    assertThat(ZetaConstants.NO_SYNC_PUBLISHER).isEqualTo("No sync publisher found, please enable zeta.sync");
  }

  @Test
  void shouldHaveExpectedRulesVersionHeader() {
    assertThat(ZetaConstants.Amqp.HEADER_RULES_VERSION).isEqualTo("rulesVersion");
  }

  @Test
  void shouldHaveExpectedRedisKeyRules() {
    assertThat(ZetaConstants.Redis.KEY_RULES).isEqualTo("zeta:rules");
  }

  @Test
  void shouldHaveExpectedExchangeNames() {
    assertThat(ZetaConstants.Exchange.REPORT).isEqualTo("zeta.reportToWorker.exchange");
    assertThat(ZetaConstants.Exchange.HEARTBEAT).isEqualTo("zeta.heartbeat.exchange");
    assertThat(ZetaConstants.Exchange.BROADCAST).isEqualTo("zeta.send.exchange");
    assertThat(ZetaConstants.Exchange.SYNC).isEqualTo("zeta.sync.exchange");
  }

  @Test
  void shouldHaveExpectedRedisLockKeyPrefix() {
    assertThat(ZetaConstants.Redis.LOCK_KEY_PREFIX).isEqualTo("zeta:lock:");
  }

  @Test
  void shouldHaveExpectedEpochHeader() {
    assertThat(ZetaConstants.Amqp.HEADER_EPOCH).isEqualTo("epoch");
  }
}
