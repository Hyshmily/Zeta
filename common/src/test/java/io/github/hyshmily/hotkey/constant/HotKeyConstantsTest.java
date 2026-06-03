package io.github.hyshmily.hotkey.constant;

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
    assertThat(HotKeyConstants.ROUTING_KEY_HOT).isEqualTo("hot.");
    assertThat(HotKeyConstants.ROUTING_KEY_COOL).isEqualTo("cool.");
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
