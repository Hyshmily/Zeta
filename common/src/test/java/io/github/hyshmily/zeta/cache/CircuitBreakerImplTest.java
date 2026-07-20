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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.impl.CircuitBreakerImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CircuitBreakerImpl} covering all state transitions,
 * backoff scenarios, disabled path, and close lifecycle.
 */
class CircuitBreakerImplTest {

  private ZetaProperties.CircuitBreaker config;
  private CircuitBreakerImpl breaker;

  @BeforeEach
  void setUp() {
    config = new ZetaProperties.CircuitBreaker();
    config.setEnabled(true);
    config.setFailThreshold(0.5);
    config.setRequestVolumeThreshold(2);
    config.setWindowTimeMs(10_000);
    config.setWindowBuckets(10);
    config.setSingleTestIntervalMs(100);
    config.setLogEnabled(false);
    breaker = new CircuitBreakerImpl(config);
  }

  @Test
  void allowRequest_whenDisabled_shouldReturnTrue() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    assertThat(cb.allowRequest()).isTrue();
  }

  @Test
  void allowRequest_whenClosed_shouldReturnTrue() {
    assertThat(breaker.allowRequest()).isTrue();
  }

  @Test
  void allowRequest_whenOpenAndNotHalfOpen_shouldReturnFalse() throws Exception {
    triggerOpen();
    // Immediately after opening, the half-open interval has not elapsed
    assertThat(breaker.allowRequest()).isFalse();
  }

  @Test
  void allowRequest_whenOpenAndHalfOpenProbe_shouldReturnTrue() throws Exception {
    triggerOpen();
    // Advance time past the singleTestIntervalMs
    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();
  }

  @Test
  void onSuccess_whenDisabled_shouldDoNothing() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    cb.onSuccess();
    assertThat(cb.isOpen()).isFalse();
  }

  @Test
  void onSuccess_whenClosed_shouldIncrementSuccess() throws Exception {
    config.setFailThreshold(0.4);
    config.setRequestVolumeThreshold(3);
    breaker = new CircuitBreakerImpl(config);
    breaker.onSuccess();
    breaker.onSuccess();
    breaker.onFailure();
    breaker.onFailure();
    breaker.onFailure();
    // rate = 3/5 = 0.6 > 0.4, volume = 5 >= 3 → open
    assertThat(breaker.isOpen()).isTrue();
  }

  @Test
  void onSuccess_whenOpen_shouldCloseBreaker() throws Exception {
    triggerOpen();
    Thread.sleep(config.getSingleTestIntervalMs() + 50);
    assertThat(breaker.allowRequest()).isTrue();
    breaker.onSuccess();
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void onFailure_whenDisabled_shouldDoNothing() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    cb.onFailure();
  }

  @Test
  void onFailure_whenEnabled_shouldIncrementFail() throws Exception {
    triggerOpen();
    assertThat(breaker.isOpen()).isTrue();
  }

  @Test
  void isOpen_whenDisabled_shouldReturnFalse() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    cb.onFailure();
    cb.onFailure();
    assertThat(cb.isOpen()).isFalse();
  }

  @Test
  void isOpen_whenOpen_shouldReturnTrue() throws Exception {
    triggerOpen();
    assertThat(breaker.isOpen()).isTrue();
  }

  @Test
  void close_shouldCancelSlideFuture() {
    breaker.close();
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void close_shouldNotThrowWhenCalledTwice() {
    breaker.close();
    breaker.close();
  }

  @Test
  void evaluateThreshold_whenBelowVolume_shouldNotOpen() {
    // Only 1 failure, need 2 (requestVolumeThreshold)
    breaker.onFailure();
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void evaluateThreshold_whenRateBelowThreshold_shouldNotOpen() {
    // 1 success + 1 failure = 50% rate, failThreshold=0.5, strictly greater needed
    breaker.onSuccess();
    breaker.onFailure();
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void fullCycle_openHalfOpenClose() throws Exception {
    triggerOpen();
    assertThat(breaker.isOpen()).isTrue();

    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();

    breaker.onSuccess();
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void logEnabled_shouldNotThrow() {
    config.setLogEnabled(true);
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);

    cb.onFailure();
    // allowRequest on open breaker triggers log in half-open path
    cb.allowRequest();
  }

  // ── Helpers ──

  private void triggerOpen() throws Exception {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    // Re-create with new config
    breaker = new CircuitBreakerImpl(config);
    breaker.onFailure();
    Thread.sleep(50);
  }
}
