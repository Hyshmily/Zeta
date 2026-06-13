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
package io.github.hyshmily.hotkey.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CacheEntry} builder covering full, normal, and degraded entry construction.
 */
class CacheEntryTest {

  /**
   * Verifies building a CacheEntry with all fields populated and reading them back correctly.
   */
  @Test
  void shouldBuildEntryWithAllFields() {
    CacheEntry entry = CacheEntry.builder()
      .value("testValue")
      .dataVersion(100L)
      .isVersionDegraded(false)
      .decisionVersion(5L)
      .hardTtlMs(300_000L)
      .hardExpireAtMs(System.currentTimeMillis() + 300_000L)
      .softTtlMs(30_000L)
      .softExpireAtMs(System.currentTimeMillis() + 30_000L)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000L)
      .normalSoftTtlMs(30_000L)
      .build();

    assertThat(entry.getValue()).isEqualTo("testValue");
    assertThat(entry.getDataVersion()).isEqualTo(100L);
    assertThat(entry.isVersionDegraded()).isFalse();
    assertThat(entry.getDecisionVersion()).isEqualTo(5L);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies building a normal (non-hot) CacheEntry with default version values and NORMAL key state.
   */
  @Test
  void shouldBuildNormalEntry() {
    CacheEntry entry = CacheEntry.builder()
      .value(42)
      .dataVersion(0L)
      .isVersionDegraded(false)
      .decisionVersion(0L)
      .hardTtlMs(300_000L)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(0L)
      .softExpireAtMs(0L)
      .keyState(KeyState.NORMAL)
      .normalHardTtlMs(300_000L)
      .normalSoftTtlMs(30_000L)
      .build();

    assertThat(entry.getValue()).isEqualTo(42);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  /**
   * Verifies building a CacheEntry with version degraded flag set and negative data version space.
   */
  @Test
  void shouldBuildDegradedEntry() {
    CacheEntry entry = CacheEntry.builder()
      .value("degraded")
      .dataVersion(Long.MIN_VALUE + 1)
      .isVersionDegraded(true)
      .build();

    assertThat(entry.isVersionDegraded()).isTrue();
    assertThat(entry.getDataVersion()).isEqualTo(Long.MIN_VALUE + 1);
  }

  @Test
  void builderDefaults_shouldBeNullForObjectZeroForLongFalseForBoolean() {
    CacheEntry entry = CacheEntry.builder().build();
    assertThat(entry.getValue()).isNull();
    assertThat(entry.getDataVersion()).isZero();
    assertThat(entry.isVersionDegraded()).isFalse();
    assertThat(entry.getDecisionVersion()).isZero();
    assertThat(entry.getHardTtlMs()).isZero();
    assertThat(entry.getHardExpireAtMs()).isZero();
    assertThat(entry.getSoftTtlMs()).isZero();
    assertThat(entry.getSoftExpireAtMs()).isZero();
    assertThat(entry.getKeyState()).isNull();
    assertThat(entry.getNormalHardTtlMs()).isZero();
    assertThat(entry.getNormalSoftTtlMs()).isZero();
  }

  @Test
  void infiniteHardExpiry_shouldAcceptMaxValue() {
    CacheEntry entry = CacheEntry.builder()
      .value("infinite")
      .hardExpireAtMs(Long.MAX_VALUE)
      .build();
    assertThat(entry.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void keyStateCOOL_shouldBuildCorrectly() {
    CacheEntry entry = CacheEntry.builder()
      .value("cooling")
      .keyState(KeyState.COOL)
      .build();
    assertThat(entry.getKeyState()).isEqualTo(KeyState.COOL);
  }

  @Test
  void keyStatePRE_COOL_shouldBuildCorrectly() {
    CacheEntry entry = CacheEntry.builder()
      .value("precooling")
      .keyState(KeyState.PRE_COOL)
      .build();
    assertThat(entry.getKeyState()).isEqualTo(KeyState.PRE_COOL);
  }

  @Test
  void valueAsString_shouldBeStored() {
    CacheEntry entry = CacheEntry.builder().value("hello").build();
    assertThat(entry.getValue()).isEqualTo("hello");
  }

  @Test
  void valueAsInteger_shouldBeStored() {
    CacheEntry entry = CacheEntry.builder().value(123).build();
    assertThat(entry.getValue()).isEqualTo(123);
  }

  @Test
  void valueAsNull_shouldBeAccepted() {
    CacheEntry entry = CacheEntry.builder().value(null).build();
    assertThat(entry.getValue()).isNull();
  }

  @Test
  void isVersionDegradedTrue_combinedWithDecisionVersion() {
    CacheEntry entry = CacheEntry.builder()
      .value("degraded-hot")
      .isVersionDegraded(true)
      .decisionVersion(10L)
      .build();
    assertThat(entry.isVersionDegraded()).isTrue();
    assertThat(entry.getDecisionVersion()).isEqualTo(10L);
  }

  @Test
  void isVersionDegradedFalse_combinedWithDecisionVersion() {
    CacheEntry entry = CacheEntry.builder()
      .value("non-degraded")
      .isVersionDegraded(false)
      .decisionVersion(20L)
      .build();
    assertThat(entry.isVersionDegraded()).isFalse();
    assertThat(entry.getDecisionVersion()).isEqualTo(20L);
  }

  @Test
  void toBuilder_shouldCopyAllFields() {
    CacheEntry original = CacheEntry.builder()
      .value("original")
      .dataVersion(1L)
      .isVersionDegraded(true)
      .keyState(KeyState.HOT)
      .build();
    CacheEntry copy = original.toBuilder().value("modified").build();
    assertThat(copy.getValue()).isEqualTo("modified");
    assertThat(copy.getDataVersion()).isEqualTo(1L);
    assertThat(copy.isVersionDegraded()).isTrue();
    assertThat(copy.getKeyState()).isEqualTo(KeyState.HOT);
  }

  @Test
  void equalsAndHashCode_shouldWorkForIdenticalEntries() {
    CacheEntry a = CacheEntry.builder().value("x").dataVersion(1L).build();
    CacheEntry b = CacheEntry.builder().value("x").dataVersion(1L).build();
    assertThat(a).isEqualTo(b);
    assertThat(a).hasSameHashCodeAs(b);
  }

  @Test
  void equalsAndHashCode_shouldDistinguishDifferentEntries() {
    CacheEntry a = CacheEntry.builder().value("x").dataVersion(1L).build();
    CacheEntry b = CacheEntry.builder().value("y").dataVersion(1L).build();
    assertThat(a).isNotEqualTo(b);
  }
}
