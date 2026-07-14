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
package io.github.hyshmily.zeta.model;

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
    CacheEntry entry = CacheEntry.builder().value("infinite").hardExpireAtMs(Long.MAX_VALUE).build();
    assertThat(entry.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void keyStateCOOL_shouldBuildCorrectly() {
    CacheEntry entry = CacheEntry.builder().value("cooling").keyState(KeyState.COOL).build();
    assertThat(entry.getKeyState()).isEqualTo(KeyState.COOL);
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
    CacheEntry entry = CacheEntry.builder().value("degraded-hot").isVersionDegraded(true).decisionVersion(10L).build();
    assertThat(entry.isVersionDegraded()).isTrue();
    assertThat(entry.getDecisionVersion()).isEqualTo(10L);
  }

  @Test
  void isVersionDegradedFalse_combinedWithDecisionVersion() {
    CacheEntry entry = CacheEntry.builder().value("non-degraded").isVersionDegraded(false).decisionVersion(20L).build();
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

  @Test
  void dataVersionMinMax_shouldAcceptBoundaryValues() {
    CacheEntry min = CacheEntry.builder().value("min").dataVersion(Long.MIN_VALUE).build();
    CacheEntry max = CacheEntry.builder().value("max").dataVersion(Long.MAX_VALUE).build();
    assertThat(min.getDataVersion()).isEqualTo(Long.MIN_VALUE);
    assertThat(max.getDataVersion()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void decisionVersionMinMax_shouldAcceptBoundaryValues() {
    CacheEntry min = CacheEntry.builder().value("min").decisionVersion(Long.MIN_VALUE).build();
    CacheEntry max = CacheEntry.builder().value("max").decisionVersion(Long.MAX_VALUE).build();
    assertThat(min.getDecisionVersion()).isEqualTo(Long.MIN_VALUE);
    assertThat(max.getDecisionVersion()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void hardExpireAtMsInPast_shouldAcceptExpiredEntry() {
    CacheEntry entry = CacheEntry.builder().value("expired").hardExpireAtMs(-1L).build();
    assertThat(entry.getHardExpireAtMs()).isNegative();
  }

  @Test
  void negativeTtlValues_shouldBeAccepted() {
    CacheEntry entry = CacheEntry.builder().value("neg").hardTtlMs(-100L).softTtlMs(-50L).build();
    assertThat(entry.getHardTtlMs()).isNegative();
    assertThat(entry.getSoftTtlMs()).isNegative();
  }

  @Test
  void veryLongValue_shouldBeStored() {
    String longStr = "a".repeat(10_000);
    CacheEntry entry = CacheEntry.builder().value(longStr).build();
    assertThat(entry.getValue()).isEqualTo(longStr);
  }

  @Test
  void specialCharactersInValue_shouldBeStored() {
    String specials = "hello\n\t\r\0unicode:\u00E9\u4E2D\u6587";
    CacheEntry entry = CacheEntry.builder().value(specials).build();
    assertThat(entry.getValue()).isEqualTo(specials);
  }

  @Test
  void toBuilder_shouldPreserveAllFieldsExceptModified() {
    CacheEntry original = CacheEntry.builder()
      .value("orig")
      .dataVersion(1L)
      .isVersionDegraded(true)
      .decisionVersion(2L)
      .hardTtlMs(100L)
      .hardExpireAtMs(200L)
      .softTtlMs(10L)
      .softExpireAtMs(20L)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(100L)
      .normalSoftTtlMs(10L)
      .build();
    CacheEntry copy = original.toBuilder().value("modified").build();
    assertThat(copy.getValue()).isEqualTo("modified");
    assertThat(copy.getDataVersion()).isEqualTo(1L);
    assertThat(copy.isVersionDegraded()).isTrue();
    assertThat(copy.getDecisionVersion()).isEqualTo(2L);
    assertThat(copy.getHardTtlMs()).isEqualTo(100L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(200L);
    assertThat(copy.getSoftTtlMs()).isEqualTo(10L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(20L);
    assertThat(copy.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(copy.getNormalHardTtlMs()).isEqualTo(100L);
    assertThat(copy.getNormalSoftTtlMs()).isEqualTo(10L);
  }

  @Test
  void equalsWithNullValue_shouldWork() {
    CacheEntry a = CacheEntry.builder().value(null).dataVersion(1L).build();
    CacheEntry b = CacheEntry.builder().value(null).dataVersion(1L).build();
    assertThat(a).isEqualTo(b);
    assertThat(a).hasSameHashCodeAs(b);
  }

  /**
   * Verifies decisionNodeId set via builder is stored and retrieved correctly.
   */
  @Test
  void decisionNodeId_shouldBeStoredAndRetrieved() {
    CacheEntry entry = CacheEntry.builder().value("v").decisionNodeId("worker-1").build();
    assertThat(entry.getDecisionNodeId()).isEqualTo("worker-1");
  }

  /**
   * Verifies decisionEpoch set via builder is stored and retrieved correctly.
   */
  @Test
  void decisionEpoch_shouldBeStoredAndRetrieved() {
    CacheEntry entry = CacheEntry.builder().value("v").decisionEpoch(5L).build();
    assertThat(entry.getDecisionEpoch()).isEqualTo(5L);
  }

  /**
   * Verifies decisionNodeId defaults to null when not explicitly set.
   */
  @Test
  void decisionNodeId_defaultNull_whenNotSet() {
    CacheEntry entry = CacheEntry.builder().value("v").build();
    assertThat(entry.getDecisionNodeId()).isNull();
  }

  /**
   * Verifies decisionEpoch defaults to zero when not explicitly set.
   */
  @Test
  void decisionEpoch_defaultZero_whenNotSet() {
    CacheEntry entry = CacheEntry.builder().value("v").build();
    assertThat(entry.getDecisionEpoch()).isZero();
  }

  /**
   * Verifies toBuilder copies decisionNodeId and decisionEpoch to the new entry.
   */
  @Test
  void toBuilder_shouldCopyDecisionFields() {
    CacheEntry original = CacheEntry.builder().value("orig").decisionNodeId("worker-1").decisionEpoch(5L).build();
    CacheEntry copy = original.toBuilder().value("new").build();
    assertThat(copy.getValue()).isEqualTo("new");
    assertThat(copy.getDecisionNodeId()).isEqualTo("worker-1");
    assertThat(copy.getDecisionEpoch()).isEqualTo(5L);
  }

  /**
   * Verifies equals and hashCode consider decisionNodeId and decisionEpoch.
   */
  @Test
  void equalsAndHashCode_shouldConsiderDecisionFields() {
    CacheEntry a = CacheEntry.builder().value("x").dataVersion(1L).decisionNodeId("worker-1").decisionEpoch(5L).build();
    CacheEntry b = CacheEntry.builder().value("x").dataVersion(1L).decisionNodeId("worker-2").decisionEpoch(5L).build();
    assertThat(a).isNotEqualTo(b);
  }

  // ── withXxx() copy methods ──

  /**
   * Creates a fully-populated base entry for testing withXxx() copy methods.
   */
  private static CacheEntry fullEntry() {
    return CacheEntry.builder()
      .value("baseValue")
      .dataVersion(100L)
      .isVersionDegraded(true)
      .decisionVersion(5L)
      .decisionNodeId("worker-1")
      .decisionEpoch(3L)
      .hardTtlMs(300_000L)
      .hardExpireAtMs(400_000L)
      .softTtlMs(30_000L)
      .softExpireAtMs(40_000L)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(150_000L)
      .normalSoftTtlMs(15_000L)
      .build();
  }

  /**
   * Verifies {@link CacheEntry#withValue} creates a copy with the given value
   * while all other fields are preserved.
   */
  @Test
  void withValue_shouldCreateCopyWithNewValue() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withValue("newValue");
    assertThat(copy.getValue()).isEqualTo("newValue");
    assertThat(copy).usingRecursiveComparison().ignoringFields("value").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withDataVersion} creates a copy with the given
   * dataVersion while all other fields are preserved.
   */
  @Test
  void withDataVersion_shouldCreateCopyWithNewDataVersion() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withDataVersion(999L);
    assertThat(copy.getDataVersion()).isEqualTo(999L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("dataVersion").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withIsVersionDegraded} creates a copy with the
   * given degraded flag while all other fields are preserved.
   */
  @Test
  void withIsVersionDegraded_shouldCreateCopyWithNewDegradedFlag() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withIsVersionDegraded(false);
    assertThat(copy.isVersionDegraded()).isFalse();
    assertThat(copy).usingRecursiveComparison().ignoringFields("isVersionDegraded").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withDecisionVersion} creates a copy with the
   * given decisionVersion while all other fields are preserved.
   */
  @Test
  void withDecisionVersion_shouldCreateCopyWithNewDecisionVersion() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withDecisionVersion(99L);
    assertThat(copy.getDecisionVersion()).isEqualTo(99L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("decisionVersion").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withDecisionNodeId} creates a copy with the
   * given decisionNodeId while all other fields are preserved.
   */
  @Test
  void withDecisionNodeId_shouldCreateCopyWithNewDecisionNodeId() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withDecisionNodeId("worker-2");
    assertThat(copy.getDecisionNodeId()).isEqualTo("worker-2");
    assertThat(copy).usingRecursiveComparison().ignoringFields("decisionNodeId").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withDecisionEpoch} creates a copy with the
   * given decisionEpoch while all other fields are preserved.
   */
  @Test
  void withDecisionEpoch_shouldCreateCopyWithNewDecisionEpoch() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withDecisionEpoch(42L);
    assertThat(copy.getDecisionEpoch()).isEqualTo(42L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("decisionEpoch").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withHardTtlMs} creates a copy with the given
   * hardTtlMs while all other fields are preserved.
   */
  @Test
  void withHardTtlMs_shouldCreateCopyWithNewHardTtl() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withHardTtlMs(600_000L);
    assertThat(copy.getHardTtlMs()).isEqualTo(600_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("hardTtlMs").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withHardExpireAtMs} creates a copy with the
   * given hardExpireAtMs while all other fields are preserved.
   */
  @Test
  void withHardExpireAtMs_shouldCreateCopyWithNewHardExpireAt() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withHardExpireAtMs(800_000L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(800_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("hardExpireAtMs").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withSoftTtlMs} creates a copy with the given
   * softTtlMs while all other fields are preserved.
   */
  @Test
  void withSoftTtlMs_shouldCreateCopyWithNewSoftTtl() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withSoftTtlMs(60_000L);
    assertThat(copy.getSoftTtlMs()).isEqualTo(60_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("softTtlMs").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withSoftExpireAtMs} creates a copy with the
   * given softExpireAtMs while all other fields are preserved.
   */
  @Test
  void withSoftExpireAtMs_shouldCreateCopyWithNewSoftExpireAt() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withSoftExpireAtMs(80_000L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(80_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("softExpireAtMs").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withKeyState} creates a copy with the given
   * keyState while all other fields are preserved.
   */
  @Test
  void withKeyState_shouldCreateCopyWithNewKeyState() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withKeyState(KeyState.COOL);
    assertThat(copy.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(copy).usingRecursiveComparison().ignoringFields("keyState").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withNormalHardTtlMs} creates a copy with the
   * given normalHardTtlMs while all other fields are preserved.
   */
  @Test
  void withNormalHardTtlMs_shouldCreateCopyWithNewNormalHardTtl() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withNormalHardTtlMs(200_000L);
    assertThat(copy.getNormalHardTtlMs()).isEqualTo(200_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("normalHardTtlMs").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withNormalSoftTtlMs} creates a copy with the
   * given normalSoftTtlMs while all other fields are preserved.
   */
  @Test
  void withNormalSoftTtlMs_shouldCreateCopyWithNewNormalSoftTtl() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withNormalSoftTtlMs(20_000L);
    assertThat(copy.getNormalSoftTtlMs()).isEqualTo(20_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("normalSoftTtlMs").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withTtl} creates a copy with all four TTL
   * fields updated while all other fields are preserved.
   */
  @Test
  void withTtl_shouldCreateCopyWithAllTtlFields() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withTtl(500L, 50L, 600L, 60L);
    assertThat(copy.getHardTtlMs()).isEqualTo(500L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(600L);
    assertThat(copy.getSoftTtlMs()).isEqualTo(50L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(60L);
    assertThat(copy)
      .usingRecursiveComparison()
      .ignoringFields("hardTtlMs", "hardExpireAtMs", "softTtlMs", "softExpireAtMs")
      .isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withHardTtl} creates a copy with hard TTL
   * and hard expire-at updated while all other fields are preserved.
   */
  @Test
  void withHardTtl_shouldCreateCopyWithNewHardTtlFields() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withHardTtl(900_000L, 950_000L);
    assertThat(copy.getHardTtlMs()).isEqualTo(900_000L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(950_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("hardTtlMs", "hardExpireAtMs").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withSoftTtl} creates a copy with soft TTL
   * and soft expire-at updated while all other fields are preserved.
   */
  @Test
  void withSoftTtl_shouldCreateCopyWithNewSoftTtlFields() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withSoftTtl(90_000L, 95_000L);
    assertThat(copy.getSoftTtlMs()).isEqualTo(90_000L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(95_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("softTtlMs", "softExpireAtMs").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withNormalTtl} creates a copy with both
   * normal TTL fields updated while all other fields are preserved.
   */
  @Test
  void withNormalTtl_shouldCreateCopyWithNewNormalTtlFields() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withNormalTtl(250_000L, 25_000L);
    assertThat(copy.getNormalHardTtlMs()).isEqualTo(250_000L);
    assertThat(copy.getNormalSoftTtlMs()).isEqualTo(25_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("normalHardTtlMs", "normalSoftTtlMs").isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withTtlAndKeyState} creates a copy with all
   * four TTL fields and keyState updated while all other fields are preserved.
   */
  @Test
  void withTtlAndKeyState_shouldCreateCopyWithTtlFieldsAndKeyState() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withTtlAndKeyState(700L, 70L, 800L, 80L, KeyState.COOL);
    assertThat(copy.getHardTtlMs()).isEqualTo(700L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(800L);
    assertThat(copy.getSoftTtlMs()).isEqualTo(70L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(80L);
    assertThat(copy.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(copy)
      .usingRecursiveComparison()
      .ignoringFields("hardTtlMs", "hardExpireAtMs", "softTtlMs", "softExpireAtMs", "keyState")
      .isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withDecisionAndTtlAndState} creates a copy
   * with decision metadata, TTL fields, and keyState updated while all other
   * fields are preserved.
   */
  @Test
  void withDecisionAndTtlAndState_shouldCreateCopyWithDecisionTtlAndState() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withDecisionAndTtlAndState(99L, "worker-9", 7L, 111L, 22L, 333L, 44L, KeyState.COOL);
    assertThat(copy.getDecisionVersion()).isEqualTo(99L);
    assertThat(copy.getDecisionNodeId()).isEqualTo("worker-9");
    assertThat(copy.getDecisionEpoch()).isEqualTo(7L);
    assertThat(copy.getHardTtlMs()).isEqualTo(111L);
    assertThat(copy.getSoftTtlMs()).isEqualTo(22L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(333L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(44L);
    assertThat(copy.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(copy)
      .usingRecursiveComparison()
      .ignoringFields(
        "decisionVersion",
        "decisionNodeId",
        "decisionEpoch",
        "hardTtlMs",
        "hardExpireAtMs",
        "softTtlMs",
        "softExpireAtMs",
        "keyState"
      )
      .isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withValueAndRefreshMeta} creates a copy with
   * value, version metadata, and expire-at timestamps updated while all other
   * fields are preserved.
   */
  @Test
  void withValueAndRefreshMeta_shouldCreateCopyWithValueVersionAndExpire() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withValueAndRefreshMeta("refreshed", 200L, false, 999_000L, 99_000L);
    assertThat(copy.getValue()).isEqualTo("refreshed");
    assertThat(copy.getDataVersion()).isEqualTo(200L);
    assertThat(copy.isVersionDegraded()).isFalse();
    assertThat(copy.getHardExpireAtMs()).isEqualTo(999_000L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(99_000L);
    assertThat(copy)
      .usingRecursiveComparison()
      .ignoringFields("value", "dataVersion", "isVersionDegraded", "hardExpireAtMs", "softExpireAtMs")
      .isEqualTo(base);
  }

  /**
   * Verifies {@link CacheEntry#withValueAndSoftTtl} creates a copy with
   * value and soft TTL fields updated while all other fields are preserved.
   */
  @Test
  void withValueAndSoftTtl_shouldCreateCopyWithValueAndSoftTtl() {
    CacheEntry base = fullEntry();
    CacheEntry copy = base.withValueAndSoftTtl("staleRefreshed", 120_000L, 125_000L);
    assertThat(copy.getValue()).isEqualTo("staleRefreshed");
    assertThat(copy.getSoftTtlMs()).isEqualTo(120_000L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(125_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("value", "softTtlMs", "softExpireAtMs").isEqualTo(base);
  }
}
