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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    CacheEntry entry = CacheEntry.builder()
      .value("degraded-hot")
      .dataVersion(Long.MIN_VALUE + 10)
      .isVersionDegraded(true)
      .decisionVersion(10L)
      .build();
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
  void draftCopy_shouldCarryAllFields() {
    CacheEntry original = CacheEntry.builder()
      .value("original")
      .dataVersion(Long.MIN_VALUE + 1)
      .isVersionDegraded(true)
      .keyState(KeyState.HOT)
      .build();
    CacheEntry copy = EntryDraft.of(original).value("modified").build();
    assertThat(copy.getValue()).isEqualTo("modified");
    assertThat(copy.getDataVersion()).isEqualTo(Long.MIN_VALUE + 1);
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
    CacheEntry min = CacheEntry.builder().value("min").dataVersion(Long.MIN_VALUE).isVersionDegraded(true).build();
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
  void negativeTtlValues_shouldBeRejected() {
    assertThatThrownBy(() -> CacheEntry.builder().value("neg").hardTtlMs(-100L).softTtlMs(-50L).build()).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  /**
   * Locks the diagnostic message of the degraded-flag assertion. The message is
   * only materialised on the failure path (Supplier-based Assert overload), so
   * the happy path allocates nothing — this test is the only thing pinning the
   * text down.
   */
  @Test
  void inconsistentDegradedFlag_shouldBeRejectedWithDiagnosticMessage() {
    assertThatThrownBy(() -> CacheEntry.builder().value("v").dataVersion(5L).isVersionDegraded(true).build())
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("isVersionDegraded(true) must equal (dataVersion < 0) for dataVersion=5");
  }

  /** Locks the diagnostic message of the decision-epoch range assertion. */
  @Test
  void outOfRangeDecisionEpoch_shouldBeRejectedWithDiagnosticMessage() {
    long epoch = 1L << 56;
    assertThatThrownBy(() -> CacheEntry.builder().value("v").decisionEpoch(epoch).build())
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("decisionEpoch out of range [0, " + ((1L << 56) - 1) + "]: " + epoch);
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
  void draftCopy_shouldPreserveAllFieldsExceptModified() {
    CacheEntry original = CacheEntry.builder()
      .value("orig")
      .dataVersion(Long.MIN_VALUE + 1)
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
    CacheEntry copy = EntryDraft.of(original).value("modified").build();
    assertThat(copy.getValue()).isEqualTo("modified");
    assertThat(copy.getDataVersion()).isEqualTo(Long.MIN_VALUE + 1);
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
   * Verifies the draft carries decisionNodeId and decisionEpoch to the new entry.
   */
  @Test
  void draftCopy_shouldCarryDecisionFields() {
    CacheEntry original = CacheEntry.builder().value("orig").decisionNodeId("worker-1").decisionEpoch(5L).build();
    CacheEntry copy = EntryDraft.of(original).value("new").build();
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

  // ── EntryDraft copy-on-write (replaces the former withXxx() family) ──

  /**
   * A deterministic stub arithmetic for draft tests: computed hard expiry is
   * duration + 1000, computed soft expiry is duration + 500 (0 stays 0).
   */
  private static final EntryDraft.ExpiryArithmetic ARITH = new EntryDraft.ExpiryArithmetic() {
    @Override
    public long hardExpireAt(long hardTtlMs) {
      return hardTtlMs + 1000;
    }

    @Override
    public long softExpireAt(long softTtlMs) {
      return softTtlMs <= 0 ? 0 : softTtlMs + 500;
    }
  };

  /** Creates a fully-populated base entry for testing draft copy-on-write. */
  private static CacheEntry fullEntry() {
    return CacheEntry.builder()
      .value("baseValue")
      .dataVersion(Long.MIN_VALUE + 100)
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

  /** Seeded draft with arithmetic — the production-shaped modification path. */
  private static EntryDraft edit(CacheEntry source) {
    return EntryDraft.of(source, ARITH);
  }

  @Test
  void draftValue_shouldCreateCopyWithNewValue() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).value("newValue").build();
    assertThat(copy.getValue()).isEqualTo("newValue");
    assertThat(copy).usingRecursiveComparison().ignoringFields("value").isEqualTo(base);
  }

  @Test
  void draftVersion_shouldCreateCopyWithNewDataVersion() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).version(999L).build();
    assertThat(copy.getDataVersion()).isEqualTo(999L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("dataVersion").isEqualTo(base);
  }

  @Test
  void draftDecision_shouldCreateCopyWithNewDecisionVersion() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).decision(new DecisionStamp(99L, "worker-1", 3L)).build();
    assertThat(copy.getDecisionVersion()).isEqualTo(99L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("decisionVersion").isEqualTo(base);
  }

  @Test
  void draftDecision_shouldCreateCopyWithNewDecisionNodeId() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).decision(new DecisionStamp(5L, "worker-2", 3L)).build();
    assertThat(copy.getDecisionNodeId()).isEqualTo("worker-2");
    assertThat(copy).usingRecursiveComparison().ignoringFields("decisionNodeId").isEqualTo(base);
  }

  @Test
  void draftDecision_shouldCreateCopyWithNewDecisionEpoch() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).decision(new DecisionStamp(5L, "worker-1", 42L)).build();
    assertThat(copy.getDecisionEpoch()).isEqualTo(42L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("decisionEpoch", "packedState").isEqualTo(base);
  }

  @Test
  void draftClearDecision_shouldResetToLocalOrigin() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).clearDecision().build();
    assertThat(copy.getDecisionVersion()).isZero();
    assertThat(copy.getDecisionNodeId()).isNull();
    assertThat(copy.getDecisionEpoch()).isZero();
    assertThat(copy)
      .usingRecursiveComparison()
      .ignoringFields("decisionVersion", "decisionNodeId", "decisionEpoch", "packedState")
      .isEqualTo(base);
  }

  @Test
  void draftHardTtl_shouldUpdateDurationAndComputedTimestamp() {
    CacheEntry base = fullEntry();
    CacheEntry copy = edit(base).hardTtl(600_000L).build();
    assertThat(copy.getHardTtlMs()).isEqualTo(600_000L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(600_000L + 1000);
    assertThat(copy).usingRecursiveComparison().ignoringFields("hardTtlMs", "hardExpireAtMs").isEqualTo(base);
  }

  @Test
  void draftHardExpiryAt_shouldUpdateTimestampOnly() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).hardExpiryAt(800_000L).build();
    assertThat(copy.getHardExpireAtMs()).isEqualTo(800_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("hardExpireAtMs").isEqualTo(base);
  }

  @Test
  void draftSoftTtl_shouldUpdateDurationAndComputedTimestamp() {
    CacheEntry base = fullEntry();
    CacheEntry copy = edit(base).softTtl(60_000L).build();
    assertThat(copy.getSoftTtlMs()).isEqualTo(60_000L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(60_000L + 500);
    assertThat(copy).usingRecursiveComparison().ignoringFields("softTtlMs", "softExpireAtMs").isEqualTo(base);
  }

  @Test
  void draftSoftTtlDisabled_shouldStampZeroTimestamp() {
    CacheEntry base = fullEntry();
    CacheEntry copy = edit(base).softTtl(0L).build();
    assertThat(copy.getSoftTtlMs()).isZero();
    assertThat(copy.getSoftExpireAtMs()).isZero();
  }

  @Test
  void draftSoftExpiryAt_shouldUpdateTimestampOnly() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).softExpiryAt(80_000L).build();
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(80_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("softExpireAtMs").isEqualTo(base);
  }

  @Test
  void draftKeyState_shouldCreateCopyWithNewKeyState() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).keyState(KeyState.COOL).build();
    assertThat(copy.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(copy).usingRecursiveComparison().ignoringFields("keyState", "packedState").isEqualTo(base);
  }

  @Test
  void draftNormalTtl_shouldUpdateBaselineOnly() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).normalTtl(250_000L, 25_000L).build();
    assertThat(copy.getNormalHardTtlMs()).isEqualTo(250_000L);
    assertThat(copy.getNormalSoftTtlMs()).isEqualTo(25_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("normalHardTtlMs", "normalSoftTtlMs").isEqualTo(base);
  }

  @Test
  void draftTtl_shouldUpdateAllFourTtlFields() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).ttl(500L, 50L).expiryAt(600L, 60L).build();
    assertThat(copy.getHardTtlMs()).isEqualTo(500L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(600L);
    assertThat(copy.getSoftTtlMs()).isEqualTo(50L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(60L);
    assertThat(copy)
      .usingRecursiveComparison()
      .ignoringFields("hardTtlMs", "hardExpireAtMs", "softTtlMs", "softExpireAtMs")
      .isEqualTo(base);
  }

  @Test
  void draftTtlWithBaseline_shouldUpdateAllSixTtlFields() {
    CacheEntry base = fullEntry();
    CacheEntry copy = edit(base).ttl(700L, 70L, 250_000L, 25_000L).build();
    assertThat(copy.getHardTtlMs()).isEqualTo(700L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(700L + 1000);
    assertThat(copy.getSoftTtlMs()).isEqualTo(70L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(70L + 500);
    assertThat(copy.getNormalHardTtlMs()).isEqualTo(250_000L);
    assertThat(copy.getNormalSoftTtlMs()).isEqualTo(25_000L);
    assertThat(copy)
      .usingRecursiveComparison()
      .ignoringFields(
        "hardTtlMs",
        "hardExpireAtMs",
        "softTtlMs",
        "softExpireAtMs",
        "normalHardTtlMs",
        "normalSoftTtlMs"
      )
      .isEqualTo(base);
  }

  @Test
  void draftTtlAndKeyState_shouldCreateCopyWithTtlFieldsAndKeyState() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).ttl(700L, 70L).expiryAt(800L, 80L).keyState(KeyState.COOL).build();
    assertThat(copy.getHardTtlMs()).isEqualTo(700L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(800L);
    assertThat(copy.getSoftTtlMs()).isEqualTo(70L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(80L);
    assertThat(copy.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(copy)
      .usingRecursiveComparison()
      .ignoringFields("hardTtlMs", "hardExpireAtMs", "softTtlMs", "softExpireAtMs", "keyState", "packedState")
      .isEqualTo(base);
  }

  @Test
  void draftDecisionTtlAndState_shouldCreateCopyWithDecisionTtlAndState() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base)
      .decision(new DecisionStamp(99L, "worker-9", 7L))
      .ttl(111L, 22L)
      .expiryAt(333L, 44L)
      .keyState(KeyState.COOL)
      .build();
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
        "keyState",
        "packedState"
      )
      .isEqualTo(base);
  }

  @Test
  void draftValueVersionAndExpiry_shouldCreateCopyWithValueVersionAndExpire() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).value("refreshed").version(200L).expiryAt(999_000L, 99_000L).build();
    assertThat(copy.getValue()).isEqualTo("refreshed");
    assertThat(copy.getDataVersion()).isEqualTo(200L);
    assertThat(copy.isVersionDegraded()).isFalse();
    assertThat(copy.getHardExpireAtMs()).isEqualTo(999_000L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(99_000L);
    assertThat(copy)
      .usingRecursiveComparison()
      .ignoringFields("value", "dataVersion", "hardExpireAtMs", "softExpireAtMs")
      .isEqualTo(base);
  }

  @Test
  void draftValueAndSoftTtl_shouldCreateCopyWithValueAndSoftTtl() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).value("staleRefreshed").softTtl(120_000L).softExpiryAt(125_000L).build();
    assertThat(copy.getValue()).isEqualTo("staleRefreshed");
    assertThat(copy.getSoftTtlMs()).isEqualTo(120_000L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(125_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("value", "softTtlMs", "softExpireAtMs").isEqualTo(base);
  }

  @Test
  void draftRearmExpiry_shouldRecomputeTimestampsFromDurations() {
    CacheEntry base = fullEntry();
    CacheEntry copy = edit(base).rearmExpiry().build();
    assertThat(copy.getHardTtlMs()).isEqualTo(300_000L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(300_000L + 1000);
    assertThat(copy.getSoftTtlMs()).isEqualTo(30_000L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(30_000L + 500);
    assertThat(copy).usingRecursiveComparison().ignoringFields("hardExpireAtMs", "softExpireAtMs").isEqualTo(base);
  }

  @Test
  void draftWithoutArithmetic_computedExpiry_shouldFailFast() {
    CacheEntry base = fullEntry();
    assertThatThrownBy(() -> EntryDraft.of(base).ttl(60_000L, 30_000L).build()).isInstanceOf(
      IllegalStateException.class
    );
    assertThatThrownBy(() -> EntryDraft.of(base).rearmExpiry().build()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void draftWithoutArithmetic_hardExpiryAfterTtl_shouldResolvePending() {
    CacheEntry base = fullEntry();
    // hardTtl arms the hard pending flag (positive duration, no arithmetic);
    // hardExpiryAt must resolve it exactly like softExpiryAt resolves the
    // soft side — the explicit override replaces the uncomputed timestamp.
    CacheEntry copy = EntryDraft.of(base).hardTtl(5000L).hardExpiryAt(999_000L).build();
    assertThat(copy.getHardTtlMs()).isEqualTo(5000L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(999_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("hardTtlMs", "hardExpireAtMs").isEqualTo(base);
  }

  @Test
  void draftWithoutArithmetic_partialPending_shouldStillFailFast() {
    CacheEntry base = fullEntry();
    // Resolving only one side leaves the other pending: the flags are
    // independent, so build() must still fail fast.
    assertThatThrownBy(() -> EntryDraft.of(base).ttl(5000L, 30_000L).hardExpiryAt(999_000L).build()).isInstanceOf(
      IllegalStateException.class
    );
    assertThatThrownBy(() -> EntryDraft.of(base).ttl(60_000L, 30_000L).softExpiryAt(90_000L).build()).isInstanceOf(
      IllegalStateException.class
    );
  }

  @Test
  void draftWithoutArithmetic_explicitTimestamps_shouldStillWork() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).hardExpiryAt(900_000L).softExpiryAt(90_000L).build();
    assertThat(copy.getHardExpireAtMs()).isEqualTo(900_000L);
    assertThat(copy.getSoftExpireAtMs()).isEqualTo(90_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("hardExpireAtMs", "softExpireAtMs").isEqualTo(base);
  }

  @Test
  void draftBlank_shouldDefaultToLogicalZero() {
    CacheEntry entry = EntryDraft.blank(ARITH).build();
    assertThat(entry.getValue()).isNull();
    assertThat(entry.getDataVersion()).isZero();
    assertThat(entry.isVersionDegraded()).isFalse();
    assertThat(entry.getDecisionVersion()).isZero();
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionEpoch()).isZero();
    assertThat(entry.getHardTtlMs()).isZero();
    assertThat(entry.getHardExpireAtMs()).isZero();
    assertThat(entry.getSoftTtlMs()).isZero();
    assertThat(entry.getSoftExpireAtMs()).isZero();
    assertThat(entry.getKeyState()).isNull();
    assertThat(entry.getNormalHardTtlMs()).isZero();
    assertThat(entry.getNormalSoftTtlMs()).isZero();
  }
}
