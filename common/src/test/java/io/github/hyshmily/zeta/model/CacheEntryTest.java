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

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.model.DecisionStamp;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CacheEntry} builder covering full, normal, and draft construction.
 */
class CacheEntryTest {

  /**
   * Verifies building a CacheEntry with all fields populated and reading them back correctly.
   */
  @Test
  void shouldBuildEntryWithAllFields() {
    CacheEntry entry = EntryDraft.blank(null)
      .value("testValue")
      .version(100L)
      .decision(DecisionStamp.hot(5L, null, 0L))
      .ttl(300_000L, 30_000L, 300_000L, 30_000L)
      .expiryAt(System.currentTimeMillis() + 300_000L, System.currentTimeMillis() + 30_000L)
      .keyState(KeyState.HOT)
      .build();

    assertThat(entry.getValue()).isEqualTo("testValue");
    assertThat(entry.getDataVersion()).isEqualTo(100L);
    assertThat(entry.getDecisionVersion()).isEqualTo(5L);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies building a normal (non-hot) CacheEntry with default version values and NORMAL key state.
   */
  @Test
  void shouldBuildNormalEntry() {
    CacheEntry entry = EntryDraft.blank(null)
      .value(42)
      .version(0L)
      .decision(DecisionStamp.hot(0L, null, 0L))
      .ttl(300_000L, 0L, 300_000L, 30_000L)
      .expiryAt(Long.MAX_VALUE, 0L)
      .keyState(KeyState.NORMAL)
      .build();

    assertThat(entry.getValue()).isEqualTo(42);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void builderDefaults_shouldBeNullForObjectZeroForLongFalseForBoolean() {
    CacheEntry entry = EntryDraft.blank(null).build();
    assertThat(entry.getValue()).isNull();
    assertThat(entry.getDataVersion()).isZero();
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
    CacheEntry entry = EntryDraft.blank(null).value("infinite").hardExpiryAt(Long.MAX_VALUE).build();
    assertThat(entry.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void keyStateCOOL_shouldBuildCorrectly() {
    CacheEntry entry = EntryDraft.blank(null).value("cooling").keyState(KeyState.COOL).build();
    assertThat(entry.getKeyState()).isEqualTo(KeyState.COOL);
  }

  @Test
  void valueAsString_shouldBeStored() {
    CacheEntry entry = EntryDraft.blank(null).value("hello").build();
    assertThat(entry.getValue()).isEqualTo("hello");
  }

  @Test
  void valueAsInteger_shouldBeStored() {
    CacheEntry entry = EntryDraft.blank(null).value(123).build();
    assertThat(entry.getValue()).isEqualTo(123);
  }

  @Test
  void valueAsNull_shouldBeAccepted() {
    CacheEntry entry = EntryDraft.blank(null).value(null).build();
    assertThat(entry.getValue()).isNull();
  }

  @Test
  void draftCopy_shouldCarryAllFields() {
    CacheEntry original = EntryDraft.blank(null).value("original").version(7L).keyState(KeyState.HOT).build();
    CacheEntry copy = EntryDraft.of(original).value("modified").build();
    assertThat(copy.getValue()).isEqualTo("modified");
    assertThat(copy.getDataVersion()).isEqualTo(7L);
    assertThat(copy.getKeyState()).isEqualTo(KeyState.HOT);
  }

  @Test
  void equalsAndHashCode_shouldWorkForIdenticalEntries() {
    CacheEntry a = EntryDraft.blank(null).value("x").version(1L).build();
    CacheEntry b = EntryDraft.blank(null).value("x").version(1L).build();
    assertThat(a).isEqualTo(b);
    assertThat(a).hasSameHashCodeAs(b);
  }

  @Test
  void equalsAndHashCode_shouldDistinguishDifferentEntries() {
    CacheEntry a = EntryDraft.blank(null).value("x").version(1L).build();
    CacheEntry b = EntryDraft.blank(null).value("y").version(1L).build();
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void dataVersionUnknownAndMax_shouldAcceptBoundaryValues() {
    CacheEntry unknown = EntryDraft.blank(null).value("unknown").version(0L).build();
    CacheEntry max = EntryDraft.blank(null).value("max").version(Long.MAX_VALUE).build();
    assertThat(unknown.getDataVersion()).isZero();
    assertThat(max.getDataVersion()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void decisionVersionMinMax_shouldAcceptBoundaryValues() {
    CacheEntry min = EntryDraft.blank(null).value("min").decision(DecisionStamp.hot(Long.MIN_VALUE, null, 0L)).build();
    CacheEntry max = EntryDraft.blank(null).value("max").decision(DecisionStamp.hot(Long.MAX_VALUE, null, 0L)).build();
    assertThat(min.getDecisionVersion()).isEqualTo(Long.MIN_VALUE);
    assertThat(max.getDecisionVersion()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void hardExpireAtMsInPast_shouldAcceptExpiredEntry() {
    CacheEntry entry = EntryDraft.blank(null).value("expired").hardExpiryAt(-1L).build();
    assertThat(entry.getHardExpireAtMs()).isNegative();
  }

  @Test
  void negativeTtlValues_shouldBeRejected() {
    assertThatThrownBy(() -> EntryDraft.blank(null).value("neg").ttl(-100L, -50L).build()).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  /** The epoch is a full long now that it lives in the decision stamp, not a 56-bit field. */
  @Test
  void fullWidthDecisionEpoch_shouldRoundTrip() {
    long epoch = Long.MAX_VALUE;
    CacheEntry entry = EntryDraft.blank(null).value("v").decision(DecisionStamp.hot(0L, "worker-1", epoch)).build();
    assertThat(entry.getDecisionEpoch()).isEqualTo(epoch);
  }

  @Test
  void veryLongValue_shouldBeStored() {
    String longStr = "a".repeat(10_000);
    CacheEntry entry = EntryDraft.blank(null).value(longStr).build();
    assertThat(entry.getValue()).isEqualTo(longStr);
  }

  @Test
  void specialCharactersInValue_shouldBeStored() {
    String specials = "hello\n\t\r\0unicode:\u00E9\u4E2D\u6587";
    CacheEntry entry = EntryDraft.blank(null).value(specials).build();
    assertThat(entry.getValue()).isEqualTo(specials);
  }

  @Test
  void draftCopy_shouldPreserveAllFieldsExceptModified() {
    CacheEntry original = EntryDraft.blank(null)
      .value("orig")
      .version(7L)
      .decision(DecisionStamp.hot(2L, null, 0L))
      .ttl(100L, 10L, 100L, 10L)
      .expiryAt(200L, 20L)
      .keyState(KeyState.HOT)
      .build();
    CacheEntry copy = EntryDraft.of(original).value("modified").build();
    assertThat(copy.getValue()).isEqualTo("modified");
    assertThat(copy.getDataVersion()).isEqualTo(7L);
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
    CacheEntry a = EntryDraft.blank(null).value(null).version(1L).build();
    CacheEntry b = EntryDraft.blank(null).value(null).version(1L).build();
    assertThat(a).isEqualTo(b);
    assertThat(a).hasSameHashCodeAs(b);
  }

  /**
   * The entry keeps the immutable stamp by reference: a seeded draft must not copy
   * its fields, and a Worker decision must cost one reference rather than three fields.
   */
  @Test
  void decisionStamp_shouldBeStoredByReference() {
    DecisionStamp stamp = DecisionStamp.hot(5L, "worker-1", 7L);
    CacheEntry entry = EntryDraft.blank(null).value("v").decision(stamp).build();
    assertThat(entry.decision()).isSameAs(stamp);

    CacheEntry copy = EntryDraft.of(entry).value("copy").build();
    assertThat(copy.decision()).isSameAs(stamp);
  }

  /**
   * Verifies decisionNodeId set through the draft is stored and retrieved correctly.
   */
  @Test
  void decisionNodeId_shouldBeStoredAndRetrieved() {
    CacheEntry entry = EntryDraft.blank(null).value("v").decision(DecisionStamp.hot(0L, "worker-1", 0L)).build();
    assertThat(entry.getDecisionNodeId()).isEqualTo("worker-1");
  }

  /**
   * Verifies decisionEpoch set through the draft is stored and retrieved correctly.
   */
  @Test
  void decisionEpoch_shouldBeStoredAndRetrieved() {
    CacheEntry entry = EntryDraft.blank(null).value("v").decision(DecisionStamp.hot(0L, "worker-1", 5L)).build();
    assertThat(entry.getDecisionEpoch()).isEqualTo(5L);
  }

  /**
   * Verifies decisionNodeId defaults to null when not explicitly set.
   */
  @Test
  void decisionNodeId_defaultNull_whenNotSet() {
    CacheEntry entry = EntryDraft.blank(null).value("v").build();
    assertThat(entry.getDecisionNodeId()).isNull();
  }

  /**
   * Verifies decisionEpoch defaults to zero when not explicitly set.
   */
  @Test
  void decisionEpoch_defaultZero_whenNotSet() {
    CacheEntry entry = EntryDraft.blank(null).value("v").build();
    assertThat(entry.getDecisionEpoch()).isZero();
  }

  /**
   * Verifies the draft carries decisionNodeId and decisionEpoch to the new entry.
   */
  @Test
  void draftCopy_shouldCarryDecisionFields() {
    CacheEntry original = EntryDraft.blank(null).value("orig").decision(DecisionStamp.hot(0L, "worker-1", 5L)).build();
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
    CacheEntry a = EntryDraft.blank(null)
      .value("x")
      .version(1L)
      .decision(DecisionStamp.hot(0L, "worker-1", 5L))
      .build();
    CacheEntry b = EntryDraft.blank(null).value("x").version(1L).build();
    assertThat(a).isNotEqualTo(b);
  }

  // ── EntryDraft copy-on-write ──

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
    return EntryDraft.blank(null)
      .value("baseValue")
      .version(100L)
      .decision(DecisionStamp.hot(5L, "worker-1", 3L))
      .ttl(300_000L, 30_000L, 150_000L, 15_000L)
      .expiryAt(400_000L, 40_000L)
      .keyState(KeyState.HOT)
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
    CacheEntry copy = EntryDraft.of(base).decision(DecisionStamp.hot(99L, "worker-1", 3L)).build();
    assertThat(copy.getDecisionVersion()).isEqualTo(99L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("decision").isEqualTo(base);
  }

  @Test
  void draftDecision_shouldCreateCopyWithNewDecisionNodeId() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).decision(DecisionStamp.hot(5L, "worker-2", 3L)).build();
    assertThat(copy.getDecisionNodeId()).isEqualTo("worker-2");
    assertThat(copy).usingRecursiveComparison().ignoringFields("decision").isEqualTo(base);
  }

  @Test
  void draftDecision_shouldCreateCopyWithNewDecisionEpoch() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).decision(DecisionStamp.hot(5L, "worker-1", 42L)).build();
    assertThat(copy.getDecisionEpoch()).isEqualTo(42L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("decision").isEqualTo(base);
  }

  @Test
  void draftClearDecision_shouldResetToLocalOrigin() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).clearDecision().build();
    assertThat(copy.getDecisionVersion()).isZero();
    assertThat(copy.getDecisionNodeId()).isNull();
    assertThat(copy.getDecisionEpoch()).isZero();
    assertThat(copy).usingRecursiveComparison().ignoringFields("decision").isEqualTo(base);
  }

  @Test
  void draftHardTtl_shouldUpdateDurationAndComputedTimestamp() {
    CacheEntry base = fullEntry();
    CacheEntry copy = edit(base).ttl(600_000L, base.getSoftTtlMs()).softExpiryAt(base.getSoftExpireAtMs()).build();
    assertThat(copy.getHardTtlMs()).isEqualTo(600_000L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(600_000L + 1000);
    assertThat(copy).usingRecursiveComparison().ignoringFields("packedMeta", "hardExpireAtMs").isEqualTo(base);
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
    assertThat(copy).usingRecursiveComparison().ignoringFields("packedMeta").isEqualTo(base);
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
      .ignoringFields("packedMeta", "hardExpireAtMs", "softTtlMs", "softExpireAtMs")
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
        "packedMeta",
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
      .ignoringFields("packedMeta", "hardExpireAtMs", "softTtlMs", "softExpireAtMs")
      .isEqualTo(base);
  }

  @Test
  void draftDecisionTtlAndState_shouldCreateCopyWithDecisionTtlAndState() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base)
      .decision(DecisionStamp.hot(99L, "worker-9", 7L))
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
      .ignoringFields("decision", "packedMeta", "hardExpireAtMs", "softTtlMs", "softExpireAtMs")
      .isEqualTo(base);
  }

  @Test
  void draftValueVersionAndExpiry_shouldCreateCopyWithValueVersionAndExpire() {
    CacheEntry base = fullEntry();
    CacheEntry copy = EntryDraft.of(base).value("refreshed").version(200L).expiryAt(999_000L, 99_000L).build();
    assertThat(copy.getValue()).isEqualTo("refreshed");
    assertThat(copy.getDataVersion()).isEqualTo(200L);
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
  void draftWithoutArithmetic_hardExpiryAfterTtl_shouldResolvePending() {
    CacheEntry base = fullEntry();
    // ttl arms the hard pending flag (positive duration, no arithmetic);
    // hardExpiryAt must resolve it exactly like softExpiryAt resolves the
    // soft side — the explicit override replaces the uncomputed timestamp.
    CacheEntry copy = EntryDraft.of(base)
      .ttl(5000L, base.getSoftTtlMs())
      .hardExpiryAt(999_000L)
      .softExpiryAt(base.getSoftExpireAtMs())
      .build();
    assertThat(copy.getHardTtlMs()).isEqualTo(5000L);
    assertThat(copy.getHardExpireAtMs()).isEqualTo(999_000L);
    assertThat(copy).usingRecursiveComparison().ignoringFields("packedMeta", "hardExpireAtMs").isEqualTo(base);
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

  @Test
  void ttlEncoding_shouldBeIdempotentAcrossUnitBoundary() {
    // Above 2^31-1 ms the encoder switches to whole seconds, rounded up. A no-op draft rebuild
    // must re-encode to the same packed int; the getters, equals and hashCode must all agree.
    long aboveMillisTier = (1L << 31) + 500;
    CacheEntry original = EntryDraft.blank(null)
      .value("x")
      .ttlDurations(aboveMillisTier, Long.MAX_VALUE)
      .expiryAt(Long.MAX_VALUE, 0L)
      .build();
    CacheEntry copy = EntryDraft.of(original).build();

    assertThat(original.getHardTtlMs()).as("seconds tier rounds up to 2,147,485,000ms").isEqualTo(2_147_485_000L);
    assertThat(original.getSoftTtlMs()).as("permanent keeps its sentinel").isEqualTo(Long.MAX_VALUE);
    assertThat(copy.getHardTtlMs()).isEqualTo(original.getHardTtlMs());
    assertThat(copy.getSoftTtlMs()).isEqualTo(Long.MAX_VALUE);
    assertThat(copy).isEqualTo(original);
    assertThat(copy).hasSameHashCodeAs(original);
  }

  @Test
  void ttlEncoding_shouldRoundTripExactAtMillisTierBoundary() {
    long exact = Integer.MAX_VALUE;
    CacheEntry entry = EntryDraft.blank(null)
      .value("x")
      .ttlDurations(exact, exact)
      .expiryAt(Long.MAX_VALUE, 0L)
      .build();

    assertThat(entry.getHardTtlMs()).isEqualTo(exact);
    assertThat(entry.getSoftTtlMs()).isEqualTo(exact);
    assertThat(EntryDraft.of(entry).build()).isEqualTo(entry);
  }

  @Test
  void permanentZeroDuration_shouldNormalizeToMaxSentinel() {
    CacheEntry entry = EntryDraft.blank(null).value("v").ttl(0L, 0L).build();

    assertThat(entry.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
    assertThat(entry.getHardTtlMs())
      .as("0 means permanent in the duration contract; the entry must store the explicit sentinel")
      .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void negativeSoftTtl_shouldBeRejectedAtTheSetter() {
    assertThatThrownBy(() -> EntryDraft.blank(null).softTtl(-1L)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void negativeNormalTtl_shouldBeRejectedAtBuild() {
    assertThatThrownBy(() -> EntryDraft.blank(null).ttl(1L, 1L, -1L, 0L).expiryAt(1L, 1L).build()).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void toString_shouldNotExposeTheCachedValue() {
    CacheEntry entry = EntryDraft.blank(null).value("customer-secret").version(1L).build();

    assertThat(entry.toString()).doesNotContain("customer-secret").contains("dataVersion");
  }
}
