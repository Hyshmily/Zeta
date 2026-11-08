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
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TtlPolicy} — the pure TTL/expiry policy module behind
 * {@link EntryLifecycle}.
 *
 * <p>Covers every stateless lifecycle computation: resolve (override vs
 * default), effective (configured values), timestamp conversion to absolute
 * expire instants with jitter, expiry predicates, and the entry-level TTL
 * transforms. No Caffeine cache required — the policy is tested directly
 * through its own interface.
 */
class TtlPolicyTest {

  private ZetaProperties ttlConfig;
  private TtlPolicy ttlPolicy;

  @BeforeEach
  void setUp() {
    ttlConfig = new ZetaProperties();
    ttlPolicy = new TtlPolicy(ttlConfig);
  }

  /**
   * Verifies that a positive hard-TTL override wins over the configured default.
   */
  @Test
  void resolveEffectiveHardTtl_withPositiveOverride_shouldUseOverride() {
    assertThat(ttlPolicy.resolveEffectiveHardTtl(120_000)).isEqualTo(120_000);
  }

  /**
   * Verifies that a non-positive hard-TTL override falls back to the configured default.
   */
  @Test
  void resolveEffectiveHardTtl_withNonPositiveOverride_shouldFallbackToDefault() {
    assertThat(ttlPolicy.resolveEffectiveHardTtl(0)).isEqualTo(ttlPolicy.effectiveHardTtlMs());
    assertThat(ttlPolicy.resolveEffectiveHardTtl(-1)).isEqualTo(ttlPolicy.effectiveHardTtlMs());
  }

  /**
   * A permanent hot policy is the floor even when the entry arrived with a finite normal TTL:
   * promotion must not turn permanent into finite.
   */
  @Test
  void resolveEffectiveHotHard_permanentHotDefaultWinsOverFiniteNormalTtl() {
    ttlConfig.setHotHardTtlMs(0L);
    TtlPolicy policy = new TtlPolicy(ttlConfig);

    assertThat(policy.resolveEffectiveHotHard(120_000L)).isZero();
    assertThat(policy.resolveEffectiveHotHard(Long.MAX_VALUE)).isZero();
  }

  /**
   * A permanent normal TTL survives promotion: the hot default is a floor, never a cap.
   */
  @Test
  void resolveEffectiveHotHard_permanentNormalTtlSurvivesPromotion() {
    ttlConfig.setHardTtlMs(0L);
    ttlConfig.setHotHardTtlMs(3_600_000L);
    TtlPolicy policy = new TtlPolicy(ttlConfig);

    assertThat(policy.resolveEffectiveHotHard(0L)).isZero();
  }

  /**
   * A disabled hot soft TTL stays disabled through promotion; a normal soft window must not
   * re-enable it.
   */
  @Test
  void resolveEffectiveHotSoft_disabledHotDefaultWinsOverFiniteNormalSoftTtl() {
    ttlConfig.setHotSoftTtlMs(0L);
    TtlPolicy policy = new TtlPolicy(ttlConfig);

    assertThat(policy.resolveEffectiveHotSoft(30_000L)).isZero();
  }

  /**
   * Verifies that effectiveHardTtlMs returns the configured default hard TTL value.
   */
  @Test
  void effectiveHardTtlMs_shouldReturnConfigValue() {
    assertThat(ttlPolicy.effectiveHardTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that effectiveHotHardTtlMs returns the configured hot-key hard TTL value.
   */
  @Test
  void effectiveHotHardTtlMs_shouldReturnConfigValue() {
    assertThat(ttlPolicy.effectiveHotHardTtlMs()).isEqualTo(3_600_000L);
  }

  /**
   * Verifies that effectiveSoftTtlMs returns the configured default soft TTL value.
   */
  @Test
  void effectiveSoftTtlMs_shouldReturnConfigValue() {
    assertThat(ttlPolicy.effectiveSoftTtlMs()).isEqualTo(30_000L);
  }

  /**
   * Verifies that effectiveHotSoftTtlMs returns the configured hot-key soft TTL value.
   */
  @Test
  void effectiveHotSoftTtlMs_shouldReturnConfigValue() {
    assertThat(ttlPolicy.effectiveHotSoftTtlMs()).isEqualTo(300_000L);
  }

  /**
   * A missing entry is soft-expired: L1 slots hold {@link CacheEntry} only, so absence is the
   * only non-entry shape.
   */
  @Test
  void isSoftExpired_shouldReturnTrueForMissingEntry() {
    assertThat(ttlPolicy.isSoftExpired(null)).isTrue();
  }

  /**
   * Verifies that a positive soft-TTL override wins over the configured default.
   */
  @Test
  void resolveEffectiveSoftTtl_withPositiveOverride_shouldUseOverride() {
    assertThat(ttlPolicy.resolveEffectiveSoftTtl(60_000)).isEqualTo(60_000);
  }

  /**
   * Verifies that a non-positive soft-TTL override falls back to the configured default.
   */
  @Test
  void resolveEffectiveSoftTtl_withNonPositiveOverride_shouldFallbackToDefault() {
    assertThat(ttlPolicy.resolveEffectiveSoftTtl(0)).isEqualTo(ttlPolicy.effectiveSoftTtlMs());
    assertThat(ttlPolicy.resolveEffectiveSoftTtl(-1)).isEqualTo(ttlPolicy.effectiveSoftTtlMs());
  }

  /**
   * Verifies that Long.MAX_VALUE (permanent entry) passes through the hard-TTL
   * timestamp conversion unchanged.
   */
  @Test
  void hardExpireAt_withMaxValue_shouldPassthrough() {
    assertThat(ttlPolicy.hardExpireAt(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that a disabled (0) configured soft TTL resolves to zero — the
   * documented "soft-expire off" value.
   */
  @Test
  void resolveEffectiveSoftTtl_withDisabledConfig_shouldReturnZero() {
    ttlConfig.setSoftTtlMs(0);
    ttlConfig.setHotSoftTtlMs(0);
    TtlPolicy disabled = new TtlPolicy(ttlConfig);
    assertThat(disabled.effectiveSoftTtlMs()).isZero();
    assertThat(disabled.resolveEffectiveSoftTtl(0)).isZero();
    assertThat(disabled.softExpireAt(disabled.resolveEffectiveSoftTtl(0))).isZero();
  }

  /**
   * Verifies that the hard-TTL timestamp conversion produces a positive timestamp within a
   * reasonable range.
   */
  @Test
  void hardExpireAt_producesPositiveTimestamp() {
    long hard = ttlPolicy.hardExpireAt(5_000);
    assertThat(hard).isGreaterThan(System.currentTimeMillis());
    assertThat(hard).isLessThan(System.currentTimeMillis() + 60_000);
  }

  /**
   * Verifies that the soft-TTL timestamp conversion produces a positive timestamp within a
   * reasonable range.
   */
  @Test
  void softExpireAt_producesPositiveTimestamp() {
    long soft = ttlPolicy.softExpireAt(5_000);
    assertThat(soft).isGreaterThan(System.currentTimeMillis());
    assertThat(soft).isLessThan(System.currentTimeMillis() + 60_000);
  }

  /**
   * Verifies that isSoftExpired returns true when softExpireAtMs is a past timestamp
   * (positive but earlier than the current time).
   */
  @Test
  void isSoftExpired_withExpiredEntry_shouldReturnTrue() {
    CacheEntry entry = EntryDraft.blank(null)
      .value("v")
      .version(1)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, 1L)
      .keyState(KeyState.HOT)
      .build();
    assertThat(ttlPolicy.isSoftExpired(entry)).isTrue();
  }

  /**
   * Verifies that isSoftExpired with softExpireAtMs=Long.MAX_VALUE returns false
   * (MAX_VALUE means never soft-expire).
   */
  @Test
  void isSoftExpired_withMaxValue_shouldReturnFalse() {
    CacheEntry entry = EntryDraft.blank(null)
      .value("v")
      .version(1)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, Long.MAX_VALUE, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
      .keyState(KeyState.HOT)
      .build();
    assertThat(ttlPolicy.isSoftExpired(entry)).isFalse();
  }

  /**
   * {@code softExpireAtMs == 0} is the documented "disabled" value: a disabled soft TTL never
   * reads as stale, so it cannot arm a background refresh on every hit.
   */
  @Test
  void isSoftExpired_withZeroExpireAt_shouldReturnFalse() {
    CacheEntry entry = EntryDraft.blank(null)
      .value("v")
      .version(1)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 0, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, 0)
      .keyState(KeyState.HOT)
      .build();
    assertThat(ttlPolicy.isSoftExpired(entry)).isFalse();
  }

  /**
   * Verifies that Long.MAX_VALUE (soft-expire disabled) passes through the soft-TTL
   * timestamp conversion unchanged.
   */
  @Test
  void softExpireAt_withMaxValue_shouldPassthrough() {
    assertThat(ttlPolicy.softExpireAt(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that custom ratio (0.5 = ±50%) produces jitter within the configured range.
   */
  @Test
  void hardExpireAt_withCustomRatio_shouldJitterWithinRange() {
    ttlConfig.setTtlJitterRatio(0.5);
    TtlPolicy highJitter = new TtlPolicy(ttlConfig);
    long ttl = 10_000;
    Stream.generate(() -> highJitter.hardExpireAt(ttl))
      .limit(100)
      .forEach(expireAt -> {
        long diff = expireAt - System.currentTimeMillis();
        assertThat(diff).isBetween(4_900L, 15_100L);
      });
  }

  /**
   * Verifies that the default jitter is applied and within range for both hard and soft TTL paths.
   */
  @Test
  void hardExpireAt_withDefaultJitter_shouldJitterWithinRange() {
    long ttl = 10_000;
    Stream.generate(() -> ttlPolicy.hardExpireAt(ttl))
      .limit(100)
      .forEach(expireAt -> {
        long diff = expireAt - System.currentTimeMillis();
        assertThat(diff).isBetween(9_000L, 11_000L);
      });
    Stream.generate(() -> ttlPolicy.softExpireAt(ttl))
      .limit(100)
      .forEach(expireAt -> {
        long diff = expireAt - System.currentTimeMillis();
        assertThat(diff).isBetween(9_000L, 11_000L);
      });
  }

  /**
   * Verifies that hardExpireAt with a custom jitter ratio uses the given ratio.
   */
  @Test
  void hardExpireAt_withCustomRatio_shouldUseGivenRatio() {
    long ttl = 10_000;
    Stream.generate(() -> ttlPolicy.hardExpireAt(ttl, 0.5))
      .limit(50)
      .forEach(expireAt -> {
        long diff = expireAt - TimeSource.currentTimeMillis();
        assertThat(diff).isBetween(5_000L, 15_000L);
      });
  }

  /**
   * Verifies that hardExpireAt with Long.MAX_VALUE and a custom ratio passes through.
   */
  @Test
  void hardExpireAt_withMaxValueAndCustomRatio_shouldPassthrough() {
    assertThat(ttlPolicy.hardExpireAt(Long.MAX_VALUE, 0.5)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that a zero jitter ratio applies no offset to either expiry path.
   */
  @Test
  void expireTimestamps_withZeroRatio_shouldNotJitter() {
    long ttl = 10_000;
    long hardDiff = ttlPolicy.hardExpireAt(ttl, 0.0) - TimeSource.currentTimeMillis();
    long softDiff = ttlPolicy.softExpireAt(ttl, 0.0) - TimeSource.currentTimeMillis();
    assertThat(hardDiff).isBetween(ttl, ttl + 100);
    assertThat(softDiff).isBetween(ttl, ttl + 100);
  }

  /**
   * Verifies that softExpireAt with a custom jitter ratio uses the given ratio.
   */
  @Test
  void softExpireAt_withCustomRatio_shouldUseGivenRatio() {
    long ttl = 10_000;
    Stream.generate(() -> ttlPolicy.softExpireAt(ttl, 0.5))
      .limit(50)
      .forEach(expireAt -> {
        long diff = expireAt - TimeSource.currentTimeMillis();
        assertThat(diff).isBetween(5_000L, 15_000L);
      });
  }

  /**
   * Verifies that softExpireAt with non-positive soft TTL returns zero.
   */
  @Test
  void softExpireAt_withNonPositiveTtlAndCustomRatio_shouldReturnZero() {
    assertThat(ttlPolicy.softExpireAt(0, 0.5)).isZero();
    assertThat(ttlPolicy.softExpireAt(-1, 0.5)).isZero();
  }

  /**
   * Verifies that softExpireAt with Long.MAX_VALUE passes through.
   */
  @Test
  void softExpireAt_withMaxValueAndCustomRatio_shouldPassthrough() {
    assertThat(ttlPolicy.softExpireAt(Long.MAX_VALUE, 0.5)).isEqualTo(Long.MAX_VALUE);
  }

  // ── EntryDraft#ttl / softTtl ──────────────

  /**
   * Verifies that {@code EntryDraft#ttl} updates the hard TTL and expire-at
   * while the soft side stays pinned and all version/state fields survive.
   */
  @Test
  void hardTtl_shouldUpdateHardTtlAndPreserveSoftTtl() {
    long now = System.currentTimeMillis();
    CacheEntry original = EntryDraft.blank(null)
      .value("v")
      .version(10L)
      .decision(DecisionStamp.hot(5, null, 0L))
      .ttl(60_000, 30_000, 300_000, 30_000)
      .expiryAt(now + 60_000, now + 30_000)
      .keyState(KeyState.HOT)
      .build();

    CacheEntry updated = EntryDraft.of(original, ttlPolicy)
      .ttl(120_000, original.getSoftTtlMs())
      .softExpiryAt(original.getSoftExpireAtMs())
      .build();

    assertThat(updated.getHardTtlMs()).isEqualTo(120_000);
    assertThat(updated.getHardExpireAtMs()).isGreaterThan(original.getHardExpireAtMs());
    assertThat(updated.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(updated.getSoftExpireAtMs()).isEqualTo(original.getSoftExpireAtMs());
    assertThat(updated.getDataVersion()).isEqualTo(10L);
    assertThat(updated.getDecisionVersion()).isEqualTo(5);
    assertThat(updated.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies that {@code EntryDraft#softTtl} updates the soft TTL and expire-at
   * while preserving the hard TTL and all version/state fields.
   */
  @Test
  void softTtl_shouldUpdateSoftTtlAndPreserveHardTtl() {
    long now = System.currentTimeMillis();
    CacheEntry original = EntryDraft.blank(null)
      .value("v")
      .version(10L)
      .decision(DecisionStamp.hot(5, null, 0L))
      .ttl(60_000, 30_000, 300_000, 30_000)
      .expiryAt(now + 60_000, now + 30_000)
      .keyState(KeyState.HOT)
      .build();

    CacheEntry updated = EntryDraft.of(original, ttlPolicy).softTtl(120_000).build();

    assertThat(updated.getSoftTtlMs()).isEqualTo(120_000);
    assertThat(updated.getSoftExpireAtMs()).isGreaterThan(original.getSoftExpireAtMs());
    assertThat(updated.getHardTtlMs()).isEqualTo(60_000);
    assertThat(updated.getHardExpireAtMs()).isEqualTo(original.getHardExpireAtMs());
    assertThat(updated.getDataVersion()).isEqualTo(10L);
    assertThat(updated.getKeyState()).isEqualTo(KeyState.HOT);
  }

  // ── Timestamp-threading overloads (single-now hit path) ────────────────

  private static CacheEntry entryWithExpiries(long hardExpireAtMs, long softExpireAtMs) {
    return EntryDraft.blank(null)
      .value("v")
      .version(1)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(hardExpireAtMs, softExpireAtMs)
      .keyState(KeyState.NORMAL)
      .build();
  }

  /**
   * The {@code now} overloads must judge identically to the clock-reading
   * forms when {@code now} is the current time: same entry, same verdict.
   */
  @Test
  void expiryPredicates_withCurrentNow_shouldAgreeWithClockForms() {
    long now = TimeSource.currentTimeMillis();
    CacheEntry fresh = entryWithExpiries(now + 60_000, now + 30_000);
    CacheEntry stale = entryWithExpiries(now - 1, now - 1);

    assertThat(ttlPolicy.isLogicallyExpired(fresh, now)).isEqualTo(ttlPolicy.isLogicallyExpired(fresh));
    assertThat(ttlPolicy.isSoftExpired(fresh, now)).isEqualTo(ttlPolicy.isSoftExpired(fresh));
    assertThat(ttlPolicy.isLogicallyExpired(stale, now)).isEqualTo(ttlPolicy.isLogicallyExpired(stale));
    assertThat(ttlPolicy.isSoftExpired(stale, now)).isEqualTo(ttlPolicy.isSoftExpired(stale));
    assertThat(ttlPolicy.isLogicallyExpired(fresh, now)).isFalse();
    assertThat(ttlPolicy.isSoftExpired(fresh, now)).isFalse();
    assertThat(ttlPolicy.isLogicallyExpired(stale, now)).isTrue();
    assertThat(ttlPolicy.isSoftExpired(stale, now)).isTrue();
  }

  /**
   * A single shared {@code now} must govern both predicates: an entry whose
   * expiries sit strictly after {@code now} reads fresh on both, even if the
   * wall clock advances past them before the second predicate runs.
   */
  @Test
  void expiryPredicates_withSharedNow_shouldJudgeOneInstant() {
    long now = TimeSource.currentTimeMillis();
    CacheEntry entry = entryWithExpiries(now + 30_000, now + 30_000);

    assertThat(ttlPolicy.isLogicallyExpired(entry, now)).isFalse();
    assertThat(ttlPolicy.isSoftExpired(entry, now)).isFalse();
    // After both expiries the same sample judges both expired.
    assertThat(ttlPolicy.isLogicallyExpired(entry, now + 60_000)).isTrue();
    assertThat(ttlPolicy.isSoftExpired(entry, now + 60_000)).isTrue();
  }

  /**
   * Permanent entries ({@code Long.MAX_VALUE}) never expire under either
   * form; disabled soft TTL ({@code 0}) never reads stale.
   */
  @Test
  void expiryPredicates_withSentinels_shouldHonorDisabledAndPermanent() {
    long now = TimeSource.currentTimeMillis();
    CacheEntry permanent = entryWithExpiries(Long.MAX_VALUE, 0);

    assertThat(ttlPolicy.isLogicallyExpired(permanent, now)).isFalse();
    assertThat(ttlPolicy.isSoftExpired(permanent, now)).isFalse();
    assertThat(ttlPolicy.isSoftExpired(null, now)).isTrue();
  }

  /**
   * The two expiry predicates share one boundary: at the deadline millisecond the
   * entry is expired, so a hard and a soft deadline in the same millisecond can never
   * disagree.
   */
  @Test
  void isSoftExpired_atTheDeadline_shouldBeExpired() {
    long now = TimeSource.currentTimeMillis();
    CacheEntry entry = entryWithExpiries(now, now);

    assertThat(ttlPolicy.isLogicallyExpired(entry, now)).isTrue();
    assertThat(ttlPolicy.isSoftExpired(entry, now)).isTrue();
    assertThat(ttlPolicy.isLogicallyExpired(entry, now - 1)).isFalse();
    assertThat(ttlPolicy.isSoftExpired(entry, now - 1)).isFalse();
  }

  /**
   * A TTL near {@link Long#MAX_VALUE} must saturate: the plain {@code now + ttl}
   * addition wrapped negative, and both predicates read a negative deadline as
   * "expired since forever", so the entry died on the write that created it.
   */
  @Test
  void toExpireTimestamp_nearLongMax_shouldSaturateInsteadOfWrapping() {
    // No jitter: the deadline is exactly the ceiling.
    assertThat(ttlPolicy.hardExpireAt(Long.MAX_VALUE - 1, 0.0)).isEqualTo(Long.MAX_VALUE);
    assertThat(ttlPolicy.softExpireAt(Long.MAX_VALUE - 1, 0.0)).isEqualTo(Long.MAX_VALUE);

    // With jitter the ceiling is only approached, but the result must never wrap negative.
    assertThat(ttlPolicy.hardExpireAt(Long.MAX_VALUE - 1)).isGreaterThan(TimeSource.currentTimeMillis());
    assertThat(ttlPolicy.softExpireAt(Long.MAX_VALUE - 1)).isGreaterThan(TimeSource.currentTimeMillis());

    // Half of MAX still fits below the ceiling: a real (far-future) timestamp, not MAX.
    long hard = ttlPolicy.hardExpireAt(Long.MAX_VALUE / 2);
    assertThat(hard).isPositive().isGreaterThan(TimeSource.currentTimeMillis());
  }

  /**
   * Jitter is symmetric and closed on both ends. With a 1 ms base the offsets are
   * {@code 10 + jitter - clockDrift} for {@code jitter ∈ [-1, 1]}, so both 9 and 11 must
   * appear.
   */
  @Test
  void computeTtlJitter_withOneMillisecondBase_shouldSpanBothSigns() {
    Set<Long> offsets = new HashSet<>();
    for (int i = 0; i < 500; i++) {
      offsets.add(ttlPolicy.hardExpireAt(10, 0.1) - TimeSource.currentTimeMillis());
    }

    assertThat(offsets).allMatch(offset -> offset >= 8 && offset <= 11);
    assertThat(offsets).contains(9L, 11L);
  }
}
