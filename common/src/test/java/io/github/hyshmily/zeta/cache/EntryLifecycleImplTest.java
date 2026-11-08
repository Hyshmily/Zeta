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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EntryLifecycleImpl}: the {@code newEntry()} draft factory
 * (decision metadata, computed expiry, packed TTL round-trips) and the clear
 * watermark lifecycle (prefix folding, pruning, effective-watermark reads).
 */
class EntryLifecycleImplTest {

  private EntryLifecycle entryLifecycle;
  private ZetaProperties ttlConfig;
  private Cache<String, CacheEntry> caffeineCache;

  @BeforeEach
  void setUp() {
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ttlConfig = new ZetaProperties();
    entryLifecycle = new EntryLifecycleImpl(caffeineCache, ttlConfig, null);
  }

  @Test
  void evictIfExpired_evictsAConcurrentReplacementThatIsAlsoExpired() {
    long now = System.currentTimeMillis();
    CacheEntry first = entryLifecycle
      .newEntry()
      .value("first")
      .version(1L)
      .ttl(1_000, 0, 1_000, 0)
      .expiryAt(now - 2L, 0L)
      .build();
    CacheEntry replacement = entryLifecycle
      .newEntry()
      .value("replacement")
      .version(2L)
      .ttl(1_000, 0, 1_000, 0)
      .expiryAt(now - 1L, 0L)
      .build();
    caffeineCache.put("key", first);
    CacheEntry raw = caffeineCache.getIfPresent("key");
    caffeineCache.put("key", replacement);

    assertThat(entryLifecycle.evictIfExpired("key", raw, now)).isNull();
    assertThat(caffeineCache.getIfPresent("key")).isNull();
  }

  // ── newEntry() draft parameter combinations ──────────────────────

  /**
   * Verifies that newEntry() with decision metadata and pre-computed
   * expire timestamps sets all fields correctly.
   */
  @Test
  void newEntry_withDecisionMetadataAndExpireTimestamps_shouldSetAllFields() {
    long now = System.currentTimeMillis();
    CacheEntry entry = entryLifecycle
      .newEntry()
      .value("value")
      .version(42)
      .decision(DecisionStamp.hot(7, "worker-1", 3))
      .ttl(60_000, 30_000, 300_000, 30_000)
      .expiryAt(now + 60_000, now + 30_000)
      .keyState(KeyState.HOT)
      .build();

    assertThat(entry.getValue()).isEqualTo("value");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getDecisionNodeId()).isEqualTo("worker-1");
    assertThat(entry.getDecisionEpoch()).isEqualTo(3);
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getHardExpireAtMs()).isEqualTo(now + 60_000);
    assertThat(entry.getSoftExpireAtMs()).isEqualTo(now + 30_000);
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(300_000);
    assertThat(entry.getNormalSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies that newEntry() with decision metadata but no expire
   * timestamps computes expire-at via applyTtl.
   */
  @Test
  void newEntry_withDecisionMetadataAndNoExpireTimestamps_shouldComputeExpire() {
    long before = System.currentTimeMillis();
    CacheEntry entry = entryLifecycle
      .newEntry()
      .value("value")
      .version(42)
      .decision(DecisionStamp.hot(7, "worker-1", 3))
      .ttl(60_000, 30_000, 300_000, 30_000)
      .keyState(KeyState.HOT)
      .build();

    assertThat(entry.getValue()).isEqualTo("value");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getDecisionNodeId()).isEqualTo("worker-1");
    assertThat(entry.getDecisionEpoch()).isEqualTo(3);
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getHardExpireAtMs()).isGreaterThan(before);
    assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getSoftExpireAtMs()).isGreaterThan(before);
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(300_000);
    assertThat(entry.getNormalSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies that newEntry() without decision node/epoch metadata (local
   * origin, no Worker) but with pre-computed expire timestamps sets all
   * fields correctly.
   */
  @Test
  void newEntry_withoutDecisionMetadataWithExpireTimestamps_shouldSetAllFields() {
    long now = System.currentTimeMillis();
    CacheEntry entry = entryLifecycle
      .newEntry()
      .value("value")
      .version(42)
      .decision(DecisionStamp.hot(7, null, 0))
      .ttl(60_000, 30_000, 300_000, 30_000)
      .expiryAt(now + 60_000, now + 30_000)
      .keyState(KeyState.NORMAL)
      .build();

    assertThat(entry.getValue()).isEqualTo("value");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionEpoch()).isZero();
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getHardExpireAtMs()).isEqualTo(now + 60_000);
    assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(300_000);
    assertThat(entry.getNormalSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  /**
   * Verifies that newEntry() with no decision metadata and no expire
   * timestamps produces a correctly built entry with timestamps computed
   * via applyTtl.
   */
  @Test
  void newEntry_rawFields_shouldComputeExpireAndSetFields() {
    long before = System.currentTimeMillis();
    CacheEntry entry = entryLifecycle
      .newEntry()
      .value("value")
      .version(42)
      .decision(DecisionStamp.hot(7, null, 0))
      .ttl(60_000, 30_000, 300_000, 30_000)
      .keyState(KeyState.NORMAL)
      .build();

    assertThat(entry.getValue()).isEqualTo("value");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionEpoch()).isZero();
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getHardExpireAtMs()).isGreaterThan(before);
    assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getSoftExpireAtMs()).isGreaterThan(before);
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(300_000);
    assertThat(entry.getNormalSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  // ── clear watermarks: prefix lifecycle ────────────────────────────

  /**
   * A prefix clear stamped at or below the table-wide watermark is a no-op:
   * the global watermark already refuses every write the prefix stamp would,
   * and the effective watermark read reflects that coverage.
   */
  @Test
  void recordPrefixClear_belowTheTableWatermark_isANoOp() {
    entryLifecycle.recordClear(100L);
    entryLifecycle.recordPrefixClear("users\u0000", 50L);

    assertThat(entryLifecycle.staleAgainstInvalidation("users\u0000k", 100L)).isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("users\u0000k", 50L)).isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("users\u0000k", 101L)).isFalse();
  }

  /**
   * A table-wide clear subsumes every prefix watermark: the prefixes report
   * the table-wide stamp and an unrelated namespace is covered too, while
   * post-clear writes stay admissible.
   */
  @Test
  void recordClear_subsumesPrefixWatermarks() {
    entryLifecycle.recordPrefixClear("users\u0000", 10L);
    entryLifecycle.recordPrefixClear("orders\u0000", 20L);

    entryLifecycle.recordClear(30L);

    assertThat(entryLifecycle.staleAgainstInvalidation("users\u0000k", 29L)).isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("orders\u0000k", 29L)).isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("cart\u0000k", 25L)).isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("cart\u0000k", 31L)).isFalse();
  }

  /**
   * Past the prefix cap the whole live set folds into the table-wide
   * watermark at the highest stamp involved — every folded prefix keeps its
   * protection — and the capacity is reclaimed, so the next distinct prefix is
   * scoped again instead of folding too (the formerly irreversible saturation).
   */
  @Test
  void recordPrefixClear_pastTheCap_foldsTheMaxStampAndReclaimsCapacity() {
    // Real-clock stamps: saturation only folds when NO watermark has aged past
    // the tombstone TTL (the age-based eviction is the first escape). Stamps
    // 1..65 decode to EPOCH — long expired — so the fresh-clock block below is
    // what reaches the fold.
    MonotonicStampClock clock = new MonotonicStampClock(1, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long base = clock.nextStamps(65);
    for (int i = 0; i < 64; i++) {
      entryLifecycle.recordPrefixClear("p" + i + " ", base + i);
    }

    // The 65th distinct prefix triggers the fold.
    entryLifecycle.recordPrefixClear("p64 ", base + 64);
    long foldStamp = base + 64;

    assertThat(entryLifecycle.staleAgainstInvalidation("p0 k", foldStamp)).as("folded prefix keeps protection").isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("p0 k", foldStamp - 1))
      .as("folded prefix keeps protection")
      .isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("p0 k", foldStamp + 1)).as("post-fold writes admitted").isFalse();

    // Capacity is reclaimed: a fresh prefix is recorded scoped, not folded.
    long freshStamp = clock.nextStamp();
    entryLifecycle.recordPrefixClear("fresh ", freshStamp);
    assertThat(entryLifecycle.staleAgainstInvalidation("p0 k", foldStamp + 1))
      .as("no second fold: the fresh prefix is recorded scoped, not folded")
      .isFalse();
    assertThat(entryLifecycle.staleAgainstInvalidation("fresh k", freshStamp)).as("scoped refusal").isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("other k", freshStamp))
      .as("unrelated namespace admitted")
      .isFalse();
  }

  /**
   * The age escape: watermarks whose stamp has aged past the tombstone TTL are
   * evicted instead of folded, so a slow drip of namespace clears never
   * advances the table-wide watermark and unrelated namespaces keep their own
   * scoped protection.
   */
  @Test
  void recordPrefixClear_pastTheCap_evictsExpiredInsteadOfFolding() {
    // Synthetic stamps 1..64 decode far older than any tombstone TTL: the 65th
    // distinct prefix evicts them all (expired) and records itself scoped —
    // the table-wide watermark must stay untouched.
    for (int i = 0; i < 64; i++) {
      entryLifecycle.recordPrefixClear("old" + i + " ", i + 1L);
    }
    entryLifecycle.recordPrefixClear("new ", 4096L);

    // No fold: the table-wide watermark stays untouched; the fresh one still refuses its namespace.
    assertThat(entryLifecycle.staleAgainstInvalidation("new k", 4096L)).isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("new k", 4097L)).isFalse();
  }

  /**
   * Replacing an existing prefix's stamp never folds, even at the cap: the
   * prefix add does not grow, so the scoped map survives unchanged.
   */
  @Test
  void recordPrefixClear_replacingAtTheCap_doesNotFold() {
    for (int i = 0; i < 64; i++) {
      entryLifecycle.recordPrefixClear("p" + i + "\u0000", 100L);
    }

    entryLifecycle.recordPrefixClear("p0\u0000", 200L);
    entryLifecycle.recordPrefixClear("p0\u0000", 50L);

    assertThat(entryLifecycle.staleAgainstInvalidation("other\u0000k", 199L))
      .as("a replace is not a fold: the table-wide watermark stays untouched")
      .isFalse();
    assertThat(entryLifecycle.staleAgainstInvalidation("p0\u0000k", 200L)).isTrue();
  }

  /**
   * A batch swaps one snapshot: 65 distinct prefixes fold once at the highest
   * stamp involved, and the cap is not consulted per entry.
   */
  @Test
  void recordPrefixClears_pastTheCap_foldsOnceAtTheHighestStamp() {
    MonotonicStampClock clock = new MonotonicStampClock(1, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long base = clock.nextStamps(65);
    Map<String, Long> clears = new LinkedHashMap<>();
    for (int i = 0; i < 65; i++) {
      clears.put("b" + i + "\u0000", base + i);
    }

    entryLifecycle.recordPrefixClears(clears);

    long foldStamp = base + 64;
    assertThat(entryLifecycle.staleAgainstInvalidation("b0\u0000k", foldStamp))
      .as("folded prefix keeps protection")
      .isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("b0\u0000k", foldStamp + 1))
      .as("post-fold writes admitted")
      .isFalse();
    assertThat(entryLifecycle.staleAgainstInvalidation("other\u0000k", foldStamp))
      .as("the fold covers the whole table")
      .isTrue();
  }

  /**
   * The batch keeps the single-prefix semantics: a stamp the table-wide
   * watermark covers is dropped, a surviving prefix refuses only its own keys.
   */
  @Test
  void recordPrefixClears_filtersSubsumedAndKeepsScopedProtection() {
    entryLifecycle.recordClear(100L);
    Map<String, Long> clears = new LinkedHashMap<>();
    clears.put("covered\u0000", 50L);
    clears.put("users\u0000", 150L);

    entryLifecycle.recordPrefixClears(clears);

    assertThat(entryLifecycle.staleAgainstInvalidation("users\u0000k", 150L)).as("scoped refusal").isTrue();
    assertThat(entryLifecycle.staleAgainstInvalidation("users\u0000k", 151L)).as("post-clear fill admitted").isFalse();
    assertThat(entryLifecycle.staleAgainstInvalidation("orders\u0000k", 150L))
      .as("unrelated namespace admitted")
      .isFalse();
    assertThat(entryLifecycle.staleAgainstInvalidation("covered\u0000k", 50L))
      .as("the table watermark covers it")
      .isTrue();
  }

  /**
   * Tombstones expire on the lifecycle's own monotonic clock: advancing
   * {@link TimeSource}'s test offset past the configured TTL must release the
   * key, not leave it pinned for a real tombstone window.
   */
  @Test
  void invalidationTombstone_expiresOnTheInjectedMonotonicClock() {
    try {
      entryLifecycle.recordInvalidation("k", 100L);
      assertThat(entryLifecycle.staleAgainstInvalidation("k", 99L)).isTrue();

      // Past the default 600s tombstone TTL, on the monotonic axis.
      TimeSource.setTimeOffsetForTest(0, 601_000L);

      assertThat(entryLifecycle.staleAgainstInvalidation("k", 99L))
        .as("the tombstone must expire once its TTL elapses on the injected clock")
        .isFalse();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }
}
