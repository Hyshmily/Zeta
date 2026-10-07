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
import io.github.hyshmily.zeta.cache.cachesupport.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.impl.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the entry lifecycle factory: {@code newEntry()} draft parameter
 * combinations (decision metadata, computed expiry, packed TTL round-trips).
 */
class EntryLifecycleImplTest {

  private EntryLifecycle entryLifecycle;
  private ZetaProperties ttlConfig;

  @BeforeEach
  void setUp() {
    Cache<String, CacheEntry> caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ttlConfig = new ZetaProperties();
    entryLifecycle = new EntryLifecycleImpl(caffeineCache, ttlConfig, CacheCompressor.NONE, null);
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
      .version(-42)
      .decision(new DecisionStamp(7, "worker-1", 3))
      .ttl(60_000, 30_000, 300_000, 30_000)
      .expiryAt(now + 60_000, now + 30_000)
      .keyState(KeyState.HOT)
      .build();

    assertThat(entry.getValue()).isEqualTo("value");
    assertThat(entry.getDataVersion()).isEqualTo(-42);
    assertThat(entry.isVersionDegraded()).isTrue();
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
      .decision(new DecisionStamp(7, "worker-1", 3))
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
      .decision(new DecisionStamp(7, null, 0))
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
      .decision(new DecisionStamp(7, null, 0))
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

}
