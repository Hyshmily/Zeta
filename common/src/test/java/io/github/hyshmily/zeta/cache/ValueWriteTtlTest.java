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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** One value-write tail for local and peer paths; the active regime owns its hard bound. */
class ValueWriteTtlTest {

  private EntryLifecycle lifecycle;

  @BeforeEach
  void setUp() {
    Cache<String, CacheEntry> cache = Caffeine.newBuilder().maximumSize(100).build();
    lifecycle = new EntryLifecycleImpl(cache, new ZetaProperties(), null);
  }

  @Test
  void elevatedValueWriteKeepsTheHardDeadlineAndRearmsSoft() {
    long hardExpireAt = TimeSource.currentTimeMillis() + 3_600_000L;
    CacheEntry hot = lifecycle
      .newEntry()
      .value("old")
      .version(1)
      .decision(DecisionStamp.hot(7, "w1", 1L))
      .ttl(3_600_000, 300_000, 60_000, 15_000)
      .expiryAt(hardExpireAt, TimeSource.currentTimeMillis() - 1L)
      .keyState(KeyState.HOT)
      .build();

    CacheEntry updated = lifecycle.writeValue(hot, "new", 2, 10_000, 5_000, true);

    assertThat(updated.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(updated.decision()).isSameAs(hot.decision());
    assertThat(updated.getHardExpireAtMs()).isEqualTo(hardExpireAt);
    assertThat(updated.getSoftExpireAtMs()).isGreaterThan(TimeSource.currentTimeMillis());
  }

  @Test
  void normalValueWriteRearmsBothDeadlines() {
    long oldHard = TimeSource.currentTimeMillis() + 10_000L;
    CacheEntry normal = lifecycle
      .newEntry()
      .value("old")
      .version(1)
      .ttl(10_000, 5_000, 10_000, 5_000)
      .expiryAt(oldHard, TimeSource.currentTimeMillis() + 5_000L)
      .keyState(KeyState.NORMAL)
      .build();

    CacheEntry updated = lifecycle.writeValue(normal, "new", 2, 60_000, 30_000, false);

    assertThat(updated.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(updated.getHardExpireAtMs()).isGreaterThan(oldHard);
  }

  @Test
  void permanentNormalBaselineSurvivesCooling() {
    CacheEntry normal = lifecycle.writeValue(null, "value", 1, 0L, 0L, false);
    assertThat(normal.getHardTtlMs()).isEqualTo(Long.MAX_VALUE);
    assertThat(normal.getNormalHardTtlMs()).isEqualTo(Long.MAX_VALUE);

    CacheEntry cooled = lifecycle.cool(normal, DecisionStamp.cool(2, "w1", 1L));

    assertThat(cooled.getHardTtlMs()).isEqualTo(Long.MAX_VALUE);
    assertThat(cooled.getNormalHardTtlMs()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void expiredElevatedResidentIsTreatedAsAbsent() {
    long now = TimeSource.currentTimeMillis();
    CacheEntry expiredHot = lifecycle
      .newEntry()
      .value("dead")
      .version(1)
      .decision(DecisionStamp.hot(7, "w1", 1L))
      .ttl(10_000, 5_000, 10_000, 5_000)
      .expiryAt(now - 1L, now - 1L)
      .keyState(KeyState.HOT)
      .build();

    CacheEntry updated = lifecycle.writeValue(expiredHot, "fresh", 2, 60_000, 30_000, true);

    assertThat(updated.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(updated.decision()).isNull();
    assertThat(updated.getHardExpireAtMs()).isGreaterThan(now);
  }

  /**
   * A permanent normal entry must stay permanent when the local TopK promotes it: the hot TTL is
   * a floor, not a cap, and {@code 0} is the permanent sentinel at this resolved layer.
   */
  @Test
  void permanentNormalEntryStaysPermanentWhenPromoted() {
    ZetaProperties properties = new ZetaProperties();
    properties.setHardTtlMs(0L);
    properties.setHotHardTtlMs(3_600_000L);
    EntryLifecycle permanentLifecycle = new EntryLifecycleImpl(
      Caffeine.newBuilder().maximumSize(100).build(),
      properties,
      null
    );

    CacheEntry promoted = permanentLifecycle.writeValue(null, "value", 1, 0L, 0L, true);

    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(promoted.getHardTtlMs()).isEqualTo(Long.MAX_VALUE);
    assertThat(promoted.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
    assertThat(promoted.getNormalHardTtlMs()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Demoting a HOT entry back to a permanent normal baseline must clear both the duration and the
   * deadline: a finite HOT deadline with a permanent duration is an inconsistent entry.
   */
  @Test
  void demotionToAPermanentNormalBaselineClearsTheFiniteHotDeadline() {
    long now = TimeSource.currentTimeMillis();
    CacheEntry hot = lifecycle
      .newEntry()
      .value("value")
      .version(1)
      .ttl(10_000, 5_000, Long.MAX_VALUE, 0L)
      .expiryAt(now + 10_000L, 0L)
      .keyState(KeyState.HOT)
      .build();

    CacheEntry demoted = lifecycle.demoteToNormal(hot);

    assertThat(demoted.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(demoted.getHardTtlMs()).isEqualTo(Long.MAX_VALUE);
    assertThat(demoted.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
    assertThat(demoted.getNormalHardTtlMs()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * An elevated value write that enables a soft window must store the duration and the deadline
   * together; leaving the duration at {@code 0} while the deadline is positive makes the entry
   * read as disabled but refreshable.
   */
  @Test
  void elevatedValueWriteRearmsItsOwnSoftWindow() {
    CacheEntry hot = lifecycle
      .newEntry()
      .value("old")
      .version(1)
      .ttl(60_000, 2_000, 60_000, 0L)
      .expiryAt(TimeSource.currentTimeMillis() + 60_000L, TimeSource.currentTimeMillis() - 1_000L)
      .keyState(KeyState.HOT)
      .build();

    CacheEntry updated = lifecycle.writeValue(hot, "new", 2, 10_000, 5_000, true);

    assertThat(updated.getSoftTtlMs()).isEqualTo(2_000);
    assertThat(updated.getSoftExpireAtMs()).isGreaterThan(TimeSource.currentTimeMillis());
  }

  @Test
  void elevatedValueWriteDoesNotReEnableADisabledSoftWindow() {
    CacheEntry hot = lifecycle
      .newEntry()
      .value("old")
      .version(1)
      .ttl(60_000, 0L, 60_000, 0L)
      .keyState(KeyState.HOT)
      .build();

    CacheEntry updated = lifecycle.writeValue(hot, "new", 2, 10_000, 5_000, true);

    assertThat(updated.getSoftTtlMs()).isZero();
    assertThat(updated.getSoftExpireAtMs()).isZero();
  }
}
