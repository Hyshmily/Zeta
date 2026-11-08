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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.LocalPromotion;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sharding.HealthView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The effective heat state and the Worker decision lease are separate axes. */
class LocalPromotionAuthorityTest {

  private Cache<String, CacheEntry> cache;
  private HotKeyDetector detector;
  private HealthView healthView;
  private EntryLifecycle lifecycle;
  private LocalPromotion promotion;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    detector = mock(HotKeyDetector.class);
    healthView = mock(HealthView.class);
    when(detector.contains(anyString())).thenReturn(true);
    lifecycle = new EntryLifecycleImpl(cache, new ZetaProperties(), healthView);
    promotion = new LocalPromotion(cache, detector, lifecycle, healthView);
  }

  @Test
  void localPromotionKeepsCoolLeaseButDoesNotBecomeWorkerManaged() {
    when(healthView.isClusterHealthy()).thenReturn(false);
    CacheEntry cool = coolEntry();
    cache.put("k", cool);

    promotion.onAdmitted("k");
    assertThat(promotion.promoteAdmitted()).isEqualTo(1);

    CacheEntry promoted = cache.getIfPresent("k");
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(promoted.isWorkerManaged()).isFalse();
    assertThat(promoted.isLocalOverrideHot()).isTrue();
    assertThat(promoted.decision()).isNotNull();
    assertThat(promoted.decision().isCool()).isTrue();
  }

  @Test
  void recoveryReassertsTheRetainedCoolLease() {
    when(healthView.isClusterHealthy()).thenReturn(false);
    cache.put("k", coolEntry());
    promotion.onAdmitted("k");
    promotion.promoteAdmitted();

    when(healthView.isClusterHealthy()).thenReturn(true);
    when(healthView.isCurrentIncarnation("w1", 1L)).thenReturn(true);
    CacheEntry promoted = cache.getIfPresent("k");

    promotion.reassertIfNeeded("k", promoted);

    CacheEntry reasserted = cache.getIfPresent("k");
    assertThat(reasserted.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(reasserted.isWorkerManaged()).isTrue();
    assertThat(reasserted.isLocalOverrideHot()).isFalse();
  }

  @Test
  void normalEntryWithCoolLeaseIsPromotableOnlyWhileDegraded() {
    CacheEntry normal = lifecycle.editEntry(coolEntry()).keyState(KeyState.NORMAL).build();

    when(healthView.isClusterHealthy()).thenReturn(false);
    assertThat(promotion.isPromotable("k", normal)).isTrue();

    when(healthView.isClusterHealthy()).thenReturn(true);
    assertThat(promotion.isPromotable("k", normal)).isFalse();
  }

  @Test
  void recoveryDropsADeadWorkersLeaseAndKeepsLocalHot() {
    when(healthView.isClusterHealthy()).thenReturn(false);
    cache.put("k", coolEntry());
    promotion.onAdmitted("k");
    promotion.promoteAdmitted();

    when(healthView.isClusterHealthy()).thenReturn(true);
    when(healthView.isCurrentIncarnation("w1", 1L)).thenReturn(false);
    promotion.reassertIfNeeded("k", cache.getIfPresent("k"));

    CacheEntry after = cache.getIfPresent("k");
    assertThat(after.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(after.decision()).isNull();
    assertThat(after.isWorkerManaged()).isFalse();
  }

  @Test
  void normalEntryWithCoolLeaseIsNotPromotedWhileHealthy() {
    when(healthView.isClusterHealthy()).thenReturn(true);
    CacheEntry normal = lifecycle.editEntry(coolEntry()).keyState(KeyState.NORMAL).build();
    cache.put("k", normal);

    promotion.onAdmitted("k");
    assertThat(promotion.promoteAdmitted()).isZero();
    assertThat(cache.getIfPresent("k").getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void localHotWithoutALeaseIsNotWorkerManaged() {
    when(healthView.isClusterHealthy()).thenReturn(true);
    cache.put(
      "k",
      lifecycle.newEntry().value("v").version(1).ttl(60_000, 15_000, 60_000, 15_000).keyState(KeyState.NORMAL).build()
    );

    promotion.onAdmitted("k");
    assertThat(promotion.promoteAdmitted()).isEqualTo(1);

    CacheEntry promoted = cache.getIfPresent("k");
    assertThat(promoted.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(promoted.decision()).isNull();
    assertThat(promoted.isWorkerManaged()).isFalse();
    assertThat(promoted.isLocalOverrideHot()).isFalse();
  }

  private CacheEntry coolEntry() {
    return lifecycle
      .newEntry()
      .value("v")
      .version(1)
      .decision(DecisionStamp.cool(7, "w1", 1L))
      .ttl(60_000, 15_000, 60_000, 15_000)
      .keyState(KeyState.COOL)
      .build();
  }
}
