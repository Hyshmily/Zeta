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
package io.github.hyshmily.zeta.hotkeydetector.heavykeeper;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@code expelledQueue} offer-failure accounting: a full expelled
 * queue silently drops the item (the admission path never blocks on a slow
 * consumer), and BOTH offer sites — admission eviction in
 * {@code admitOrEvict} and the decay drop in {@code decayMembership} — feed
 * the same {@code expelledLogCounter} whose value rate-limits the WARN (one
 * per {@code EXPELLED_LOG_INTERVAL} failures). Before the accounting fix,
 * {@code decayMembership} ignored the offer result, so decay drops were
 * lost with no trace.
 */
class HeavyKeeperExpelledQueueTest {

  /** Reflection seam (same style as CurMeasurementTest): the log-rate counter is private. */
  private static AtomicInteger expelledLogCounter(HeavyKeeper hk) throws Exception {
    Field f = HeavyKeeper.class.getDeclaredField("expelledLogCounter");
    f.setAccessible(true);
    return (AtomicInteger) f.get(hk);
  }

  /**
   * A decay drop into a full queue is accounted (one counter increment per
   * failed offer) and the member is still removed from the TopK set — the
   * accounting must not resurrect or wedge the member.
   */
  @Test
  void decayDrop_withFullQueue_shouldBeAccountedAndMemberRemoved() throws Exception {
    HeavyKeeper hk = new HeavyKeeper(2, 1024, 4, 0.9, 1, 1, 3);
    hk.warm(Map.of("a", 1L, "b", 1L));
    // Fill the single-slot queue so both decay drops below fail to offer.
    assertThat(hk.expelled().offer(new Item("dummy", 1L))).isTrue();

    hk.fading(); // halves 1 → 0: both members decay-drop, both offers fail

    assertThat(hk.estimatedSize()).as("decay-dropped members leave the TopK set").isZero();
    assertThat(expelledLogCounter(hk).get()).as("one accounted failure per dropped member").isEqualTo(2);
  }

  /**
   * The admission-eviction site keeps its existing accounting (regression
   * guard for the shared counter): an eviction into a full queue increments
   * the same counter once.
   */
  @Test
  void admissionEvict_withFullQueue_shouldBeAccounted() throws Exception {
    HeavyKeeper hk = new HeavyKeeper(1, 1024, 4, 0.9, 1, 1, 3);
    assertThat(hk.addDirect("a", 10).isHotKey()).isTrue();
    assertThat(hk.expelled().offer(new Item("dummy", 1L))).isTrue();

    AddResult result = hk.addDirect("b", 20);

    assertThat(result.isHotKey()).isTrue();
    assertThat(result.expelledKey()).isEqualTo("a");
    assertThat(hk.list()).extracting(Item::key).containsExactly("b");
    assertThat(expelledLogCounter(hk).get()).as("eviction offer failure accounted").isEqualTo(1);
  }
}
