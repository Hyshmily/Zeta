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
package io.github.hyshmily.zeta.cache.cachesupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.EntryDraft;
import io.github.hyshmily.zeta.model.KeyState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the refault distance admission gate (ADR-0079, the
 * {@code mm/workingset.c} port).
 *
 * <p>Coverage follows the ADR's validation list: the cold-key sentinel (a
 * never-evicted key admitted after the clock advanced), SIZE-only clock
 * filtering, distance boundary behavior, the reject loop's self-correction via
 * EXPIRED anchor re-stamps, EXPLICIT evidence clearing, 32-bit lap optimism,
 * collision optimism, shadow-mode counting, the solo-flight clock purity (the
 * gate's own churn never advances the clock), and the table sizing/derivation
 * and capacity fail-fast rules.
 */
class RefaultAdmissionTest {

  private static final String KEY = "refault:key";
  private static final long CAPACITY = 1000;

  private static ZetaProperties.CacheConfig config(ZetaProperties.RefaultAdmissionMode mode) {
    ZetaProperties.CacheConfig cfg = new ZetaProperties.CacheConfig();
    cfg.setMaxSize((int) CAPACITY);
    cfg.setRefaultAdmission(mode);
    return cfg;
  }

  private static ZetaProperties.CacheConfig configWithBits(ZetaProperties.RefaultAdmissionMode mode, int bits) {
    ZetaProperties.CacheConfig cfg = config(mode);
    cfg.setRefaultShadowBits(bits);
    return cfg;
  }

  private static RefaultAdmission onGate() {
    return RefaultAdmission.from(config(ZetaProperties.RefaultAdmissionMode.ON));
  }

  private static int bucketOf(RefaultAdmission gate, String key) {
    int h = key.hashCode();
    return (h ^ (h >>> 16)) & (gate.tableSlots() - 1);
  }

  /**
   * Simulate {@code n} capacity evictions of "other" keys (clock advances,
   * KEY's shadow untouched) — collision-free by construction, so distances are
   * deterministic.
   */
  private static void churn(RefaultAdmission gate, int n) {
    int keyBucket = bucketOf(gate, KEY);
    int emitted = 0;
    for (int i = 0; emitted < n; i++) {
      String other = "other:" + i;
      if (bucketOf(gate, other) != keyBucket) {
        gate.onRemoval(other, RemovalCause.SIZE);
        emitted++;
      }
    }
  }

  @Test
  @DisplayName("cold key admits without evidence, even after the clock advanced")
  void coldKeyAdmitsWithoutEvidence() {
    RefaultAdmission gate = onGate();
    churn(gate, 5000);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.lastDistance()).isEqualTo(-1);
    assertThat(gate.admitCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("SIZE eviction stamps the shadow; distance measures churn since, boundary at capacity")
  void sizeEvictionStampsAndDistanceMeasuresChurn() {
    RefaultAdmission gate = onGate();
    gate.onRemoval(KEY, RemovalCause.SIZE);
    assertThat(gate.evictionClockValue()).isEqualTo(1);

    // Distance 0 (just evicted): trivially would have survived.
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.lastDistance()).isZero();

    // Churn exactly to capacity: dist == capacity → boundary admits.
    churn(gate, (int) CAPACITY);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.lastDistance()).isEqualTo(CAPACITY);

    // One more eviction: dist = capacity + 1 → reject.
    churn(gate, 1);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.REJECT);
    assertThat(gate.lastDistance()).isEqualTo(CAPACITY + 1);
    assertThat(gate.rejectCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("clock advances on SIZE only — EXPIRED/EXPLICIT/REPLACED never distort distances")
  void clockAdvancesOnSizeOnly() {
    RefaultAdmission gate = onGate();
    gate.onRemoval(KEY, RemovalCause.SIZE);
    long clock = gate.evictionClockValue();
    gate.onRemoval("a", RemovalCause.EXPIRED);
    gate.onRemoval("b", RemovalCause.EXPIRED);
    gate.onRemoval("c", RemovalCause.EXPLICIT);
    gate.onRemoval("d", RemovalCause.REPLACED);
    gate.onRemoval("e", RemovalCause.COLLECTED);
    assertThat(gate.evictionClockValue()).isEqualTo(clock);
  }

  @Test
  @DisplayName("reject loop self-corrects: EXPIRED re-stamps the anchor, flood stop re-admits")
  void rejectLoopSelfCorrectsViaExpiredRestamp() {
    RefaultAdmission gate = onGate();
    gate.onRemoval(KEY, RemovalCause.SIZE);
    churn(gate, (int) CAPACITY + 1);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.REJECT);

    // The solo-flight entry the caller would have stored ends its (short) TTL:
    // the EXPIRED stamp re-anchors the shadow at the current clock WITHOUT
    // advancing it — the kernel's re-eviction of an inactive page.
    gate.onRemoval(KEY, RemovalCause.EXPIRED);
    assertThat(gate.evictionClockValue()).isEqualTo(CAPACITY + 2);

    // Flood stopped (no further churn): distance measured from the cycle end
    // is 0 → the key re-admits. Sustained pressure would keep re-rejecting.
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.lastDistance()).isZero();

    // Flood resumes: fresh churn beyond capacity re-rejects.
    churn(gate, (int) CAPACITY + 1);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.REJECT);
  }

  @Test
  @DisplayName("EXPLICIT invalidation clears evidence — freshly written data re-admits as cold")
  void explicitInvalidationClearsEvidence() {
    RefaultAdmission gate = onGate();
    gate.onRemoval(KEY, RemovalCause.SIZE);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.lastDistance()).isZero();

    gate.onRemoval(KEY, RemovalCause.EXPLICIT);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.lastDistance()).isEqualTo(-1);
  }

  @Test
  @DisplayName("32-bit lap reads as a small distance — optimistic, per the kernel's own analysis")
  void lapWrapIsOptimistic() {
    RefaultAdmission gate = onGate();
    // Stamp the shadow 15 ticks below the 32-bit ceiling.
    gate.forceEvictionClockForTest(0xFFFFFFF0L);
    gate.onRemoval(KEY, RemovalCause.SIZE);
    assertThat(gate.evictionClockValue()).isEqualTo(0xFFFFFFF1L);

    // Lap: the clock wraps and runs 0x20 past zero — a naive signed diff would
    // go hugely negative; the masked subtraction yields the true small churn
    // (0x100000020 − 0xFFFFFFF1 = 0x2F).
    gate.forceEvictionClockForTest(0x1_0000_0020L);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.lastDistance()).isEqualTo(0x2F);
  }

  @Test
  @DisplayName("bucket collision overwrites the shadow with a newer stamp — optimistic admit")
  void collisionIsOptimistic() {
    RefaultAdmission gate = RefaultAdmission.from(configWithBits(ZetaProperties.RefaultAdmissionMode.ON, 8));
    gate.onRemoval(KEY, RemovalCause.SIZE);
    churn(gate, (int) CAPACITY + 1);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.REJECT);

    // Another key hashing into the same 256-slot bucket re-stamps it with a
    // recent clock — KEY's next verdict turns optimistic (small distance).
    // With 4096 trials against 256 slots a collision on KEY's bucket is
    // statistically certain (expected ~256 trials).
    boolean refreshed = false;
    for (int i = 0; i < 4096 && !refreshed; i++) {
      gate.onRemoval("collision:" + i, RemovalCause.SIZE);
      refreshed = gate.admit(KEY) == RefaultAdmission.Decision.ADMIT && gate.lastDistance() >= 0;
    }
    assertThat(refreshed).as("a colliding eviction must eventually refresh KEY's slot").isTrue();
  }

  @Test
  @DisplayName("shadow mode computes and counts but always admits")
  void shadowModeAlwaysAdmits() {
    RefaultAdmission gate = RefaultAdmission.from(config(ZetaProperties.RefaultAdmissionMode.SHADOW));
    assertThat(gate.gating()).isTrue();
    assertThat(gate.enforcing()).isFalse();

    gate.onRemoval(KEY, RemovalCause.SIZE);
    churn(gate, (int) CAPACITY + 1);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.rejectCount()).as("would-reject counted in shadow").isEqualTo(1);
    assertThat(gate.lastDistance()).isEqualTo(CAPACITY + 1);
  }

  @Test
  @DisplayName("off mode is inert: no gating, no table, stray admits are harmless")
  void offModeIsInert() {
    RefaultAdmission gate = RefaultAdmission.from(config(ZetaProperties.RefaultAdmissionMode.OFF));
    assertThat(gate.gating()).isFalse();
    assertThat(gate.enforcing()).isFalse();
    assertThat(gate.tableSlots()).isZero();
    gate.onRemoval(KEY, RemovalCause.SIZE);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.evictionClockValue()).isZero();
    assertThat(gate.admitCount()).isZero();
  }

  @Test
  @DisplayName("table auto-derivation covers 8x capacity; explicit bits win; out-of-range fails fast")
  void tableSizing() {
    // 8 × 1000 = 8000 → ceilLog2 = 13 → 8192 slots, inside the [2^8, 2^18] clamp.
    assertThat(onGate().tableSlots()).isEqualTo(1 << 13);

    // Explicit override wins.
    assertThat(RefaultAdmission.from(configWithBits(ZetaProperties.RefaultAdmissionMode.ON, 10)).tableSlots())
      .isEqualTo(1 << 10);

    // Auto clamp: huge capacity hits the 2^18 ceiling instead of exploding.
    ZetaProperties.CacheConfig huge = config(ZetaProperties.RefaultAdmissionMode.ON);
    huge.setMaxSize(10_000_000);
    assertThat(RefaultAdmission.from(huge).tableSlots()).isEqualTo(1 << 18);

    // Out-of-range explicit bits fail fast at construction (binding time).
    assertThatThrownBy(() -> RefaultAdmission.from(configWithBits(ZetaProperties.RefaultAdmissionMode.ON, 7)))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RefaultAdmission.from(configWithBits(ZetaProperties.RefaultAdmissionMode.ON, 25)))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("weight-mode deployments compare against refault-capacity-entries")
  void weightModeCapacityOverride() {
    ZetaProperties.CacheConfig cfg = config(ZetaProperties.RefaultAdmissionMode.ON);
    cfg.setRefaultCapacityEntries(50);
    RefaultAdmission gate = RefaultAdmission.from(cfg);
    assertThat(gate.capacityEntries()).isEqualTo(50);

    gate.onRemoval(KEY, RemovalCause.SIZE);
    churn(gate, 50);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    churn(gate, 1);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.REJECT);
  }

  @Test
  @DisplayName("clock rate gauge reports per-second eviction rate after two scrapes")
  void clockRateEwma() throws Exception {
    RefaultAdmission gate = onGate();
    assertThat(gate.clockRatePerSec()).isZero();
    churn(gate, 100);
    Thread.sleep(5);
    double rate = gate.clockRatePerSec();
    assertThat(rate).isPositive();
  }

  /** A solo-flight {@code CacheEntry} as the load path stores it on a reject (ADR-0079). */
  private static CacheEntry soloFlightEntry() {
    return EntryDraft.blank(null)
      .value("v")
      .ttl(200, 0, 200, 0)
      .hardExpiryAt(System.currentTimeMillis() + 200)
      .keyState(KeyState.NORMAL)
      .soloFlight(true)
      .build();
  }

  @Test
  @DisplayName("solo-flight SIZE eviction re-stamps the anchor without advancing the clock")
  void soloFlightSizeEvictionStampsWithoutAdvancingClock() {
    RefaultAdmission gate = onGate();
    gate.onRemoval(KEY, RemovalCause.SIZE);
    churn(gate, (int) CAPACITY + 1);
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.REJECT);
    long clockBefore = gate.evictionClockValue();

    // The solo-flight entry stored after that reject is evicted by SIZE (window
    // churn under the scan flood), not by its own TTL. Its anchor re-stamps at
    // the current clock WITHOUT advancing it: counting the gate's own residue
    // would feed the reject rate back into every distance measured against it.
    gate.onRemoval(KEY, soloFlightEntry(), RemovalCause.SIZE);
    assertThat(gate.evictionClockValue()).isEqualTo(clockBefore);

    // Anchor refreshed: with no further true turnover the distance is 0, and the
    // key re-admits exactly like the EXPIRED self-correction path.
    assertThat(gate.admit(KEY)).isEqualTo(RefaultAdmission.Decision.ADMIT);
    assertThat(gate.lastDistance()).isZero();
  }

  @Test
  @DisplayName("sustained solo-flight churn never inflates distances — no reject-rate feedback loop")
  void soloFlightChurnDoesNotInflateDistances() {
    RefaultAdmission gate = onGate();
    gate.onRemoval(KEY, RemovalCause.SIZE);
    long clockAfterFirst = gate.evictionClockValue();
    assertThat(clockAfterFirst).isEqualTo(1);

    // A scan flood rejected wholesale: every reject stores a solo-flight that
    // window churn evicts by SIZE. None of that may advance the clock —
    // otherwise the clock inflates with the reject rate itself and the gate
    // degenerates into rejecting everything it has ever seen leave.
    for (int i = 0; i < 10_000; i++) {
      gate.onRemoval("solo:" + i, soloFlightEntry(), RemovalCause.SIZE);
    }
    assertThat(gate.evictionClockValue()).isEqualTo(clockAfterFirst);
  }

  @Test
  @DisplayName("non-positive capacity fails fast — weight mode without the override must not arm")
  void nonPositiveCapacityFailsFast() {
    // Weight mode with max-size zeroed (it is not the entry bound) and no
    // override: every evidenced distance would exceed a non-positive capacity
    // term and mass-reject — the gate refuses to arm instead.
    ZetaProperties.CacheConfig weightMode = config(ZetaProperties.RefaultAdmissionMode.ON);
    weightMode.setMaxSize(0);
    assertThatThrownBy(() -> RefaultAdmission.from(weightMode))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("refault-capacity-entries");

    // The override rescues the deployment.
    weightMode.setRefaultCapacityEntries(50_000);
    assertThat(RefaultAdmission.from(weightMode).capacityEntries()).isEqualTo(50_000);
  }
}
