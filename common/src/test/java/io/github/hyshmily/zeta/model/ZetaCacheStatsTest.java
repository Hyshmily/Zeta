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

class ZetaCacheStatsTest {

  @Test
  void hitRate_shouldBeDerivedFromCounters() {
    ZetaCacheStats stats = new ZetaCacheStats(3L, 1L, 7L, 42L);
    assertThat(stats.hitRate()).isEqualTo(0.75d);
  }

  @Test
  void hitRate_shouldBeZeroWithoutRequests() {
    assertThat(new ZetaCacheStats(0L, 0L, 0L, 0L).hitRate()).isZero();
  }

  @Test
  void canonicalConstructor_shouldIgnoreAnInconsistentSuppliedRate() {
    ZetaCacheStats stats = new ZetaCacheStats(3L, 1L, 0.1d, 7L, 42L);
    assertThat(stats.hitRate()).isEqualTo(0.75d);
  }
}
