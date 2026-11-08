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

import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * design decisions: version stamps invariant behind pre-read stamping: the stamp is allocated before
 * the value read, so a write that lands during the read outranks the load.
 */
class PreReadStampLowerBoundTest {

  @Test
  @DisplayName("a write allocated during the read outranks the load's pre-read stamp")
  void writeDuringRead_outranksTheLoad() {
    try (StampTestSupport support = new StampTestSupport()) {
      List<String> order = new ArrayList<>();

      String served = support.cache.get(
        "k",
        () -> {
          order.add("read");
          // A concurrent write lands while the reader is blocked.
          support.cache.putThrough("k", "written", 0L, 0L);
          return "loaded";
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      );

      assertThat(served).isEqualTo("loaded");
      assertThat(order).containsExactly("read");
      CacheEntry entry = (CacheEntry) support.l1.getIfPresent("k");
      assertThat(entry.getValue()).isEqualTo("written");
      // Load stamp = 1, write stamp = 2: the write is strictly newer.
      assertThat(entry.getDataVersion()).isEqualTo(2L);
      assertThat(support.stamps.get()).isEqualTo(2L);
    }
  }
}
