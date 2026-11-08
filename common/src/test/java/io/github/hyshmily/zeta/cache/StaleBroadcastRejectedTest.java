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

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.sync.local.DefaultSyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * design decisions: version stamps invariant I2: a broadcast older than the loaded value must not roll
 * the entry back.
 */
class StaleBroadcastRejectedTest {

  @Test
  @DisplayName("a stale REFRESH does not overwrite a newer resident value")
  void staleRefresh_doesNotOverwrite() {
    try (StampTestSupport support = new StampTestSupport()) {
      support.l1.put("k", StampTestSupport.entry("old", 10L));
      TestSyncHandler handler = new TestSyncHandler(support.l1, key -> "fresh", support.lifecycle, List.of(), null);

      handler.handleRefresh(new SyncMessage(1L, "k", SyncType.REFRESH, 5L));

      CacheEntry kept = (CacheEntry) support.l1.getIfPresent("k");
      assertThat(kept.getValue()).isEqualTo("old");
      assertThat(kept.getDataVersion()).isEqualTo(10L);

      // A genuinely newer broadcast still applies.
      handler.handleRefresh(new SyncMessage(2L, "k", SyncType.REFRESH, 15L));
      assertThat(((CacheEntry) support.l1.getIfPresent("k")).getValue()).isEqualTo("fresh");
    }
  }
}
