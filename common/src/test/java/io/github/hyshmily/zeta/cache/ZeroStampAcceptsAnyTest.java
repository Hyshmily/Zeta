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
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * design decisions: version stamps invariant I4: an unknown resident stamp ({@code 0}) makes the
 * weakest claim and accepts any real stamp.
 */
class ZeroStampAcceptsAnyTest {

  @Test
  @DisplayName("an entry stamped 0 accepts the next real broadcast")
  void unknownEntry_acceptsAnyRealStamp() {
    try (StampTestSupport support = new StampTestSupport()) {
      support.l1.put("k", StampTestSupport.entry("old", 0L));
      TestSync handler = new TestSync(support.l1, key -> "fresh", support.lifecycle, List.of(), null);

      handler.handleRefresh(new SyncMessage(1L, "k", SyncType.REFRESH, 7L));

      CacheEntry updated = (CacheEntry) support.l1.getIfPresent("k");
      assertThat(updated.getValue()).isEqualTo("fresh");
      assertThat(updated.getDataVersion()).isEqualTo(7L);
    }
  }
}
