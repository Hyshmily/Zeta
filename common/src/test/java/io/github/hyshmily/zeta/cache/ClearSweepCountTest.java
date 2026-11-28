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
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.sync.local.SyncEvent;
import io.github.hyshmily.zeta.sync.local.SyncHook;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The clear sweep reports the entries it took away. A resident past its hard deadline is swept
 * (and its renewal cancelled) but was already unservable, so counting it would inflate
 * {@code afterClear} by every expired entry the walk passed; a resident newer than the clear stamp
 * is left alone and is not counted either.
 */
class ClearSweepCountTest {

  @Test
  @DisplayName("afterClear counts the live removals only")
  void clearAll_countsLiveRemovalsOnly() {
    try (StampTestSupport support = new StampTestSupport()) {
      support.l1.put("live", StampTestSupport.entry("v", 1L));
      support.l1.put("expired", expiredEntry("v", 1L));
      support.l1.put("newer", StampTestSupport.entry("v", 90L));

      List<Integer> affected = new ArrayList<>();
      SyncHook hook = new SyncHook() {
        @Override
        public void afterClear(List<SyncEvent> clears, int affectedEntries) {
          affected.add(affectedEntries);
        }
      };
      TestSync handler = new TestSync(support.l1, null, support.lifecycle, List.of(hook), null);

      handler.handleClearAll(50L);

      assertThat(affected).containsExactly(1);
      assertThat(support.l1.asMap()).containsOnlyKeys("newer");
    }
  }

  /** A NORMAL entry whose hard deadline has already passed on the monotonic axis. */
  private static CacheEntry expiredEntry(String value, long dataVersion) {
    return EntryDraft.blank(null)
      .value(value)
      .version(dataVersion)
      .ttl(1_000L, 0L)
      .expiryAt(TimeSource.monotonicMillis() - 1L, Long.MAX_VALUE)
      .build();
  }
}
