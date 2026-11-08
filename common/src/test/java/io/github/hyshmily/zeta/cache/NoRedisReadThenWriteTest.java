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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * design decisions: version stamps invariant I1: a successful local write is visible to the next local
 * read, without Redis and without waiting for a TTL.
 */
class NoRedisReadThenWriteTest {

  @Test
  @DisplayName("get -> put -> get reads the written value")
  void readThenWrite_thenReadSeesTheWrite() {
    try (StampTestSupport support = new StampTestSupport()) {
      GetPolicy options = GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH);

      assertThat((Object) support.cache.get("k", () -> "v1", options)).isEqualTo("v1");
      support.cache.putThrough("k", "v2", 0L, 0L);

      assertThat((Object) support.cache.get("k", () -> "ignored", options)).isEqualTo("v2");
      CacheEntry entry = (CacheEntry) support.l1.getIfPresent("k");
      assertThat(entry.getValue()).isEqualTo("v2");
      assertThat(entry.getDataVersion()).isPositive();
    }
  }
}
