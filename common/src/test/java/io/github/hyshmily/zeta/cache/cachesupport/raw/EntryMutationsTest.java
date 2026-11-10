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
package io.github.hyshmily.zeta.cache.cachesupport.raw;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the contract every entry-mutating caller relies on: the pre/post images
 * are always the ones the bin lock observed — including an absent key and a
 * no-op — and the mutation still runs on an absent key.
 */
class EntryMutationsTest {

  private Cache<String, CacheEntry> cache;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().build();
  }

  @Test
  @DisplayName("an absent key with a null no-op runs the mutation and reports the absent image")
  void absentKey_runsTheMutation_andReportsTheObservedImage() {
    boolean[] ran = new boolean[1];

    EntryMutations.Applied applied = EntryMutations.apply(cache, "k", existing -> {
      ran[0] = true;
      return existing;
    });

    assertThat(ran[0]).as("the mutation must run even when the key is absent").isTrue();
    assertThat(applied.changed()).isFalse();
    assertThat(applied.previous()).isNull();
    assertThat(applied.current()).as("the observed post-image is what a serving caller reads").isNull();
    assertThat(cache.getIfPresent("k")).isNull();
  }

  @Test
  @DisplayName("a creation reports a null pre-image and the stored post-image")
  void absentKey_creation_reportsBothImages() {
    CacheEntry created = entry("v");

    EntryMutations.Applied applied = EntryMutations.apply(cache, "k", existing -> created);

    assertThat(applied.changed()).isTrue();
    assertThat(applied.previous()).isNull();
    assertThat(applied.current()).isSameAs(created);
    assertThat(cache.getIfPresent("k")).isSameAs(created);
  }

  @Test
  @DisplayName("returning the resident instance reports no change but the resident image")
  void presentKey_unchanged_reportsTheResidentImage() {
    CacheEntry resident = entry("v");
    cache.put("k", resident);

    EntryMutations.Applied applied = EntryMutations.apply(cache, "k", existing -> existing);

    assertThat(applied.changed()).isFalse();
    assertThat(applied.current()).isSameAs(resident);
    assertThat(cache.getIfPresent("k")).isSameAs(resident);
  }

  @Test
  @DisplayName("a rewrite reports the replaced and the stored entries")
  void presentKey_rewrite_reportsBothImages() {
    CacheEntry resident = entry("v1");
    CacheEntry replacement = entry("v2");
    cache.put("k", resident);

    EntryMutations.Applied applied = EntryMutations.apply(cache, "k", existing -> replacement);

    assertThat(applied.changed()).isTrue();
    assertThat(applied.previous()).isSameAs(resident);
    assertThat(applied.current()).isSameAs(replacement);
    assertThat(cache.getIfPresent("k")).isSameAs(replacement);
  }

  @Test
  @DisplayName("a removal reports the removed entry and a null post-image")
  void presentKey_removal_reportsBothImages() {
    CacheEntry resident = entry("v");
    cache.put("k", resident);

    EntryMutations.Applied applied = EntryMutations.apply(cache, "k", existing -> null);

    assertThat(applied.changed()).isTrue();
    assertThat(applied.previous()).isSameAs(resident);
    assertThat(applied.current()).isNull();
    assertThat(cache.getIfPresent("k")).isNull();
  }

  private static CacheEntry entry(String value) {
    return EntryDraft.blank(null)
      .value(value)
      .version(1)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
      .keyState(KeyState.NORMAL)
      .build();
  }
}
