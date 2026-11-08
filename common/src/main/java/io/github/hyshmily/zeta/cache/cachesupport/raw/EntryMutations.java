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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.util.Internal;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.Nullable;

/**
 * The atomic entry-mutation primitive: one {@code compute} on the L1 bin, with
 * the pre- and post-images reported to the caller when the store actually
 * changed.
 *
 * <p>Every read-modify-write of an entry has the same shape — decide from the
 * resident snapshot, rebuild or remove it, and report whether this call
 * applied. This class owns that pattern once and reports the outcome by
 * reference identity instead: returning the input instance means "no change", a
 * new instance means "rewritten", {@code null} means "removed".
 *
 * <p>The mutation runs whether or not an entry is present, so a mutation that
 * records lifecycle state (an invalidation tombstone) still runs on an absent
 * key.
 */
@Internal
public final class EntryMutations {

  private EntryMutations() {}

  /**
   * Applies one copy-on-write mutation to the key's resident entry.
   *
   * @param cache    the L1 table
   * @param key      the key to mutate
   * @param mutation receives the current entry ({@code null} when absent) and
   *                 returns the entry to store: the same instance for "no
   *                 change", a new instance for a rewrite, or {@code null} for
   *                 a removal
   * @return the applied pre/post-images, or {@code null} when this call left
   *         the store unchanged
   */
  @Nullable
  public static Applied apply(Cache<String, CacheEntry> cache, String key, UnaryOperator<CacheEntry> mutation) {
    Applied[] applied = new Applied[1];
    cache
      .asMap()
      .compute(key, (k, existing) -> {
        CacheEntry next = mutation.apply(existing);
        if (next != existing) {
          applied[0] = new Applied(existing, next);
        }
        return next;
      });
    return applied[0];
  }

  /**
   * Applies one copy-on-write mutation without reporting the pre/post-images — the
   * load-fill path, which never inspects them.
   *
   * @param cache    the L1 table
   * @param key      the key to mutate
   * @param mutation receives the current entry ({@code null} when absent) and
   *                 returns the entry to store
   */
  public static void mutate(Cache<String, CacheEntry> cache, String key, UnaryOperator<CacheEntry> mutation) {
    cache.asMap().compute(key, (k, existing) -> mutation.apply(existing));
  }

  /**
   * The pre- and post-images of one applied mutation.
   *
   * @param previous the entry the mutation replaced, or {@code null} when it
   *                 created one
   * @param current  the entry now stored, or {@code null} when the mutation
   *                 removed the key
   */
  public record Applied(@Nullable CacheEntry previous, @Nullable CacheEntry current) {}
}
