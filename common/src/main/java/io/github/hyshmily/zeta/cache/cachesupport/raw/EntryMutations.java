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
 * The atomic entry-mutation primitive: one {@code compute} on the L1 bin,
 * reporting the pre- and post-images that compute observed.
 *
 * <p>Every read-modify-write of an entry has the same shape — decide from the
 * resident snapshot, rebuild or remove it, and report whether this call
 * applied. This class owns that pattern once and derives the outcome by
 * reference identity instead: the input instance as the post-image means "no
 * change", a new instance means "rewritten", {@code null} means "removed".
 *
 * <p>The mutation runs whether or not an entry is present, so a mutation that
 * records lifecycle state (an invalidation tombstone) still runs on an absent
 * key.
 */
@Internal
public final class EntryMutations {

  private EntryMutations() {}

  /**
   * Applies one copy-on-write mutation to the key's resident entry and reports
   * the images the bin lock actually observed.
   *
   * <p>The report is unconditional: an absent key, a concurrent removal and a
   * no-op rewrite all answer with the compute's own pre/post pair, so a caller
   * that serves what it reconciled never has to fall back to the snapshot it
   * read before taking the lock.
   *
   * @param cache    the L1 table
   * @param key      the key to mutate
   * @param mutation receives the current entry ({@code null} when absent) and
   *                 returns the entry to store: the same instance for "no
   *                 change", a new instance for a rewrite, or {@code null} for
   *                 a removal
   * @return the observed pre/post-images; {@link Applied#changed()} tells
   *         whether this call rewrote or removed the entry
   */
  public static Applied apply(Cache<String, CacheEntry> cache, String key, UnaryOperator<CacheEntry> mutation) {
    Applied[] applied = new Applied[1];
    cache
      .asMap()
      .compute(key, (k, existing) -> {
        CacheEntry next = mutation.apply(existing);
        applied[0] = new Applied(existing, next);
        return next;
      });
    return applied[0];
  }

  /**
   * The no-op report for a caller that decided before the bin lock: the same
   * entry is both images.
   *
   * @param entry the resident the caller observed, or {@code null}
   * @return an unchanged report
   */
  public static Applied unchanged(@Nullable CacheEntry entry) {
    return new Applied(entry, entry);
  }

  /**
   * Applies one copy-on-write mutation and returns the post-image the bin lock
   * observed — the load-fill and write-through paths, which use the returned
   * entry (or its version) instead of a pre/post pair.
   *
   * @param cache    the L1 table
   * @param key      the key to mutate
   * @param mutation receives the current entry ({@code null} when absent) and
   *                 returns the entry to store: the same instance for "no
   *                 change", a new instance for a rewrite, or {@code null} for
   *                 a removal
   * @return the entry now stored, or {@code null} when the key is absent
   */
  public static @Nullable CacheEntry mutate(
    Cache<String, CacheEntry> cache,
    String key,
    UnaryOperator<CacheEntry> mutation
  ) {
    return cache.asMap().compute(key, (k, existing) -> mutation.apply(existing));
  }

  /**
   * The pre- and post-images of one mutation, captured inside the bin lock.
   *
   * @param previous the entry the mutation replaced, or {@code null} when the
   *                 key was absent
   * @param current  the entry now stored, or {@code null} when the key is
   *                 absent
   */
  public record Applied(@Nullable CacheEntry previous, @Nullable CacheEntry current) {
    /**
     * Whether this call rewrote, created or removed the entry.
     *
     * @return {@code true} when the post-image differs from the pre-image
     */
    public boolean changed() {
      return previous != current;
    }
  }
}
