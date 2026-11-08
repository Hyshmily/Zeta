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
import org.jspecify.annotations.Nullable;

/**
 * A read-only probe that leaves the cache's statistics and eviction policy untouched.
 *
 * <p>{@code Cache.getIfPresent} is not quiet: with Caffeine's default configuration it
 * records a hit for a caller read (the application's own {@code cache.gets} metrics) and
 * offers the node to the read buffer that feeds the frequency sketch. Internal maintenance
 * probes — a residency check, a stale-entry snapshot, a background-refresh capture — must
 * not make a key look hotter or colder than the traffic did. Caffeine's
 * {@code policy().getIfPresentQuietly} skips both; a replacement {@link Cache} bean may not
 * support {@code policy()}, so the fallback is {@code asMap().get}, which is stats-quiet but
 * still policy-active.
 *
 * <p>The support probe runs once per wrapper, never per lookup.
 *
 * @param <K> the cache key type
 * @param <V> the cache value type
 */
@Internal
public final class QuietCache<K, V> {

  private final Cache<K, V> cache;
  private final boolean quiet;

  /**
   * Wrap a cache, detecting once whether its {@code policy()} view is supported.
   *
   * @param cache the cache to probe
   */
  public QuietCache(Cache<K, V> cache) {
    this.cache = cache;
    this.quiet = supportsPolicy(cache);
  }

  /**
   * Return the value without recording a hit/miss or updating the eviction policy.
   *
   * @param key the key to look up
   * @return the value, or {@code null} when absent or expired
   */
  public @Nullable V getIfPresent(K key) {
    return quiet ? cache.policy().getIfPresentQuietly(key) : cache.asMap().get(key);
  }

  private static boolean supportsPolicy(Cache<?, ?> cache) {
    try {
      cache.policy();
      return true;
    } catch (UnsupportedOperationException e) {
      return false;
    }
  }
}
