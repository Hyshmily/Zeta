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
package io.github.hyshmily.zeta.cache.loader;

import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.springframework.util.Assert;

/**
 * Registry that binds key prefixes to {@link ZetaLoadingSpec}s, enabling the
 * Caffeine {@code LoadingCache} style of use: register a loader once, then
 * read via {@code Zeta.get(cacheKey)} without passing a reader at every call
 * site.
 *
 * <p>Lookups use <b>longest-prefix match</b>: for key {@code "user:profile:42"}
 * with prefixes {@code "user:"} and {@code "user:profile:"} registered, the
 * more specific {@code "user:profile:"} spec wins. The Spring Cache namespace
 * ({@code cacheName + "::" + key}) is prefix-compatible, so a spec registered
 * under {@code "users::"} covers that cache's keys.
 *
 * <p><b>Thread safety:</b> registrations may happen at runtime; {@link #match}
 * is lock-free (a {@link ConcurrentSkipListMap} underneath).
 *
 * <p>An empty registry is a no-op: every consumer falls back to its previous
 * behaviour (the Redis value channel), so adopting the registry is opt-in and
 * zero-impact for existing deployments.
 */
public class ZetaLoaderRegistry {

  private final ConcurrentNavigableMap<String, ZetaLoadingSpec<?>> specs = new ConcurrentSkipListMap<>();

  /**
   * Register (or replace) the spec for a key prefix.
   *
   * @param keyPrefix the key prefix, e.g. {@code "user:"}; must not be empty
   * @param spec      the loading spec to apply to matching keys; never {@code null}
   */
  public void register(String keyPrefix, ZetaLoadingSpec<?> spec) {
    Assert.hasText(keyPrefix, "keyPrefix must not be empty");
    Objects.requireNonNull(spec, "spec must not be null");
    specs.put(keyPrefix, spec);
  }

  /**
   * Remove the spec registered for a key prefix (no-op when absent).
   *
   * @param keyPrefix the prefix to remove
   */
  public void unregister(String keyPrefix) {
    if (keyPrefix != null) {
      specs.remove(keyPrefix);
    }
  }

  /** Remove all registrations. */
  public void clear() {
    specs.clear();
  }

  /** @return the number of registered prefixes */
  public int size() {
    return specs.size();
  }

  /** @return {@code true} when nothing is registered (all consumers fall back) */
  public boolean isEmpty() {
    return specs.isEmpty();
  }

  /**
   * Find the spec for a cache key via longest-prefix match.
   *
   * <p>Fast path: a single {@code floorEntry} probe resolves the common case
   * (the greatest registered key at or before the cache key is its matching
   * prefix). Fallback: a descending walk over the key's own prefixes covers the
   * rare case where a non-prefix registration sorts between the matching
   * prefix and the key (e.g. prefixes {@code "ab"}, {@code "abZ"} and key
   * {@code "aba"}).
   *
   * <p><b>Call-chain contract:</b> this method is invoked in exactly two places —
   * {@code Zeta#requireRegisteredSpec} (application read path, needs the full
   * spec: loader + TTL + semantics) and {@link PrefixRoutedLoader#load} (cluster
   * path, needs the value only). New value-fetching paths must reuse one of the
   * two, never roll their own lookup.
   *
   * @param cacheKey the cache key; never {@code null}
   * @return the winning spec, or {@code null} when no registered prefix matches
   */
  @Nullable
  public ZetaLoadingSpec<?> match(String cacheKey) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    if (specs.isEmpty()) {
      return null;
    }
    Map.Entry<String, ZetaLoadingSpec<?>> floor = specs.floorEntry(cacheKey);
    if (floor != null && cacheKey.startsWith(floor.getKey())) {
      return floor.getValue();
    }
    for (int i = cacheKey.length(); i > 0; i--) {
      ZetaLoadingSpec<?> spec = specs.get(cacheKey.substring(0, i));
      if (spec != null) {
        return spec;
      }
    }
    return null;
  }
}
