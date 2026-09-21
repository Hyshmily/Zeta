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

/**
 * Root loader SPI, shared by both value-fetching paths (ADR-0070):
 * <ul>
 *   <li><b>Application read path</b> — {@link ZetaLoadingSpec} pairs an implementation
 *       with per-prefix TTL/semantics; the no-reader {@code Zeta.get(key)} overloads
 *       route a key to its spec via {@link ZetaLoaderRegistry#match} and invoke the
 *       loader through the wrapped policy reader.</li>
 *   <li><b>Cluster sync plane</b> — the {@code hotKeyClusterLoader} bean (a
 *       {@link PrefixRoutedLoader} over a {@link RedisValueLoader}) fetches
 *       authoritative values for Worker HOT warm-up and peer REFRESH.</li>
 * </ul>
 *
 * <p><b>Call-chain contract:</b> {@link ZetaLoaderRegistry#match} is invoked in exactly
 * two places — {@code Zeta#requireRegisteredSpec} (needs the full spec: loader + TTL +
 * semantics) and {@link PrefixRoutedLoader#load} (needs the value only). New
 * value-fetching paths must reuse one of the two, never roll their own lookup.
 *
 * <h3>Contract</h3>
 * <ul>
 *   <li>Implementations must be thread-safe.</li>
 *   <li>Returning {@code null} signals "key absent at the data source" — subject to
 *       the null-caching policy of the consuming {@link ZetaLoadingSpec}.</li>
 *   <li>Throwing an unchecked exception signals a load failure: consuming paths treat
 *       it like any reader failure (swallowed as a miss, or propagated when
 *       {@code failOnError} is set on the spec / cluster handlers treat it as a
 *       skipped load).</li>
 * </ul>
 *
 * <p>Typical registration:
 * <pre>{@code
 * registry.register("user:", ZetaLoadingSpec.<User>builder()
 *     .loader(repo::findById)              // receives the fully qualified cache key
 *     .hardTtl(30 * 60_000L)               // expireAfterWrite equivalent (ms)
 *     .softTtl(5 * 60_000L)                // refreshAfterWrite equivalent (ms)
 *     .build());
 * }</pre>
 *
 * @param <V> the value type produced by this loader
 * @see ZetaLoadingSpec
 * @see ZetaLoaderRegistry
 */
public interface CacheLoader<V> {

  /**
   * Load the value for the given cache key from the application data source.
   *
   * @param cacheKey the fully qualified cache key; never {@code null}
   * @return the loaded value, or {@code null} if the key does not exist
   */
  V load(String cacheKey);
}
