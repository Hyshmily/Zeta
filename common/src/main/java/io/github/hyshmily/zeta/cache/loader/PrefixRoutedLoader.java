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

import io.github.hyshmily.zeta.Internal;
import java.util.Objects;

/**
 * Prefix-routing {@link CacheLoader} for the cluster sync plane: consults the
 * {@link ZetaLoaderRegistry} first and falls back to the Redis value channel.
 * Swapped in for the {@code hotKeyClusterLoader} bean whenever a registry bean
 * exists, so the cluster paths (Worker HOT warm-up via
 * {@code DefaultWorkerDecisionHandler.handleHot}, peer REFRESH via
 * {@code DefaultSyncDecisionHandler.handleRefresh}) can load through the
 * application's registered loaders without any handler changes.
 *
 * <p><b>Call-chain contract:</b> {@link ZetaLoaderRegistry#match} is invoked in
 * exactly two places — {@code Zeta#requireRegisteredSpec} (application read path,
 * needs the full spec) and {@link #load} here (cluster path, needs the value
 * only). This class is the second and last of the two.
 *
 * <p>Values are returned in raw form, exactly like {@link RedisValueLoader}:
 * the consuming handler applies wrapping / compression. Unchecked exceptions
 * from a registered loader propagate to the consumer, whose existing
 * load-failure handling (rate-limited warn, treated as value-not-found) applies
 * unchanged.
 */
@Internal
public class PrefixRoutedLoader implements CacheLoader<Object> {

  private final ZetaLoaderRegistry registry;
  private final CacheLoader<Object> fallback;

  public PrefixRoutedLoader(ZetaLoaderRegistry registry, CacheLoader<Object> fallback) {
    this.registry = Objects.requireNonNull(registry, "registry must not be null");
    this.fallback = Objects.requireNonNull(fallback, "fallback must not be null");
  }

  @Override
  public Object load(String cacheKey) {
    ZetaLoadingSpec<?> spec = registry.match(cacheKey);
    if (spec != null) {
      return spec.loader().load(cacheKey);
    }
    return fallback.load(cacheKey);
  }
}
