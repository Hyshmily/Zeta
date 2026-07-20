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
package io.github.hyshmily.zeta.annotation.annotationsupporter;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jspecify.annotations.NonNull;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

/**
 * Spring {@link CacheManager} implementation that lazily creates
 * {@link ZetaSpringCache} instances for each cache name requested by
 * {@code @Cacheable} / {@code @CachePut} / {@code @CacheEvict} annotations.
 *
 * <p>Caches are created on first access via {@link #getCache(String)} using
 * double-checked locking (ConcurrentHashMap read, then {@code synchronized}
 * create method, then re-check). This avoids redundant creation under
 * concurrent access while keeping the common read-fast path lock-free.
 *
 * <p>The {@link #getMissingCache(String)} method is {@code protected} so that
 * subclasses may override it to customise cache creation, following the
 * standard Spring extensibility pattern.
 *
 * @see ZetaSpringCache
 * @see org.springframework.cache.CacheManager
 */
@Internal
public class ZetaCacheManager implements CacheManager {

  private final ConcurrentMap<String, Cache> cacheMap = new ConcurrentHashMap<>();
  private final Zeta zeta;
  private final ZetaProperties properties;

  /**
   * Create a new {@code ZetaCacheManager}.
   *
   * @param zeta     the HotKey facade
   * @param properties the HotKey configuration properties
   */
  public ZetaCacheManager(Zeta zeta, ZetaProperties properties) {
    this.zeta = zeta;
    this.properties = properties;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns the existing cache for the given name, or lazily creates and
   * registers one via {@link #getMissingCache(String)}. Creation is thread-safe
   * using double-checked locking.
   *
   * @param name the cache name
   * @return the cache instance, or {@code null} if creation fails
   */
  @Override
  public Cache getCache(@NonNull String name) {
    Cache cache = cacheMap.get(name);
    if (cache != null) {
      return cache;
    }
    return createAndRegisterCache(name);
  }

  private synchronized Cache createAndRegisterCache(String name) {
    Cache existing = cacheMap.get(name);
    if (existing != null) {
      return existing;
    }

    Cache cache = getMissingCache(name);
    if (cache != null) {
      cacheMap.put(name, cache);
    }
    return cache;
  }

  /**
   * Create a new {@link ZetaSpringCache} for the given name. This method
   * is {@code protected} so that subclasses may override it to customize
   * cache creation.
   *
   * @param name the cache name
   * @return a new {@link ZetaSpringCache} that allows null values
   */
  public Cache getMissingCache(String name) {
    return new ZetaSpringCache(name, zeta, properties, true);
  }

  /**
   * {@inheritDoc}
   *
   * @return an unmodifiable view of the registered cache names
   */
  @Override
  @NonNull
  public Collection<String> getCacheNames() {
    return Collections.unmodifiableSet(cacheMap.keySet());
  }
}
