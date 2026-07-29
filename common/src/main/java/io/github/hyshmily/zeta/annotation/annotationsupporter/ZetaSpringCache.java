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
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.CachePolicy;
import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractValueAdaptingCache;

/**
 * Spring {@link Cache} adapter that wraps the HotKey {@link Zeta} facade behind the
 * standard Spring caching abstraction.
 *
 * <p>This class is the <b>storage-policy enforcement point</b> (the <em>how</em>
 * layer) of the annotation integration: it reads the per-invocation
 * {@link CachePolicy} from {@link ZetaCacheContext} and translates it into the
 * appropriate {@link Zeta} facade calls. It deliberately does <b>not</b> decide
 * whether the intercepted method runs — that is the aspect's job.
 *
 * <p>All reads route through the soft-expire entry point, which transparently
 * degrades to plain {@code computeIfAbsent} when soft-expire is globally
 * disabled. Soft-expire is therefore governed solely by global configuration,
 * never by the presence of a TTL override.
 */
@Internal
public class ZetaSpringCache extends AbstractValueAdaptingCache {

  private final String name;
  private final Zeta zeta;
  private final ZetaProperties properties;

  public ZetaSpringCache(String name, Zeta zeta, ZetaProperties properties, boolean allowNullValues) {
    super(allowNullValues);
    this.name = name;
    this.zeta = zeta;
    this.properties = properties;
  }

  private String prefixedKey(Object key) {
    Objects.requireNonNull(key, "Cache key must not be null");
    return name + properties.getSpringCache().getKeySeparator() + key;
  }

  @Override
  @NonNull
  public String getName() {
    return name;
  }

  @Override
  @NonNull
  public Object getNativeCache() {
    return zeta;
  }

  @Override
  @Nullable
  public Object lookup(@NonNull Object key) {
    String prefixed = prefixedKey(key);
    Object value = zeta.peek(prefixed).orElse(null);
    if (value != null) {
      return value;
    }
    var localCache = zeta.getLocalCache();
    if (localCache != null) {
      Object raw = localCache.getIfPresent(prefixed);
      if (raw instanceof CacheEntry entry && entry.getValue() == NullValue.INSTANCE) {
        return org.springframework.cache.support.NullValue.INSTANCE;
      }
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Single entry point for {@code @Cacheable} reads: the resolved
   * {@link CachePolicy} (TTL overrides, null-caching decision) flows into the
   * atomic soft-expire compute path. TTL suppliers are evaluated lazily, so
   * SpEL expressions cost nothing on a plain cache hit. A cached
   * {@code null} (sentinel) surfaces as {@code null} without re-invoking the
   * value loader.
   */
  @Override
  @Nullable
  @SuppressWarnings("all")
  public <T> T get(@NonNull Object key, @NonNull Callable<T> valueLoader) {
    String prefixed = prefixedKey(key);
    CachePolicy policy = ZetaCacheContext.get().current();

    Supplier<Object> loader = () -> {
      try {
        return valueLoader.call();
      } catch (Exception e) {
        throw new Cache.ValueRetrievalException(key, valueLoader, e);
      }
    };

    CachePolicy fullPolicy = new CachePolicy(
      policy.hardTtlMs(), policy.softTtlMs(),
      policy.nullCaching(), policy.skipBroadcast(),
      policy.stalePolicy(), loader, policy.reportEnabled()
    );
    return zeta.computeIfAbsentWithSoftExpire(prefixed, fullPolicy)
      .map(v -> (T) fromStoreValue(v))
      .orElse(null);
  }

  @Override
  public void put(@NonNull Object key, @Nullable Object value) {
    String prefixed = prefixedKey(key);
    Object storeValue = toStoreValue(value);

    boolean skipBroadcast = ZetaCacheContext.get().current().skipBroadcast();
    if (skipBroadcast) {
      zeta.putLocal(prefixed, storeValue);
    } else {
      zeta.putThrough(prefixed, storeValue, () -> {});
    }
  }

  @Override
  public void evict(@NonNull Object key) {
    String prefixed = prefixedKey(key);
    boolean skip = ZetaCacheContext.get().current().skipBroadcast();
    if (skip) {
      zeta.invalidate(prefixed, false);
    } else {
      zeta.invalidate(prefixed);
    }
  }

  @Override
  public void clear() {
    zeta.invalidateAllLocal();
  }
}
