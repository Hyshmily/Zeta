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

  @Override
  @Nullable
  @SuppressWarnings("all")
  public <T> T get(@NonNull Object key, @NonNull Callable<T> valueLoader) {
    String prefixed = prefixedKey(key);
    ZetaCacheContext cv = ZetaCacheContext.get();
    boolean skipDetection = cv.isSkipDetection();

    Supplier<Object> loader = () -> {
      try {
        return valueLoader.call();
      } catch (Exception e) {
        throw new Cache.ValueRetrievalException(key, valueLoader, e);
      }
    };

    // Fast path: if value is already cached, return immediately without
    // resolving SpEL TTL. On cache hit the existing entry already carries
    // its own TTL; zero overrides mean "use defaults" in HotKeyCache.
    var localCache = zeta.getLocalCache();
    if (localCache != null) {
      Object raw = localCache.getIfPresent(prefixed);
      if (raw != null) {
        if (raw instanceof CacheEntry entry) {
          if (entry.getValue() == NullValue.INSTANCE) return null;
          // skipDetection: return cached value without loading
          if (skipDetection) {
            return (T) fromStoreValue(entry.getValue());
          }
          // Non-null cached value on regular path: still call computeIfAbsent
          // (for Worker reporting + local promotion) but with 0 TTL override
          // so SpEL is never triggered.
          Object result = zeta.computeIfAbsent(prefixed, loader, 0L, 0L, true);
          return result != null ? (T) fromStoreValue(result) : null;
        }
        // Bare (non-CacheEntry) value
        return (T) fromStoreValue(raw);
      }
    }

    // Cache miss: resolve TTL (may trigger SpEL supplier) then load.
    long hardTtlMs = cv.getHardTtlMs();
    long softTtlMs = cv.getSoftTtlMs();
    boolean hasTtlOverride = hardTtlMs > 0 || softTtlMs > 0;
    boolean allowReport = !skipDetection;

    Object result = hasTtlOverride
      ? zeta.computeIfAbsentWithSoftExpire(prefixed, loader, hardTtlMs, softTtlMs, allowReport)
      : zeta.computeIfAbsent(prefixed, loader, 0L, 0L, allowReport);

    return result != null ? (T) fromStoreValue(result) : null;
  }

  @Override
  public void put(@NonNull Object key, @Nullable Object value) {
    String prefixed = prefixedKey(key);
    Object storeValue = toStoreValue(value);

    ZetaCacheContext cv = ZetaCacheContext.get();
    boolean skipBroadcast = cv.isSkipBroadcast();

    if (skipBroadcast) {
      zeta.putLocal(prefixed, storeValue);
    } else {
      zeta.putThrough(prefixed, storeValue, () -> {});
    }
  }

  @Override
  public void evict(@NonNull Object key) {
    String prefixed = prefixedKey(key);
    ZetaCacheContext cv = ZetaCacheContext.get();
    boolean skip = cv.isSkipBroadcast();
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
