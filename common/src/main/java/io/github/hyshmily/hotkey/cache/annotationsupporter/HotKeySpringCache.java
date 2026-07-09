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
package io.github.hyshmily.hotkey.cache.annotationsupporter;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.cache.annotationsupporter.HotKeyCacheContext.ContextValues;
import io.github.hyshmily.hotkey.model.CacheEntry;
import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractValueAdaptingCache;


/**
 * Spring {@link Cache} adapter that wraps the HotKey {@link HotKey} facade behind the
 * standard Spring caching abstraction.
 *
 * <p>All cache keys are prefixed with the cache name and the configured key separator
 * ( , default {@code "::"}) to
 * avoid collisions in the shared Caffeine instance.
 *
 * <p>Delegation:
 * <ul>
 *   <li>{@link #lookup} — uses {@link HotKey#peek} (side-effect-free, no hot-key
 *       detection or reporting)</li>
 *   <li>{@link #get(Object, Callable)} — uses {@link HotKey#get} (triggers hot-key
 *       detection and reporting)</li>
 *   <li>{@link #put} — uses {@link HotKey#putThrough} with a no-op writer (the
 *       mutating method was already executed by the {@code CacheInterceptor})</li>
 *   <li>{@link #evict} — uses {@link HotKey#invalidate}</li>
 *   <li>{@link #clear} — uses {@link HotKey#invalidateAllLocal}</li>
 * </ul>
 *
 * <p>This class is stateless and thread-safe.
 */
@Internal
public class HotKeySpringCache extends AbstractValueAdaptingCache {

  private final String name;
  private final HotKey hotKey;
  private final HotKeyProperties properties;

  /**
   * Create a new {@code HotKeySpringCache}.
   *
   * @param name            the cache name (used for key prefixing)
   * @param hotKey          the HotKey facade
   * @param properties      configuration properties (for key separator)
   * @param allowNullValues whether {@code null} values are permitted
   */
  public HotKeySpringCache(String name, HotKey hotKey, HotKeyProperties properties, boolean allowNullValues) {
    super(allowNullValues);
    this.name = name;
    this.hotKey = hotKey;
    this.properties = properties;
  }

  /**
   * Prefix the given cache key with the cache name to avoid collisions in the
   * shared Caffeine instance.
   *
   * @param key the original cache key
   * @return {@code name + separator + key}
   */
  private String prefixedKey(Object key) {
    Objects.requireNonNull(key, "Cache key must not be null");
    return name + properties.getSpringCache().getKeySeparator() + key;
  }

  /**
   * {@inheritDoc}
   *
   * @return the cache name
   */
  @Override
  @NonNull
  public String getName() {
    return name;
  }

  /**
   * {@inheritDoc}
   *
   * @return the underlying {@link HotKey} facade
   */
  @Override
  @NonNull
  public Object getNativeCache() {
    return hotKey;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Performs a side-effect-free look up via {@link HotKey#peek}. No hot-key
   * detection or reporting is triggered.
   *
   * @param key the cache key
   * @return the cached value, or {@code null} if not present
   */
  @Override
  @Nullable
  protected Object lookup(@NonNull Object key) {
    String prefixed = prefixedKey(key);
    Object value = hotKey.peek(prefixed).orElse(null);
    if (value != null) {
      return value;
    }
    // peek() unwraps HotKey's NullValue sentinel to null → Optional.empty().
    // Check raw cache to distinguish "cached null" from "cache miss".
    var localCache = hotKey.getLocalCache();
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
   * <p>Retrieves the value via {@link HotKey#get}, which triggers hot-key
   * detection and reporting. If the {@code valueLoader} throws, the exception
   * is wrapped in a {@link ValueRetrievalException}.
   *
   * <p>If a TTL override is set in {@link HotKeyCacheContext} for the current
   * thread (e.g., by {@code HotKeyCacheExtensionAspect} from a {@code @HotKeyCacheTTL}
   * annotation), it is passed to {@link HotKey#get(String, Supplier, long, long, boolean)}.
   * Otherwise, the global default TTL is used (hardTtlMs=0).
   *
   * <p>If {@link HotKeyCacheContext#isAllowNull()} returns {@code true} (set by
   * {@code @NullCaching(true)}), a cache miss that produces a {@code null} result
   * is stored via an internal sentinel so that subsequent lookups return
   * {@code null} without invoking the loader again.
   *
   * @param key         the cache key
   * @param valueLoader the callable to load the value on a cache miss
   * @param <T>         the value type
   * @return the cached or newly loaded value
   */
  @Override
  @Nullable
  @SuppressWarnings("unchecked")
  public <T> T get(@NonNull Object key, @NonNull Callable<T> valueLoader) {
    String prefixed = prefixedKey(key);

    // Fast path: avoid redundant loader invocation for cached NullValue sentinel
    // (stored by annotation path via @NullCaching or HotKeyReadQuery).
    var localCache = hotKey.getLocalCache();
    if (localCache != null) {
      Object raw = localCache.getIfPresent(prefixed);
      if (raw instanceof CacheEntry entry && entry.getValue() == NullValue.INSTANCE) {
        return null;
      }
    }

    ContextValues cv = HotKeyCacheContext.get().getValues();
    long hardTtlMs = cv != null ? cv.hardTtlMs() : 0L;
    long softTtlMs = cv != null ? cv.softTtlMs() : 0L;

    Supplier<Object> loader = () -> {
      try {
        return valueLoader.call();
      } catch (Exception e) {
        throw new Cache.ValueRetrievalException(key, valueLoader, e);
      }
    };

    boolean hasTtlOverride = hardTtlMs > 0 || softTtlMs > 0;

    Object result = hasTtlOverride
      ? hotKey.computeIfAbsentWithSoftExpire(prefixed, loader, hardTtlMs, softTtlMs)
      : hotKey.computeIfAbsent(prefixed, loader);

    if (result != null) {
      return (T) fromStoreValue(result);
    }

    return null;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Stores the value via {@link HotKey#putThrough} with a no-op writer,
   * since the mutating method was already executed by the {@code CacheInterceptor}.
   *
   * @param key   the cache key
   * @param value the value to cache (maybe {@code null} if null values are allowed)
   */
  @Override
  public void put(@NonNull Object key, @Nullable Object value) {
    String prefixed = prefixedKey(key);
    Object storeValue = toStoreValue(value);

    ContextValues cv = HotKeyCacheContext.get().getValues();
    boolean skipBroadcast = cv != null && cv.skipBroadcast();

    if (skipBroadcast) {
      hotKey.putLocal(prefixed, storeValue);
    } else {
      hotKey.putThrough(prefixed, storeValue, () -> {});
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Invalidates the entry via {@link HotKey#invalidate}, which broadcasts
   * a sync message to peer instances.
   *
   * @param key the cache key to evict
   */
  @Override
  public void evict(@NonNull Object key) {
    String prefixed = prefixedKey(key);
    ContextValues cv = HotKeyCacheContext.get().getValues();
    if (cv != null && cv.skipBroadcast()) {
      hotKey.invalidate(prefixed, false);
    } else {
      hotKey.invalidate(prefixed);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Invalidates all local entries via {@link HotKey#invalidateAllLocal}.
   * Broadcast is intentionally suppressed: {@code clear()} is a local-only
   * defensive measure.
   */
  @Override
  public void clear() {
    hotKey.invalidateAllLocal();
  }
}
