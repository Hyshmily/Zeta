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
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
 *   <li>{@link #clear} — uses {@link HotKey#invalidateAll}</li>
 * </ul>
 *
 * <p>This class is stateless and thread-safe.
 */
public class HotKeySpringCache extends AbstractValueAdaptingCache {

  private final String name;
  private final HotKey hotKey;
  private final HotKeyProperties properties;

  /**
   * Tracks keys whose value is explicitly {@code null} (via {@code @NullCaching(true)}).
   * Enables a side-effect-free {@link #lookup} to distinguish "found null in cache"
   * from "not in cache" — information that a single {@link Optional} return cannot convey.
   */
  private final Set<String> nullCachedKeys = ConcurrentHashMap.newKeySet();

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
    if (nullCachedKeys.contains(prefixed)) {
      return null; // null value was intentionally cached
    }
    return hotKey.peek(prefixed).orElse(null);
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
   * annotation), it is passed to {@link HotKey#get(String, Supplier, long, long)}.
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

    long hardTtlMs = HotKeyCacheContext.get().getHardTtlMs();
    long softTtlMs = HotKeyCacheContext.get().getSoftTtlMs();
    boolean allowNull = HotKeyCacheContext.get().isAllowNull();

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

    // Cache miss: store NullValue sentinel if null-caching is enabled
    if (allowNull) {
      if (HotKeyCacheContext.get().isSkipBroadcast()) {
        hotKey.putLocal(prefixed, null);
      } else {
        hotKey.putThrough(prefixed, null, () -> {});
      }
      nullCachedKeys.add(prefixed);
    }

    return null;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Stores the value via {@link HotKey#putThrough} with a no-op writer,
   * since the mutating method was already executed by the {@code CacheInterceptor}.
   *
   * <p>If the value is {@code null} (allowed only when {@code allowNullValues}
   * is {@code true}), the key is tracked in {@link #nullCachedKeys} so that
   * subsequent side-effect-free {@link #lookup} calls distinguish "cached null"
   * from "not in cache".
   *
   * @param key   the cache key
   * @param value the value to cache (maybe {@code null} if null values are allowed)
   */
  @Override
  public void put(@NonNull Object key, @Nullable Object value) {
    String prefixed = prefixedKey(key);
    Object storeValue = toStoreValue(value);
    nullCachedKeys.remove(prefixed);

    if (HotKeyCacheContext.get().isSkipBroadcast()) {
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
    nullCachedKeys.remove(prefixed);
    if (HotKeyCacheContext.get().isSkipBroadcast()) {
      hotKey.evictLocal(prefixed);
    } else {
      hotKey.invalidate(prefixed);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Invalidates all entries via {@link HotKey#invalidateAll}.
   */
  @Override
  public void clear() {
    nullCachedKeys.clear();
    /*
     * This is a defensive measure, we agreed that such out-of-bounds behavior shouldn't happen in a distributed cluster
     * Banned broadcast is essential.
     */
    hotKey.invalidateAll();
  }
}
