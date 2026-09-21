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
import io.github.hyshmily.zeta.model.CachePolicy;
import jakarta.annotation.Nullable;
import org.jspecify.annotations.NonNull;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractValueAdaptingCache;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

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
 * <p>Reads preserve the facade's core invariant that <b>every read triggers both
 * hot-key detection and Worker reporting</b>: the non-sync lookup (Spring's
 * {@code cache.get(key)} path) routes through {@link Zeta#peekAndTag(String)},
 * which serves the value, evaluates the rules once, and counts the served hit;
 * the sync read routes through the soft-expire entry point — with a globally
 * disabled soft TTL (entries carrying {@code softExpireAtMs = 0}) it serves
 * plain hits with no background refresh, so the annotation path costs nothing
 * beyond a plain compute. Soft-expire is therefore governed solely by global
 * configuration, never by the presence of a TTL override.
 */
@Internal
public class ZetaSpringCache extends AbstractValueAdaptingCache {

  private final String name;
  /**
   * Precomputed {@code name + keySeparator} for {@link #prefixedKey} — the
   * separator is bound from {@code ZetaProperties} once at startup, so the
   * per-op getter chain and re-concatenation are wasted work on the hottest
   * annotation-path operation.
   */
  private final String keyPrefix;
  private final Zeta zeta;
  private final ZetaProperties properties;

  public ZetaSpringCache(String name, Zeta zeta, ZetaProperties properties, boolean allowNullValues) {
    super(allowNullValues);
    this.name = name;
    this.zeta = zeta;
    this.properties = properties;
    this.keyPrefix = name + properties.getSpringCache().getKeySeparator();
  }

  private String prefixedKey(Object key) {
    Objects.requireNonNull(key, "Cache key must not be null");
    return keyPrefix + key;
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

  /**
   * {@inheritDoc}
   *
   * <p>The detecting non-sync read for {@code @Cacheable} (Spring's
   * {@code cache.get(key)} path): a single {@link Zeta#peekAndTag(String)}
   * call serves the value with one hash lookup and one rule evaluation —
   * a hit (value or null sentinel) feeds the local HeavyKeeper detector and
   * the Worker reporter with the facade read path's rule handling (whitelist
   * detects without reporting, blocked keys throw, a miss does neither).
   * The peek itself never invokes a loader, so a miss stays a pure
   * miss — Spring then invokes the cached method and stores the result via
   * {@link #put}. This keeps the annotation path aligned with the core
   * invariant that every read triggers detection AND reporting.
   */
  @Override
  @Nullable
  @SuppressWarnings("all")
  public Object lookup(@NonNull Object key) {
    String prefixed = prefixedKey(key);
    Object served = zeta.peekAndTag(prefixed);
    // A sentinel hit is still a read (counted inside peekAndTag); surface
    // Spring's marker so Spring skips the method (fromStoreValue maps it to null).
    return served == null
      ? null
      : served == NullValue.INSTANCE
        ? org.springframework.cache.support.NullValue.INSTANCE
        : served;
  }

  /**
   * {@inheritDoc}
   *
   * <p>The sync read entry for {@code @Cacheable(sync = true)}: the resolved
   * {@link CachePolicy} (TTL overrides, null-caching decision) flows into the
   * atomic soft-expire compute path. TTL suppliers are evaluated lazily, so
   * SpEL expressions cost nothing on a plain cache hit. A cached
   * {@code null} (sentinel) surfaces as {@code null} without re-invoking the
   * value loader. The non-sync read path enters via {@link #lookup}, which
   * applies the same detection/reporting invariant without a loader.
   */
  @Override
  @Nullable
  @SuppressWarnings("unchecked")
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

    return zeta
      .computeIfAbsentWithSoftExpire(prefixed, policy.withReader(loader))
      .map(v -> (T) fromStoreValue(v))
      .orElse(null);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The store path for every non-sync {@code @Cacheable} miss and every
   * {@code @CachePut}. Consumes the policy the aspect pushed for this
   * invocation:
   * <ul>
   *   <li>{@code null} result — stored as Zeta's {@link NullValue#INSTANCE}
   *       sentinel with the short {@code zeta.local.null-value-ttl-seconds}
   *       TTL (the same sentinel and TTL the load path writes), unless
   *       {@code @NullCaching(false)} is active, in which case no entry is
   *       written and the next lookup re-invokes the method.</li>
   *   <li>Non-{@code null} result — the {@code @CacheTTL} override (static or
   *       SpEL) is resolved here and passed to the TTL-carrying put overloads;
   *       {@code 0} (= no annotation) resolves to the configured defaults,
   *       matching the sync path's ReadPolicy. The lazy suppliers are
   *       evaluated exactly once per store — a store always creates an entry,
   *       so the lazy-evaluation contract is met.</li>
   * </ul>
   * Spring's own marker is never stored.
   */
  @Override
  public void put(@NonNull Object key, @Nullable Object value) {
    String prefixed = prefixedKey(key);
    Object storeValue = toStoreValue(value);
    CachePolicy policy = ZetaCacheContext.get().current();

    boolean skipBroadcast = policy.skipBroadcast();
    boolean isNullValue = storeValue instanceof org.springframework.cache.support.NullValue;

    if (isNullValue) {
      if (!policy.nullCaching()) {
        // @NullCaching(false): a null result leaves no entry — the next call
        // re-invokes the method. Nothing to store and nothing to broadcast.
        return;
      }
      // Null-caching write: align with the load path — Zeta's sentinel + short
      // null TTL (Long.MAX_VALUE when null-value TTL is disabled, matching
      // TtlPolicy.computeNullExpireAt's disabled semantics). The @CacheTTL
      // override does not apply: the sentinel TTL is its own knob.
      long nullTtlMs = properties.effectiveNullTtlMs();
      if (skipBroadcast) {
        zeta.putLocal(prefixed, NullValue.INSTANCE, CachePolicy.of(nullTtlMs, nullTtlMs));
      } else {
        zeta.putThrough(prefixed, NullValue.INSTANCE, () -> {}, CachePolicy.of(nullTtlMs, nullTtlMs));
      }
      return;
    }

    // Clamp negatives to 0 (= default): the static annotation values are
    // non-negative by contract, but an unvalidated SpEL result is not.
    long hardTtlMs = Math.max(0L, policy.hardTtlMs().getAsLong());
    long softTtlMs = Math.max(0L, policy.softTtlMs().getAsLong());
    if (skipBroadcast) {
      zeta.putLocal(prefixed, storeValue, CachePolicy.of(hardTtlMs, softTtlMs));
    } else {
      zeta.putThrough(prefixed, storeValue, () -> {}, CachePolicy.of(hardTtlMs, softTtlMs));
    }
  }

  @Override
  public void evict(@NonNull Object key) {
    String prefixed = prefixedKey(key);
    boolean skip = ZetaCacheContext.get().current().skipBroadcast();
    if (skip) {
      zeta.invalidate(prefixed, CachePolicy.defaults().withSkipBroadcast(true));
    } else {
      zeta.invalidate(prefixed);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Invalidates <b>only this cache's</b> keys — the L1 keys carrying this
   * cache's {@code name + keySeparator} prefix — so {@code @CacheEvict(allEntries =
   * true)} on one cache never nukes the entries of other caches (the previous
   * behavior wiped the entire L1). Implementation is an O(n) scan of the local
   * L1 key set (acceptable: {@code clear()} is a rare, explicit operation)
   * followed by one batched {@link Zeta#invalidate(Collection, CachePolicy)},
   * broadcast unless {@code @SkipBroadcast} applies so peers evict the same
   * namespace. Entries written after the snapshot are left for the next
   * eviction cycle.
   */
  @Override
  public void clear() {
    com.github.benmanes.caffeine.cache.Cache<String, Object> localCache = zeta.getLocalCache();
    if (localCache == null) {
      return;
    }

    List<String> keys = localCache
      .asMap()
      .keySet()
      .stream()
      .filter(k -> k.startsWith(keyPrefix))
      .toList();
    if (keys.isEmpty()) {
      return;
    }

    boolean skipBroadcast = ZetaCacheContext.get().current().skipBroadcast();
    zeta.invalidate(keys, CachePolicy.defaults().withSkipBroadcast(skipBroadcast));
  }
}
