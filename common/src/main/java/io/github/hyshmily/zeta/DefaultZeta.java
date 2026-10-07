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
package io.github.hyshmily.zeta;

import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.loader.ZetaLoaderRegistry;
import io.github.hyshmily.zeta.cache.loader.ZetaLoadingSpec;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.HotKey;
import io.github.hyshmily.zeta.model.InvalidatePolicy;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.model.WritePolicy;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.RuleService;
import io.github.hyshmily.zeta.scheduler.TimedRefreshCoordinator;
import io.github.hyshmily.zeta.sync.distributedlock.AutoReleaseLock;
import io.github.hyshmily.zeta.sync.distributedlock.LockProvider;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

/**
 * Default implementation of the {@link Zeta} facade — the sole public API
 * entry point. This adapter owns no logic of its own beyond validation and
 * delegation: reads/writes/invalidations go to {@link HotKeyCache}, rule
 * administration to {@code RuleService}, timed refresh scheduling to
 * {@code TimedRefreshCoordinator}, and the administrative views
 * ({@link Zeta.RuleAdmin rules()}, {@link Zeta.RefreshAdmin refresh()},
 * {@link Zeta.StatsAdmin stats()}) are projected from the same components.
 *
 * <p>
 * All cache read/write/invalidation operations are dispatched through this
 * class, which delegates to {@link HotKeyCache} for L1 orchestration, version
 * tracking, and cross-instance broadcast, and to {@link TopK} (HeavyKeeper)
 * for local and cluster-wide hot-key detection queries. The detection view
 * ({@link Zeta.DetectorAdmin detector()}) is a dedicated inner object rather
 * than implemented directly: its method signatures collide with the
 * deprecated direct detection methods this class must keep for compatibility,
 * so the directs delegate to the view.
 *
 * <p>
 * Rule management (blacklist/whitelist) is also exposed here, with pattern
 * matching for exact, prefix, wildcard, and regex rules.
 *
 * <p>
 * <b>Thread safety:</b> All public methods are thread-safe. The underlying
 * Caffeine cache and HeavyKeeper sketch are designed for concurrent access.
 *
 * <p>
  * <b>API shape — two-arity convention:</b> every operation family exposes
  * exactly two positional forms — the common-case convenience form, and a
  * full-control form whose last parameter is the family's closed policy
  * record ({@link ReadPolicy}, {@link WritePolicy}, {@link InvalidatePolicy}).
  * Intermediate positional overloads (per-parameter TTLs,
  * boolean flags) intentionally do not exist: customize via
  * {@code ReadPolicy.defaults().withHardTtl(...)}-style chains, or via the
  * fluent {@link #read(String)}/{@link #write(String)} builders for complex
  * chains. Each policy record carries exactly the knobs its family honors,
  * so there is no honored/ignored matrix to consult. The single
  * exception to the no-boolean rule is {@link #tag(String, boolean, boolean)},
  * the transport for the {@code @Intercept} annotation path.
 *
 * <p>
 * Depending on the runtime mode, some dependencies may be absent:
 * <ul>
 * <li><b>App-only mode</b> (default): all services available.</li>
 * <li><b>Worker-only mode</b> ({@code zeta.worker.enabled=true}):
 * cache and app‑side TopK are absent; only the Worker TopK is available.</li>
 * </ul>
 * Methods whose backing service is absent throw {@link ZetaModeException}
 * (cache read/write) or return empty / zero (TopK queries).
 *
 * <p>
 * <b>Version constraint for permanent entries:</b> Entries created with
 * {@link Long#MAX_VALUE} hard TTL ("permanent" entry) can outlive the Redis
 * version key ({@code zeta:version:{key}}), whose TTL defaults to 7 days
 * ({@code zeta.local.versionKeyTtlMinutes}). When the version key expires,
 * the next write produces a version of 1 (INCR restart), which is numerically
 * lower than the existing entry's version on peer instances — causing
 * {@link io.github.hyshmily.zeta.util.version.VersionGuard#shouldSkipForSync}
 * to reject the update silently. Avoid {@link Long#MAX_VALUE} hard TTL on keys
 * that participate in cross-instance cache sync, or ensure
 * {@code versionKeyTtlMinutes} exceeds the entry's actual lifetime.
 */
public class DefaultZeta implements Zeta, Zeta.RefreshAdmin, Zeta.StatsAdmin, DisposableBean {

  private final HotKeyCache hotKeyCache;

  private final HotKeyDetector appHotKeyDetector;

  private final LockProvider lockProvider;

  @Nullable
  private final RuleService ruleService;

  /**
   * Facade-plane view over {@link #ruleService} — a validating adapter, never
   * the service itself, so the facade keeps its reject-blank-key contract.
   */
  private final Zeta.RuleAdmin ruleAdmin;

  @Nullable
  private final ZetaLoaderRegistry loaderRegistry;

  private final TimedRefreshCoordinator refreshCoordinator;

  /**
   * Detection-observation view over {@link #appHotKeyDetector} and
   * {@link #hotKeyCache}. Deliberately always active (no INACTIVE variant
   * unlike rules/refresh/stats): the per-method {@code require*} guards and
   * null-tolerant query branches already encode the Worker-only behavior
   * exactly, so a second mode switch would only duplicate it.
   */
  private final Zeta.DetectorAdmin detectorAdmin;

  /**
   * Create a facade implementation with an optional distributed lock provider and
   * loader registry (ADR-0070).
   *
   * <p>
   * Each parameter may be {@code null} depending on the deployment mode.
   *
   * @param hotKeyCache       the cache orchestrator (maybe {@code null} in
   *                          Worker-only mode)
   * @param appHotKeyDetector the app-side local TopK detector (maybe {@code null}
   *                          in Worker-only mode)
   * @param lockProvider      the distributed lock provider (maybe {@code null}
   *                          when no Redis)
   * @param loaderRegistry    the prefix→loader registry backing the no-reader
   *                          {@code get(cacheKey)} overloads (maybe {@code null})
   * @param ruleService       the rule administration service (maybe {@code null} in
   *                          Worker-only mode)
   */
  public DefaultZeta(
    HotKeyCache hotKeyCache,
    HotKeyDetector appHotKeyDetector,
    LockProvider lockProvider,
    @Nullable ZetaLoaderRegistry loaderRegistry,
    @Nullable RuleService ruleService
  ) {
    this.hotKeyCache = hotKeyCache;
    this.appHotKeyDetector = appHotKeyDetector;
    this.lockProvider = lockProvider;
    this.loaderRegistry = loaderRegistry;
    this.ruleService = ruleService;
    this.ruleAdmin = ruleService == null ? null : new ValidatingRuleAdmin(ruleService);
    this.refreshCoordinator = new TimedRefreshCoordinator();
    this.detectorAdmin = new DetectorView();
  }

  @Override
  public void tag(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("tag");
    hotKeyCache.recordAccess(cacheKey);
  }

  @Override
  public void tag(String cacheKey, TagMode mode) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(mode, "mode must not be null");
    requireAppCache("tag");
    hotKeyCache.recordAccess(cacheKey, mode);
  }

  @Override
  public <T> Optional<T> get(String cacheKey) {
    ZetaLoadingSpec<?> spec = requireRegisteredSpec(cacheKey, "get");
    return get(cacheKey, spec.toReadPolicy(cacheKey));
  }

  @Override
  public <T> Optional<T> getWithSoftExpire(String cacheKey) {
    ZetaLoadingSpec<?> spec = requireRegisteredSpec(cacheKey, "getWithSoftExpire");
    return getWithSoftExpire(cacheKey, spec.toReadPolicy(cacheKey, StalePolicy.SOFT_REFRESH));
  }

  @Override
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    return get(cacheKey, ReadPolicy.of(reader));
  }

  @Override
  public <T> Optional<T> get(String cacheKey, ReadPolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("get");
    requireAppDetector("get");
    return hotKeyCache.get(cacheKey, policy);
  }

  @Override
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    return getWithSoftExpire(cacheKey, ReadPolicy.of(reader));
  }

  @Override
  public <T> Optional<T> getWithSoftExpire(String cacheKey, ReadPolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("getWithSoftExpire");
    requireAppDetector("getWithSoftExpire");
    return hotKeyCache.getWithSoftExpire(cacheKey, policy);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Map<String, T> getAll(Iterable<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<ZetaLoadingSpec<?>, List<String>> groups = groupKeysBySpec(cacheKeys, "getAll");
    Map<String, Optional<T>> merged = new LinkedHashMap<>();
    for (Map.Entry<ZetaLoadingSpec<?>, List<String>> group : groups.entrySet()) {
      ZetaLoadingSpec<?> spec = group.getKey();
      List<String> keys = group.getValue();
      Function<String, T> groupReader = key -> (T) spec.loader().load(key);
      merged.putAll(
        hotKeyCache.get(keys, groupReader, spec.hardTtlMs(), spec.softTtlMs(), spec.reportEnabled(), spec.failOnError())
      );
    }
    return unwrapPresent(merged);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Map<String, T> getAllWithSoftExpire(Iterable<String> cacheKeys) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Map<ZetaLoadingSpec<?>, List<String>> groups = groupKeysBySpec(cacheKeys, "getAllWithSoftExpire");
    Map<String, Optional<T>> merged = new LinkedHashMap<>();
    for (Map.Entry<ZetaLoadingSpec<?>, List<String>> group : groups.entrySet()) {
      ZetaLoadingSpec<?> spec = group.getKey();
      List<String> keys = group.getValue();
      Function<String, T> groupReader = key -> (T) spec.loader().load(key);
      merged.putAll(
        hotKeyCache.getWithSoftExpire(
          keys,
          groupReader,
          spec.hardTtlMs(),
          spec.softTtlMs(),
          spec.reportEnabled(),
          spec.failOnError()
        )
      );
    }
    return unwrapPresent(merged);
  }

  /**
   * Unwrap a batch engine result for the facade: present values carried over,
   * empty optionals (miss or cached null) omitted. The engine keeps
   * {@code Optional} as its internal null-vs-miss channel; the facade never
   * exposes it (a map cannot distinguish "absent" from "empty" per key).
   *
   * @param engineResult the engine batch result
   * @param <T>          the value type
   * @return insertion-ordered map of key → present value
   */
  private static <T> Map<String, T> unwrapPresent(Map<String, Optional<T>> engineResult) {
    Map<String, T> out = new LinkedHashMap<>();
    for (Map.Entry<String, Optional<T>> e : engineResult.entrySet()) {
      if (e.getValue().isPresent()) {
        out.put(e.getKey(), e.getValue().get());
      }
    }
    return out;
  }

  @Override
  public <T> Map<String, T> getAll(Iterable<String> cacheKeys, Function<? super String, ? extends T> reader) {
    return getAll(cacheKeys, reader, ReadPolicy.defaults());
  }

  @Override
  public <T> Map<String, T> getAll(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    ReadPolicy policy
  ) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("getAll");
    requireAppDetector("getAll");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    return unwrapPresent(
      hotKeyCache.get(cacheKeys, reader, hardTtlMs, softTtlMs, policy.reportEnabled(), policy.failOnError())
    );
  }

  @Override
  public <T> Map<String, T> getAllWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader
  ) {
    return getAllWithSoftExpire(cacheKeys, reader, ReadPolicy.defaults());
  }

  @Override
  public <T> Map<String, T> getAllWithSoftExpire(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    ReadPolicy policy
  ) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("getAllWithSoftExpire");
    requireAppDetector("getAllWithSoftExpire");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    return unwrapPresent(
      hotKeyCache.getWithSoftExpire(
        cacheKeys,
        reader,
        hardTtlMs,
        softTtlMs,
        policy.reportEnabled(),
        policy.failOnError()
      )
    );
  }

  /**
   * Routes each key to its winning {@link ZetaLoadingSpec} and groups the keys
   * by spec (insertion-ordered) for the batch no-reader operations. Fails fast —
   * with an actionable message — when no registry is wired or any key matches
   * no registered prefix: the batch never partially resolves.
   *
   * @param cacheKeys the keys to route; never {@code null}
   * @param operation the calling operation name (used in error messages)
   * @return keys grouped by winning spec
   */
  private Map<ZetaLoadingSpec<?>, List<String>> groupKeysBySpec(Iterable<String> cacheKeys, String operation) {
    requireAppCache(operation);
    requireAppDetector(operation);
    if (loaderRegistry == null) {
      throw new IllegalStateException(
        "No ZetaLoaderRegistry is wired — register one (ZetaLoaderRegistry bean) or use " +
          operation +
          "(keys, reader) for these keys"
      );
    }
    Map<ZetaLoadingSpec<?>, List<String>> groups = new LinkedHashMap<>();
    for (String key : cacheKeys) {
      ZetaLoadingSpec<?> spec = loaderRegistry.match(key);
      if (spec == null) {
        throw new IllegalStateException(
          "No CacheLoader registered for key '" +
            key +
            "' — register a matching prefix via ZetaLoaderRegistry.register(prefix, spec), or use " +
            operation +
            "(keys, reader)"
        );
      }
      groups.computeIfAbsent(spec, s -> new ArrayList<>()).add(key);
    }
    return groups;
  }

  /**
   * Resolves the winning {@link ZetaLoadingSpec} for a no-reader call, failing
   * fast with an actionable message when no registry is wired or no prefix
   * matches.
   */
  private ZetaLoadingSpec<?> requireRegisteredSpec(String cacheKey, String operation) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache(operation);
    requireAppDetector(operation);
    if (loaderRegistry == null) {
      throw new IllegalStateException(
        "No ZetaLoaderRegistry is wired — register one (ZetaLoaderRegistry bean) or use " +
          operation +
          "(key, reader) for this key: " +
          cacheKey
      );
    }
    ZetaLoadingSpec<?> spec = loaderRegistry.match(cacheKey);
    if (spec == null) {
      throw new IllegalStateException(
        "No CacheLoader registered for key '" +
          cacheKey +
          "' — register a matching prefix via ZetaLoaderRegistry.register(prefix, spec), or use " +
          operation +
          "(key, reader)"
      );
    }
    return spec;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V computeIfAbsent(String cacheKey) {
    ZetaLoadingSpec<?> spec = requireRegisteredSpec(cacheKey, "computeIfAbsent");
    return (V) hotKeyCache.computeIfAbsent(cacheKey, spec.toReadPolicy(cacheKey)).orElse(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V computeIfAbsent(String cacheKey, Supplier<V> loader) {
    Objects.requireNonNull(loader, "loader must not be null");
    return (V) hotKeyCache.computeIfAbsent(cacheKey, ReadPolicy.of(loader)).orElse(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Optional<T> computeIfAbsentOptional(String cacheKey, ReadPolicy policy) {
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("computeIfAbsent");
    requireAppDetector("computeIfAbsent");
    return hotKeyCache.computeIfAbsent(cacheKey, policy);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey) {
    ZetaLoadingSpec<?> spec = requireRegisteredSpec(cacheKey, "computeIfAbsentWithSoftExpire");
    return (V) hotKeyCache
      .computeIfAbsentWithSoftExpire(cacheKey, spec.toReadPolicy(cacheKey, StalePolicy.SOFT_REFRESH))
      .orElse(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V computeIfAbsentWithSoftExpire(String cacheKey, Supplier<V> loader) {
    Objects.requireNonNull(loader, "loader must not be null");
    return (V) hotKeyCache.computeIfAbsentWithSoftExpire(cacheKey, ReadPolicy.of(loader)).orElse(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Optional<T> computeIfAbsentWithSoftExpireOptional(String cacheKey, ReadPolicy policy) {
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("computeIfAbsentWithSoftExpire");
    requireAppDetector("computeIfAbsentWithSoftExpire");
    return hotKeyCache.computeIfAbsentWithSoftExpire(cacheKey, policy);
  }

  @Override
  public <T> Optional<T> peek(@NotNull @NotBlank String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("peek");
    return hotKeyCache.peek(cacheKey);
  }

  @Override
  @Internal
  public Object peekAndTag(String cacheKey) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    requireAppCache("peekAndTag");
    return hotKeyCache.peekAndTag(cacheKey);
  }

  @Override
  public <T> void putThrough(String cacheKey, T value, Runnable writer, WritePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(value, "value must not be null");
    Objects.requireNonNull(writer, "writer must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    requireAppCache("putThrough");
    hotKeyCache.putThrough(cacheKey, value, writer, hardTtlMs, softTtlMs, !policy.skipBroadcast());
  }

  @Override
  public void putLocal(String cacheKey, Object value, WritePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(value, "value must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    if (hotKeyCache == null) {
      return; // Worker-only mode: silently no-op
    }
    hotKeyCache.putLocal(cacheKey, value, hardTtlMs, softTtlMs);
  }

  @Override
  public void invalidate(String cacheKey, InvalidatePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("invalidate");
    hotKeyCache.invalidate(cacheKey, !policy.skipBroadcast());
  }

  @Override
  public void invalidate(Collection<String> cacheKeys, InvalidatePolicy policy) {
    Objects.requireNonNull(cacheKeys, "cacheKeys must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("invalidate");
    hotKeyCache.invalidate(cacheKeys, !policy.skipBroadcast());
  }

  @Override
  public void invalidateAfterPut(String cacheKey, Runnable mutation, InvalidatePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(mutation, "mutation must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("invalidateAfterPut");
    requireAppDetector("invalidateAfterPut");
    hotKeyCache.invalidateAfterPut(cacheKey, mutation, !policy.skipBroadcast());
  }

  @Override
  public void invalidateAfterPut(Map<String, ? extends Runnable> mutations, InvalidatePolicy policy) {
    Objects.requireNonNull(mutations, "mutations must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    requireAppCache("invalidateAfterPut");
    requireAppDetector("invalidateAfterPut");
    hotKeyCache.invalidateAfterPut(mutations, !policy.skipBroadcast());
  }

  @Override
  public void invalidateAllLocal() {
    requireAppCache("invalidateAllLocal");
    hotKeyCache.invalidateAllLocal();
  }

  @Override
  public long estimatedSize() {
    if (hotKeyCache == null) {
      return 0L;
    }
    return hotKeyCache.estimatedSize();
  }

  @Override
  public ZetaCacheStats snapshot() {
    if (hotKeyCache == null) {
      return null;
    }
    return hotKeyCache.stats();
  }

  @Override
  public Map<String, Object> snapshotValues(int limit) {
    if (hotKeyCache == null) {
      return Map.of();
    }
    return hotKeyCache.snapshotValues(limit);
  }

  @Override
  public List<String> localKeysWithPrefix(String prefix) {
    if (hotKeyCache == null) {
      return List.of();
    }
    return hotKeyCache.localKeysWithPrefix(prefix);
  }

  @Override
  @Nullable
  public AutoReleaseLock tryLock(String key, long expire, TimeUnit unit) {
    Assert.hasText(key, "key must not be empty");
    Assert.isTrue(expire >= 0, "expire must not be negative");
    Objects.requireNonNull(unit, "unit must not be null");
    return lockProvider != null ? lockProvider.tryLock(key, expire, unit) : null;
  }

  @Override
  @Nullable
  public AutoReleaseLock tryLock(
    String key,
    long expire,
    TimeUnit unit,
    int tryLockLockCount,
    int tryLockInquiryCount,
    int tryLockUnlockCount
  ) {
    Assert.hasText(key, "key must not be empty");
    Assert.isTrue(expire >= 0, "expire must not be negative");
    Objects.requireNonNull(unit, "unit must not be null");
    return lockProvider != null
      ? lockProvider.tryLock(key, expire, unit, tryLockLockCount, tryLockInquiryCount, tryLockUnlockCount)
      : null;
  }

  @Override
  public boolean tryLockAndRun(String key, long expire, TimeUnit unit, Runnable action) {
    Assert.hasText(key, "key must not be empty");
    Assert.isTrue(expire >= 0, "expire must not be negative");
    Objects.requireNonNull(unit, "unit must not be null");
    Objects.requireNonNull(action, "action must not be null");
    if (lockProvider == null) {
      return false;
    }
    try (AutoReleaseLock lock = tryLock(key, expire, unit)) {
      if (lock != null) {
        action.run();
        return true;
      }
      return false;
    }
  }

  @Override
  public boolean tryLockAndRun(
    String key,
    long expire,
    TimeUnit unit,
    Runnable action,
    int tryLockLockCount,
    int tryLockInquiryCount,
    int tryLockUnlockCount
  ) {
    Assert.hasText(key, "key must not be empty");
    Assert.isTrue(expire >= 0, "expire must not be negative");
    Objects.requireNonNull(unit, "unit must not be null");
    Objects.requireNonNull(action, "action must not be null");
    try (AutoReleaseLock lock = tryLock(key, expire, unit, tryLockLockCount, tryLockInquiryCount, tryLockUnlockCount)) {
      if (lock != null) {
        action.run();
        return true;
      }
      return false;
    }
  }

  /**
   * Resolve the hard/soft TTL overrides from a read policy, evaluating
   * each lazy supplier exactly once, and assert both are non-negative.
   * Centralizes the boundary decomposition shared by every policy-form
   * read/batch entry point; the assertion messages are intentional contract
   * text and must not be altered.
   *
   * @param policy the read policy supplying the TTL overrides
   * @return the resolved hard and soft TTL in milliseconds
   */
  private static ResolvedTtls resolveTtlsOrAssert(ReadPolicy policy) {
    long hardTtlMs = policy.hardTtlMs().getAsLong();
    long softTtlMs = policy.softTtlMs().getAsLong();
    Assert.isTrue(hardTtlMs >= 0, "hardTtlMs must not be negative");
    Assert.isTrue(softTtlMs >= 0, "softTtlMs must not be negative");
    return new ResolvedTtls(hardTtlMs, softTtlMs);
  }

  /**
   * Resolve the hard/soft TTL overrides from a write policy — same contract
   * as the read-policy overload, for the write/refresh entry points.
   *
   * @param policy the write policy supplying the TTL overrides
   * @return the resolved hard and soft TTL in milliseconds
   */
  private static ResolvedTtls resolveTtlsOrAssert(WritePolicy policy) {
    long hardTtlMs = policy.hardTtlMs().getAsLong();
    long softTtlMs = policy.softTtlMs().getAsLong();
    Assert.isTrue(hardTtlMs >= 0, "hardTtlMs must not be negative");
    Assert.isTrue(softTtlMs >= 0, "softTtlMs must not be negative");
    return new ResolvedTtls(hardTtlMs, softTtlMs);
  }

  /** Resolved hard/soft TTL pair extracted from a policy. */
  private record ResolvedTtls(long hardTtlMs, long softTtlMs) {}

  @Override
  public <T> void registerRefresh(String key, Supplier<T> supplier, ReadPolicy policy) {
    Assert.hasText(key, "key must not be empty");
    Objects.requireNonNull(supplier, "supplier must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    requireAppCache("registerRefresh");
    long intervalMs = TimedRefreshCoordinator.intervalFor(hotKeyCache.resolveEffectiveSoftTtl(softTtlMs));
    // Freeze the per-tick policy outside the coordinator's map lock; the tick
    // does not rebuild the ReadPolicy on every firing.
    var ref = new Object() {
      ReadPolicy tick = ReadPolicy.of(
        supplier,
        hardTtlMs,
        softTtlMs,
        policy.nullCaching(),
        policy.reportEnabled(),
        StalePolicy.SOFT_REFRESH
      );
    };
    if (policy.failOnError()) {
      ref.tick = ref.tick.withFailOnError();
    }
    refreshCoordinator.register(key, intervalMs, () -> getWithSoftExpire(key, ref.tick));
  }

  @Override
  public void unregisterRefresh(String key) {
    Assert.hasText(key, "key must not be empty");
    refreshCoordinator.unregister(key);
  }

  /**
   * Cancel all timed refreshes and shut down the refresh scheduler.
   * Called automatically by Spring when this bean is destroyed.
   */
  @Override
  public void destroy() {
    refreshCoordinator.destroy();
  }

  @Override
  public <V> void refresh(String cacheKey, Supplier<V> loader, WritePolicy policy) {
    Assert.hasText(cacheKey, "cacheKey must not be empty");
    Objects.requireNonNull(loader, "loader must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    ResolvedTtls ttls = resolveTtlsOrAssert(policy);
    long hardTtlMs = ttls.hardTtlMs();
    long softTtlMs = ttls.softTtlMs();
    requireAppCache("refresh");
    V value = loader.get();
    hotKeyCache.invalidate(cacheKey, false);
    // A null loader result must not NPE inside putThrough after the entry was
    // already evicted — the eviction alone is the refresh contract for a
    // missing value (the next read reloads through the application reader).
    if (value != null) {
      hotKeyCache.putThrough(cacheKey, value, () -> {}, hardTtlMs, softTtlMs, !policy.skipBroadcast());
    }
  }

  @Override
  public void refreshAll(Map<String, Supplier<?>> loaders) {
    Objects.requireNonNull(loaders, "loaders must not be null");
    requireAppCache("refreshAll");
    loaders.forEach(this::refresh);
  }

  @Override
  public boolean isApp() {
    return hotKeyCache != null;
  }

  /**
   * Returns a human-readable label of the current deployment mode.
   */
  private String currentModeLabel() {
    if (hotKeyCache != null) {
      return "App-only mode";
    }
    // The facade is only ever created with a null cache in Worker-only
    // deployments (ZetaFacadeAutoConfiguration wires it via ObjectProvider);
    // a cache-less facade in an app deployment would be a wiring error.
    return "Worker-only mode (no app-side cache)";
  }

  private void requireAppCache(String operation) {
    if (hotKeyCache == null) {
      throw new ZetaModeException(operation, currentModeLabel(), "App-mode cache");
    }
  }

  /**
   * Requires app-side TopK detector; throws {@link ZetaModeException} when
   * the app-side detector is unavailable (Worker-only mode).
   *
   * @param operation the API operation name
   */
  private void requireAppDetector(String operation) {
    if (appHotKeyDetector == null) {
      throw new ZetaModeException(operation, currentModeLabel(), "App-mode TopK detector");
    }
  }

  private static final Zeta.RuleAdmin INACTIVE_RULE_ADMIN = new Zeta.RuleAdmin() {
    @Override
    public void addBlacklist(String keyPattern) {
      throw new ZetaModeException(
        "addBlacklist",
        "Worker-only mode (no app-side cache)",
        "App-mode rule administration"
      );
    }

    @Override
    public void removeBlacklist(String keyPattern) {
      throw new ZetaModeException(
        "removeBlacklist",
        "Worker-only mode (no app-side cache)",
        "App-mode rule administration"
      );
    }

    @Override
    public void addWhitelist(String keyPattern) {
      throw new ZetaModeException(
        "addWhitelist",
        "Worker-only mode (no app-side cache)",
        "App-mode rule administration"
      );
    }

    @Override
    public void removeWhitelist(String keyPattern) {
      throw new ZetaModeException(
        "removeWhitelist",
        "Worker-only mode (no app-side cache)",
        "App-mode rule administration"
      );
    }

    @Override
    public Rule.RuleAction evaluateRule(String cacheKey) {
      return Rule.RuleAction.ALLOW;
    }

    @Override
    public boolean isBlacklisted(String cacheKey) {
      return false;
    }

    @Override
    public boolean isWhitelisted(String cacheKey) {
      return false;
    }

    @Override
    public List<Rule> getAllRules() {
      return List.of();
    }

    @Override
    public void clearAllRules() {
      throw new ZetaModeException(
        "clearAllRules",
        "Worker-only mode (no app-side cache)",
        "App-mode rule administration"
      );
    }

    @Override
    public void broadcastAllLocalRulesManually() {
      throw new ZetaModeException(
        "broadcastAllLocalRulesManually",
        "Worker-only mode (no app-side cache)",
        "App-mode rule administration"
      );
    }
  };

  private static final Zeta.RefreshAdmin INACTIVE_REFRESH_ADMIN = new Zeta.RefreshAdmin() {
    @Override
  public <T> void registerRefresh(String key, Supplier<T> supplier, ReadPolicy policy) {
      throw new ZetaModeException(
        "registerRefresh",
        "Worker-only mode (no app-side cache)",
        "App-mode refresh scheduling"
      );
    }

    @Override
    public void unregisterRefresh(String key) {
      // no-op: the worker mode has no scheduler to unregister from
    }
  };

  private static final Zeta.StatsAdmin INACTIVE_STATS_ADMIN = new Zeta.StatsAdmin() {
    @Override
    public long estimatedSize() {
      return 0L;
    }

    @Override
    public ZetaCacheStats snapshot() {
      return null;
    }

    @Override
    public Map<String, Object> snapshotValues(int limit) {
      return Map.of();
    }

    @Override
    public List<String> localKeysWithPrefix(String prefix) {
      return List.of();
    }
  };

  @Override
  public Zeta.RuleAdmin rules() {
    return ruleAdmin != null ? ruleAdmin : INACTIVE_RULE_ADMIN;
  }

  @Override
  public Zeta.RefreshAdmin refresh() {
    return isApp() ? this : INACTIVE_REFRESH_ADMIN;
  }

  @Override
  public Zeta.StatsAdmin stats() {
    return isApp() ? this : INACTIVE_STATS_ADMIN;
  }

  @Override
  public Zeta.DetectorAdmin detector() {
    return detectorAdmin;
  }

  /**
   * Facade-plane guard over {@link RuleService}: rejects blank key patterns
   * before delegating.
   *
   * <p>
   * This adapter carries the <i>validation</i> half of the rule contract.
   * Before rule management was extracted, the facade asserted on blank keys
   * while the cache engine silently skipped invalid ones; collapsing both into
   * {@code RuleService} would have forced one behaviour on both planes. Keeping
   * the assertion here preserves the facade contract and lets
   * {@code RuleService} stay a lenient engine-side component for callers that
   * hold it directly.
   *
   * <p>
   * The batch forms are deliberately <b>not</b> overridden: they are
   * {@code default} methods on {@link Zeta.RuleAdmin} that fan out over the
   * single-key forms, so they inherit this validation (and per-key delegation)
   * for free.
   */
  private static final class ValidatingRuleAdmin implements Zeta.RuleAdmin {

    private final RuleService delegate;

    ValidatingRuleAdmin(RuleService delegate) {
      this.delegate = delegate;
    }

    @Override
    public void addBlacklist(String keyPattern) {
      Assert.hasText(keyPattern, "keyPattern must not be empty");
      delegate.addBlacklist(keyPattern);
    }

    @Override
    public void removeBlacklist(String keyPattern) {
      Assert.hasText(keyPattern, "keyPattern must not be empty");
      delegate.removeBlacklist(keyPattern);
    }

    @Override
    public void addWhitelist(String keyPattern) {
      Assert.hasText(keyPattern, "keyPattern must not be empty");
      delegate.addWhitelist(keyPattern);
    }

    @Override
    public void removeWhitelist(String keyPattern) {
      Assert.hasText(keyPattern, "keyPattern must not be empty");
      delegate.removeWhitelist(keyPattern);
    }

    @Override
    public Rule.RuleAction evaluateRule(String cacheKey) {
      Assert.hasText(cacheKey, "cacheKey must not be empty");
      return delegate.evaluateRule(cacheKey);
    }

    @Override
    public boolean isBlacklisted(String cacheKey) {
      Assert.hasText(cacheKey, "cacheKey must not be empty");
      return delegate.isBlacklisted(cacheKey);
    }

    @Override
    public boolean isWhitelisted(String cacheKey) {
      Assert.hasText(cacheKey, "cacheKey must not be empty");
      return delegate.isWhitelisted(cacheKey);
    }

    @Override
    public List<Rule> getAllRules() {
      return delegate.getAllRules();
    }

    @Override
    public void clearAllRules() {
      delegate.clearAllRules();
    }

    @Override
    public void broadcastAllLocalRulesManually() {
      delegate.broadcastAllLocalRulesManually();
    }
  }

  /**
   * Detection-observation view over the facade's detector and cache.
   * Non-static inner class so the bodies stay identical call-for-call with
   * the former direct methods (same {@code require*} guards, same
   * null-detector-tolerant query branches) — the directs now delegate here.
   */
  private final class DetectorView implements Zeta.DetectorAdmin {

    @Override
    public boolean isLocalHotKey(String cacheKey) {
      Assert.hasText(cacheKey, "cacheKey must not be empty");
      requireAppCache("isHot");
      requireAppDetector("isHot");
      return hotKeyCache.isHot(cacheKey);
    }

    @Override
    public void notifyLocalDetectorDirect(String cacheKey, long count) {
      requireAppDetector("notifyLocalDetectorDirect");
      appHotKeyDetector.addDirect(cacheKey, count);
    }

    @Override
    public void notifyLocalDetectorDirect(Map<String, Long> keyCounts) {
      requireAppDetector("notifyLocalDetectorDirect");
      appHotKeyDetector.addDirect(keyCounts);
    }

    @Override
    public void notifyLocalDetector(String cacheKey) {
      requireAppDetector("notifyLocalDetector");
      appHotKeyDetector.add(cacheKey);
    }

    @Override
    public void notifyLocalDetector(String cacheKey, long count) {
      requireAppDetector("notifyLocalDetector");
      appHotKeyDetector.add(cacheKey, count);
    }

    @Override
    public void notifyLocalDetector(Map<String, Long> keyCounts) {
      requireAppDetector("notifyLocalDetector");
      appHotKeyDetector.add(keyCounts);
    }

    @Override
    public List<HotKey> localTopKeys(int n) {
      return appHotKeyDetector != null
        ? appHotKeyDetector
            .listTopN(n)
            .stream()
            .map(item -> new HotKey(item.key(), item.count()))
            .toList()
        : List.of();
    }

    @Override
    public List<HotKey> localTopKeys() {
      return appHotKeyDetector != null
        ? appHotKeyDetector
            .list()
            .stream()
            .map(item -> new HotKey(item.key(), item.count()))
            .toList()
        : List.of();
    }

    @Override
    public BlockingQueue<Item> returnLocalExpelledHotKeys() {
      return appHotKeyDetector != null ? appHotKeyDetector.expelled() : new LinkedBlockingQueue<>();
    }

    @Override
    public long returnLocalTotalDataStreams() {
      return appHotKeyDetector != null ? appHotKeyDetector.total() : 0L;
    }
  }
}
