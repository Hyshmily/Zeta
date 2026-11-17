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
package io.github.hyshmily.zeta.sync;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.EntryView;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.List;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

/**
 * The cluster-loader read and hook-dispatch tails shared by the sync and Worker
 * decision planes.
 *
 * <p>Both planes load the authoritative value through the same optional
 * {@link CacheLoader} and run the same "one terminal callback per message"
 * hook contract; keeping the two rules here stops the sync and Worker contexts
 * from drifting apart.
 */
@Internal
public final class DecisionPlaneSupport {

  private DecisionPlaneSupport() {}

  /**
   * One loader read: the value, or the reason none is usable. {@code declined} distinguishes
   * a loader-admission refusal from a clean miss or a failure for callers that report a
   * typed skip reason.
   *
   * @param value    the usable value, or {@code null}
   * @param declined whether the loader returned a value its own {@code isCacheable} refused
   */
  public record LoadResult(@Nullable Object value, boolean declined) {
    static final LoadResult EMPTY = new LoadResult(null, false);
  }

  /**
   * The read-only entry snapshot both planes hand to their policy, or {@code null} when the
   * key is absent; {@link EntryView#logicallyExpired()} on the snapshot reports expiry.
   *
   * @param cache      the L1 table
   * @param key        the key to snapshot
   * @param ttlPolicy  the expiry arithmetic
   * @return the snapshot, or {@code null} when the key is absent
   */
  public static @Nullable EntryView view(Cache<String, CacheEntry> cache, String key, TtlPolicy ttlPolicy) {
    return EntryView.of(cache.asMap().get(key), ttlPolicy, TimeSource.currentTimeMillis());
  }

  /**
   * Loads a value through the optional cluster loader, keeping the declined-vs-missing
   * distinction.
   *
   * @param loader        the authoritative loader; may be {@code null}
   * @param key           the key to load; never {@code null}
   * @param log           the plane's logger
   * @param failureFormat the SLF4J format for a loader failure (one {@code {}} argument: the key)
   * @return the load result; never {@code null}
   */
  public static LoadResult load(
    @Nullable CacheLoader<String, Object> loader,
    String key,
    Logger log,
    String failureFormat
  ) {
    if (loader == null) {
      return LoadResult.EMPTY;
    }
    try {
      Object value = loader.load(key);
      if (value == null) {
        return LoadResult.EMPTY;
      }
      if (!loader.isCacheable(value)) {
        log.debug("Cluster load declined by loader admission: key={}", key);
        return new LoadResult(null, true);
      }
      return new LoadResult(value, false);
    } catch (Exception e) {
      log.warn(failureFormat, key, e);
      return LoadResult.EMPTY;
    }
  }

  /**
   * Runs one hook callback per registered hook; a broken hook never breaks the decision
   * apply. The caller checks for an empty list first, so no capturing callback is
   * allocated in the default no-hook deployment.
   *
   * @param hooks    the registered hooks
   * @param hookType the hook interface name used in the WARN line
   * @param hookName the callback name used in the WARN line
   * @param key      the key the decision touched
   * @param callback the callback to invoke for each hook
   * @param log      the plane's logger
   * @param <H>      the hook type
   */
  public static <H> void fireHooks(
    List<H> hooks,
    String hookType,
    String hookName,
    String key,
    Consumer<H> callback,
    Logger log
  ) {
    for (H hook : hooks) {
      try {
        callback.accept(hook);
      } catch (Exception e) {
        log.warn("{}.{} failed for key={}", hookType, hookName, key, e);
      }
    }
  }
}
