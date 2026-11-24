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
package io.github.hyshmily.zeta.cache.cachesupport.raw;

import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.function.Supplier;

/**
 * A value loaded from the data source together with the {@code dataVersion}
 * stamp taken <b>before</b> the value read (design decisions: version stamps — pre-read stamping).
 *
 * <p>Carrier used by the read path (single and batch miss loads, soft-expire
 * refresh) so the stamped version is a lower bound: any write allocated after
 * the load began carries a strictly greater stamp and therefore applies.
 *
 * @param value       the loaded value; may be {@code null} (reader returned
 *                    null — a null result is never cached)
 * @param dataVersion the pre-read stamp; {@code 0} means unknown and makes the
 *                    weakest claim (accepts any real stamp)
 */
@Internal
public record VersionedValue(Object value, long dataVersion) {
  /**
   * The one stamped reload shared by every background warming path: allocate the stamp before the
   * load, and decline a value the loader will not store.
   *
   * @param key    the key to reload
   * @param loader the warmable source
   * @param clock  the node-local stamp allocator
   * @return a supplier producing the stamped value, or {@code null} when the source is empty or
   *         the loader declines it
   */
  public static Supplier<VersionedValue> stampedReload(
    String key,
    CacheLoader<String, Object> loader,
    StampClock clock
  ) {
    return () -> {
      long stamp = clock.nextStamp();
      Object value = loader.reload(key);
      return value != null && loader.isCacheable(value) ? new VersionedValue(value, stamp) : null;
    };
  }
}
