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
package io.github.hyshmily.hotkey.cache.codec;

import com.github.benmanes.caffeine.cache.Weigher;
import io.github.hyshmily.hotkey.model.CacheEntry;
import org.jspecify.annotations.NonNull;

/**
 * Rough heap-weight estimator for {@link CacheEntry} values.
 *
 * <p>Used when {@code hotkey.local.cache.max-weight} is set. {@code String} values
 * are weighted by {@code length() * 2} (UTF-16 byte width, bounds object overhead
 * separately). Raw {@code byte[]} values are weighted by {@code length}. All other
 * types get a flat {@code 1024} byte estimate. Object headers and Caffeine internal
 * metadata are accounted via the overhead constants.
 */
@SuppressWarnings("all")
public final class DefaultWeigher implements Weigher<String, Object> {

  public static final DefaultWeigher INSTANCE = new DefaultWeigher();

  private static final int STRING_OVERHEAD = 48;      // String obj(24) + byte[] header(16) + padding
  private static final int BYTE_ARRAY_OVERHEAD = 24;  // byte[] header(16) + padding
  private static final int CACHE_ENTRY_OVERHEAD = 80; // ~5 long/int fields + object header + padding
  private static final int ENTRY_OVERHEAD = 512;      // Caffeine AccessOrderNode(40) + CHM.Node(32) + refs + alignment

  private DefaultWeigher() {}

  @Override
  public int weigh(String key, @NonNull Object value) {
    int valueWeight = valueOf(value);
    return (key.length() << 1) + valueWeight + ENTRY_OVERHEAD;
  }

  private static int valueOf(Object v) {
    if (v instanceof String s) return (s.length() << 1) + STRING_OVERHEAD;
    if (v instanceof byte[] b) return b.length + BYTE_ARRAY_OVERHEAD;
    if (v instanceof CacheEntry ce) return valueOf(ce.getValue()) + CACHE_ENTRY_OVERHEAD;
    return 1024;
  }
}
