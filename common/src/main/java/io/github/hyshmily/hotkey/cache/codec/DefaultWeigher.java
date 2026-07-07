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

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOf;

import com.github.benmanes.caffeine.cache.Weigher;
import io.github.hyshmily.hotkey.cache.annotationsupporter.NullValue;
import io.github.hyshmily.hotkey.model.CacheEntry;
import java.util.Collection;
import java.util.Map;
import org.jspecify.annotations.NonNull;

/**
 * Heap-weight estimator for {@link CacheEntry} values backed by Lucene's {@link
 * org.apache.lucene.util.RamUsageEstimator}.
 *
 * <p>Used when {@code hotkey.local.cache.max-weight} is set. Delegates object header / field /
 * alignment calculations to Lucene's {@code RamUsageEstimator} (which auto-detects compressed
 * OOPs, object alignment, and JVM pointer sizes), then adds conservative estimates for
 * variable-length data and Caffeine internal metadata.
 */
@SuppressWarnings("all")
public enum DefaultWeigher implements Weigher<String, Object> {
  INSTANCE;

  private static final int ENTRY_OVERHEAD = 512;
  private static final int COLLECTION_ELEMENT_WEIGHT = 200;
  private static final int MAP_ENTRY_WEIGHT = 350;

  @Override
  public int weigh(@NonNull String key, @NonNull Object value) {
    long keyWeight = shallowSizeOf(key) + ((long) key.length() << 1);
    long total = keyWeight + valueOf(value) + ENTRY_OVERHEAD;
    return (int) Math.min(total, Integer.MAX_VALUE);
  }

  private static long valueOf(Object v) {
    if (v instanceof CacheEntry ce) {
      return shallowSizeOf(ce) + valueOf(ce.getValue());
    }
    if (v instanceof NullValue) {
      return shallowSizeOf(v);
    }
    if (v instanceof String s) {
      return shallowSizeOf(s) + ((long) s.length() << 1);
    }
    if (v instanceof byte[] b) {
      return shallowSizeOf(b);
    }
    if (v instanceof Collection<?> c) {
      return shallowSizeOf(c) + (long) Math.max(1, c.size()) * COLLECTION_ELEMENT_WEIGHT;
    }
    if (v instanceof Map<?, ?> m) {
      return shallowSizeOf(m) + (long) Math.max(1, m.size()) * MAP_ENTRY_WEIGHT;
    }
    if (v instanceof Object[] a) {
      return shallowSizeOf(a) + (long) a.length * NUM_BYTES_OBJECT_REF;
    }
    return shallowSizeOf(v) + 1024;
  }
}
