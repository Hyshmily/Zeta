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
 * are weighted by their UTF‑8 byte length (estimated without allocation).
 * Raw {@code byte[]} values are weighted by their length. All other types get
 * a flat {@code 1024} byte estimate. The {@link CacheEntry} wrapper overhead
 * (~200 bytes) is added on top.
 */
@SuppressWarnings("all")
public final class DefaultWeigher implements Weigher<String, Object> {

  public static final DefaultWeigher INSTANCE = new DefaultWeigher();
  private static final int ENTRY_OVERHEAD = 200;

  private DefaultWeigher() {}

  @Override
  public int weigh(String key, @NonNull Object value) {
    int valueWeight = valueOf(value);
    return (key.length() << 1) + valueWeight + ENTRY_OVERHEAD;
  }

  private static int valueOf(Object v) {
    if (v instanceof String s) return utf8Length(s);
    if (v instanceof byte[] b) return b.length;
    if (v instanceof CacheEntry ce) return valueOf(ce.getValue());
    return 1024;
  }

  /**
   * Estimates the number of bytes a {@link String} would occupy when
   * encoded in UTF‑8, without actually allocating a byte array.
   */
  private static int utf8Length(String s) {
    if (isAscii(s)) {
      return s.length();
    }
    int length = 0;
    final int len = s.length();
    for (int i = 0; i < len; i++) {
      char c = s.charAt(i);
      if (c < 0x80) {
        length++;
      } else if (c < 0x800) {
        length += 2;
      } else if (Character.isSurrogate(c)) {
        length += 4;
        i++;
      } else {
        length += 3;
      }
    }
    return length;
  }

  private static boolean isAscii(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) > 0x7F) {
        return false;
      }
    }
    return true;
  }
}
