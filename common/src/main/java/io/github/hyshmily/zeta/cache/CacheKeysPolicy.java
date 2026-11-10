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
package io.github.hyshmily.zeta.cache;

import io.github.hyshmily.zeta.util.Internal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Static utility for validating cache key inputs across the library.
 * <p>
 * Defines the contract for what constitutes a valid cache key: keys must be
 * non-null and non-blank. Validation happens once, at each entry boundary: the
 * facade asserts its single-key arguments (fail-fast with
 * {@code IllegalArgumentException}) and filters its collection arguments
 * through {@link #validKeys} (invalid elements are skipped); the annotation
 * integration rejects a null logical key and prefixes a non-blank namespace;
 * and the sync layer guards the wire. Engine methods ({@link HotKeyCache},
 * {@link AccessCounter}) therefore assume valid keys on every path —
 * single-key and collection alike.
 */
@Internal
public final class CacheKeysPolicy {

  /** Delimiter between an annotation namespace and the logical key it addresses. */
  public static final char NAMESPACE_DELIMITER = '\u0000';

  /**
   * Builds the physical key prefix of one annotation namespace.
   *
   * @param cache the namespace name (non-null, non-blank)
   * @return {@code cache + NAMESPACE_DELIMITER}
   */
  public static String namespacePrefix(String cache) {
    return cache + NAMESPACE_DELIMITER;
  }

  /**
   * Checks whether the given cache key is invalid (null or blank).
   *
   * @param cacheKey the key to validate (may be {@code null})
   * @return {@code true} if the key is {@code null} or blank
   */
  public static boolean invalidCacheKey(String cacheKey) {
    return cacheKey == null || cacheKey.isBlank();
  }

  /**
   * Filters a caller-supplied key collection down to its valid elements,
   * preserving the caller's order and any duplicate. The facade's collection
   * verbs call this before delegating, so the engine below never re-checks a
   * key: a collection whose every element is invalid becomes an empty list,
   * which the facade treats as a no-op.
   *
   * @param cacheKeys the keys to filter (never {@code null}; {@code null}
   *                  elements are allowed and dropped)
   * @return a new list holding only the non-null, non-blank keys
   */
  public static List<String> validKeys(Collection<String> cacheKeys) {
    List<String> keys = new ArrayList<>(cacheKeys.size());
    for (String key : cacheKeys) {
      if (!invalidCacheKey(key)) {
        keys.add(key);
      }
    }
    return keys;
  }

  /**
   * Utility class — no instantiation.
   */
  private CacheKeysPolicy() {}
}
