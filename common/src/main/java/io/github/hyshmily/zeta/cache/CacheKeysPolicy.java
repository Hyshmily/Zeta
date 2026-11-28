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
import org.jspecify.annotations.Nullable;

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

  /** Delimiter between a TYPED annotation namespace and the logical key it addresses. */
  public static final char NAMESPACE_DELIMITER = '\u0000';

  /** Delimiter between a PLAIN annotation namespace and the logical key it addresses. */
  public static final char PLAIN_DELIMITER = ':';

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
   * The logical name of a key's namespace: the segment before the delimiter, without the delimiter
   * its encoding uses. A TYPED key ({@code users\u0000u-1}) and a PLAIN key ({@code users:u-1}) in
   * the same namespace both answer {@code users}, so a caller can compare a key against a namespace
   * name without knowing which layer produced it.
   *
   * @param cacheKey the physical key
   * @return the bare namespace name, or {@code null} when the key carries no namespace
   */
  public static @Nullable String logicalNamespaceOf(String cacheKey) {
    int idx = delimiterIndex(cacheKey);
    return idx < 0 ? null : cacheKey.substring(0, idx);
  }

  /**
   * The physical namespace prefix of a key, including the delimiter that belongs to its format.
   *
   * <p>{@link #logicalNamespaceOf(String)} drops the delimiter because a policy lookup by logical
   * name does not need it; a registry keyed by physical prefix does. The two forms must not be
   * mixed: a TYPED prefix ends with U+0000 and a PLAIN prefix ends with {@code ':'}.
   *
   * @param cacheKey the physical key
   * @return the prefix including its delimiter, or {@code null} when the key has no namespace
   */
  public static @Nullable String physicalPrefixOf(String cacheKey) {
    int idx = delimiterIndex(cacheKey);
    return idx < 0 ? null : cacheKey.substring(0, idx + 1);
  }

  /**
   * The bare prefix of a physical key: the same two-level split as {@link #logicalNamespaceOf},
   * without the annotation delimiter and with a caller-supplied fallback.
   *
   * <p>The circuit-breaker shard id and the namespace policy must resolve the same key to the same
   * bucket, so both use this one rule; the shard id is opaque and only drops the trailing
   * delimiter to keep the map key compact. This is {@link #logicalNamespaceOf(String)} with a
   * fallback instead of {@code null}.
   *
   * @param key           the physical key, or {@code null}
   * @param defaultBucket the bucket for a key with no namespace
   * @return the bare prefix, or {@code defaultBucket}
   */
  public static String prefixOf(@Nullable String key, String defaultBucket) {
    if (key == null) {
      return defaultBucket;
    }
    String namespace = logicalNamespaceOf(key);
    return namespace != null ? namespace : defaultBucket;
  }

  /**
   * The index of the delimiter that ends the namespace: the first of either form. The index is the
   * one every accessor slices at — the TYPED prefix keeps the delimiter at it, the PLAIN prefix
   * includes it — so no caller repeats the {@code -1} arithmetic. A delimiter at index zero means
   * the key has no non-empty prefix.
   *
   * @param key the physical key
   * @return the delimiter index, or {@code -1} when the key has no namespace
   */
  private static int delimiterIndex(String key) {
    for (int i = 0, len = key.length(); i < len; i++) {
      if (isDelimiter(key.charAt(i))) {
        return i > 0 ? i : -1;
      }
    }
    return -1;
  }

  /**
   * Whether {@code c} is one of the two namespace delimiters.
   *
   * @param c the character to test
   * @return whether it separates a namespace from its key
   */
  public static boolean isDelimiter(char c) {
    return c == NAMESPACE_DELIMITER || c == PLAIN_DELIMITER;
  }

  /**
   * Whether {@code text} contains either namespace delimiter, so it cannot be a namespace name.
   *
   * @param text the candidate name
   * @return whether a delimiter occurs anywhere in it
   */
  public static boolean containsDelimiter(String text) {
    return text.indexOf(NAMESPACE_DELIMITER) >= 0 || text.indexOf(PLAIN_DELIMITER) >= 0;
  }

  /**
   * Whether {@code text} ends with either namespace delimiter, so it is already a physical prefix.
   *
   * @param text the candidate prefix
   * @return whether its last character is a delimiter
   */
  public static boolean hasTrailingDelimiter(String text) {
    return !text.isEmpty() && isDelimiter(text.charAt(text.length() - 1));
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
