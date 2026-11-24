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
package io.github.hyshmily.zeta.sync.local;

import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One sync event as seen by {@link SyncPolicy} and {@link SyncHook}. It carries no payload and no
 * batch structure; a key operation holds the physical key, a table-wide clear holds {@code null},
 * and a prefix clear holds its prefix. {@link #key()} is therefore the one field whose meaning the
 * operation determines.
 *
 * @param id      the publishing message's trace id; orders nothing
 * @param key     the affected key, the clear prefix, or {@code null} for a table-wide clear
 * @param type    the wire operation
 * @param version the positive data stamp of the operation
 */
public record SyncEvent(long id, @Nullable String key, SyncType type, long version) {
  public SyncEvent {
    Objects.requireNonNull(type, "type must not be null");
    if (type == SyncType.INVALIDATE_ALL) {
      if (key != null) {
        throw new IllegalArgumentException("a table-wide clear must not carry a key");
      }
    } else if (key == null) {
      throw new IllegalArgumentException(type + " requires a key");
    }
  }

  /** Whether this event belongs to the clear family. */
  public boolean isClear() {
    return type.isClear();
  }

  /** Whether this event carries a value path: a PUT payload or a peer REFRESH reload. */
  public boolean isValue() {
    return type.isValue();
  }

  /** The prefix of a prefix clear, or {@code null} for every other operation. */
  public @Nullable String prefix() {
    return type == SyncType.INVALIDATE_PREFIX ? key : null;
  }

  /**
   * The logical namespace this event names, or {@code null} when it names none. A key event answers
   * with its key's namespace; a prefix clear answers only when the prefix starts with a complete
   * namespace (a prefix shorter than a name spans several, and a table-wide clear holds no key at
   * all). Inside one namespace a TYPED key and a PLAIN key answer with the same bare name.
   *
   * @return the namespace name, or {@code null}
   */
  public @Nullable String namespace() {
    return key == null ? null : CacheKeysPolicy.logicalNamespaceOf(key);
  }

  /**
   * Whether this event can affect {@code namespace}: a key event inside it, a table-wide clear, or
   * a prefix clear that overlaps it. The comparison is by logical name, so a trailing delimiter is
   * accepted and ignored.
   *
   * @param namespace the namespace name (non-null, non-blank)
   * @return {@code true} when the event may touch the namespace
   * @throws IllegalArgumentException when {@code namespace} is blank
   */
  public boolean affectsNamespace(String namespace) {
    Objects.requireNonNull(namespace, "namespace must not be null");
    // Strip first: a bare delimiter or blank-plus-delimiter is not a namespace name, and comparing
    // one would let a table-wide or prefix clear match every input.
    String name = CacheKeysPolicy.hasTrailingDelimiter(namespace)
      ? namespace.substring(0, namespace.length() - 1)
      : namespace;
    if (name.isBlank()) {
      throw new IllegalArgumentException("namespace must not be blank");
    }
    if (isClear()) {
      String prefix = prefix();
      return prefix == null || prefixAffects(prefix, name);
    }
    return name.equals(CacheKeysPolicy.logicalNamespaceOf(key));
  }

  /**
   * Whether a prefix clear can reach any key of {@code namespace}. The prefix either sits inside
   * the namespace (it starts with the name and the next character is a delimiter, so it cannot
   * reach a sibling such as {@code users} from {@code user}) or is a raw prefix of the name, in
   * which case it matches every key of the namespace.
   */
  private static boolean prefixAffects(String prefix, String namespace) {
    if (namespace.startsWith(prefix)) {
      return true;
    }
    if (!prefix.startsWith(namespace)) {
      return false;
    }
    return prefix.length() == namespace.length() || CacheKeysPolicy.isDelimiter(prefix.charAt(namespace.length()));
  }

  /**
   * Converts one decoded message to its policy/hook event.
   *
   * @param message the decoded message; must not be null
   * @return the event
   */
  public static SyncEvent from(SyncMessage message) {
    return from(message, message.version());
  }

  /**
   * Converts one decoded message to its policy/hook event at the version actually applied.
   *
   * @param message the decoded message; must not be null
   * @param version the version the mutation is applied at
   * @return the event
   */
  public static SyncEvent from(SyncMessage message, long version) {
    String key = message.type() == SyncType.INVALIDATE_ALL ? null : message.cacheKey();
    return new SyncEvent(message.id(), key, message.type(), version);
  }
}
