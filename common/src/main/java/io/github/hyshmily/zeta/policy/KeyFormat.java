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
package io.github.hyshmily.zeta.policy;

import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import org.jspecify.annotations.Nullable;

/**
 * The resolved physical key format of one namespace.
 *
 * <p>This enum carries no {@code INHERIT}: an annotation declaration uses {@link Option} and is
 * resolved against the class/global default before a plan is built, so an unresolved value never
 * reaches the runtime path.
 */
public enum KeyFormat {
  /**
   * Type-tagged, length-prefixed, self-delimiting encoding ({@code namespace + '\u0000' + key}).
   * Supports multi-argument default keys and cannot collide across argument boundaries or
   * scalar types.
   */
  TYPED,

  /**
   * Human-readable single-scalar encoding ({@code namespace + ':' + key}). A namespace that
   * chooses it must lock one logical key type and must not use a multi-argument default key.
   */
  PLAIN;

  /**
   * The delimiter between the namespace and the logical key.
   *
   * @return {@code ':'} for {@link #PLAIN}, otherwise U+0000
   */
  public char delimiter() {
    return this == PLAIN ? CacheKeysPolicy.PLAIN_DELIMITER : CacheKeysPolicy.NAMESPACE_DELIMITER;
  }

  /** The declaration vocabulary; the runtime sees the resolved {@link KeyFormat} only. */
  public enum Option {
    /** Use the class-level or global default. */
    INHERIT(null),
    /** Use the type-tagged default-key encoding. */
    TYPED(KeyFormat.TYPED),
    /** Use the single-scalar, human-readable encoding. */
    PLAIN(KeyFormat.PLAIN);

    private final @Nullable KeyFormat format;

    Option(@Nullable KeyFormat format) {
      this.format = format;
    }

    /**
     * Resolve this declaration to a runtime format.
     *
     * @return the concrete format, or {@code null} for {@link #INHERIT}
     */
    public @Nullable KeyFormat toFormat() {
      return format;
    }
  }
}
