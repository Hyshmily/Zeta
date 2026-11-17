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

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One sync event as seen by {@link SyncPolicy} and {@link SyncHook}. This is the clean
 * event model: it carries no payload, no batch structure and no internal {@link Batch}.
 *
 * <p>A value/invalidate event is a {@link KeyEvent}; a table-wide or prefix clear is a
 * {@link ClearEvent}. The clear family is not per-key, so its event has an optional
 * prefix rather than a synthetic cache key.
 */
public sealed interface SyncEvent permits SyncEvent.KeyEvent, SyncEvent.ClearEvent {
  /** The publishing message's trace id; orders nothing. */
  long id();

  /** The wire operation. */
  SyncType type();

  /** The positive data stamp of the operation. */
  long version();

  /**
   * A single-key operation: INVALIDATE, REFRESH or PUT.
   *
   * @param id       the trace id
   * @param cacheKey the affected cache key; never {@code null}
   * @param type     the operation; never {@code null} and never a clear
   * @param version  the positive data stamp
   */
  record KeyEvent(long id, String cacheKey, SyncType type, long version) implements SyncEvent {
    public KeyEvent {
      Objects.requireNonNull(cacheKey, "cacheKey must not be null");
      Objects.requireNonNull(type, "type must not be null");
      if (type.isClear()) {
        throw new IllegalArgumentException("a KeyEvent cannot carry a clear type: " + type);
      }
    }
  }

  /**
   * A table-wide or prefix clear.
   *
   * @param id      the trace id
   * @param prefix  the physical key prefix, or {@code null} for a table-wide clear
   * @param type    INVALIDATE_ALL or INVALIDATE_PREFIX
   * @param version the positive clear stamp
   */
  record ClearEvent(long id, @Nullable String prefix, SyncType type, long version) implements SyncEvent {
    public ClearEvent {
      Objects.requireNonNull(type, "type must not be null");
      if (!type.isClear()) {
        throw new IllegalArgumentException("a ClearEvent must carry a clear type: " + type);
      }
    }
  }

  /**
   * Converts one decoded message to its policy/hook event.
   *
   * @param message the decoded message; must not be null
   * @return the matching event
   */
  static SyncEvent from(SyncMessage message) {
    return from(message, message.version());
  }

  /**
   * Converts one decoded message to its policy/hook event at the version actually applied.
   *
   * @param message the decoded message; must not be null
   * @param version the version the mutation is applied at
   * @return the matching event
   */
  static SyncEvent from(SyncMessage message, long version) {
    if (message.type().isClear()) {
      String prefix = message.type() == SyncType.INVALIDATE_PREFIX ? message.cacheKey() : null;
      return new ClearEvent(message.id(), prefix, message.type(), version);
    }
    return new KeyEvent(message.id(), message.cacheKey(), message.type(), version);
  }
}
