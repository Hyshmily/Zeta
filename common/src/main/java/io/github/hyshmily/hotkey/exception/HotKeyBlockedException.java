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
package io.github.hyshmily.hotkey.exception;

import lombok.Getter;

/**
 * Thrown when a cache get operation is blocked by a blacklist rule.
 * <p>
 * Unlike returning {@link java.util.Optional#empty()}, throwing prevents
 * callers from silently bypassing the block via
 * {@link java.util.Optional#orElseGet}.  The calling code must either
 * catch this exception or let it propagate.
 */
@Getter
public class HotKeyBlockedException extends RuntimeException {

  /**
   * -- GETTER --
   *  Return the key that was blocked.
   *
   */
  private final String cacheKey;

  /**
   * Creates a new exception for the blocked key.
   *
   * @param cacheKey the key that was blocked by a blacklist rule
   */
  public HotKeyBlockedException(String cacheKey) {
    super("Cache key blocked by rule: " + cacheKey);
    this.cacheKey = cacheKey;
  }
}
