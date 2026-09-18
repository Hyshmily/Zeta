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
package io.github.hyshmily.zeta.exception;

import lombok.Getter;

/**
 * Thrown when a cache get operation is blocked by a blacklist rule.
 * <p>
 * Unlike returning {@link java.util.Optional#empty()}, throwing prevents
 * callers from silently bypassing the block via
 * {@link java.util.Optional#orElseGet}.  The calling code must either
 * catch this exception or let it propagate.
 * <p>
 * Blacklisted keys are typically high-QPS keys and this exception is
 * constructed per read, so it is built <b>without a stack trace</b>
 * ({@code writableStackTrace=false}, ADR-0063): the throw sites are fixed
 * guards inside {@code HotKeyCache} and {@link #getSourceClass()},
 * {@link #getCacheKey()} and {@link #getTimestamp()} already carry the full
 * diagnostics. {@code getStackTrace()} returns an empty array by design.
 */
@Getter
public class ZetaBlockedException extends ZetaContextException {

  /** The cache key that was blocked by a blacklist rule. */
  private final String cacheKey;

  /**
   * Creates a new exception for the blocked key.
   *
   * @param sourceClass the simple name of the class that detected the block
   * @param cacheKey    the key that was blocked by a blacklist rule
   */
  public ZetaBlockedException(String sourceClass, String cacheKey) {
    super(sourceClass, "Cache key blocked by rule: " + cacheKey, null, false, false);
    this.cacheKey = cacheKey;
  }
}
