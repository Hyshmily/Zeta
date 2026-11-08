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

import io.github.hyshmily.zeta.cache.cachesupport.loader.LoadStatus;
import java.io.Serial;
import lombok.Getter;

/**
 * A read-path load was suppressed by protection machinery (per-load timeout, executor
 * saturation).
 *
 * <p>Unlike "source returned null" (an empty read that is never cached), a suppression means the
 * data source was never consulted or never answered — mapping it to {@code null} would let 500s
 * masquerade as 404s. A timeout or executor rejection throws this exception; a breaker-open load
 * does not, because the caller serves the stale fallback instead. Keeps its stack trace (a
 * diagnostic, not hot-path control flow).
 */
@Getter
public class ZetaSuppressedException extends IllegalStateException {

  @Serial
  private static final long serialVersionUID = 1L;

  /** Which protection machinery suppressed the load. */
  private final LoadStatus status;

  /** The key whose load was suppressed. */
  private final String cacheKey;

  /**
   * Creates a suppression failure for the given key.
   *
   * @param cacheKey the key whose load was suppressed
   * @param status   the suppression reason (a {@code SUPPRESSED_*} status)
   */
  public ZetaSuppressedException(String cacheKey, LoadStatus status) {
    super("Load suppressed (" + status + ") for key=" + cacheKey);
    this.status = status;
    this.cacheKey = cacheKey;
  }
}
