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

import io.github.hyshmily.zeta.cache.cachesupport.LoadStatus;
import lombok.Getter;

/**
 * A read-path load was suppressed by protection machinery (circuit breaker
 * open, per-load timeout, executor saturation) while the caller demanded
 * fail-fast semantics.
 *
 * <p>Unlike "source returned null" (an empty result cached as a
 * sentinel entry), a suppression means the data source was never
 * consulted or never answered — mapping it to {@code null} would let 500s
 * masquerade as 404s. Thrown only when {@code failOnError} is set; the
 * default swallow path still resolves suppressed loads to empty. Keeps its
 * stack trace (a fail-fast diagnostic, not hot-path control flow — contrast
 * {@link ZetaBlockedException}).
 */
@Getter
public class ZetaSuppressedException extends ZetaContextException {

  /** Which protection machinery suppressed the load. */
  private final LoadStatus status;

  /**
   * Creates a suppression failure for the given key.
   *
   * @param sourceClass the simple name of the throwing class
   * @param cacheKey    the key whose load was suppressed
   * @param status      the suppression reason (a SUPPRESSED_* status)
   */
  public ZetaSuppressedException(String sourceClass, String cacheKey, LoadStatus status) {
    super(sourceClass, "Load suppressed (" + status + ") for key=" + cacheKey, null);
    this.status = status;
  }
}
