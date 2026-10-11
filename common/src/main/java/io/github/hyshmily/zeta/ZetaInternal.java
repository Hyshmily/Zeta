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
package io.github.hyshmily.zeta;

import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.exception.ZetaModeException;

/**
 * Internal extension of the {@link Zeta} facade for the Spring Cache adapter.
 *
 * <p>Carries the annotation-path peek ({@link #peekAndTag(String)}) that was
 * previously a public (though {@code @Internal}-marked) member of
 * {@link Zeta}: a served hit feeds hot-key detection and Worker reporting
 * with a single hash lookup plus a single rule evaluation. Application code
 * must prefer {@link Zeta#peek(String)} (side-effect-free) or
 * {@link Zeta#get} (loads on miss) — the detecting peek is reserved for
 * {@code ZetaSpringCache.lookup}.
 *
 * <p>The sole implementation is {@link DefaultZeta}. Custom {@link Zeta}
 * implementations backing the annotation integration must implement this
 * interface; {@code ZetaSpringCache} prefers it at runtime and falls back to
 * the deprecated {@link Zeta#peekAndTag(String)} only for legacy impls.
 */
@Internal
public interface ZetaInternal extends Zeta {
  /**
   * Annotation-path peek for the Spring Cache adapter: look up a cached value
   * <b>and</b> feed hot-key detection and Worker reporting when a hit is
   * served.
   *
   * @param cacheKey the key to look up
   * @return the unwrapped user value,
   *         {@link io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue#INSTANCE}
   *         for a cached-null sentinel hit, or {@code null} on a true miss;
   *         detection/reporting fire only on the two hit forms
   * @throws ZetaModeException    when no cache is available (Worker-only mode)
   * @throws ZetaBlockedException when the key matches a blacklist rule
   */
  Object peekAndTag(String cacheKey);
}
