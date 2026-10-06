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
package io.github.hyshmily.zeta.annotation.annotationsupporter;

import io.github.hyshmily.zeta.Internal;

/**
 * Read-only configuration view for the Spring-Cache integration layer
 * (ADR-0082 pattern): the annotation packages depend on this interface —
 * owned here, where it is consumed — instead of the assembly package's
 * {@code ZetaProperties}, so the dependency points one way
 * (autoconfigure → annotation).
 *
 * <p>Read semantics mirror the previous direct reads: the key separator is
 * snapshotted once at construction by each consumer (a later mutation of the
 * mutable properties bean must not make live components disagree on key
 * names), while the null-value TTL is read live on every null write.
 */
@Internal
public interface SpringCacheSettings {

  /**
   * Separator joining the Spring cache name and the resolved key
   * ({@code zeta.spring-cache.key-separator}, default {@code "::"}).
   *
   * @return the key separator, never {@code null} on a bound bean
   */
  String keySeparator();

  /**
   * Effective TTL for null/cache-miss sentinel entries in milliseconds.
   *
   * @return the null-value TTL in ms ({@link Long#MAX_VALUE} when disabled)
   */
  long effectiveNullTtlMs();
}
