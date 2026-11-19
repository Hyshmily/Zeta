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
package io.github.hyshmily.zeta.annotation;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * One cache-side failure handed to {@link CacheFailurePolicy}. The record is
 * created only on the failure path and is never retained past the callback.
 *
 * @param verb             the failed operation
 * @param cache            the annotation namespace
 * @param keys             the physical keys the operation addressed; one key for
 *                         a single write/invalidation, every attempted key for a
 *                         batch, and empty for a namespace clear
 * @param value            the domain value of a failed write, or {@code null}
 * @param error            the engine exception
 * @param beforeInvocation whether a removal/clear ran before the method body
 */
public record CacheFailure(
  CacheVerb verb,
  String cache,
  List<String> keys,
  @Nullable Object value,
  RuntimeException error,
  boolean beforeInvocation
) {
  public CacheFailure {
    keys = List.copyOf(keys);
  }
}
