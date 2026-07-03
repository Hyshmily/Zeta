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
package io.github.hyshmily.hotkey.cache.loader;

import io.github.hyshmily.hotkey.Internal;

/**
 * Strategy interface for loading a cache entry value from the underlying
 * data store (typically Redis L2) by key.
 *
 * <p>Replaces the earlier {@link java.util.function.Function Function&lt;String, Object&gt;}
 * contract with a named SPI that is easier to trace, decorate, and compose.
 *
 * <p>Implementations must be thread-safe. Returning {@code null} signals
 * that the key does not exist in the data store.
 */
@Internal
public interface CacheLoader {

  /**
   * Load the value for the given cache key from the underlying store.
   *
   * @param cacheKey the fully qualified cache key; never {@code null}
   * @return the cached value, or {@code null} if absent
   */
  Object load(String cacheKey);
}
