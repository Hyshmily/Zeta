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
package io.github.hyshmily.hotkey.cache.annotationsupporter;

/**
 * Internal sentinel representing an explicitly cached {@code null} value.
 *
 * <p>Stored inside {@link io.github.hyshmily.hotkey.model.CacheEntry} when the caller caches a null (via
 * {@code @NullCaching(true)}). The sentinel is unwrapped back to {@code null}
 * when read, allowing the cache to distinguish "entry found but value is null"
 * from "no entry found".
 */
public final class NullValue {

  /** Singleton instance. */
  public static final NullValue INSTANCE = new NullValue();

  private NullValue() {}
}
