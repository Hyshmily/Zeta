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
package io.github.hyshmily.hotkey.cache.fluentAPI;

import io.github.hyshmily.hotkey.HotKey;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fluent write command for the HotKey cache.
 *
 * <p>Provides a builder-pattern API for write-through and invalidate
 * operations.  Created via {@link HotKey#write(String)}.
 *
 * <h3>Usage</h3>
 * <pre>
 *   hotKey.write("user:42")
 *       .withHardTtl(30_000)
 *       .putThrough(newValue, dbWriter);
 *
 *   hotKey.write("user:42")
 *       .putBeforeInvalidate(dbMutation);
 *
 *   hotKey.write("user:42")
 *       .invalidate();
 * </pre>
 *
 * <p>Instances are single-use.
 */
public class HotKeyWriteCommand<T> {

  private final HotKey hotKey;
  private final String cacheKey;
  private long hardTtlMs = 0;
  private long softTtlMs = 0;
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * Creates a new write command bound to the given cache key.
   *
   * @param hotKey   the HotKey facade
   * @param cacheKey the cache key to operate on
   */
  public HotKeyWriteCommand(HotKey hotKey, String cacheKey) {
    this.hotKey = hotKey;
    this.cacheKey = cacheKey;
  }

  /**
   * Override the hard TTL for this write operation.
   *
   * @param hardTtlMs hard TTL in milliseconds (0 = use configured default)
   * @return this command instance
   */
  public HotKeyWriteCommand<T> withHardTtl(long hardTtlMs) {
    this.hardTtlMs = hardTtlMs;
    return this;
  }

  /**
   * Override the soft TTL for this write operation.
   *
   * @param softTtlMs soft TTL in milliseconds (0 = use configured default)
   * @return this command instance
   */
  public HotKeyWriteCommand<T> withSoftTtl(long softTtlMs) {
    this.softTtlMs = softTtlMs;
    return this;
  }

  /**
   * Write-through: execute the writer, then update L1 and broadcast.
   *
   * @param value  the value to cache
   * @param writer the data-source mutation to execute before caching
   */
  public void putThrough(T value, Runnable writer) {
    if (!executed.compareAndSet(false, true)) {
      throw new IllegalStateException("HotKeyWriteCommand can only be executed once");
    }
    hotKey.putThrough(cacheKey, value, writer, hardTtlMs, softTtlMs);
  }

  /**
   * Execute a mutation, then invalidate L1 and broadcast.
   * Next {@code get()} will re-fetch from the reader.
   *
   * @param mutation the mutation to execute
   */
  public void putBeforeInvalidate(Runnable mutation) {
    if (!executed.compareAndSet(false, true)) {
      throw new IllegalStateException("HotKeyWriteCommand can only be executed once");
    }
    hotKey.putBeforeInvalidate(cacheKey, mutation);
  }

  /**
   * Invalidate L1 and broadcast an invalidation to peers.
   * Next {@code get()} will re-fetch from the reader.
   */
  public void invalidate() {
    if (!executed.compareAndSet(false, true)) {
      throw new IllegalStateException("HotKeyWriteCommand can only be executed once");
    }
    hotKey.invalidate(cacheKey);
  }
}
