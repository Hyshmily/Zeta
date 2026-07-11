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
package io.github.hyshmily.zeta.cache.fluentAPI;

import io.github.hyshmily.zeta.Zeta;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fluent write command for the HotKey cache.
 *
 * <p>Provides a builder-pattern API for write-through and invalidate
 * operations.  Created via {@link Zeta#write(String)}.
 *
 * <h3>Usage</h3>
 * <pre>
 *   hotKey.write("user:42")
 *       .withHardTtl(30_000)
 *       .putThrough(newValue, dbWriter);
 *
 *   hotKey.write("user:42")
 *       .invalidateAfterPut(dbMutation);
 *
 *   hotKey.write("user:42")
 *       .invalidate();
 * </pre>
 *
 * <p>Instances are single-use.
 */
public class ZetaWriteCommand<T> {

  private final Zeta zeta;
  private final String cacheKey;
  private long hardTtlMs = 0;
  private long softTtlMs = 0;
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * Creates a new write command bound to the given cache key.
   *
   * @param zeta   the HotKey facade
   * @param cacheKey the cache key to operate on
   */
  public ZetaWriteCommand(Zeta zeta, String cacheKey) {
    this.zeta = zeta;
    this.cacheKey = cacheKey;
  }

  /**
   * Override the hard TTL for this write operation.
   *
   * @param hardTtlMs hard TTL in milliseconds (0 = use configured default)
   * @return this command instance
   */
  public ZetaWriteCommand<T> withHardTtl(long hardTtlMs) {
    this.hardTtlMs = hardTtlMs;
    return this;
  }

  /**
   * Override the soft TTL for this write operation.
   *
   * @param softTtlMs soft TTL in milliseconds (0 = use configured default)
   * @return this command instance
   */
  public ZetaWriteCommand<T> withSoftTtl(long softTtlMs) {
    this.softTtlMs = softTtlMs;
    return this;
  }

  /**
   * Write-through: execute the writer, then update L1 and send.
   *
   * @param value  the value to cache
   * @param writer the data-source mutation to execute before caching
   */
  public void putThrough(T value, Runnable writer) {
    if (!executed.compareAndSet(false, true)) {
      throw new IllegalStateException("ZetaWriteCommand can only be executed once");
    }
    zeta.putThrough(cacheKey, value, writer, hardTtlMs, softTtlMs, true);
  }

  /**
   * Execute a mutation, then invalidate L1 and send.
   * Next {@code get()} will re-fetch from the reader.
   *
   * @param mutation the mutation to execute
   */
  public void putBeforeInvalidate(Runnable mutation) {
    if (!executed.compareAndSet(false, true)) {
      throw new IllegalStateException("ZetaWriteCommand can only be executed once");
    }
    zeta.invalidateAfterPut(cacheKey, mutation);
  }

  /**
   * Invalidate L1 and send an invalidation to peers.
   * Next {@code get()} will re-fetch from the reader.
   */
  public void invalidate() {
    if (!executed.compareAndSet(false, true)) {
      throw new IllegalStateException("ZetaWriteCommand can only be executed once");
    }
    zeta.invalidate(cacheKey);
  }
}
