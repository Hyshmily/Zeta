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
package io.github.hyshmily.zeta.sync.local;

/**
 * The sync handler's only write path: the framework owns the data-version guard,
 * invalidation tombstones, clear watermarks, dedup-flight kills and hook dispatch.
 *
 * <p>Every apply method consumes a reduced {@link Batch}; {@link #skip(SyncMessage)}
 * reports one covered value message. The handler never re-implements batch reduction or
 * version comparison. A custom handler that refuses a value message must call
 * {@link #skip(SyncMessage)} so the terminal {@code onRefreshSkipped} callback still
 * fires.
 */
public interface SyncContext {
  /**
   * Apply a value batch: deserialize a PUT payload or load a REFRESH through the cluster
   * loader, apply the loader's admission rule, guard the write and report the terminal
   * hook.
   *
   * @param batch the reduced batch whose winner is a PUT or REFRESH
   */
  void value(Batch.Value batch);

  /**
   * Apply a single-key INVALIDATE: record the tombstone and remove the resident atomically.
   *
   * @param message the invalidation message
   */
  void invalidate(SyncMessage message);

  /**
   * Apply a clear batch: record the table-wide and prefix watermarks, kill the covered
   * pre-clear flights and sweep the table once. A prefix clear at or below the table-wide
   * stamp is dropped, because the table-wide stamp already removes every key it could
   * target.
   *
   * @param batch the clear batch
   */
  void clear(Batch.Clear batch);

  /**
   * Report a value message that produced no effect: {@code onRefreshSkipped} for a PUT or
   * REFRESH, a DEBUG line for the removal types.
   *
   * @param message the refused message; never {@code null}
   */
  void skip(SyncMessage message);
}
