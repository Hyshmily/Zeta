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
 * Strategy interface for processing cache-sync batches received from peer application
 * instances.
 *
 * <p>All methods are defaults that run the framework's standard pipeline through
 * {@link SyncContext}. Override only the operation whose policy you need to change; the
 * others keep the default. The framework owns per-key ordering, batch reduction, version
 * comparison, invalidation tombstones, clear watermarks and hook dispatch — a policy
 * never sees an AMQP message or a cache entry.
 *
 * <p>A value batch carries PUT/REFRESH messages and is already reduced to its winner; an
 * invalidate batch is a value batch whose winner is an INVALIDATE; a clear batch carries
 * every table-wide and prefix clear of the granted window. The context applies all three
 * under the same guards.
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to replace the
 * default behaviour; {@code @ConditionalOnMissingBean} wires it.
 */
public interface SyncPolicy {
  /**
   * Handle a value batch: a PUT payload decode or a REFRESH authoritative load.
   *
   * @param batch   the reduced batch whose winner is a PUT or REFRESH
   * @param context the framework's guarded write path
   */
  default void value(Batch.Value batch, SyncContext context) {
    context.value(batch);
  }

  /**
   * Handle a single-key INVALIDATE.
   *
   * @param batch   the reduced batch whose winner is an INVALIDATE
   * @param context the framework's guarded write path
   */
  default void invalidate(Batch.Value batch, SyncContext context) {
    context.invalidate(batch.winner());
  }

  /**
   * Handle a clear batch. A table-wide clear and one or more prefix clears share the
   * batch and are applied in one sweep; a prefix clear already covered by the table-wide
   * stamp is dropped by the context.
   *
   * @param batch   the clear batch
   * @param context the framework's guarded write path
   */
  default void clear(Batch.Clear batch, SyncContext context) {
    context.clear(batch);
  }
}
