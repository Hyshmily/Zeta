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
package io.github.hyshmily.zeta.sync.dispatcher;

import io.github.hyshmily.zeta.Internal;

/**
 * Read-only snapshot of a {@link PerKeyOrderedDispatcher}'s overload gate and backlog.
 *
 * <p>Mirrors the introspection LMAX Disruptor exposes on its queue structure
 * ({@code Sequencer#remainingCapacity()}, {@code getBufferSize()}, and
 * {@code ConsumerRepository#hasBacklog(long, boolean)}) so that a producer-facing capacity limit
 * can be told apart from "no traffic" without waiting for the gate to actually close
 * (ADR-0072, decision D-1).
 *
 * <p>The snapshot is taken at one instant; the dispatcher keeps mutating, so the values are
 * indicative rather than a consistent point-in-time view. {@link #pendingUnits()} is exact at the
 * instant of the {@code sum()} it reads, and {@link #activeKeys()} is a
 * {@link java.util.concurrent.ConcurrentHashMap#size()} estimate.
 *
 * @param pendingUnits  weighted units currently charged to the gate (submitted, not yet executed)
 * @param maxPendingUnits configured gate capacity in the same weighted units
 * @param activeKeys    keys whose worker currently holds work (running or queued)
 * @param dropped       cumulative submissions dropped by the global gate
 * @param rejected      cumulative submissions rejected because the key's own queue was full
 */
@Internal
public record DispatcherStats(
  long pendingUnits,
  long maxPendingUnits,
  int activeKeys,
  long dropped,
  long rejected
) {
  /**
   * Weighted units still admissible before the global gate starts dropping.
   *
   * <p>Clamped at zero: a concurrent submitter may have charged the gate past
   * {@code maxPendingUnits} after the snapshot was read, and a negative "remaining capacity"
   * would be meaningless to a metric consumer.
   *
   * @return the remaining gate capacity in weighted units, never negative
   */
  public long remainingUnits() {
    return Math.max(0L, maxPendingUnits - pendingUnits);
  }

  /**
   * Whether any submitted task is still pending — the analogue of Disruptor's
   * {@code hasBacklog()}.
   *
   * @return {@code true} if at least one task is charged to the gate
   */
  public boolean backlogged() {
    return pendingUnits > 0L;
  }
}
