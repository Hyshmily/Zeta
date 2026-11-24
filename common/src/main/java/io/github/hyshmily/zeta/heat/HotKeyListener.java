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
package io.github.hyshmily.zeta.heat;

import io.github.hyshmily.zeta.sync.worker.DecisionSkipReason;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;

/**
 * A best-effort observer of heat lease changes, for metrics, audit and business bookkeeping.
 *
 * <p><b>Observation only.</b> Unlike {@link HotKeyHandler}, a listener receives no
 * {@link HotKeyContext}: it cannot load, warm, cancel or write anything, and returning from a
 * callback changes nothing. Many listeners may be registered; each applied or skipped decision
 * reaches every one of them.
 *
 * <p><b>Ordering.</b> A decision-driven callback runs on the per-key ordered dispatcher, so events
 * for one key arrive in the order the framework applied them, and a replaced dead lease's
 * {@code onCool} runs before the new lease's {@code onHot}. Different keys run concurrently.
 *
 * <p><b>Passive ends are asynchronous.</b> A lease that ends without a Worker COOL — evicted from
 * the bounded table, or found dead by a cluster-heat query — reports {@code onCool} on the lease
 * table's notification executor, which is also the executor Caffeine runs its own maintenance on.
 * That callback can therefore arrive after a newer {@code onHot} for the same key, and it must not
 * block: blocking it stalls the lease table's expiry and eviction work. Fold such an event by
 * {@link HeatLease#stamp()} — ignore it unless it names the lease the listener currently holds.
 *
 * <p><b>Loss.</b> The Worker broadcasts each decision once, and neither a lost HOT nor a lost COOL
 * is replayed or reconciled. Safety-critical business state must therefore carry its own timeout
 * instead of assuming a COOL will arrive.
 */
@SuppressWarnings("unused")
public interface HotKeyListener {
  /**
   * A HOT decision was applied to the lease table. Fires for every applied HOT, including a repeat
   * broadcast that renews a live lease, so a listener that counts "newly hot keys" must gate on
   * {@code newlyHot} itself.
   *
   * @param decision the applied HOT decision
   * @param newlyHot whether this grant changed the key from cold to hot; {@code false} for a repeat
   *                 broadcast or a takeover that continued a live lease
   */
  default void onHot(WorkerDecision decision, boolean newlyHot) {}

  /**
   * A live HOT lease ended. Fires only when there was a lease to end. A COOL that merely wrote its
   * tombstone — a key this node never held, or one already past its lease — fires neither this
   * method nor {@link #onCoolSkipped}: the decision was applied (it advanced the tombstone) and had
   * no lease to report. {@link HotKeyHandler#onCool} still runs for it. The reason distinguishes a
   * cluster COOL from an incarnation death and a table eviction, so a listener can tell "the cluster
   * cooled it" from "we dropped the lease".
   *
   * @param lease  the lease that ended, with its key, version and epoch
   * @param reason why it ended
   */
  default void onCool(HeatLease lease, HeatLease.EndReason reason) {}

  /**
   * A HOT decision was refused before it could change the lease table. Nothing was applied, so no
   * {@code onHot} follows.
   *
   * @param decision the refused HOT decision
   * @param reason   why it was refused (a stale version, a decision covered by a newer one in the
   *                 same batch, or an issuing incarnation already dead)
   */
  default void onHotSkipped(WorkerDecision decision, DecisionSkipReason reason) {}

  /**
   * A COOL decision was refused before it could end a lease. Nothing was applied, so no
   * {@code onCool} follows: a COOL from a previous Worker owner that would kill a lease a
   * different, live incarnation still owns lands here rather than ending that lease.
   *
   * @param decision the refused COOL decision
   * @param reason   why it was refused (a stale version, a ghost COOL, or a decision covered by a
   *                 newer one in the same batch)
   */
  default void onCoolSkipped(WorkerDecision decision, DecisionSkipReason reason) {}
}
