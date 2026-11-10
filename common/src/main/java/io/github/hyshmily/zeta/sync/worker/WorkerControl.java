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
package io.github.hyshmily.zeta.sync.worker;

import java.util.List;

/**
 * Framework-only control plane of the Worker context. Kept out of the public SPI: the
 * listener uses it for batch reduction and the consumer-thread rejection fallback, while a
 * {@link WorkerPolicy} sees only the decision it is asked about.
 */
interface WorkerControl {
  /**
   * Reduce a consecutive COOL run: only the decision that survives the version guard can
   * affect the final state. Covered decisions are reported through
   * {@link WorkerDecisionHook#onCoolSkipped} here.
   *
   * <p>A run whose messages come from different Worker nodes is returned unchanged: the
   * ghost-COOL guard depends on the resident's owner and the sender's node, so an earlier
   * same-owner COOL can legitimately disable the guard for a later cross-node COOL.
   *
   * @param batch the COOL run, never empty, all messages for the same key
   * @return the effective decisions to hand to the policy; empty when every decision is
   *         already outranked
   */
  List<WorkerDecision> reduceCool(List<WorkerDecision> batch);

  /**
   * Consumer-thread fallback for a COOL the ordered dispatcher refused: the same in-memory
   * compute without dispatching hooks, because the AMQP consumer thread must never wait on
   * user code.
   *
   * @param decision the COOL decision to apply inline; must not be null and must be COOL
   */
  void coolNow(WorkerDecision decision);
}
