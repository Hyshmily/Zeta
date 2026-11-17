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

import io.github.hyshmily.zeta.cache.EntryView;
import org.jspecify.annotations.Nullable;

/**
 * Strategy interface for Worker HOT/COOL decisions.
 *
 * <p>The framework owns the whole lifecycle: per-key ordering, version comparison,
 * invalidation tombstones, ghost-COOL ownership, TTL arithmetic, the value load and the
 * terminal {@link WorkerDecisionHook} callback. A policy only decides what the framework
 * should do with an already-resolved decision, so it cannot forget to settle a HOT or fire
 * a skip hook.
 *
 * <p>A policy runs on the ordered dispatcher, never on the AMQP consumer thread. The
 * {@link EntryView} it receives is a read-only snapshot; the framework re-checks every
 * version and tombstone when it applies the returned action.
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to replace the
 * default behaviour; {@code @ConditionalOnMissingBean} wires it.
 */
public interface WorkerPolicy {
  /**
   * Decide what to do with a HOT decision whose value has already been resolved.
   *
   * <p>Return {@link HotAction.Promote} to apply the value (the policy may replace it with
   * {@link HotValue#supplied}; the replacement still passes the framework's version and
   * invalidation-tombstone guards), or {@link HotAction.Skip} to refuse it. The framework
   * reports exactly one terminal hook for the decision.
   *
   * @param decision the HOT decision; never {@code null} and never COOL
   * @param value    the resolved candidate value; never {@code null}
   * @param current  the resident snapshot taken when the policy is called, or {@code null}
   * @return the action to settle; never {@code null}
   */
  default HotAction hot(WorkerDecision decision, HotValue value, @Nullable EntryView current) {
    return new HotAction.Promote(value);
  }

  /**
   * Decide whether a COOL decision should be applied.
   *
   * @param decision the COOL decision; never {@code null} and never HOT
   * @param current  the current resident snapshot, or {@code null} when the key is absent
   * @return {@link CoolAction#DOWNGRADE} to apply, or a skip with its reason
   */
  default CoolAction cool(WorkerDecision decision, @Nullable EntryView current) {
    return CoolAction.DOWNGRADE;
  }
}
