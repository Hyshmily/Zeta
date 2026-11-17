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

import java.util.Objects;

/**
 * What a COOL decision should do: apply the downgrade or skip it with a reason.
 *
 * <p>The framework settles the action through the guarded write path, so the terminal
 * {@link WorkerDecisionHook#onCoolSkipped} or
 * {@link WorkerDecisionHook#afterCoolDowngrade} callback is reported exactly once.
 */
public sealed interface CoolAction permits CoolAction.Downgrade, CoolAction.Skip {
  /** Apply the COOL downgrade. */
  record Downgrade() implements CoolAction {}

  /**
   * Refuse the downgrade and report the reason.
   *
   * @param reason why the downgrade is skipped; never {@code null}
   */
  record Skip(CoolSkipReason reason) implements CoolAction {
    public Skip {
      Objects.requireNonNull(reason, "reason must not be null");
    }
  }

  CoolAction DOWNGRADE = new Downgrade();
  CoolAction REJECT = new Skip(CoolSkipReason.POLICY_REJECTED);
}
