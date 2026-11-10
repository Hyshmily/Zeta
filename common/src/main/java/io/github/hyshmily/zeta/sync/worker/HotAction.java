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
 * What a HOT decision should do after its prepass: promote one value or skip with a reason.
 *
 * <p>The framework resolves a HOT decision into this type, then hands it to
 * {@link WorkerPolicy#hot(WorkerDecision, HotValue)}. A policy may replace the resolved
 * value and must return the final action; the framework settles it through the guarded
 * write path, so the terminal hook is reported exactly once.
 */
public sealed interface HotAction permits HotAction.Promote, HotAction.Skip {
  /**
   * Promote the given value to HOT.
   *
   * @param value the value and the data version it carries; never {@code null}
   */
  record Promote(HotValue value) implements HotAction {
    public Promote {
      Objects.requireNonNull(value, "value must not be null");
    }
  }

  /**
   * Refuse the promotion and report the reason.
   *
   * @param reason why the promotion is skipped; never {@code null}
   */
  record Skip(HotSkipReason reason) implements HotAction {
    public Skip {
      Objects.requireNonNull(reason, "reason must not be null");
    }
  }
}
