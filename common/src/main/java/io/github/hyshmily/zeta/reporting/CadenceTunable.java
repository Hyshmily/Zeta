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
package io.github.hyshmily.zeta.reporting;

import io.github.hyshmily.zeta.util.Internal;
import java.util.function.Consumer;

/**
 * A {@link KeyReporter} whose feed-loop tuner can drive an external tide cadence.
 *
 * <p>The access counter owns the tide the reporter samples; this capability is the explicit seam
 * between them, so wiring does not depend on the concrete reporter implementation.
 */
@Internal
public interface CadenceTunable {
  /**
   * Bind the sink the feed-loop tuner applies its computed interval to.
   *
   * @param adjuster the interval sink, or {@code null} to stop tuning
   */
  void setCadenceAdjuster(Consumer<Long> adjuster);
}
