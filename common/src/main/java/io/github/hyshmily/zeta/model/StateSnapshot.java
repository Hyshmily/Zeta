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
package io.github.hyshmily.zeta.model;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import lombok.Builder;
import lombok.experimental.Accessors;

/**
 * Immutable snapshot of a key's state machine state at a point in time.
 *
 * <p>Captured <em>before</em> a mutation in the state machine's
 * {@code evaluate} method and carried by {@link ZetaDecision} for
 * failure rollback.  The
 * {@code key} field enables the single-argument overload of
 * {@link ZetaBayesianSM#rollbackToPreviousState(ZetaBayesianSM.StateSnapshot)}.
 *
 * <p>Fluent accessors ({@code key()}, {@code currentState()}, etc.) preserve
 * the same API as the previous {@code record} representation.
 */
@Accessors(fluent = true)
@Builder
public record StateSnapshot(String key, String currentState, int hotStreak, int coolStreak) {}
