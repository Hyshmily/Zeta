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

import io.github.hyshmily.zeta.detection.ZetaStateMachine;

/**
 * Immutable snapshot of a key's state machine state at a point in time.
 *
 * <p>Captured <em>before</em> a mutation in the state machine's
 * {@code evaluate} method and carried by {@link ZetaDecision} for
 * failure rollback.  The
 * {@code key} field enables the single-argument overload of
 * {@link ZetaStateMachine#rollbackToPreviousState(ZetaStateMachine.StateSnapshot)}.
 *
 * @param key          the cache key (never {@code null})
 * @param currentState the lifecycle stage at snapshot time ({@link
 *                     ZetaStateMachine.State} enum name, never {@code null})
 * @param hotStreak    consecutive hot-window count at snapshot time
 * @param coolStreak   consecutive cold-window count at snapshot time
 */
public record StateSnapshot(String key, String currentState, int hotStreak, int coolStreak) {}
