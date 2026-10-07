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

/**
 * A public, read-only view of one locally-detected hot key.
 *
 * <p>This is the facade-facing shape for the local TopK statistics. It exists
 * so that observation APIs never leak the detector's internal
 * {@code Item} record: the internal type couples the key to detector-internal
 * bookkeeping (slot identity, admission metadata) that is both meaningless and
 * unsafe to expose — a caller that mutated it would corrupt the detector's
 * counting structure.
 *
 * @param key   the cache key, never {@code null}
 * @param count the estimated access count for the key within the current
 *              detection window
 */
public record HotKey(String key, long count) {}
