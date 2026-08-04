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

import io.github.hyshmily.zeta.Internal;

/**
 * A value loaded from the data source together with the {@code dataVersion}
 * probed from Redis after the value read (ADR-0033 — Read-Path Version
 * Stamping).
 *
 * <p>Carrier used by the read path (miss load, soft-expire refresh, null
 * sentinel) so the stamped version is never lower than the value's true
 * version, and the 4-case comparison ({@code VersionGuard.shouldSkipForSync})
 * can reject late stale broadcasts that a zero-stamped entry would admit.
 *
 * @param value      the loaded value; may be {@code null} (reader returned
 *                   null — a {@code NullValue} sentinel is stamped instead)
 * @param dataVersion the probed version; meaningful only when
 *                   {@code stamped} is {@code true}
 * @param stamped    whether the probe succeeded; {@code false} withholds the
 *                   stamp (fail-open) and callers fall back to the legacy
 *                   version handling
 */
@Internal
public record VersionedValue(Object value, long dataVersion, boolean stamped) {}
