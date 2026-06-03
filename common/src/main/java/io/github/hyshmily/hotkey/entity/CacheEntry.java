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
package io.github.hyshmily.hotkey.entity;

import lombok.Builder;
import lombok.Getter;

/**
 * A value stored in the L1 cache together with its versions (dataVersion,
 * decisionVersion), TTL metadata,
 * and hot-key state.
 *
 * <p>{@code isVersionDegraded} indicates whether the {@code dataVersion} was obtained
 * from Redis INCR (normal) or fell back to node-local counter ({@code Long.MIN_VALUE + counter})
 * (degraded) — see {@code VersionResult} for the degraded-detection logic
 * used during broadcast reception.
 * <p>{@code decisionVersion} tracks Worker HOT/COOL decisions and is always
 * monotonically increasing (never degraded).
 * <p>The normal-state TTLs recorded at entry creation are preserved across state
 * transitions in {@code normalHardTtlMs} and {@code normalSoftTtlMs}.
 */
@Getter
@Builder
public class CacheEntry {

  private final Object value;
  private final long dataVersion;
  private final boolean isVersionDegraded;
  private final long decisionVersion;
  private final long hardTtlMs;
  private final long hardExpireAtMs;
  private final long softTtlMs;
  private final long softExpireAtMs;
  private final KeyState keyState;
  /** Normal-state hard TTL recorded at entry creation, preserved across HOT/COOL transitions. */
  private final long normalHardTtlMs;
  /** Normal-state soft TTL recorded at entry creation, preserved across HOT/COOL transitions. */
  private final long normalSoftTtlMs;
}
