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
package io.github.hyshmily.zeta.sync.local;

/**
 * Why a sync event produced no value/invalidate/clear effect. One reason per code path so
 * the hook and a Micrometer tag can distinguish them.
 */
public enum SyncSkipReason {
  /** The live resident already carries a strictly newer data version. */
  VERSION_STALE,

  /** A per-key tombstone or a table/prefix clear watermark already covers the event. */
  INVALIDATION_TOMBSTONE,

  /** A REFRESH loader returned no cacheable value. */
  VALUE_NOT_FOUND,

  /** A PUT payload could not be decoded. */
  PAYLOAD_UNREADABLE,

  /** The loader's {@code isCacheable} declined the resolved or policy-supplied value. */
  VALUE_DECLINED_BY_LOADER,

  /** A higher-version message in the same granted batch superseded this one. */
  COVERED_BY_NEWER_EVENT,

  /** The policy returned {@link SyncAdmission#SKIP} or {@link SyncValueAction#SKIP}. */
  POLICY_REJECTED,

  /** The policy returned an invalid action or threw; the event was settled as a skip. */
  POLICY_FAILED,

  /** The policy asked to drop the value path; the framework invalidated locally instead. */
  DEGRADED_TO_INVALIDATE,

  /** A peer REFRESH targeted an absent key that is not eligible for cold prewarm. */
  ABSENT_NOT_PREWARMABLE,

  /** An absent key belongs to a dynamic-TTL namespace with no static TTL baseline. */
  TTL_NOT_PREWARMABLE,

  /** The async refresh executor rejected the load under backpressure; degraded locally. */
  REFRESH_REJECTED,

  /** A peer REFRESH loader threw; the value event was degraded locally instead of retried. */
  REFRESH_FAILED,
}
