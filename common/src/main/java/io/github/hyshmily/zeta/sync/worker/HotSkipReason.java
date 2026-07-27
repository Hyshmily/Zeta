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

import io.github.hyshmily.zeta.Internal;

/**
 * Reason why a Worker HOT decision was not applied to the local cache.
 */
@Internal
public enum HotSkipReason {

  /** The SRE adaptive rate limiter dropped the request (backpressure). */
  SRE_THROTTLED,

  /** A newer decision version is already present in the L1 cache. */
  VERSION_STALE,

  /** Value not found in Redis and no degraded entry exists in L1. */
  VALUE_NOT_FOUND
}
