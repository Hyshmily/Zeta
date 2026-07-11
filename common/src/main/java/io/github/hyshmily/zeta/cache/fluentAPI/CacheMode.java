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
package io.github.hyshmily.zeta.cache.fluentAPI;

import io.github.hyshmily.zeta.Zeta;

/**
 * Cache access mode for {@link ZetaReadQuery}.
 *
 * <p>Controls whether the read query uses
 * {@link Zeta#get} or
 * {@link Zeta#getWithSoftExpire} semantics.
 */
public enum CacheMode {
  /** Standard cache read — returns immediately if the key is not in L1. */
  GET,

  /**
   * Soft-expiry read — returns a stale entry if available while a background
   * refresh is triggered.  Useful for read-heavy workloads where latency is
   * more critical than absolute freshness.
   */
  GET_WITH_SOFT_EXPIRE,
}
