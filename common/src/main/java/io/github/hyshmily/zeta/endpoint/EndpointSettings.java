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
package io.github.hyshmily.zeta.endpoint;

import io.github.hyshmily.zeta.Internal;

/**
 * Read-only configuration view for the Actuator endpoints (ADR-0082
 * pattern): the endpoint package depends on this interface — owned here,
 * where it is consumed — instead of the assembly package's
 * {@code ZetaProperties}, so the dependency points one way
 * (autoconfigure → endpoint).
 *
 * <p>All reads are live (per scrape), matching the previous direct reads.
 */
@Internal
public interface EndpointSettings {

  /** L1 Caffeine maximum entry count ({@code zeta.local.cache.max-size}). */
  int cacheMaxSize();

  /** L1 Caffeine maximum weight ({@code zeta.local.cache.max-weight}, 0 = unbounded). */
  long cacheMaxWeight();

  /** SingleFlight dedup map bound ({@code zeta.local.inflight-max-size}). */
  int inflightMaxSize();

  /** SingleFlight entry TTL in seconds ({@code zeta.local.inflight-ttl-seconds}). */
  int inflightTtlSeconds();

  /** SingleFlight load timeout in seconds ({@code zeta.local.inflight-timeout-seconds}). */
  int inflightTimeoutSeconds();

  /** Null/cache-miss sentinel TTL in seconds ({@code zeta.local.null-value-ttl-seconds}). */
  int nullValueTtlSeconds();
}
