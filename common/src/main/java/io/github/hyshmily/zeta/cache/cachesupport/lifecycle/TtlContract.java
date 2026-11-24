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
package io.github.hyshmily.zeta.cache.cachesupport.lifecycle;

/**
 * The TTL contract one namespace declares.
 *
 * <p>A contract is a property of a namespace, not of a value: every entry of {@code quotes}
 * shares one hard and soft baseline. The registry records declarations and resolves them against
 * the live global configuration, so a cold prewarm path can construct an entry under the same
 * lifetime the application declared.
 *
 * <p>{@code dynamicTtl} marks a namespace whose baseline is not knowable without the original
 * invocation (a SpEL TTL, or two methods whose declarations disagree). Such a namespace can still
 * renew a resident entry from that entry's own stored durations, but it must not be prewarmed:
 * guessing the global default would create a value under a lifetime its owner never declared.
 *
 * @param hardTtlMs   the resolved normal hard TTL (positive, or {@link Long#MAX_VALUE})
 * @param softTtlMs   the resolved normal soft TTL ({@code 0} = disabled)
 * @param dynamicTtl  whether the baseline is unknowable at prewarm time
 */
public record TtlContract(long hardTtlMs, long softTtlMs, boolean dynamicTtl) {
  /**
   * Whether a cold node may load this namespace without an invocation.
   *
   * @return {@code true} when the declared TTL is static and unambiguous
   */
  public boolean prewarmable() {
    return !dynamicTtl;
  }
}
