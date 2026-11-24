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
package io.github.hyshmily.zeta.heat;

import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import io.github.hyshmily.zeta.model.DecisionStamp;
import org.jspecify.annotations.Nullable;

/**
 * One cluster heat lease: the key, the Worker decision that granted it, and when this node
 * observed it. It carries no TTL and no value; those belong to the data plane.
 *
 * @param key             the physical key
 * @param stamp           the Worker decision stamp
 * @param grantedAtMonoMs the monotonic observation time
 */
public record HeatLease(String key, DecisionStamp stamp, long grantedAtMonoMs) {
  /**
   * @return whether the lease is a HOT grant
   */
  public boolean isHot() {
    return stamp.isHot();
  }

  /**
   * @return whether the lease is a COOL tombstone
   */
  public boolean isCool() {
    return stamp.isCool();
  }

  /**
   * The logical name of this lease's namespace, with its encoding delimiter removed: {@code users}
   * for both {@code users\u0000u-1} and {@code users:u-1}.
   *
   * @return the namespace name, or {@code null} when the key carries none
   */
  public @Nullable String namespace() {
    return CacheKeysPolicy.logicalNamespaceOf(key);
  }

  /** Why a cluster heat lease ended. */
  public enum EndReason {
    /** A Worker COOL decision revoked the lease. */
    COOL,
    /** The issuing Worker incarnation is gone or restarted. */
    INCARNATION_DEAD,
    /** The bounded lease table evicted the entry. */
    EVICTED,
  }
}
