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

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The identity of one Worker heat decision: its operation and causal position.
 *
 * <p>The stamp is carried by a {@link io.github.hyshmily.zeta.heat.HeatLease}, never by a
 * {@code CacheEntry}. It records the operation together with its causal identity: the version
 * orders decisions of one Worker incarnation, the node id scopes the version space, the epoch
 * detects a Worker restart, and {@link #kind()} distinguishes a HOT grant from a COOL revoke.
 *
 * <p>This is the single value type for decision metadata: the Worker listener hands it to
 * {@code HeatDirectory}, the COOL-run reducer advances it, and {@code VersionGuard} compares it.
 * A {@code null} stamp is the local origin.
 *
 * @param decisionVersion the causal decision version; {@code 0} when unallocated
 * @param decisionNodeId  the Worker node ID that produced the decision, or
 *                        {@code null} when the message carried none
 * @param decisionEpoch   the epoch (restart counter) of the decision Worker
 * @param kind            the operation the lease establishes
 */
public record DecisionStamp(
  long decisionVersion,
  @Nullable String decisionNodeId,
  long decisionEpoch,
  DecisionKind kind
) {
  /** Rejects a null kind; the lease type is part of the wire contract. */
  public DecisionStamp {
    Objects.requireNonNull(kind, "kind must not be null");
  }

  /**
   * Creates a HOT decision lease.
   *
   * @param decisionVersion the causal decision version
   * @param decisionNodeId  the Worker node ID
   * @param decisionEpoch   the Worker incarnation epoch
   * @return a HOT stamp
   */
  public static DecisionStamp hot(long decisionVersion, @Nullable String decisionNodeId, long decisionEpoch) {
    return new DecisionStamp(decisionVersion, decisionNodeId, decisionEpoch, DecisionKind.HOT);
  }

  /**
   * Creates a COOL decision lease.
   *
   * @param decisionVersion the causal decision version
   * @param decisionNodeId  the Worker node ID
   * @param decisionEpoch   the Worker incarnation epoch
   * @return a COOL stamp
   */
  public static DecisionStamp cool(long decisionVersion, @Nullable String decisionNodeId, long decisionEpoch) {
    return new DecisionStamp(decisionVersion, decisionNodeId, decisionEpoch, DecisionKind.COOL);
  }

  /**
   * Whether this lease is a HOT decision.
   *
   * @return {@code true} for a HOT lease
   */
  public boolean isHot() {
    return kind == DecisionKind.HOT;
  }

  /**
   * Whether this stamp carries a usable Worker identity. A missing or blank node id is not an
   * identity: its version and epoch counters are node-local and cannot be compared with anyone
   * else's.
   *
   * @return whether the node id is present and non-blank
   */
  public boolean hasNodeId() {
    return decisionNodeId != null && !decisionNodeId.isBlank();
  }

  /**
   * Whether both stamps were produced by the same Worker node. Two anonymous stamps never match,
   * even when both node ids are the same blank string.
   *
   * @param other the other stamp
   * @return whether both carry the same non-blank node id
   */
  public boolean sameNodeAs(DecisionStamp other) {
    return hasNodeId() && decisionNodeId.equals(other.decisionNodeId);
  }

  /**
   * Whether this lease is a COOL decision.
   *
   * @return {@code true} for a COOL lease
   */
  public boolean isCool() {
    return kind == DecisionKind.COOL;
  }
}
