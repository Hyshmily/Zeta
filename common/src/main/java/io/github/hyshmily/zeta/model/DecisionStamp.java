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

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The Worker decision lease carried by a {@link CacheEntry}. Only present when
 * the entry has a Worker origin (HOT/COOL broadcasts); local entries carry no
 * decision metadata.
 *
 * <p>The stamp records the operation together with its causal identity: the
 * version orders decisions of one Worker incarnation, the node id scopes the
 * version space, the epoch detects a Worker restart, and {@link #kind()}
 * distinguishes a HOT lease from a COOL lease. The effective {@code keyState}
 * remains separate: a COOL lease can outlive its state while an entry is
 * re-warmed normally, or while a degraded App locally promotes it.
 *
 * <p>This is the single value type for decision metadata across the entry
 * pipeline: {@code CacheEntry#decision()} exposes it from a raw cache value,
 * {@link EntryDraft#decision(DecisionStamp)} applies it to a draft, and the
 * worker decision handler and the background refresh carry it between read and
 * write sites. A {@code null} stamp is the local origin.
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
   * Whether this lease is a COOL decision.
   *
   * @return {@code true} for a COOL lease
   */
  public boolean isCool() {
    return kind == DecisionKind.COOL;
  }
}
