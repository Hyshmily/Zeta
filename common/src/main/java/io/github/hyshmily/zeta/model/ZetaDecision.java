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

/**
 * A decision emitted by the Worker's sliding-window / state-machine pipeline,
 * instructing application instances how to treat a specific cache key.
 *
 * <p>A decision is produced at a state transition — a promotion into the Worker's HOT set, or a
 * cool-down out of it — and sent to every application instance via RabbitMQ. Its
 * {@link #type()} is a {@link DecisionType}.
 *
 * <p><b>Causal version.</b> {@link #decisionVersion()} is allocated by the HOT registry
 * <em>inside the per-key lock that performed the transition</em>, not by the broadcaster at send
 * time. Version order therefore equals transition order
 * even when the send is delegated to an asynchronous buffer or to a different
 * thread (eviction): a decision that lost the race to the wire but describes an
 * older state can never overwrite a newer one on the receivers.
 * {@code 0} means "not allocated" and is reserved for test or diagnostic decisions:
 * the Worker dispatcher rejects an unversioned decision instead of assigning a
 * send-time version, which would invert causality for a decision that waited in a queue.
 *
 * <p><b>No rollback payload.</b> A decision describes what the Worker observed, not what it
 * managed to deliver: the pipeline is fire-and-forget, so a failed send is never rolled back
 * into the state machine (design decisions: sync messages). The decision carries no
 * pre-mutation image — it is a key, a type and a version.
 *
 * <p>Use the static factory methods ({@link #hot}, {@link #cool}) for concise
 * construction, and the shared {@link #NONE} constant for a no-op. {@code type}
 * and {@code cacheKey} are required: a null in either position fails at
 * construction instead of surfacing later in the broadcaster or a receiver.
 *
 * @param type            the decision type (never {@code null})
 * @param cacheKey        the affected cache key (never {@code null})
 * @param decisionVersion causal version allocated under the per-key lock; {@code 0} when unallocated
 */
public record ZetaDecision(DecisionType type, String cacheKey, long decisionVersion) {
  /**
   * Shared singleton for a no-op decision: it addresses no key ({@code cacheKey}
   * is empty) and carries version {@code 0}, so it is never broadcast.
   */
  public static final ZetaDecision NONE = new ZetaDecision(DecisionType.NONE, "", 0L);

  /**
   * Fails at construction when {@code type} or {@code cacheKey} is null: neither
   * can be serialized or applied downstream, so the producer's boundary owns
   * the rejection.
   */
  public ZetaDecision {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
  }

  /**
   * Possible decision outcomes for a hot-key evaluation.
   * <ul>
   *   <li>{@link #HOT} — key exceeds the frequency threshold and should be cached</li>
   *   <li>{@link #COOL} — key frequency dropped below cooldown threshold</li>
   *   <li>{@link #NONE} — no action is required</li>
   * </ul>
   */
  public enum DecisionType {
    /** Key exceeds the frequency threshold and should be promoted to L1 cache. */
    HOT,
    /** Key frequency has dropped below the cooldown threshold; the entry reverts to the normal TTL regime. */
    COOL,
    /** No action is required for this key during this evaluation cycle. */
    NONE,
  }

  /**
   * Creates an unversioned decision ({@link #decisionVersion()} {@code 0}).
   *
   * <p>Only for call sites that never race another writer for the key —
   * production decisions are versioned under the state machine's per-key lock.
   *
   * @param type     the decision type
   * @param cacheKey the affected cache key
   */
  public ZetaDecision(DecisionType type, String cacheKey) {
    this(type, cacheKey, 0L);
  }

  /**
   * Create a HOT decision for the given key.
   *
   * @param cacheKey the affected cache key
   * @return a new {@code ZetaDecision} with type {@link DecisionType#HOT} and
   *         {@code decisionVersion} {@code 0}
   */
  public static ZetaDecision hot(String cacheKey) {
    return new ZetaDecision(DecisionType.HOT, cacheKey, 0L);
  }

  /**
   * Create a versioned HOT decision.
   *
   * @param cacheKey        the affected cache key
   * @param decisionVersion the causal version allocated under the per-key lock
   * @return a new {@code ZetaDecision} with type {@link DecisionType#HOT}
   */
  public static ZetaDecision hot(String cacheKey, long decisionVersion) {
    return new ZetaDecision(DecisionType.HOT, cacheKey, decisionVersion);
  }

  /**
   * Create a COOL decision for the given key.
   *
   * @param cacheKey the affected cache key
   * @return a new {@code ZetaDecision} with type {@link DecisionType#COOL} and
   *         {@code decisionVersion} {@code 0}
   */
  public static ZetaDecision cool(String cacheKey) {
    return new ZetaDecision(DecisionType.COOL, cacheKey, 0L);
  }

  /**
   * Create a versioned COOL decision.
   *
   * @param cacheKey        the affected cache key
   * @param decisionVersion the causal version allocated under the per-key lock
   * @return a new {@code ZetaDecision} with type {@link DecisionType#COOL}
   */
  public static ZetaDecision cool(String cacheKey, long decisionVersion) {
    return new ZetaDecision(DecisionType.COOL, cacheKey, decisionVersion);
  }
}
