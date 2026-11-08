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
package io.github.hyshmily.zeta.util.clock;

import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.NodeIdentity;

/**
 * Single source of time-sortable stamps for a node — the {@code dataVersion} values that
 * order data mutations (design decisions: version stamps).
 *
 * <p>A stamp is a positive, time-sortable {@code long} that orders data mutations across
 * instances. It answers one question only: <em>which of two values was written later?</em>
 * It is deliberately <b>not</b> a duration clock — TTLs, soft expiry and value-birth
 * stamps keep using the per-JVM monotonic clock ({@code TimeSource.monotonic*}).
 * Mixing the two lets an NTP step rewrite entry lifetimes.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>{@link #nextStamp()} is strictly increasing within this JVM and never returns a
 *       value at or below any stamp previously passed to {@link #observe(long)}.</li>
 *   <li>{@link #nextStamp()} never throws for a wall-clock rewind — the rewind is
 *       absorbed by the merge floor rather than propagated into the write path,
 *       because the sync plane has no fallback version space (design
 *       decisions: version stamps decision 7). The one refusal is timestamp-space
 *       exhaustion: allocation throws {@link IllegalStateException} instead of
 *       wrapping into the sign bit.</li>
 *   <li>{@link #observe(long)} merges only stamps within the implementation's
 *       forward-drift allowance, so one broken peer clock cannot pin this node's
 *       floor into the future.</li>
 *   <li>Stamps are compared numerically across nodes. The comparison is only as good as
 *       the clock relationship between nodes; {@link #observe(long)} closes the part of
 *       that gap which has actually been observed.</li>
 * </ul>
 *
 * @see MonotonicStampClock
 */
@Internal
public interface StampClock {
  /** Width of the node identity embedded in a stamp. */
  int NODE_BITS = NodeIdentity.NODE_BITS;

  /** Width of the sequence window backing {@link #MAX_ATOMIC_RANGE}. */
  int SEQUENCE_BITS_FOR_RANGE = 12;

  /** Custom epoch (2025-01-01T00:00:00Z) shared by every stamp producer. */
  long EPOCH = 1735689600000L;

  /** Shift of the 41-bit timestamp field above the node and sequence bits. */
  int TIMESTAMP_SHIFT = SEQUENCE_BITS_FOR_RANGE + NODE_BITS;

  /**
   * Upper bound of one atomic {@link #nextStamps(int)} reservation: the widest
   * contiguous block any implementation guarantees in a single allocation
   * (the 12-bit sequence window of the shared stamp layout). Callers reserving
   * more stamps must loop in chunks of this size.
   */
  int MAX_ATOMIC_RANGE = 1 << SEQUENCE_BITS_FOR_RANGE;

  /**
   * Decode the epoch-millis timestamp carried by a stamp.
   *
   * @param stamp a positive stamp in the shared layout
   * @return epoch-millis encoded in the stamp's timestamp field
   */
  static long timestampOf(long stamp) {
    return EPOCH + (stamp >>> TIMESTAMP_SHIFT);
  }

  /**
   * Allocate the next stamp for a data mutation or a value load.
   *
   * <p>Load paths must call this <b>before</b> reading the value: a pre-read stamp is a
   * lower bound ("this value is at least as new as this instant"), so any write allocated
   * after the read began wins. A post-read stamp is an upper bound and pins stale values
   * once two independent clocks are involved (design decisions: version stamps decision 6).
   *
   * @return a positive stamp strictly greater than every stamp returned or accepted by observe
   * @throws IllegalStateException when the timestamp field is exhausted — the generator
   *         refuses rather than wrapping into the sign bit
   */
  long nextStamp();

  /**
   * Atomically reserve a contiguous range of {@code add} stamps and return the base
   * stamp: the caller owns exactly {@code [base, base + add - 1]} and may assign them
   * in any order. One CAS — no concurrent {@link #nextStamp()} or {@link #nextStamps(int)}
   * call can land inside the reserved block, so a concurrent single write is ordered
   * against the whole batch at once: its stamp is either older than every batch stamp or
   * newer than every batch stamp, never interleaved. That is what lets a batched
   * invalidation hand each entry its own version while the per-key version guard sees a
   * batch-relative order that is never partial (design decisions: version stamps).
   *
   * <p>{@code add} must be in {@code [1, MAX_ATOMIC_RANGE]}; a larger batch cannot fit
   * one sequence window and would silently break the exclusive-reservation guarantee, so
   * it is rejected (argument contract — callers chunk by {@link #MAX_ATOMIC_RANGE}).
   *
   * @param count how many stamps to reserve ({@code 1..MAX_ATOMIC_RANGE})
   * @return the first stamp of the reserved contiguous range, strictly greater than every
   *         stamp returned or observed so far
   * @throws IllegalStateException when the timestamp field is exhausted — the generator
   *         refuses rather than wrapping into the sign bit
   */
  long nextStamps(int count);

  /**
   * Merge a stamp observed from a peer (an incoming sync message, an applied entry, or a
   * worker decision that carries one) and report whether it was accepted into the local
   * floor.
   *
   * <p>Without the merge a node whose wall clock lags loses every conflict permanently:
   * its writes are numerically below stamps every peer already holds. Values outside
   * {@code (0, Long.MAX_VALUE)} are not orderable and are refused. {@code Long.MAX_VALUE}
   * itself is reserved as the saturated sentinel and is never issued by the generator.
   *
   * <p>The merge is bounded: a stamp that leads local physical time by more than the
   * implementation's forward-drift allowance is refused (and reported), so one broken peer
   * clock cannot drag this node's floor into the future.
   *
   * @param stamp the observed stamp
   * @return {@code true} when the stamp was already at or below the local floor or was
   *         merged into it — the caller may persist it as a data version, because local
   *         {@link #nextStamp()} allocations are guaranteed to sort above it;
   *         {@code false} when the stamp was refused by the anti-drift guard (or is
   *         non-positive/saturated) — the caller MUST NOT persist it as an authoritative
   *         data version, or every later local write to that key loses every version
   *         comparison until wall time catches up (clamp the version to a fresh
   *         {@link #nextStamp()} or degrade the mutation to a local invalidation)
   */
  boolean observe(long stamp);

  /**
   * How far behind the fastest observed stamp this node currently is, in milliseconds.
   *
   * <p>This is the quantity that bounds convergence (design decisions: version stamps consequence 1) and is
   * exported as the {@code zeta.version.clock.skew} gauge.
   *
   * @return the observed lead in milliseconds, {@code 0} when nothing has been observed
   */
  long skewMillis();
}
