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
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Lock-free {@link StampClock} — the production {@code dataVersion} source
 * (design decisions: version stamps). A single CAS variable carries the whole stamp:
 *
 * <pre>
 *   nextStamp(): wall clock caught up → fresh window at sequence 0 (this node's identity)
 *                otherwise           → this node's identity + sequence + 1 (field-aware,
 *                                      never the identity of an observed peer stamp);
 *                                      overflow or a peer above this node's bits rolls
 *                                      the timestamp forward
 *   observe(s)  = last = max(last, s), while s leads local time by at most the
 *                 configured forward-drift bound
 * </pre>
 *
 * <h2>Bit layout — the shared stamp layout</h2>
 * <pre>
 *   0 | 41 bits ms since EPOCH | 2 bits datacenter | 8 bits worker | 12 bits sequence
 * </pre>
 *
 * <ul>
 *   <li><b>Sign (1 bit):</b> always 0 — stamps are positive, so {@code 0}
 *       stays the "nothing yet" floor value and is never handed out.</li>
 *   <li><b>Timestamp (41 bits):</b> milliseconds since {@link StampClock#EPOCH}
 *       (~69 years).</li>
 *   <li><b>Node identity (10 bits):</b> the value
 *       {@link NodeIdentity#getNodeId()} returns — two nodes with distinct ids
 *       occupy disjoint low-bit ranges. Nodes that share an id can mint the
 *       same stamp in the same millisecond (both start at sequence 0), so the
 *       id must be unique per instance.</li>
 *   <li><b>Sequence (12 bits):</b> increments via the CAS; overflow past
 *       4095 rolls the timestamp forward by one millisecond.</li>
 * </ul>
 *
 * <p>Every stamp decodes with {@link StampClock#timestampOf(long)}; message trace IDs use
 * {@link io.github.hyshmily.zeta.util.TimeSource} instead and carry no version meaning.
 *
 * <p>The contract here is narrow: allocation never blocks, stamps are strictly increasing
 * node-locally, and {@link #observe} floors the clock above every accepted stamp. Allocation
 * throws {@link IllegalStateException} only when the 41-bit timestamp space is exhausted — a
 * stamp in the reserved top window makes the next millisecond roll land in the sign bit, so
 * refusing is the only way to keep the "always positive" invariant. "Running ahead" and the
 * lock-free allocation design are in design decisions: version stamps.
 *
 * <p><b>Cost.</b> One volatile read + one CAS (retry only under contention)
 * + one cached clock read. No locks, no spin-waits, no allocation.
 */
@Internal
@Slf4j
public class MonotonicStampClock implements StampClock {

  /** Mask of the 12-bit sequence field. */
  private static final long SEQ_MASK = (1L << SEQUENCE_BITS_FOR_RANGE) - 1;

  /**
   * Default cap on how far ahead of local physical time an observed peer stamp
   * may be. Mirrors the anti-drift guard in mature HLC implementations (uhlc
   * defaults to 500 ms): without it, one broken peer clock drags every node's
   * floor into the future and keeps it there. Zeta's LWW contract tolerates
   * bounded skew, so the default is deliberately loose — it rejects clearly
   * broken stamps, not ordinary NTP skew.
   */
  public static final long DEFAULT_MAX_FORWARD_DRIFT_MS = 60_000L;

  private static final int NODE_SHIFT = SEQUENCE_BITS_FOR_RANGE;

  /**
   * Largest stamp this clock will ever issue. {@link Long#MAX_VALUE} itself is
   * reserved as the "saturated" sentinel that {@link #observe} refuses, so the
   * generator stops one value short of it.
   */
  private static final long MAX_STAMP = Long.MAX_VALUE - 1L;

  /**
   * Largest timestamp offset that still fits the 41-bit field. Checked before
   * shifting: a left shift silently drops the bits above 41, so an
   * out-of-range physical clock would otherwise wrap into a plausible-looking
   * window and issue stamps from the past instead of failing.
   */
  private static final long MAX_TIMESTAMP = (1L << (Long.SIZE - 1 - TIMESTAMP_SHIFT)) - 1L;

  /**
   * Highest peer-stamp timestamp (epoch millis) refused by the forward-drift
   * guard. An accepted stamp raises {@link #last}, which already carries the
   * same evidence; this watermark keeps the skew gauge visible exactly when a
   * peer clock breaches {@code #maxForwardDriftMs} — the moment an operator
   * most needs the signal — instead of dropping to 0 because the local floor
   * was never moved.
   */
  private final AtomicLong maxObservedPeerEpochMs = new AtomicLong(0L);

  /**
   * Highest stamp this node has ever issued or observed. {@code 0} means
   * "nothing yet"; every real stamp is positive, so {@code 0} is never a
   * value the clock hands out.
   */
  private final AtomicLong last = new AtomicLong(0L);

  /** Physical clock reading, in Unix epoch milliseconds. */
  private final LongSupplier epochMillis;

  /** Maximum accepted lead of an observed stamp over local physical time. */
  private final long maxForwardDriftMs;

  /**
   * This node's static low bits: the 10-bit node identity shifted into
   * position. Constant for the lifetime of the clock (pinned at construction
   * from the node id argument).
   */
  private final long nodeBase;

  /**
   * Creates a clock with an explicit node id and forward-drift bound.
   *
   * @param nodeId            the node identity in {@code [0, 1023]}
   * @param maxForwardDriftMs how far an observed stamp may lead local physical
   *                          time before it is refused ({@code >= 0})
   */
  public MonotonicStampClock(long nodeId, long maxForwardDriftMs) {
    // Validate BEFORE resolving the physical clock: the production
    // physicalClock() starts the TimeSource ticker, and the ticker must not
    // be started as a side effect of arguments that are about to be rejected
    // (a constructor that throws must not leak a background daemon).
    if (nodeId < 0 || nodeId >= (1L << NODE_BITS)) {
      throw new IllegalArgumentException("nodeId must be 0.." + ((1L << NODE_BITS) - 1) + ", got " + nodeId);
    }
    if (maxForwardDriftMs < 0L) {
      throw new IllegalArgumentException("maxForwardDriftMs must be >= 0, got " + maxForwardDriftMs);
    }

    this.nodeBase = nodeId << NODE_SHIFT;
    this.epochMillis = physicalClock();
    this.maxForwardDriftMs = maxForwardDriftMs;
  }

  /**
   * Resolve the production physical clock and start its ticker. Called from the
   * constructor — validation has already rejected bad arguments by then, so a
   * throwing clock never leaks a background daemon.
   */
  private static LongSupplier physicalClock() {
    TimeSource.start();
    return TimeSource::currentTimeMillis;
  }

  @Override
  public long nextStamp() {
    return allocate(1);
  }

  @Override
  public long nextStamps(int count) {
    if (count < 1 || count > MAX_ATOMIC_RANGE) {
      throw new IllegalArgumentException("nextStamps add must be 1.." + MAX_ATOMIC_RANGE + ", got " + count);
    }
    return allocate(count);
  }

  /**
   * Allocate one contiguous block of {@code count} stamps.
   *
   * <p>Both public entry points share this one state machine, so a range
   * allocation applies the same base rebuild as a single allocation: a floor
   * carrying this node's own non-zero sequence is recognised as this node's
   * window, not mistaken for a higher-ranked peer's.
   *
   * <p><b>Clock-read amortization.</b> The physical clock is sampled once, before
   * the CAS loop, not once per retry. A CAS retry means another thread just won
   * and pushed {@code last} forward — the tight spin that follows gains nothing
   * from re-reading {@code TimeSource}: the winner either advanced the timestamp
   * window or only the sequence, and in both cases the retry's decision depends
   * on {@code last}, not on a clock reading a few nanoseconds fresher. The
   * trade-off is that a stamp allocated after a long preemption may sit slightly
   * behind wall time; it stays strictly increasing and cross-node comparable, so
   * the LWW contract holds — only the "stamp tracks wall time" property is
   * relaxed, by design, in exchange for not hammering the TimeSource cache line
   * from every allocating thread under contention.
   *
   * <p><b>Pre-EPOCH clock.</b> A physical clock configured before {@link #EPOCH}
   * is refused outright. Clamping into window 0 would let this node keep issuing
   * stamps that sort below every correctly-clocked node — LWW would fail silently
   * and no operator would see it. Failing at allocation surfaces the
   * misconfiguration at startup instead of corrupting data at runtime.
   *
   * @param count the block size, {@code 1..MAX_ATOMIC_RANGE}
   * @return the lowest stamp of the block; the block occupies
   *         {@code [base, base + count - 1]}
   */
  private long allocate(int count) {
    // Sampled once (see Javadoc): CAS retries reuse this reading.
    long nowTs = epochMillis.getAsLong() - EPOCH;

    // Pre-EPOCH physical clock: refuse, do not clamp. See Javadoc.
    if (nowTs < 0L) {
      throw clockBeforeEpoch(nowTs);
    }

    // The node base is invariant across retries: hoisting it out of the CAS loop
    // keeps every spin a pure base + compareAndExchange, with no volatile
    // re-reads between attempts.
    long cur = last.get();
    for (;;) {
      long first = base(count, cur, nowTs, nodeBase);
      // base() answers -1 for a window past the 41-bit timestamp field: shifting it would
      // drop the high bits and mint a stamp from the past. Refuse before the block tail is
      // computed, so the sentinel can never be turned into a positive value by the addition.
      if (first < 1L) {
        throw timestampSpaceExhausted(first, count, cur);
      }
      long top = first + (count - 1L);
      // The only other illegal result is the reserved saturated sentinel. Refusing here keeps
      // every issued stamp positive; a silent negative would be persisted and propagated
      // cluster-wide.
      if (top > MAX_STAMP) {
        throw timestampSpaceExhausted(first, count, cur);
      }
      // compareAndExchange returns the witness on failure, so a retry does not
      // need a second last.get() — the same state machine as observe().
      long witness = last.compareAndExchange(cur, top);
      if (witness == cur) {
        return first;
      }
      cur = witness;
    }
  }

  /**
   * Compute the first stamp of one allocation block. Pure: does not touch
   * {@link #last}, does not check the block tail, does not CAS.
   *
   * <p>The target window is the later of the floor's timestamp and physical time.
   * Inside it this node's base is {@code ownBase = timestamp | nodeBase}; the block
   * starts at the later of that base and {@code cur + 1}, so it always ranks strictly
   * above the floor — whether the floor is this node's own stamp, a peer's stamp, or
   * a value from an earlier millisecond. A block that would cross the sequence
   * ceiling moves to the next millisecond instead, staying contiguous. Rebuilding the
   * low bits from {@code nodeBase} (never reusing the peer's node bits) keeps two
   * observing nodes from minting the same value.
   *
   * <p>The caller owns the clock read, the CAS loop and the block-tail validation
   * against {@link #MAX_STAMP}; this method only answers "what is the next usable
   * base".
   *
   * @param count block size, {@code 1..MAX_ATOMIC_RANGE}
   * @param cur   current floor (snapshot of {@link #last}), {@code >= 0}
   * @param nowTs physical time minus {@link StampClock#EPOCH} in ms, {@code >= 0}
   * @param nodeBits this clock's node bits, resolved once per allocation
   * @return the block's first stamp; the block occupies
   *         {@code [base, base + count - 1]} and {@code base > cur}, or
   *         {@code -1} when the window is past the 41-bit timestamp field
   */
  private static long base(int count, long cur, long nowTs, long nodeBits) {
    long windowTs = Math.max(cur >>> TIMESTAMP_SHIFT, nowTs);
    if (windowTs > MAX_TIMESTAMP) {
      return -1L;
    }
    long ownBase = (windowTs << TIMESTAMP_SHIFT) | nodeBits;
    long candidate = Math.max(ownBase, cur + 1L);
    if (candidate + count - 1L <= (ownBase | SEQ_MASK)) {
      return candidate;
    }
    long nextTs = windowTs + 1L;
    return nextTs <= MAX_TIMESTAMP ? (nextTs << TIMESTAMP_SHIFT) | nodeBits : -1L;
  }

  private static IllegalStateException timestampSpaceExhausted(long base, long count, long cur) {
    return new IllegalStateException(
      "stamp timestamp space exhausted (41 bits since " + EPOCH + "): base=" + base + ", add=" + count + ", last=" + cur
    );
  }

  /** Refuse allocation when the physical clock is behind {@link StampClock#EPOCH}. */
  private static IllegalStateException clockBeforeEpoch(long nowTs) {
    return new IllegalStateException(
      "physical clock is before EPOCH (" +
        EPOCH +
        "): nowTs=" +
        nowTs +
        "; refusing to issue a stamp that would sort below every correctly-clocked node"
    );
  }

  @Override
  public boolean observe(long stamp) {
    // Entry guard: 0 is the unset floor, negative values are unorderable, and
    // Long.MAX_VALUE is the saturated sentinel. Rejecting the sentinel here
    // matters: its timestamp field decodes to 2094, so letting it reach the
    // anti-drift probe would record that future instant in
    // maxObservedPeerEpochMs and distort skewMillis() permanently.
    if (stamp <= 0L || stamp > MAX_STAMP) {
      return false;
    }

    long cur = last.get();
    // Fast path: last is monotonic (allocate and observe only ever raise it), so
    // once stamp <= cur it can never raise the floor — cur only grows from here.
    // Most observed stamps land here, without a physical-clock read or a CAS.
    if (stamp <= cur) {
      return true;
    }

    long peerEpochMs = StampClock.timestampOf(stamp);
    long leadMs = peerEpochMs - epochMillis.getAsLong();
    if (leadMs > maxForwardDriftMs) {
      // Anti-drift: one broken peer clock must not drag every node's floor
      // into the future and keep it there. The caller learns the refusal through
      // the false return and must not persist the stamp as a data version —
      // every later local write to that key would lose every version comparison
      // until wall time catches up (see StampClock.observe). Mirrors the
      // reject-beyond-delta rule of HLC implementations (uhlc defaults to
      // 500 ms; Zeta's LWW contract tolerates wider skew, hence a looser cap).
      //
      // The refused stamp is the only one that needs the separate watermark:
      // an accepted stamp raises `last`, and skewMillis() already reads
      // timestampOf(last). Writing the watermark on the accepted path would add
      // a read + CAS to every observation for no observable gain.
      recordObservedPeerTime(peerEpochMs);
      log.debug(
        "Refusing observed stamp {}ms ahead of local time (max {}ms); local clock floor unchanged",
        leadMs,
        maxForwardDriftMs
      );
      return false;
    }

    // Read-then-CAS: most observed stamps are at or below the local floor, and
    // accumulateAndGet still executes a locked compareAndSet(prev, prev) on
    // every one of them — a write to the line every allocator is spinning on.
    // compareAndExchange returns the witness on failure, so a retry needs no
    // second read. The stamp <= witness arm covers a concurrent observe that
    // already raised the floor past our stamp: there is nothing left to do.
    for (;;) {
      long witness = last.compareAndExchange(cur, stamp);
      if (witness == cur || stamp <= witness) {
        return true;
      }
      cur = witness;
    }
  }

  /** Monotonic max on the observed-peer timestamp watermark; CAS only on a new high. */
  private void recordObservedPeerTime(long peerEpochMs) {
    long curMax = maxObservedPeerEpochMs.get();
    while (peerEpochMs > curMax) {
      long witness = maxObservedPeerEpochMs.compareAndExchange(curMax, peerEpochMs);
      if (witness == curMax) {
        return;
      }
      curMax = witness;
    }
  }

  @Override
  public long skewMillis() {
    // The refused-outlier watermark and the local floor both feed the gauge:
    // the floor alone reports 0 exactly when a peer breaches maxForwardDriftMs
    // (the refusal leaves it untouched), hiding the one anomaly the gauge
    // exists for.
    long current = last.get();
    long observedPeerMs = maxObservedPeerEpochMs.get();
    long highestKnownMs = Math.max(current > 0L ? StampClock.timestampOf(current) : 0L, observedPeerMs);
    if (highestKnownMs <= 0L) {
      return 0L;
    }
    // The timestamp field is the top bits above the 22-bit node+sequence
    // space in the shared layout. A burst-driven clock that
    // runs ahead of the wall clock reports positive skew — by design (the
    // gauge reads "how far ahead of local now the newest stamp is").
    return Math.max(0L, highestKnownMs - epochMillis.getAsLong());
  }
}
