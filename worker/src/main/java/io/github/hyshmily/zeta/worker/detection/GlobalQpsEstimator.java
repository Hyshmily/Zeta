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

package io.github.hyshmily.zeta.worker.detection;

import io.github.hyshmily.zeta.util.TimeSource;

/**
 * A sliding‑window based estimator of the overall qps (queries per second)
 * across all keys in the current shard.
 *
 * <p>Uses the same circular‑buffer algorithm as {@link SlidingWindowDetector}
 * but aggregates all key counts into a single window rather than maintaining
 * per-key buffers.  This provides a lightweight, global throughput estimate
 * that drives dynamic threshold adaptation via {@link ThresholdLearner}.
 *
 * <p>The write method ({@link #addTotal}) is {@code synchronized} and safe
 * to call from multiple consumer threads concurrently.  The read-only methods
 * ({@link #getWindowTotal}, {@link #getQps}) may be called concurrently
 * without blocking; a stale-but-consistent snapshot is acceptable — the
 * padded volatile slice fields guarantee individual element visibility.
 *
 *
 * <p><b>Known limitation — per‑shard only:</b> This estimator only sees
 * traffic routed to <em>this</em> Worker shard by the consistent‑hash ring.
 * It does <b>not</b> reflect global cluster qps. When the cluster scales
 * (e.g. 5→3 Workers), per‑shard load redistributes non‑linearly, and the
 * derived threshold will shift accordingly. Not suitable for
 * cluster‑wide adaptive decisions without cross‑Worker coordination.
 *
 * @see ThresholdLearner
 * @see SlidingWindowDetector
 */
public class GlobalQpsEstimator {

  /** Number of time slices that form one complete sliding window. */
  private final int windowSize;

  /** Bitmask for circular buffer index (length - 1, where length is a power of two). */
  private final int lengthMask;

  /** Duration of a single time slice in milliseconds. */
  private final long timeMillisPerSlice;

  /**
   * Doubled circular buffer of per-slice aggregate counters.
   * Each slice is a {@link QpsSlice} with the counter embedded and padded onto its own
   * cache line, preventing false sharing between the writer (single, under the
   * {@code addTotal} monitor) and the lock-free readers.
   */
  private final QpsSlice[] slices;

  private long lastAddTotalTime;

  private static final class QpsSlice extends QpsPadding.SliceRef {}

  /**
   * Creates a global qps estimator with a sliding window partitioned into the
   * given number of slices.
   *
   * <p>The window duration must be evenly divisible by the number of slices,
   * otherwise rounding inaccuracies will occur in window-boundary calculations.
   *
   * @param windowDurationMs total duration of the sliding window in milliseconds;
   *                         must be positive
   * @param slices           number of slices within the window; must be at least 1
   */
  public GlobalQpsEstimator(long windowDurationMs, int slices) {
    if (slices <= 0) throw new IllegalArgumentException("slices must be positive, got " + slices);
    int aligned = slices;
    if ((aligned & (aligned - 1)) != 0) {
      aligned = Integer.highestOneBit(aligned - 1) << 1;
    }
    // The pre-alignment guard (windowDurationMs >= slices) is NOT sufficient:
    // aligning slices UP to the next power of two can push timeMillisPerSlice
    // to 0 (e.g. durationMs=15, slices=10 -> aligned=16 -> 15/16 = 0), which
    // would throw ArithmeticException on every addTotal and report Infinity
    // from getQps, discarding every report batch.
    if (windowDurationMs < aligned) {
      throw new IllegalArgumentException(
        "windowDurationMs (" + windowDurationMs + ") must be >= aligned slices (" + aligned + ") to avoid division by zero"
      );
    }
    this.windowSize = aligned;
    this.lengthMask = (aligned << 1) - 1;
    this.timeMillisPerSlice = windowDurationMs / aligned;
    this.slices = new QpsSlice[aligned * 2];
    for (int i = 0; i < this.slices.length; i++) {
      this.slices[i] = new QpsSlice();
    }
  }

  /**
   * Adds the sum of all per-key counts in a batch to the current time slice.
   *
   * <p>Also clears stale slices that have fallen out of the window, keeping
   * the circular buffer consistent.  This method is {@code synchronized} to
   * protect the clear-before-add sequence against concurrent calls from
   * multiple {@link io.github.hyshmily.zeta.worker.ingest.ReportConsumer}
   * threads.
   *
   * @param totalCount the total number of access counts across all keys in
   *                   the batch; must be non-negative
   */
  public synchronized void addTotal(long totalCount) {
    long now = TimeSource.monotonicMillis();
    int currentIndex = (int) ((now / timeMillisPerSlice) & lengthMask);

    // Detect infrequent-call gap: if more than windowSize slices elapsed,
    // all previously written data is stale — reset the entire buffer.
    if (lastAddTotalTime > 0) {
      long elapsedSlices = (now - lastAddTotalTime) / timeMillisPerSlice;
      if (elapsedSlices >= windowSize) {
        for (QpsSlice slice : slices) {
          slice.value = 0;
        }
      } else if (elapsedSlices > 0) {
        // Invariant: length == 2 * windowSize (doubled circular buffer).
        // Same invariant as SlidingWindowDetector.addCount: stale slices that
        // have rolled outside the new summation range are the elapsedSlices
        // oldest slots of the previous window, starting at:
        //   (currentIndex + windowSize - elapsedSlices + length) % length
        int clearStart = (currentIndex + windowSize - (int) elapsedSlices) & lengthMask;
        for (int i = 0; i < elapsedSlices; i++) {
          slices[(clearStart + i) & lengthMask].value = 0;
        }
      }
    }
    lastAddTotalTime = now;

    slices[currentIndex].value += totalCount;
  }

  /**
   * Returns the total access count in the current sliding window.
   *
   * <p>Walks backwards from the current time slice to sum the most recent
   * {@link #windowSize} slices.  This is a lock-free read and may return a
   * slightly stale value if called concurrently with {@link #addTotal}.
   *
   * @return sum of all slice counters within the active window; {@code 0}
   *         if no accesses have been recorded
   */
  public long getWindowTotal() {
    long now = TimeSource.monotonicMillis();
    int currentIndex = (int) ((now / timeMillisPerSlice) & lengthMask);
    long sum = 0;
    for (int i = 0; i < windowSize; i++) {
      int idx = (currentIndex - i) & lengthMask;
      sum += slices[idx].value;
    }
    return sum;
  }

  /**
   * Returns the estimated queries per second based on the current window.
   *
   * <p>The estimate is computed as {@code getWindowTotal() / windowDurationSeconds},
   * where {@code windowDurationSeconds = (windowSize * timeMillisPerSlice) / 1000.0}.
   * Returns {@code 0.0} if no accesses have been recorded in the current window.
   *
   * <p>Consumed by {@link ThresholdLearner} to dynamically adjust the hot-key
   * threshold in response to overall traffic changes.
   *
   * @return the estimated qps value (may be {@code 0.0}; never negative)
   */
  public double getQps() {
    long total = getWindowTotal();
    double windowSeconds = (windowSize * timeMillisPerSlice) / 1000.0;
    return total / windowSeconds;
  }
}

final class QpsPadding {

  private QpsPadding() {}

  @SuppressWarnings("all")
  abstract static class PadLead {

    byte p000, p001, p002, p003, p004, p005, p006, p007;
    byte p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023;
    byte p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039;
    byte p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055;
    byte p056, p057, p058, p059, p060, p061, p062, p063;
    byte p064, p065, p066, p067, p068, p069, p070, p071;
    byte p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087;
    byte p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103;
    byte p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119;
  }

  /**
   * A single slice counter with the {@code volatile long} embedded directly in the padded
   * holder (unlike a referenced {@code AtomicLong}, which would live in an unpadded heap
   * object). Lead and trailing padding keep the field more than a cache line away from any
   * other slice's counter, eliminating false sharing between the single synchronized writer
   * and the lock-free readers.
   */
  @SuppressWarnings("all")
  abstract static class SliceRef extends PadLead {

    volatile long value;

    byte a0, a1, a2, a3, a4, a5, a6, a7;
    byte a8, a9, a10, a11, a12, a13, a14, a15;
    byte a16, a17, a18, a19, a20, a21, a22, a23;
    byte a24, a25, a26, a27, a28, a29, a30, a31;
    byte a32, a33, a34, a35, a36, a37, a38, a39;
    byte a40, a41, a42, a43, a44, a45, a46, a47;
    byte a48, a49, a50, a51, a52, a53, a54, a55;
  }
}
