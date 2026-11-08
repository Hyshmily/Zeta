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
package io.github.hyshmily.zeta.worker.detection.lifecycle;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A hierarchical timer wheel for the cool-down schedule: insert, move and fire in amortized O(1),
 * with one entry as the node.
 *
 * <p><b>Shape.</b> Five levels of {@code 64 / 64 / 32 / 4 / 1} buckets with spans of
 * {@code 1ms / 64ms / 4096ms / 131072ms / 524288ms}. A deadline is placed in the finest level
 * whose enclosing span differs from the current clock: level {@code L} holds every deadline in
 * the same {@code L+1} span as {@link #clockMs}, but in a later {@code L} slot. The levels are
 * therefore time-orthogonal — every level-{@code L} deadline is earlier than every
 * level-{@code L+1} deadline — so the first occupied bucket found from level 0 is the next due
 * instant, and a cascade can only move a still-future entry to a finer level. A deadline beyond
 * the wheel's horizon stays in the top bucket and is re-evaluated on each of its (rare)
 * rotations, which preserves the deadline without adding levels for hour-scale {@code cool-after}
 * values. The bucket layout follows Caffeine's {@code TimerWheel}; the units are milliseconds
 * because the Worker's clocks (and the metric) are millisecond-based.
 *
 * <p><b>Node budget.</b> The node is the {@link HE} itself: its wheel-specific state is
 * two references and one {@code int} link state, its deadline is the entry's existing
 * {@code nextCheckAt}, and a schedule or re-arm allocates nothing (design decisions: worker
 * cooling).
 *
 * <p><b>Concurrency.</b> {@link #lock} guards every link, the bucket bits and the wheel clock. The
 * registry calls {@link #schedule}, {@link #reschedule} and {@link #deschedule} while holding the
 * key's stripe lock (stripe → wheel). {@link #advance} detaches due entries and appends the whole due
 * list to the owner's private batch under this lock; the owning thread evaluates them after
 * releasing it, so the wheel lock is never held while a stripe lock is taken (wheel → stripe is
 * never nested). An entry detached into the batch is {@link #DETACHED}: a concurrent cool
 * can no longer unlink it, and the owner discards it when its registry tombstone is set.
 */
final class TimerWheel {

  /** No check is linked: the entry is cold, or its owner has taken it out to evaluate. */
  static final int UNSCHEDULED = 0;
  /** The entry is linked in one of the buckets. */
  static final int SCHEDULED = 1;
  /** The entry is in the owner's pending list; a concurrent re-arm must not touch its links. */
  static final int DETACHED = 2;

  /** Bucket counts per level; powers of two so the index is a mask. */
  private static final int[] BUCKETS = { 64, 64, 32, 4, 1 };

  /**
   * {@code log2} of one bucket's span in milliseconds: level {@code L} advances one slot every
   * {@code 1 << SHIFT[L]} ms. Also the shift from a millisecond tick to that level's bucket index.
   * Derived from {@link #BUCKETS}: the shift is the accumulated {@code log2} of the finer levels'
   * bucket counts, so the two arrays cannot drift.
   */
  private static final int[] SHIFT = new int[BUCKETS.length];

  static {
    int shift = 0;
    for (int level = 0; level < BUCKETS.length; level++) {
      SHIFT[level] = shift;
      shift += Integer.numberOfTrailingZeros(BUCKETS[level]);
    }
  }

  /**
   * Non-empty bucket bits per level, one bit per slot. Every bucket count is at most 64, so one
   * word per level replaces the linear sentinel walk in {@link #nextDelay()}. A bit is set on every
   * link; it is cleared exactly when a cascade drains a bucket and lazily when {@code nextDelay}
   * finds a bucket an unlink emptied. A stale set bit costs one sentinel check, while a missing bit
   * would hide a deadline — so clearing is conservative.
   */
  @GuardedBy("lock")
  private final long[] occupied = new long[BUCKETS.length];

  private final ReentrantLock lock = new ReentrantLock();

  /** Wakes the owner when an entry is scheduled earlier than its current sleep. */
  private final Semaphore wakeup = new Semaphore(0);

  /** Bucket sentinels per level; each bucket is a circular doubly-linked list of entries. */
  @GuardedBy("lock")
  private final HE[][] wheel;

  /**
   * The batch the owner drains; {@link #advance} appends {@link #pending} to its tail. After that
   * the list belongs to the advancing thread alone: its nodes are {@code DETACHED}, so a concurrent
   * re-arm or cool leaves their links untouched. Appending instead of locking per pop is what makes
   * a burst of N due checks cost one wheel lock acquisition instead of N+1, and leaves a batch an
   * evaluation failed to finish at the head for the next poll.
   */
  private final HE draining = HE.sentinel();

  /**
   * Due entries waiting to be handed to the owner. A schedule or cascade that finds an entry due
   * appends here under {@link #lock}; the owner never traverses this list directly.
   */
  @GuardedBy("lock")
  private final HE pending = HE.sentinel();

  /**
   * The deadline the owner is sleeping toward; {@code Long.MAX_VALUE} when the wheel is idle, and
   * {@code 0} while the owner is awake or a wake signal is already pending. The zero budget is
   * what collapses a mass-heat burst into one semaphore release: schedules that land while the
   * owner is running see {@code 0} and leave the signal alone, instead of each paying a release
   * that the owner's next {@code drainPermits} would throw away.
   */
  @GuardedBy("lock")
  private long wakeAtMs = Long.MAX_VALUE;

  /** Entries currently linked in buckets (detached ones are not counted). */
  @GuardedBy("lock")
  private int scheduledCount;

  /** Wheel clock, monotonic milliseconds; moved by {@link #advance}. */
  @GuardedBy("lock")
  private long clockMs;

  TimerWheel() {
    this.clockMs = TimeSource.monotonicMillis();
    this.wheel = new HE[BUCKETS.length][];
    for (int level = 0; level < BUCKETS.length; level++) {
      wheel[level] = new HE[BUCKETS[level]];
      for (int i = 0; i < BUCKETS[level]; i++) {
        wheel[level][i] = HE.sentinel();
      }
    }
  }

  /**
   * Links a newly promoted entry's first check.
   *
   * @param entry the entry; {@link HE#nextCheckAt} is already set
   */
  void schedule(HE entry) {
    lock.lock();
    try {
      linkScheduled(entry);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Moves an entry whose deadline changed. A detached entry is left alone: its owner is about to
   * evaluate it and will link the fresh deadline on the way out.
   *
   * @param entry the entry; {@link HE#nextCheckAt} is already updated
   */
  void reschedule(HE entry) {
    lock.lock();
    try {
      if (entry.wheelState == DETACHED) {
        // The owner is about to evaluate it and will link the fresh deadline.
        return;
      }
      if (entry.wheelState == SCHEDULED) {
        unlink(entry);
        scheduledCount--;
      }
      linkScheduled(entry);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Removes a cooled or aborted entry from the schedule. A detached entry is left in the pending
   * list; the owner will find the entry gone from the registry and drop it.
   *
   * <p>The registry calls this while holding the entry's stripe lock, and the only paths that set
   * {@link #SCHEDULED} — {@link #schedule} and {@link #reschedule} — run under that same stripe.
   * An entry observed unscheduled here therefore cannot become scheduled before the wheel lock is
   * taken, so the unlocked check can return without acquiring it; the in-lock check remains the
   * authoritative one.
   *
   * @param entry the entry to unschedule
   */
  void deschedule(HE entry) {
    if (entry.wheelState != SCHEDULED) {
      return;
    }
    lock.lock();
    try {
      if (entry.wheelState != SCHEDULED) {
        return;
      }
      unlink(entry);
      entry.wheelState = UNSCHEDULED;
      scheduledCount--;
    } finally {
      lock.unlock();
    }
  }

  private void linkScheduled(HE entry) {
    if (entry.nextCheckAt <= clockMs) {
      // Due already: the owner's next drain evaluates it without waiting for a tick.
      link(pending, entry);
      entry.wheelState = DETACHED;
      wakeIfAsleep();
      return;
    }
    linkBucket(bucketIndex(entry.nextCheckAt), entry);
    entry.wheelState = SCHEDULED;
    scheduledCount++;
    if (entry.nextCheckAt < wakeAtMs) {
      wakeIfAsleep();
    }
  }

  /**
   * Wakes the owner at most once between two waits. Called under {@link #lock} by every path that
   * makes work available before the current sleep target.
   */
  private void wakeIfAsleep() {
    if (wakeAtMs > 0L) {
      wakeup.release();
      wakeAtMs = 0L;
    }
  }

  /**
   * Takes the next entry from the owner's batch, or {@code null} when it is drained. The entry is
   * marked unscheduled, so its evaluation may re-arm it normally.
   *
   * <p>Called only by the advancing thread, and lock-free: {@link #advance} appended the whole due
   * list to {@link #draining}, which no other thread touches. A re-arm or cool of a detached node
   * is a no-op under the wheel lock, so the traversal cannot race a link mutation. The state write
   * below is the one wheel mutation outside the wheel lock: the node has already left every shared
   * list, and {@link HE#wheelState} is volatile so the one-way
   * {@code DETACHED -> UNSCHEDULED} transition is visible to a concurrent reader under that lock.
   *
   * @return the next due entry, or {@code null}
   */
  HE pollDetached() {
    HE head = draining.wheelNext;
    if (head == draining) {
      return null;
    }
    unlink(head);
    head.wheelState = UNSCHEDULED;
    return head;
  }

  /**
   * Advances the wheel to {@code nowMs}, detaches every entry whose deadline has passed, and appends
   * the whole due list to the owner's batch for {@link #pollDetached}.
   *
   * <p>The append happens even when the clock did not move: a schedule of an already-due entry links
   * it into {@link #pending}, and the owner must be able to drain it without waiting for the next
   * millisecond. Appending instead of swapping keeps a batch an evaluation failed to finish at the
   * head, so the next advance's entries follow it in arrival order and one traversal drains both.
   *
   * @param nowMs the current monotonic time
   */
  void advance(long nowMs) {
    lock.lock();
    try {
      // The owner runs from here to its next wait: schedules landing in that window can be picked
      // up by nextDelay without a release.
      wakeAtMs = 0L;
      if (nowMs > clockMs) {
        long previous = clockMs;
        clockMs = nowMs;
        for (int level = 0; level < BUCKETS.length; level++) {
          // Both operands are shifted by the same amount, so any constant an unsigned shift would
          // add is cancelled by the subtraction: the signed difference is exact on both sides of
          // zero, and the clock only moves forward.
          long delta = (nowMs >> SHIFT[level]) - (previous >> SHIFT[level]);
          if (delta <= 0L) {
            break;
          }
          cascade(level, previous >>> SHIFT[level], delta);
        }
      }
      splicePending();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Moves the due list to the tail of the owner's batch and resets its sentinel. A batch left
   * undrained by a failed evaluation therefore stays ahead of the entries this advance detached, and
   * the next {@link #pollDetached()} reaches both in one traversal. The {@code pending} list is
   * locked; the owner's batch is only ever touched by the advancing thread.
   */
  private void splicePending() {
    if (pending.wheelNext == pending) {
      return;
    }
    HE first = pending.wheelNext;
    HE last = pending.wheelPrev;
    HE tail = draining.wheelPrev;
    tail.wheelNext = first;
    first.wheelPrev = tail;
    last.wheelNext = draining;
    draining.wheelPrev = last;
    pending.wheelNext = pending;
    pending.wheelPrev = pending;
  }

  /**
   * Processes the buckets crossed at one level: future entries cascade into a finer level, due
   * entries move to {@link #pending} for the owner.
   *
   * @param level         the wheel level
   * @param previousTicks the tick index the previous advance stopped in
   * @param delta         the number of ticks crossed
   */
  private void cascade(int level, long previousTicks, long delta) {
    HE[] buckets = wheel[level];
    int mask = buckets.length - 1;
    int steps = (int) Math.min(delta, buckets.length);
    // Levels 0-3: the previous tick's slot was consumed by the previous advance and is empty by
    // construction, because bucketIndex never places a deadline in the slot being consumed. Level
    // 4 has one bucket, so the mask wraps the start to 0 and its single slot is scanned.
    int start = (int) ((previousTicks + 1L) & mask);
    int end = start + steps;
    for (int i = start; i < end; i++) {
      int slot = i & mask;
      HE head = buckets[slot];
      occupied[level] &= ~(1L << slot);
      HE node = head.wheelNext;
      if (node == head) {
        continue;
      }

      // Detach the bucket by resetting its sentinel; the nodes still form a forward chain through
      // wheelNext, and each one's links are overwritten by the re-link below. Walking forward with
      // the captured next pointer is safe because no path unlinks an unprocessed node here.
      head.wheelNext = head;
      head.wheelPrev = head;
      while (node != head) {
        HE next = node.wheelNext;
        if (node.nextCheckAt > clockMs) {
          linkBucket(bucketIndex(node.nextCheckAt), node);
        } else {
          link(pending, node);
          node.wheelState = DETACHED;
          scheduledCount--;
        }
        node = next;
      }
    }
  }

  /**
   * The bucket a deadline belongs to: the finest level whose enclosing span differs from the
   * current clock. A deadline in the same {@code L+1} span as {@link #clockMs} is distinguished by
   * its absolute {@code L} slot, so it can never land in the slot the wheel is currently
   * consuming. That is what makes the levels time-orthogonal and the cascade strictly descend.
   *
   * <p>Precondition: {@code timeMs > clockMs}; both call sites check it. A due deadline goes to
   * {@link #pending} instead.
   *
   * @param timeMs the deadline
   * @return the bucket packed as {@code (level << 6) | slot}; every level has at most 64 slots
   */
  private int bucketIndex(long timeMs) {
    long xor = timeMs ^ clockMs;
    for (int level = 0; level < BUCKETS.length - 1; level++) {
      if ((xor >>> SHIFT[level + 1]) == 0L) {
        return (level << 6) | (int) ((timeMs >>> SHIFT[level]) & (BUCKETS[level] - 1));
      }
    }
    return (BUCKETS.length - 1) << 6;
  }

  /** Links a node into the bucket named by {@link #bucketIndex(long)} and marks the slot. */
  private void linkBucket(int bucket, HE node) {
    int level = bucket >>> 6;
    int slot = bucket & 63;
    link(wheel[level][slot], node);
    occupied[level] |= 1L << slot;
  }

  /**
   * Waits until the next bucket is due or a newly scheduled entry wakes the owner. Must be called
   * by the owner thread only.
   *
   * @throws InterruptedException when the scheduler shuts down
   */
  @SuppressWarnings("all")
  void awaitNext() throws InterruptedException {
    long sleepMs;
    lock.lock();
    try {
      // Permits are hints that a schedule may have moved the next deadline. A permit released
      // while this thread was awake is already reflected in nextDelay(), so consuming it would
      // only return immediately and burn a loop iteration. Draining under the lock makes every
      // earlier link visible to nextDelay(); a later link releases a fresh permit after the wait
      // begins.
      wakeup.drainPermits();
      long delay = nextDelay();
      if (delay == Long.MAX_VALUE) {
        wakeAtMs = Long.MAX_VALUE;
        sleepMs = Long.MAX_VALUE;
      } else {
        // nextDelay is relative to the wheel clock, which was sampled before this batch was
        // evaluated. The sleep must be measured from the real clock: waiting the full delay from
        // here would add the batch's evaluation time to every wait, and wakeAtMs would describe
        // an instant later than the actual sleep target.
        long deadline = clockMs + delay;
        sleepMs = deadline - TimeSource.monotonicMillis();
        wakeAtMs = sleepMs > 0L ? deadline : 0L;
      }
    } finally {
      lock.unlock();
    }
    if (sleepMs > 0L) {
      wakeup.tryAcquire(sleepMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Milliseconds until the next occupied bucket (or a detached entry waiting to be evaluated).
   * Levels are time-orthogonal, so the first occupied bucket in level order is the earliest. Each
   * level's {@link #occupied} word skips the empty span in one load, and a stale bit left by an
   * unlink is cleared here the first time it is found empty.
   *
   * @return the delay, or {@link Long#MAX_VALUE} when the wheel is empty
   */
  @GuardedBy("lock")
  private long nextDelay() {
    if (pending.wheelNext != pending) {
      // The next advance appends this list to the owner's batch, so wake immediately instead of
      // adding a millisecond of latency to an already-due schedule or an exception's leftover batch.
      return 0L;
    }

    for (int level = 0; level < BUCKETS.length; level++) {
      long bits = occupied[level];
      if (bits == 0L) {
        continue;
      }

      HE[] buckets = wheel[level];
      long ticks = clockMs >>> SHIFT[level];
      long spanMask = (1L << SHIFT[level]) - 1L;
      int mask = buckets.length - 1;
      int start = (int) (ticks & mask);
      while (bits != 0L) {
        int ahead = nextOccupied(bits, buckets.length, start);
        int slot = (start + ahead) & mask;

        if (buckets[slot].wheelNext != buckets[slot]) {
          // A node is never placed in the current slot; max(1) keeps a violated invariant to a
          // one-tick wake instead of a zero-delay spin. A current-slot hit must wait one full
          // span tick, not one millisecond.
          long bucketsAhead = Math.max(1L, ahead);
          long delay = (bucketsAhead << SHIFT[level]) - (clockMs & spanMask);
          return Math.max(1L, delay);
        }

        // An unlink emptied this bucket without clearing its bit; drop it and keep looking.
        bits &= ~(1L << slot);
        occupied[level] = bits;
      }
    }
    return Long.MAX_VALUE;
  }

  /**
   * The distance in slots from {@code start} to the first set bit, rotating within the level's
   * bucket count.
   *
   * <p>{@code occupied[level]} never has a bit at or above {@code length}, so the search is two
   * trailing-zero counts: the first covers the slots from {@code start} to the end, and the second
   * wraps to the head. No mask or {@code length == 64} special case is needed, and the result is
   * always in {@code [0, length)}.
   *
   * @param bits   the level's occupied bits; never {@code 0}
   * @param length the level's bucket count
   * @param start  the current slot, where the search begins
   * @return a slot distance in {@code [0, length)}
   */
  private static int nextOccupied(long bits, int length, int start) {
    long fromStart = bits >>> start;
    return fromStart != 0L ? Long.numberOfTrailingZeros(fromStart) : Long.numberOfTrailingZeros(bits) + length - start;
  }

  /** @return checks currently linked in the wheel's buckets */
  int scheduledCount() {
    lock.lock();
    try {
      return scheduledCount;
    } finally {
      lock.unlock();
    }
  }

  /** Wakes the owner; used by shutdown. */
  void signal() {
    lock.lock();
    try {
      wakeAtMs = 0L;
      wakeup.release();
    } finally {
      lock.unlock();
    }
  }

  /** Adds the entry at the tail of the list. */
  private static void link(HE head, HE node) {
    node.wheelPrev = head.wheelPrev;
    node.wheelNext = head;
    head.wheelPrev.wheelNext = node;
    head.wheelPrev = node;
  }

  /** Removes the entry from its list; only called for a linked node. */
  private static void unlink(HE node) {
    node.wheelPrev.wheelNext = node.wheelNext;
    node.wheelNext.wheelPrev = node.wheelPrev;
    node.wheelPrev = null;
    node.wheelNext = null;
  }
}
