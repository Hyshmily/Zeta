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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.rule.HotKeyRule;
import io.github.hyshmily.zeta.worker.detection.rule.MatchType;
import io.github.hyshmily.zeta.worker.detection.rule.RuleBinding;
import io.github.hyshmily.zeta.worker.detection.rule.RuleResolver;
import io.github.hyshmily.zeta.worker.detection.rule.RuleSet;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Exact firing, cascading and unlinking of the hierarchical wheel. */
class TimerWheelTest {

  private static final HotKeyRule RULE = new HotKeyRule("test", MatchType.PREFIX, "k", 0, 0, 1, 0L, 0L, 60_000L, null);

  /** The wheel only needs an identity node; the binding carries the rule for the registry tests. */
  private static final RuleBinding BINDING = new RuleResolver(
    RuleSet.of(List.of(), RULE),
    4,
    new WorkerDetectionMetrics(null)
  ).binding(RULE.id());

  private static HE entry(String key, long dueMs) {
    HE entry = new HE(key, BINDING, 0L, 1L);
    entry.nextCheckAt = dueMs;
    return entry;
  }

  /** Every level fires at the exact millisecond it was scheduled for, and exactly once. */
  @Test
  void firesEveryLevelAtItsExactMillisecond() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      int count = 500;
      int[] due = new int[count];
      HE[] nodes = new HE[count];
      for (int i = 0; i < count; i++) {
        due[i] = 1 + ((i * 137) % 5_000);
        nodes[i] = entry("k" + i, due[i]);
        wheel.schedule(nodes[i]);
      }

      int fired = 0;
      long maxLateness = 0;
      for (long now = 1; now <= 6_000; now++) {
        wheel.advance(now);
        HE node;
        while ((node = wheel.pollDetached()) != null) {
          int index = Integer.parseInt(node.key().substring(1));
          assertThat(node.nextCheckAt).isEqualTo(due[index]);
          assertThat(now).as("fired early").isGreaterThanOrEqualTo(node.nextCheckAt);
          maxLateness = Math.max(maxLateness, now - node.nextCheckAt);
          fired++;
        }
      }
      assertThat(fired).isEqualTo(count);
      assertThat(maxLateness).as("millisecond wheel fires exactly").isZero();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /** A ten-minute deadline crosses the top bucket and still fires on its millisecond. */
  @Test
  void aLongDeadlineIsCascadedDownToItsMillisecond() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      HE node = entry("k", 600_000L);
      wheel.schedule(node);

      wheel.advance(599_999L);
      assertThat(wheel.pollDetached()).isNull();

      wheel.advance(600_000L);
      assertThat(wheel.pollDetached()).isSameAs(node);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A deadline beyond the wheel's horizon is re-linked into the same top bucket on each rotation
   * and still fires on its exact millisecond.
   */
  @Test
  void aDeadlineBeyondTheHorizonSurvivesRepeatedTopRotations() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      HE node = entry("k", 7_200_000L);
      wheel.schedule(node);

      // The top level rotates every 524_288 ms; a two-hour deadline outlives thirteen rotations.
      for (long now = 524_288L; now <= 6_815_744L; now += 524_288L) {
        wheel.advance(now);
        assertThat(wheel.pollDetached()).as("fired early at %d ms", now).isNull();
      }

      wheel.advance(7_199_999L);
      assertThat(wheel.pollDetached()).as("fired before the deadline").isNull();

      wheel.advance(7_200_000L);
      assertThat(wheel.pollDetached()).isSameAs(node);
      assertThat(wheel.scheduledCount()).isZero();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A permit released while the owner is awake is already reflected in the next deadline. The
   * wait must drain those permits instead of returning once per stale signal.
   */
  @Test
  void awaitNextDoesNotSpinThroughStaleWakeups() throws Exception {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      wheel.schedule(entry("a", 10_000L));
      wheel.schedule(entry("b", 10_000L));
      wheel.schedule(entry("c", 10_000L));

      CountDownLatch started = new CountDownLatch(1);
      Thread waiter = new Thread(() -> {
        started.countDown();
        try {
          wheel.awaitNext();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      waiter.start();
      assertThat(started.await(3, TimeUnit.SECONDS)).isTrue();
      Thread.sleep(100L);

      assertThat(waiter.isAlive()).as("stale permits must not wake the wheel early").isTrue();
      waiter.interrupt();
      waiter.join(3_000L);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * The sleep is measured from the real clock, not from the wheel clock sampled before the batch.
   * A 400 ms batch on a 1000 ms deadline must sleep about 600 ms, not the full 1000 ms.
   */
  @Test
  void awaitNextSleepsOnlyTheRemainingTimeAfterTheBatch() throws Exception {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      wheel.schedule(entry("k", 1_000L));

      // Simulate 400 ms spent evaluating the batch. The wheel clock is still at zero.
      TimeSource.setTimeOffsetForTest(0, -base + 400);

      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch finished = new CountDownLatch(1);
      Thread waiter = new Thread(() -> {
        started.countDown();
        try {
          wheel.awaitNext();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          finished.countDown();
        }
      });
      waiter.start();
      try {
        assertThat(started.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(finished.await(1_000L, TimeUnit.MILLISECONDS))
          .as("the wait was shortened by the batch time")
          .isTrue();
      } finally {
        if (waiter.isAlive()) {
          waiter.interrupt();
        }
        waiter.join(3_000L);
      }
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * Zero and negative monotonic instants are real. An earlier schedule must wake a sleeping owner
   * whose current sleep target is negative; a zero-awake sentinel cannot see that deadline.
   */
  @Test
  void anEarlierScheduleWakesTheOwnerWhenTheSleepTargetIsNegative() throws Exception {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base - 20_000L);
      TimerWheel wheel = new TimerWheel();
      long clock = TimeSource.monotonicMillis();
      assertThat(clock).isNegative();
      wheel.schedule(entry("far", clock + 5_000L));

      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch finished = new CountDownLatch(1);
      Thread waiter = new Thread(() -> {
        started.countDown();
        try {
          wheel.awaitNext();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          finished.countDown();
        }
      });
      waiter.start();
      try {
        assertThat(started.await(3, TimeUnit.SECONDS)).isTrue();
        long deadline = System.nanoTime() + 3_000L * 1_000_000L;
        while (waiter.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
          Thread.sleep(5L);
        }
        assertThat(waiter.getState())
          .as("the owner is sleeping toward the negative deadline")
          .isEqualTo(Thread.State.TIMED_WAITING);

        wheel.schedule(entry("near", clock));
        assertThat(finished.await(1_000L, TimeUnit.MILLISECONDS))
          .as("the earlier deadline must release the sleeping owner")
          .isTrue();
      } finally {
        if (waiter.isAlive()) {
          waiter.interrupt();
        }
        waiter.join(3_000L);
      }
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A deadline inside the current coarse span is placed in a later slot, never in the slot the
   * wheel is currently consuming: a 4095 ms deadline must not be treated as due one 64 ms tick
   * later.
   */
  @Test
  void aDeadlineInsideTheCurrentCoarseSpanIsNotPlacedInTheCurrentSlot() throws Exception {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      wheel.advance(65L);
      wheel.schedule(entry("far", 4_160L));

      Method nextDelay = TimerWheel.class.getDeclaredMethod("nextDelay");
      nextDelay.setAccessible(true);
      long delay = (long) nextDelay.invoke(wheel);

      assertThat(delay).as("a 4095 ms deadline is not treated as due in 63 ms").isGreaterThan(4_000L);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * An unlink can empty a bucket without touching the occupied word; the next delay must clear the
   * stale bit and still report an empty wheel instead of waking for a bucket that no longer exists.
   */
  @Test
  void deschedulingTheOnlyEntryLeavesNoStaleBucketWakeup() throws Exception {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      HE node = entry("k", 10_000L);
      wheel.schedule(node);
      wheel.deschedule(node);

      Method nextDelay = TimerWheel.class.getDeclaredMethod("nextDelay");
      nextDelay.setAccessible(true);
      assertThat((long) nextDelay.invoke(wheel)).isEqualTo(Long.MAX_VALUE);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void rescheduleMovesTheDeadlineAndDescheduleCancelsIt() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      HE moved = entry("moved", 100L);
      HE cancelled = entry("cancelled", 100L);
      wheel.schedule(moved);
      wheel.schedule(cancelled);
      assertThat(wheel.scheduledCount()).isEqualTo(2);

      moved.nextCheckAt = 200L;
      wheel.reschedule(moved);
      wheel.deschedule(cancelled);
      assertThat(wheel.scheduledCount()).isEqualTo(1);

      wheel.advance(100L);
      assertThat(wheel.pollDetached()).as("the moved deadline is not due yet").isNull();

      wheel.advance(200L);
      assertThat(wheel.pollDetached()).isSameAs(moved);
      assertThat(wheel.pollDetached()).as("the cancelled check never fires").isNull();
      assertThat(wheel.scheduledCount()).isZero();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /** A due entry is detached exactly once; the owner's poll clears its state for the next re-arm. */
  @Test
  void aDetachedEntryIsHandedOverExactlyOnce() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      HE node = entry("k", 10L);
      wheel.schedule(node);

      wheel.advance(10L);
      assertThat(wheel.pollDetached()).isSameAs(node);
      assertThat(wheel.pollDetached()).isNull();

      // The registry's hand-off after the poll clears the state, then its re-arm links it again.
      node.wheelState = TimerWheel.UNSCHEDULED;
      node.nextCheckAt = 20L;
      wheel.reschedule(node);
      wheel.advance(20L);
      assertThat(wheel.pollDetached()).isSameAs(node);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * While a due entry is detached, a concurrent re-arm must not touch the owner's list: the owner
   * re-evaluates and re-links the fresh deadline on the way out.
   */
  @Test
  void aRescheduleOfADetachedEntryIsDeferredToTheOwner() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      HE node = entry("k", 10L);
      wheel.schedule(node);

      wheel.advance(10L);
      node.nextCheckAt = 50L;
      wheel.reschedule(node);

      HE detached = wheel.pollDetached();
      assertThat(detached).isSameAs(node);
      assertThat(node.wheelState).as("the hand-off stays detached for the registry").isEqualTo(TimerWheel.DETACHED);
      // The registry clears the hand-off under the key's stripe lock before it re-arms.
      node.wheelState = TimerWheel.UNSCHEDULED;
      node.nextCheckAt = 50L;
      wheel.reschedule(node);

      wheel.advance(49L);
      assertThat(wheel.pollDetached()).isNull();
      wheel.advance(50L);
      assertThat(wheel.pollDetached()).isSameAs(node);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * An undrained batch is already due. The next wait must return immediately instead of sleeping
   * toward the pending list or a bucket that cannot make the batch any later.
   */
  @Test
  void anUndrainedBatchWakesTheOwnerImmediately() throws Exception {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      wheel.schedule(entry("k", 10L));
      wheel.advance(10L);

      Method nextDelay = TimerWheel.class.getDeclaredMethod("nextDelay");
      nextDelay.setAccessible(true);
      assertThat((long) nextDelay.invoke(wheel)).isZero();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A batch left unprocessed by a failed evaluation stays ahead of the entries a later advance
   * detaches: one poll traversal reaches the remainder first and the new due entries behind it.
   */
  @Test
  void anUnfinishedBatchStaysAheadOfTheNextAdvancesEntries() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      TimerWheel wheel = new TimerWheel();
      HE first = entry("first", 10L);
      HE second = entry("second", 20L);
      wheel.schedule(first);

      // The owner advances but the evaluation throws before polling: the batch stays in hand.
      wheel.advance(10L);

      wheel.schedule(second);
      wheel.advance(20L);

      assertThat(wheel.pollDetached()).isSameAs(first);
      assertThat(wheel.pollDetached()).isSameAs(second);
      assertThat(wheel.pollDetached()).isNull();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }
}
