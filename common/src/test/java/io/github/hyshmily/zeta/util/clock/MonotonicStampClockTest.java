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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.util.TimeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for the lock-free {@code dataVersion} clock (design decisions: version stamps): the
 * design decisions: version stamps StampClock contract holds without a monitor — strict node-local
 * increase, observe-floor, decodable layout, and burst-driven sequence
 * overflow that rolls the timestamp forward instead of blocking.
 */
class MonotonicStampClockTest {

  @Test
  void nextStamp_isStrictlyIncreasing() {
    MonotonicStampClock clock = new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long prev = 0;
    for (int i = 0; i < 10_000; i++) {
      long next = clock.nextStamp();
      assertThat(next).isPositive().isGreaterThan(prev);
      prev = next;
    }
  }

  @Test
  void concurrentAllocation_isUniqueAndMonotone() throws Exception {
    MonotonicStampClock clock = new MonotonicStampClock(2L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    int threads = 8;
    int perThread = 20_000;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    Set<Long> seen = ConcurrentHashMap.newKeySet();
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);
    List<Throwable> failures = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      pool.submit(() -> {
        ready.countDown();
        try {
          go.await();
          long prev = -1;
          for (int i = 0; i < perThread; i++) {
            long s = clock.nextStamp();
            if (s <= prev || !seen.add(s)) {
              synchronized (failures) {
                failures.add(new AssertionError("non-monotone or duplicate stamp: " + s));
              }
              return;
            }
            prev = s;
          }
        } catch (Throwable e) {
          synchronized (failures) {
            failures.add(e);
          }
        }
      });
    }
    ready.await();
    go.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    assertThat(failures).isEmpty();
    assertThat(seen).hasSize(threads * perThread);
  }

  @Test
  void observe_advancesTheFloorAboveEveryPeerStamp() {
    MonotonicStampClock clock = new MonotonicStampClock(3L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long local = clock.nextStamp();
    long peerFarAhead = StampClock.timestampOf(local) + 60_000; // a peer a minute ahead
    // Build a stamp in the peer's future window with the same layout.
    long peerStamp = peerStampNear(peerFarAhead);
    clock.observe(peerStamp);
    for (int i = 0; i < 100; i++) {
      assertThat(clock.nextStamp()).isGreaterThan(peerStamp);
    }
    // Non-positive and saturated stamps are ignored.
    clock.observe(0L);
    clock.observe(-5L);
    clock.observe(Long.MAX_VALUE);
    assertThat(clock.nextStamp()).isPositive();
  }

  private static long peerStampNear(long epochMillis) {
    // timeBase layout: (ms - EPOCH) << 22 | node << 12
    return ((epochMillis - StampClock.EPOCH) << 22) | (7L << 12);
  }

  @Test
  void observedPeerStamp_doesNotLeakThePeersNodeIdentity() {
    MonotonicStampClock clock = new MonotonicStampClock(3L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long peerTs = TimeSource.currentTimeMillis() - StampClock.EPOCH + 60_000L;
    // Peer node 7 sorts ABOVE this node (3) in the same window — the branch
    // that used to copy `cur & ~SEQ_MASK` and mint node-7 stamps.
    long peerStamp = (peerTs << 22) | (7L << 12) | 5L;
    clock.observe(peerStamp);

    long prev = peerStamp;
    for (int i = 0; i < 100; i++) {
      long next = clock.nextStamp();
      assertThat(next).isGreaterThan(prev);
      assertThat((next >>> 12) & 0x3FFL).as("node identity must stay local").isEqualTo(3L);
      prev = next;
    }
  }

  @Test
  void mutuallyObservedClocks_neverMintTheSameStamp() {
    MonotonicStampClock nodeA = new MonotonicStampClock(8L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    MonotonicStampClock nodeB = new MonotonicStampClock(9L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long aStamp = nodeA.nextStamp();
    long bStamp = nodeB.nextStamp();
    // Each node observes the other's newest stamp. Before the identity fix,
    // A (lower node bits) adopted B's identity and both minted bStamp + 1.
    nodeA.observe(bStamp);
    nodeB.observe(aStamp);

    Set<Long> seen = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < 2_000; i++) {
      assertThat(seen.add(nodeA.nextStamp())).as("nodeA duplicate at iter %s", i).isTrue();
      assertThat(seen.add(nodeB.nextStamp())).as("nodeB duplicate at iter %s", i).isTrue();
    }
  }

  @Test
  void burstOverflow_rollsTheTimestampForwardInsteadOfBlocking() {
    MonotonicStampClock clock = new MonotonicStampClock(4L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long first = clock.nextStamp();
    // Exhaust a window's sequence space and keep going: no blocking, strictly
    // increasing stamps, all decodable slightly ahead of the wall clock.
    long prev = first;
    for (int i = 0; i < 10_000; i++) {
      long next = clock.nextStamp();
      assertThat(next).isGreaterThan(prev);
      prev = next;
    }
    long skew = clock.skewMillis();
    assertThat(skew).isGreaterThanOrEqualTo(0L);
  }

  @Test
  void layout_isDecodableByTheSharedTimestampDecoder() {
    MonotonicStampClock clock = new MonotonicStampClock(5L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long stamp = clock.nextStamp();
    long decoded = StampClock.timestampOf(stamp);
    long now = TimeSource.currentTimeMillis();
    // The stamp encodes the current time within one window of tolerance.
    assertThat(Math.abs(decoded - now)).isLessThan(5_000L);
  }

  @Test
  void sameMillisecondAllocationsOnDistinctNodes_doNotCollide() {
    MonotonicStampClock nodeA = new MonotonicStampClock(8L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    MonotonicStampClock nodeB = new MonotonicStampClock(9L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    Set<Long> seen = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < 5_000; i++) {
      long a = nodeA.nextStamp();
      if (!seen.add(a)) {
        throw new AssertionError("duplicate within nodeA at iter " + i + ": " + Long.toHexString(a));
      }
      long b = nodeB.nextStamp();
      if (!seen.add(b)) {
        throw new AssertionError(
          "nodeB collided at iter " + i + ": B=" + Long.toHexString(b) + " A_last=" + Long.toHexString(a)
        );
      }
    }
  }
}
