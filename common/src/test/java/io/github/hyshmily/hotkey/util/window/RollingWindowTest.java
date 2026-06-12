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
package io.github.hyshmily.hotkey.util.window;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RollingWindowTest {

  @Test
  void addAndSum() {
    RollingWindow w = new RollingWindow(5, 500);
    w.add(10);
    w.add(20);
    assertEquals(30, w.sum());
  }

  @Test
  void max_empty() {
    RollingWindow w = new RollingWindow(5, 500);
    assertEquals(0, w.max());
  }

  @Test
  void max_nonZero() {
    RollingWindow w = new RollingWindow(5, 500);
    w.add(3);
    w.add(7);
    w.add(2);
    assertEquals(12, w.max());
  }

  @Test
  void minNonZero_allZero() {
    RollingWindow w = new RollingWindow(5, 500);
    assertEquals(Long.MAX_VALUE, w.minNonZero());
  }

  @Test
  void reset() {
    RollingWindow w = new RollingWindow(3, 300);
    w.add(100);
    w.add(200);
    assertEquals(300, w.sum());
    w.reset();
    assertEquals(0, w.sum());
  }

  @Test
  void singleBucket() {
    RollingWindow w = new RollingWindow(1, 100);
    w.add(42);
    assertEquals(42, w.sum());
    assertEquals(42, w.max());
  }

  @Test
  void add_shouldWorkConcurrently() throws InterruptedException {
    int threadCount = 10;
    int addsPerThread = 1000;
    RollingWindow w = new RollingWindow(10, 1000);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      Thread worker = new Thread(() -> {
        for (int i = 0; i < addsPerThread; i++) {
          w.add(1);
        }
        latch.countDown();
      });
      worker.start();
    }

    latch.await();
    assertEquals((long) threadCount * addsPerThread, w.sum());
  }

  @Test
  void timeAdvance_shouldZeroExpiredBuckets() throws InterruptedException {
    RollingWindow w = new RollingWindow(3, 600);
    w.add(10);
    Thread.sleep(700); // > 600 ms → full window elapsed
    w.add(20);
    // After a full window, the old bucket (10) should be zeroed
    assertEquals(20, w.sum());
  }

  @Test
  void size() {
    assertEquals(4, new RollingWindow(4, 400).size());
    assertEquals(1, new RollingWindow(1, 100).size());
  }
}
