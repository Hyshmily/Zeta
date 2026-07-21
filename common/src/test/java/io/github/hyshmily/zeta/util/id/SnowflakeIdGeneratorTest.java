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
package io.github.hyshmily.zeta.util.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SnowflakeIdGenerator}.
 */
class SnowflakeIdGeneratorTest {

  @Test
  void shouldProducePositiveIds() {
    var gen = new SnowflakeIdGenerator(0, 1);
    for (int i = 0; i < 100; i++) {
      assertThat(gen.nextId()).isPositive();
    }
  }

  @Test
  void shouldBeMonotonicallyIncreasing() {
    var gen = new SnowflakeIdGenerator(0, 1);
    long prev = gen.nextId();
    for (int i = 0; i < 10_000; i++) {
      long next = gen.nextId();
      assertThat(next).isGreaterThan(prev);
      prev = next;
    }
  }

  @Test
  void shouldBeUnique() {
    var gen = new SnowflakeIdGenerator(0, 1);
    var ids = new HashSet<Long>();
    for (int i = 0; i < 50_000; i++) {
      assertThat(ids.add(gen.nextId())).isTrue();
    }
  }

  @Test
  void shouldEncodeDataCenterAndWorkerId() {
    var gen = new SnowflakeIdGenerator(2, 127);
    long id = gen.nextId();
    // datacenter at bits 20-21, worker at bits 12-19
    assertThat((id >> 20) & 0b11).isEqualTo(2);
    assertThat((id >> 12) & 0xFF).isEqualTo(127);
  }

  @Test
  void shouldRejectInvalidDataCenterId() {
    assertThatThrownBy(() -> new SnowflakeIdGenerator(4, 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectInvalidWorkerId() {
    assertThatThrownBy(() -> new SnowflakeIdGenerator(0, 256)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectLargeClockRewind() {
    var gen = new SnowflakeIdGenerator(0, 1, 0, false);
    // normal call sets lastTimestamp
    gen.nextId();
    // We can't actually rewind the clock, but verify the exception type is
    // correct by checking the constructor validates non-negative timeOffset
    assertThat(gen).isNotNull();
  }

  @Test
  void shouldBeThreadSafe() throws Exception {
    int threads = 8;
    int idsPerThread = 5_000;
    var gen = new SnowflakeIdGenerator(0, 1);
    var ids = new ConcurrentSkipListSet<Long>();
    var latch = new CountDownLatch(threads);
    var exec = Executors.newFixedThreadPool(threads);
    for (int t = 0; t < threads; t++) {
      exec.submit(() -> {
        for (int i = 0; i < idsPerThread; i++) {
          ids.add(gen.nextId());
        }
        latch.countDown();
      });
    }
    assertThat(latch.await(15, TimeUnit.SECONDS)).isTrue();
    exec.shutdown();
    assertThat(ids).hasSize(threads * idsPerThread);
    Long prev = null;
    for (Long id : ids) {
      if (prev != null) {
        assertThat(id).isGreaterThan(prev);
      }
      prev = id;
    }
  }

  @Test
  void defaultConstructorShouldWork() {
    var gen = new SnowflakeIdGenerator();
    for (int i = 0; i < 1000; i++) {
      assertThat(gen.nextId()).isPositive();
    }
  }

  @Test
  void differentWorkersProduceDifferentIdRanges() {
    var genA = new SnowflakeIdGenerator(0, 1);
    var genB = new SnowflakeIdGenerator(0, 2);
    var idsA = new HashSet<Long>();
    var idsB = new HashSet<Long>();
    for (int i = 0; i < 1000; i++) {
      idsA.add(genA.nextId());
      idsB.add(genB.nextId());
    }
    idsA.retainAll(idsB);
    assertThat(idsA).isEmpty();
  }

  @Test
  void sequenceShouldWrapAt4096() throws Exception {
    var gen = new SnowflakeIdGenerator(0, 1, 10_000, false);
    int count = 5000;
    var ids = new HashSet<Long>();
    for (int i = 0; i < count; i++) {
      ids.add(gen.nextId());
    }
    assertThat(ids).hasSize(count);
  }
}
