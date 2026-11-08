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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * The detector accumulator's capacity gate.
 *
 * <p>The gate judges the merged map's size. A stable hot set overlaps almost completely, so a
 * pre-merge sum would flush at roughly half the cap and split one key's counts across two batches;
 * the post-merge check lets one merge overshoot by at most the snapshot's own size.
 *
 * <p>A fresh counter anchors {@code lastDetectorFlushMs} at construction, so its first tide
 * accumulates like every other one instead of flushing a partial batch immediately.
 */
class AccessCounterTideTest {

  /** Below the cap on its own, and exactly the overlap shape that used to double-count. */
  private static final int HOT_SET = 9_000;

  private static Map<String, Long> hotSet(String prefix, int size) {
    Map<String, Long> snapshot = new HashMap<>(size * 2);
    for (int i = 0; i < size; i++) {
      snapshot.put(prefix + i, 1L);
    }
    return snapshot;
  }

  private static AccessCounter counterFor(HotKeyDetector detector) {
    return new AccessCounter(detector, null, mock(ScheduledExecutorService.class), 50L, 500L);
  }

  /**
   * A repeated hot-set snapshot must accumulate, not flush: the merged size stays under the cap, so
   * the sketch ingest waits for the detector interval.
   */
  @Test
  void repeatedHotSnapshots_doNotFlushAgain() {
    HotKeyDetector detector = mock(HotKeyDetector.class);
    AccessCounter counter = counterFor(detector);
    Map<String, Long> hot = hotSet("k", HOT_SET);

    counter.onTide(hot, 50L);
    counter.onTide(hot, 50L);
    counter.onTide(hot, 50L);

    verify(detector, times(0)).ingest(any());
  }

  /**
   * Crossing the cap with genuinely new keys flushes ONE merged batch whose counts are summed, not
   * two batches split across the residual and the snapshot.
   */
  @Test
  void crossingTheCap_flushesOneMergedBatch() {
    HotKeyDetector detector = mock(HotKeyDetector.class);
    // The accumulator is reused and cleared after ingest, so capture a copy at
    // call time instead of holding the live map reference.
    List<Map<String, Long>> ingested = new java.util.ArrayList<>();
    org.mockito.Mockito.doAnswer(invocation -> {
      ingested.add(new HashMap<>(invocation.getArgument(0)));
      return null;
    })
      .when(detector)
      .ingest(any());

    AccessCounter counter = counterFor(detector);
    Map<String, Long> hot = hotSet("k", HOT_SET);

    counter.onTide(hot, 50L);
    counter.onTide(hot, 50L);
    counter.onTide(hot, 50L);
    counter.onTide(hotSet("n", AccessCounter.DEFAULT_MAX_ACCUMULATED_KEYS - HOT_SET + 1), 50L);

    assertThat(ingested).hasSize(1);
    assertThat(ingested.get(0))
      .as("one merged batch, not a residual plus a snapshot")
      .hasSize(AccessCounter.DEFAULT_MAX_ACCUMULATED_KEYS + 1)
      .containsEntry("k0", 3L);
  }

  /**
   * A tide that alone fills the cap is never merged into the accumulator: the
   * scan's 100k-key ceiling must not drive the reused map's table capacity.
   */
  @Test
  void snapshotAtCap_isIngestedSeparatelyFromResidual() {
    HotKeyDetector detector = mock(HotKeyDetector.class);
    List<Map<String, Long>> ingested = new java.util.ArrayList<>();
    org.mockito.Mockito.doAnswer(invocation -> {
      ingested.add(new HashMap<>(invocation.getArgument(0)));
      return null;
    })
      .when(detector)
      .ingest(any());

    AccessCounter counter = counterFor(detector);
    counter.onTide(hotSet("k", 100), 50L);
    counter.onTide(hotSet("n", AccessCounter.DEFAULT_MAX_ACCUMULATED_KEYS), 50L);

    assertThat(ingested).hasSize(2);
    assertThat(ingested.get(0)).as("the residual batch first").hasSize(100).containsEntry("k0", 1L);
    assertThat(ingested.get(1))
      .as("the cap-sized snapshot follows without being merged")
      .hasSize(AccessCounter.DEFAULT_MAX_ACCUMULATED_KEYS)
      .doesNotContainKey("k0");
  }

  /**
   * A due tide merges into the residual and hands the sketch one combined batch:
   * the overlapping hot set is updated once, not once per source map.
   */
  @Test
  void dueTide_mergesIntoTheResidualBeforeIngest() {
    HotKeyDetector detector = mock(HotKeyDetector.class);
    List<Map<String, Long>> ingested = new java.util.ArrayList<>();
    org.mockito.Mockito.doAnswer(invocation -> {
      ingested.add(new HashMap<>(invocation.getArgument(0)));
      return null;
    })
      .when(detector)
      .ingest(any());

    AccessCounter counter = counterFor(detector);
    Map<String, Long> hot = hotSet("k", 10);
    counter.onTide(hot, 50L);
    try {
      TimeSource.setTimeOffsetForTest(0L, 600L);
      counter.onTide(hot, 50L);
    } finally {
      TimeSource.setTimeOffsetForTest(0L, 0L);
    }

    assertThat(ingested).hasSize(1);
    assertThat(ingested.get(0)).hasSize(10).containsEntry("k0", 2L);
  }
}
