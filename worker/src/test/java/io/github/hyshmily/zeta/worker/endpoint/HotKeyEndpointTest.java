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
package io.github.hyshmily.zeta.worker.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.worker.detection.lifecycle.CoolingScheduler;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HE;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import io.github.hyshmily.zeta.worker.detection.rule.HotKeyRule;
import io.github.hyshmily.zeta.worker.detection.rule.MatchType;
import io.github.hyshmily.zeta.worker.detection.rule.RuleResolver;
import io.github.hyshmily.zeta.worker.detection.rule.RuleSet;
import io.github.hyshmily.zeta.worker.detection.window.SlidingWindowDetector;
import io.github.hyshmily.zeta.worker.detection.window.WindowGeometry;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The summary exposes effective runtime state only; the two elapsed fields never go negative. */
class HotKeyEndpointTest {

  @Test
  void durationsAreNonNegativeWhenTheEntrySnapshotIsAheadOfTheClock() {
    HotKeyRule rule = new HotKeyRule("r", MatchType.EXACT, "k", 0, 0, 1, 0L, 0L, 60_000L, null);
    HotKeyRegistry registry = mock(HotKeyRegistry.class);
    HE entry = mock(HE.class);
    when(registry.entry("k")).thenReturn(entry);
    when(entry.rule()).thenReturn(rule);
    when(entry.promotedAt()).thenReturn(Long.MAX_VALUE);
    when(entry.belowSince()).thenReturn(Long.MAX_VALUE - 1L);
    when(entry.nextCheckAt()).thenReturn(Long.MAX_VALUE);
    when(entry.promotedVersion()).thenReturn(7L);

    SlidingWindowDetector detector = mock(SlidingWindowDetector.class);
    when(detector.windowSum("k")).thenReturn(0L);

    HotKeyEndpoint endpoint = new HotKeyEndpoint(
      mock(RuleResolver.class),
      WindowGeometry.of(1_000, 16),
      registry,
      mock(CoolingScheduler.class),
      detector
    );

    Map<String, Object> view = endpoint.info("k");

    assertThat(view.get("promotedForMs")).isEqualTo(0L);
    assertThat(view.get("belowForMs")).isEqualTo(0L);
    assertThat(view.get("promotedVersion")).isEqualTo(7L);
  }

  /**
   * An overdue check reports the remaining time as negative: that is the "overdue by" reading the
   * operator uses while the wheel hand-off or the repair sweep is still pending, not an error.
   */
  @Test
  void anOverdueCheckReportsNegativeRemainingTime() {
    HotKeyRule rule = new HotKeyRule("r", MatchType.EXACT, "k", 0, 0, 1, 0L, 0L, 60_000L, null);
    HotKeyRegistry registry = mock(HotKeyRegistry.class);
    HE entry = mock(HE.class);
    when(registry.entry("k")).thenReturn(entry);
    when(entry.rule()).thenReturn(rule);
    when(entry.promotedAt()).thenReturn(1L);
    when(entry.belowSince()).thenReturn(0L);
    when(entry.nextCheckAt()).thenReturn(1L);
    when(entry.promotedVersion()).thenReturn(1L);

    HotKeyEndpoint endpoint = new HotKeyEndpoint(
      mock(RuleResolver.class),
      WindowGeometry.of(1_000, 16),
      registry,
      mock(CoolingScheduler.class),
      mock(SlidingWindowDetector.class)
    );

    assertThat((Long) endpoint.info("k").get("nextCheckInMs")).isNegative();
  }

  @Test
  @SuppressWarnings("unchecked")
  void summaryExposesOnlyEffectiveRuntimeState() {
    HotKeyRule rule = new HotKeyRule("user-prefix", MatchType.PREFIX, "user:", 0, 0, 160, 0L, 0L, 60_000L, null);
    HotKeyRule defaults = new HotKeyRule("default", MatchType.PREFIX, "default:", 0, 0, 100, 0L, 0L, 60_000L, null);
    RuleResolver rules = new RuleResolver(RuleSet.of(List.of(rule), defaults), 10, new WorkerDetectionMetrics(null));
    HotKeyRegistry registry = mock(HotKeyRegistry.class);
    when(registry.activeCount()).thenReturn(1234);
    when(registry.rejectedCount()).thenReturn(12L);
    CoolingScheduler cooling = mock(CoolingScheduler.class);
    when(cooling.scheduled()).thenReturn(900);

    HotKeyEndpoint endpoint = new HotKeyEndpoint(
      rules,
      WindowGeometry.of(1_000, 16),
      registry,
      cooling,
      mock(SlidingWindowDetector.class)
    );

    Map<String, Object> summary = endpoint.info(null);

    Map<String, Object> detection = (Map<String, Object>) summary.get("detection");
    assertThat(detection).containsOnlyKeys("window", "minHotCount", "rulesHash");
    assertThat(detection).containsEntry("window", "992ms/16");

    Map<String, Object> hot = (Map<String, Object>) summary.get("hot");
    assertThat(hot).containsOnlyKeys("active", "rejected");

    Map<String, Object> cool = (Map<String, Object>) summary.get("cooling");
    assertThat(cool).containsOnlyKeys("scheduled");

    List<Map<String, Object>> ruleViews = (List<Map<String, Object>>) summary.get("rules");
    assertThat(ruleViews).hasSize(2);
    assertThat(ruleViews.get(0)).containsOnlyKeys("id", "effectiveHotQps", "matches", "promotions", "aborts", "cools");
  }
}
