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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HE;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import io.github.hyshmily.zeta.worker.detection.rule.RuleBinding;
import io.github.hyshmily.zeta.worker.detection.rule.RuleResolver;
import io.github.hyshmily.zeta.worker.detection.window.SlidingWindowDetector;
import io.github.hyshmily.zeta.worker.dispatch.DecisionDispatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The sweep's relief handoff and the already-hot report fast path. */
class DetectionPipelineTest {

  private SlidingWindowDetector detector;
  private RuleResolver rules;
  private HotKeyRegistry registry;
  private DecisionDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    detector = mock(SlidingWindowDetector.class);
    rules = mock(RuleResolver.class);
    registry = mock(HotKeyRegistry.class);
    dispatcher = mock(DecisionDispatcher.class);
  }

  private DetectionPipeline pipeline(long retentionMs) {
    return new DetectionPipeline(detector, rules, registry, dispatcher, retentionMs, true);
  }

  @Test
  void sweepUsesTheWindowAlignedIdleBoundaryWhenReliefWasRequested() {
    when(detector.consumeReliefRequest()).thenReturn(true);

    pipeline(120_000L).sweep();

    verify(detector).evictIdle();
    verify(detector, never()).evictStale(anyLong());
  }

  @Test
  void sweepUsesTheRetentionThresholdForAnOrdinaryScan() {
    when(detector.consumeReliefRequest()).thenReturn(false);

    pipeline(120_000L).sweep();

    verify(detector).evictStale(120_000L);
  }

  @Test
  void anAlreadyHotReportRecordsTheCountWithoutComputingTheWindowSum() {
    when(registry.isHot("hot")).thenReturn(true);
    long batchSample = 1_234L;
    long batchSlice = 7L;
    when(detector.sliceIndex(batchSample)).thenReturn(batchSlice);

    pipeline(120_000L).onReport("hot", 5L, batchSample, batchSlice);

    verify(detector).recordCountAtSlice("hot", 5L, batchSlice);
    verify(detector, never()).addCountAtSlice(anyString(), anyLong(), anyLong());
  }

  @Test
  void anAcceptedPromotionIsHandedToTheDispatcher() {
    ZetaDecision decision = ZetaDecision.hot("k", 42L);
    HE entry = stubPromotion(decision);

    pipeline(120_000L).onReport("k", 2L, 1_234L, 7L);

    verify(dispatcher).submitPromotion(entry);
  }

  @Test
  void aRefusedPromotionIsNeverHandedToTheDispatcher() {
    when(detector.sliceIndex(1_234L)).thenReturn(7L);
    when(detector.addCountAtSlice("k", 2L, 7L)).thenReturn(9L);
    RuleBinding binding = mock(RuleBinding.class);
    when(rules.resolveIfEligible("k", 9L)).thenReturn(binding);
    when(registry.promoteEntry("k", binding, 1_234L)).thenReturn(null);

    pipeline(120_000L).onReport("k", 2L, 1_234L, 7L);

    verify(dispatcher, never()).submitPromotion(any());
  }

  private HE stubPromotion(ZetaDecision decision) {
    when(detector.sliceIndex(1_234L)).thenReturn(7L);
    when(detector.addCountAtSlice("k", 2L, 7L)).thenReturn(9L);
    RuleBinding binding = mock(RuleBinding.class);
    when(rules.resolveIfEligible("k", 9L)).thenReturn(binding);
    HE entry = mock(HE.class);
    when(entry.hotDecision()).thenReturn(decision);
    when(entry.promotedVersion()).thenReturn(decision.decisionVersion());
    when(registry.promoteEntry("k", binding, 1_234L)).thenReturn(entry);
    return entry;
  }
}
