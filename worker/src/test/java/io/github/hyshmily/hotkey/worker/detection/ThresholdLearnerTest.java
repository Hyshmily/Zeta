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
package io.github.hyshmily.hotkey.worker.detection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.worker.config.WorkerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link ThresholdLearner}.
 */
@ExtendWith(MockitoExtension.class)
class ThresholdLearnerTest {

  @Mock
  private GlobalQpsEstimator qpsEstimator;

  @Mock
  private SlidingWindowDetector detector;

  private WorkerProperties properties;

  @BeforeEach
  void setUp() {
    properties = new WorkerProperties();
    properties.getGlobalQpsDynamicThreshold().setLearningPeriodMs(0);
    properties.getGlobalQpsDynamicThreshold().setHotThresholdRatio(0.01);
  }

  /**
   * Verifies that the {@link ThresholdLearner} skips processing during the initial learning period.
   */
  @Test
  void shouldSkipDuringLearningPeriod() {
    properties.getGlobalQpsDynamicThreshold().setLearningPeriodMs(Long.MAX_VALUE);
    ThresholdLearner learner = new ThresholdLearner(qpsEstimator, detector, properties);
    learner.run();
    verifyNoInteractions(qpsEstimator);
  }

  /**
   * Verifies that the learner skips threshold update when the current QPS is zero.
   */
  @Test
  void shouldSkipWhenQpsIsZero() {
    when(qpsEstimator.getQps()).thenReturn(0.0);
    ThresholdLearner learner = new ThresholdLearner(qpsEstimator, detector, properties);
    learner.run();
    verifyNoInteractions(detector);
  }

  /**
   * Verifies that the learner updates the detector threshold when QPS exceeds the change tolerance.
   */
  @Test
  void shouldUpdateThresholdWhenQpsExceedsTolerance() {
    when(qpsEstimator.getQps()).thenReturn(100_000.0);
    when(detector.getThreshold()).thenReturn(500L);
    ThresholdLearner learner = new ThresholdLearner(qpsEstimator, detector, properties);
    learner.run();
    // newThreshold = 100000 * 0.01 = 1000
    verify(detector).setThreshold(1000L);
  }

  /**
   * Verifies that the learner skips threshold update when the QPS change is within the configured tolerance.
   */
  @Test
  void shouldSkipThresholdUpdateWhenChangeWithinTolerance() {
    when(qpsEstimator.getQps()).thenReturn(105.0);
    when(detector.getThreshold()).thenReturn(100L);
    ThresholdLearner learner = new ThresholdLearner(qpsEstimator, detector, properties);
    learner.run();
    // newThreshold = 105 * 0.01 = 1.05 -> max(10, 1) = 10
    // changeRate = |10 - 100| / 100 = 0.9 > 0.5 -> should update
    verify(detector).setThreshold(10L);
  }

  /**
   * Verifies that the learner floors the threshold at a minimum value of 10.
   */
  @Test
  void shouldFloorThresholdAtTen() {
    when(qpsEstimator.getQps()).thenReturn(50.0);
    when(detector.getThreshold()).thenReturn(500L);
    ThresholdLearner learner = new ThresholdLearner(qpsEstimator, detector, properties);
    learner.run();
    // newThreshold = max(10, floor(50 * 0.01)) = max(10, 0) = 10
    verify(detector).setThreshold(10L);
  }

  /**
   * Verifies that exceptions thrown by the QPS estimator are caught and do not propagate.
   */
  @Test
  void shouldCatchExceptionsGracefully() {
    when(qpsEstimator.getQps()).thenThrow(new RuntimeException("test error"));
    ThresholdLearner learner = new ThresholdLearner(qpsEstimator, detector, properties);
    learner.run();
    // should not propagate exception
    verifyNoInteractions(detector);
  }
}
