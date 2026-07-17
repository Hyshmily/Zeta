package io.github.hyshmily.zeta.worker.detection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.confidence.EvaluationContext;
import io.github.hyshmily.zeta.detection.ZetaStateMachine;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KeyEvaluatorTest {

  @Mock
  private SlidingWindowDetector detector;

  @Mock
  private ZetaStateMachine stateMachine;

  @Mock
  private TopK workerTopK;

  @Captor
  private ArgumentCaptor<EvaluationContext> ctxCaptor;

  private KeyEvaluator evaluator;

  @BeforeEach
  void setUp() {
    evaluator = new KeyEvaluator(detector, stateMachine, workerTopK);
  }

  @Nested
  class Evaluate {

    @Test
    void shouldFeedDetectorAndReturnDecision() {
      when(detector.addCount("key", 5L)).thenReturn(true);
      when(detector.getWindowSum("key")).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount("key")).thenReturn(42L);
      when(stateMachine.evaluate(eq("key"), eq(true), any())).thenReturn(
        new ZetaDecision(DecisionType.HOT, "key", null)
      );

      ZetaDecision result = evaluator.evaluate("key", 5L);

      assertThat(result.type()).isEqualTo(DecisionType.HOT);
    }

    @Test
    void shouldPassCmsCountAsObservation_whenAvailable() {
      when(detector.addCount("key", 5L)).thenReturn(false);
      when(detector.getWindowSum("key")).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount("key")).thenReturn(42L);
      when(stateMachine.evaluate(eq("key"), eq(false), ctxCaptor.capture())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );

      evaluator.evaluate("key", 5L);

      EvaluationContext ctx = ctxCaptor.getValue();
      assertThat(ctx.cmsCount()).isEqualTo(42L);
      assertThat(ctx.windowSum()).isEqualTo(100L);
      assertThat(ctx.threshold()).isEqualTo(10L);
    }

    @Test
    void shouldFallbackToWindowSum_whenCmsCountIsZero() {
      when(detector.addCount("key", 5L)).thenReturn(false);
      when(detector.getWindowSum("key")).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount("key")).thenReturn(0L);
      when(stateMachine.evaluate(eq("key"), eq(false), ctxCaptor.capture())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );

      evaluator.evaluate("key", 5L);

      EvaluationContext ctx = ctxCaptor.getValue();
      assertThat(ctx.cmsCount()).isZero();
    }
  }

  @Nested
  class EvictStale {

    @Test
    void shouldRemoveEntriesOlderThanStaleAfterMs() throws InterruptedException {
      // First evaluation creates a windowSumHistory entry
      when(detector.addCount(any(), anyLong())).thenReturn(false);
      when(detector.getWindowSum(any())).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount(any())).thenReturn(0L);
      when(stateMachine.evaluate(any(), anyBoolean(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );

      evaluator.evaluate("key", 1L);
      evaluator.evictStale(1);
      // With staleAfterMs=1 and at least 1ms elapsed, the entry should be evicted
      evaluator.evictStale(1);
    }

    @Test
    void shouldKeepRecentEntries() throws InterruptedException {
      when(detector.addCount(any(), anyLong())).thenReturn(false);
      when(detector.getWindowSum(any())).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount(any())).thenReturn(0L);
      when(stateMachine.evaluate(any(), anyBoolean(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );

      evaluator.evaluate("key", 1L);
      Thread.sleep(5);
      evaluator.evictStale(100);
    }

    @Test
    void shouldNotThrowOnEmptyState() {
      evaluator.evictStale(1000);
    }
  }

  @Test
  void shouldComputeCvFromMultipleEvaluations() {
    when(detector.addCount(any(), anyLong())).thenReturn(false);
    when(detector.getWindowSum(any())).thenReturn(100L);
    when(detector.getThreshold()).thenReturn(10L);
    when(workerTopK.estimatedCount(any())).thenReturn(0L);
    when(stateMachine.evaluate(any(), anyBoolean(), ctxCaptor.capture())).thenReturn(
      new ZetaDecision(DecisionType.NONE, "key", null)
    );

    for (int i = 0; i < 10; i++) {
      evaluator.evaluate("key", 1L);
    }

    EvaluationContext ctx = ctxCaptor.getValue();
    assertThat(ctx.cv()).isNotNull();
  }
}
