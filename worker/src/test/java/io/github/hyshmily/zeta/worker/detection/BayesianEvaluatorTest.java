package io.github.hyshmily.zeta.worker.detection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.EvaluationContext;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import io.github.hyshmily.zeta.worker.rule.impl.FastLaneRuleManagerImpl;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BayesianEvaluatorTest {

  @Mock
  private SlidingWindowDetector detector;

  @Mock
  private ZetaBayesianSM stateMachine;

  @Mock
  private TopK workerTopK;

  @Captor
  private ArgumentCaptor<EvaluationContext> ctxCaptor;

  private Evaluator evaluator;

  @BeforeEach
  void setUp() {
    evaluator = new Evaluator(detector, stateMachine, workerTopK, new FastLaneRuleManagerImpl(List.of()), null);
  }

  @Nested
  class Evaluate {

    @Test
    void shouldReturnDecision() {
      when(detector.addCount("key", 5L)).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount("key")).thenReturn(42L);
      when(stateMachine.evaluate(eq("key"), eq(true), eq(false), any())).thenReturn(
        new ZetaDecision(DecisionType.HOT, "key", null)
      );

      ZetaDecision result = evaluator.evaluate("key", 5L);
      assertThat(result.type()).isEqualTo(DecisionType.HOT);
    }

    @Test
    void shouldPassCmsCountAsObservation_whenAvailable() {
      when(detector.addCount("key", 5L)).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount("key")).thenReturn(42L);
      when(stateMachine.evaluate(eq("key"), eq(true), eq(false), ctxCaptor.capture())).thenReturn(
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
      when(detector.addCount("key", 5L)).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount("key")).thenReturn(0L);
      when(stateMachine.evaluate(eq("key"), eq(true), eq(false), ctxCaptor.capture())).thenReturn(
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
      when(detector.addCount(any(), anyLong())).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount(any())).thenReturn(0L);
      when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );

      evaluator.evaluate("key", 1L);
      evaluator.evictStale(1);
      evaluator.evictStale(1);
    }

    @Test
    void shouldKeepRecentEntries() throws InterruptedException {
      when(detector.addCount(any(), anyLong())).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount(any())).thenReturn(0L);
      when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), any())).thenReturn(
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
    when(detector.addCount(any(), anyLong())).thenReturn(100L);
    when(detector.getThreshold()).thenReturn(10L);
    when(workerTopK.estimatedCount(any())).thenReturn(0L);
    when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), ctxCaptor.capture())).thenReturn(
      new ZetaDecision(DecisionType.NONE, "key", null)
    );

    for (int i = 0; i < 10; i++) {
      evaluator.evaluate("key", 1L);
    }

    EvaluationContext ctx = ctxCaptor.getValue();
    assertThat(ctx.cv()).isNotNull();
  }

  @Nested
  class FastLane {

    private Evaluator fastLaneEvaluator;

    @BeforeEach
    void setUp() {
      FastLaneRuleManager ruleManager = new FastLaneRuleManagerImpl(List.of(
        new FastLaneRuleManager.FastLaneRule("hot:*", 500)
      ));
      fastLaneEvaluator = new Evaluator(detector, stateMachine, workerTopK, ruleManager, null);
    }

    @Test
    void shouldPassIsFastlaneTrueWhenFastLaneRuleMatchesAndAboveThreshold() {
      when(detector.addCount("hot:key", 10L)).thenReturn(600L);
      when(stateMachine.evaluate(eq("hot:key"), eq(true), eq(true), any())).thenReturn(
        new ZetaDecision(DecisionType.HOT, "hot:key", null)
      );

      ZetaDecision result = fastLaneEvaluator.evaluate("hot:key", 10L);
      assertThat(result.type()).isEqualTo(DecisionType.HOT);
    }

    @Test
    void shouldPassIsFastlaneFalseWhenFastLaneRuleMatchesButBelowThreshold() {
      when(detector.addCount("hot:key", 10L)).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount("hot:key")).thenReturn(0L);
      when(stateMachine.evaluate(eq("hot:key"), eq(true), eq(false), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "hot:key", null)
      );
      fastLaneEvaluator.evaluate("hot:key", 10L);
      verify(stateMachine).evaluate(eq("hot:key"), eq(true), eq(false), any());
    }

    @Test
    void shouldPassIsFastlaneFalseWhenKeyDoesNotMatchAnyRule() {
      when(detector.addCount("normal:key", 10L)).thenReturn(200L);
      when(detector.getThreshold()).thenReturn(10L);
      when(workerTopK.estimatedCount("normal:key")).thenReturn(0L);
      when(stateMachine.evaluate(eq("normal:key"), eq(true), eq(false), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "normal:key", null)
      );
      fastLaneEvaluator.evaluate("normal:key", 10L);
      verify(stateMachine).evaluate(eq("normal:key"), eq(true), eq(false), any());
    }
  }
}
