package io.github.hyshmily.zeta.worker.detection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
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

  @Captor
  private ArgumentCaptor<EvaluationContext> ctxCaptor;

  private Evaluator evaluator;

  @BeforeEach
  void setUp() {
    // The momentum time constant is derived from the detector's window span in
    // the constructor — stub a realistic span (16 slices × 63ms ≈ 1s) so the
    // moving-average math in the tests below is exercised at production scale.
    when(detector.getWindowSize()).thenReturn(16);
    when(detector.getTimeMillisPerSlice()).thenReturn(63L);
    evaluator = new DefaultEvaluator(detector, stateMachine, new FastLaneRuleManagerImpl(List.of()));
  }

  @Nested
  class Evaluate {

    @Test
    void shouldReturnDecision() {
      when(detector.addCount("key", 5L)).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(eq("key"), eq(true), eq(false), any(), any())).thenReturn(
        new ZetaDecision(DecisionType.HOT, "key", null)
      );

      ZetaDecision result = evaluator.evaluate("key", 5L);
      assertThat(result.type()).isEqualTo(DecisionType.HOT);
    }

    @Test
    void shouldSeedWindowAverageWithWindowSum() {
      when(detector.addCount("key", 5L)).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(eq("key"), eq(true), eq(false), ctxCaptor.capture(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );

      evaluator.evaluate("key", 5L);

      EvaluationContext ctx = ctxCaptor.getValue();
      // First evaluation seeds the window average with one full window sum —
      // momentum 1.0 (neutral) is exactly the right first impression.
      assertThat(ctx.cmsCount()).isEqualTo(100L);
      assertThat(ctx.windowSum()).isEqualTo(100L);
      assertThat(ctx.threshold()).isEqualTo(10L);
      assertThat(ctx.adjustedLogThreshold()).isEqualTo(ctx.logThreshold());
    }

    @Test
    void momentumIsNeutralForSustainedKey() {
      when(detector.addCount(eq("key"), anyLong())).thenReturn(1000L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), ctxCaptor.capture(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );

      for (int i = 0; i < 30; i++) {
        evaluator.evaluate("key", 50L);
      }

      // Sustained window level: average ≈ windowSum → momentum ≈ 1 → the bar
      // is unadjusted (the old unit-broken EMA pegged momentum at a clamp and
      // shifted the bar by a constant ln(10) for every key with history).
      EvaluationContext ctx = ctxCaptor.getValue();
      assertThat(ctx.adjustedLogThreshold()).isCloseTo(ctx.logThreshold(), org.assertj.core.data.Offset.offset(0.05));
    }

    @Test
    void momentumRaisesBarForBurstSpike() {
      when(detector.addCount(eq("key"), anyLong())).thenReturn(1000L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), ctxCaptor.capture(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );
      for (int i = 0; i < 30; i++) {
        evaluator.evaluate("key", 50L);
      }

      // Sudden 5× spike: the average lags behind → momentum < 1 → bar raised.
      when(detector.addCount(eq("key"), anyLong())).thenReturn(5000L);
      evaluator.evaluate("key", 250L);

      EvaluationContext ctx = ctxCaptor.getValue();
      assertThat(ctx.adjustedLogThreshold()).isGreaterThan(ctx.logThreshold() + 1.0);
    }

    @Test
    void momentumLowersBarForCoolingKey() {
      when(detector.addCount(eq("key"), anyLong())).thenReturn(1000L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), ctxCaptor.capture(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );
      for (int i = 0; i < 30; i++) {
        evaluator.evaluate("key", 50L);
      }

      // Window collapses to 1/10 while the average still carries history →
      // momentum > 1 → bar lowered (sustained key stays HOT more easily).
      when(detector.addCount(eq("key"), anyLong())).thenReturn(100L);
      evaluator.evaluate("key", 5L);

      EvaluationContext ctx = ctxCaptor.getValue();
      assertThat(ctx.adjustedLogThreshold()).isLessThan(ctx.logThreshold() - 2.0);
    }
  }

  @Nested
  class EvictStale {

    @Test
    void shouldRemoveEntriesOlderThanStaleAfterMs() {
      when(detector.addCount(any(), anyLong())).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), any(), any())).thenReturn(
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
      when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), any(), any())).thenReturn(
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
    when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), ctxCaptor.capture(), any())).thenReturn(
      new ZetaDecision(DecisionType.NONE, "key", null)
    );

    for (int i = 0; i < 10; i++) {
      evaluator.evaluate("key", 1L);
    }

    EvaluationContext ctx = ctxCaptor.getValue();
    assertThat(ctx.cv()).isFinite();
  }

  @Nested
  class FastLane {

    private Evaluator fastLaneEvaluator;

    @BeforeEach
    void setUp() {
      FastLaneRuleManager ruleManager = new FastLaneRuleManagerImpl(
        List.of(new FastLaneRuleManager.FastLaneRule("hot:*", 500))
      );
      fastLaneEvaluator = new DefaultEvaluator(detector, stateMachine, ruleManager);
    }

    @Test
    void shouldPassIsFastlaneTrueWhenFastLaneRuleMatchesAndAboveThreshold() {
      when(detector.addCount("hot:key", 10L)).thenReturn(600L);
      when(stateMachine.evaluate(eq("hot:key"), eq(true), eq(true), any(), any())).thenReturn(
        new ZetaDecision(DecisionType.HOT, "hot:key", null)
      );

      ZetaDecision result = fastLaneEvaluator.evaluate("hot:key", 10L);
      assertThat(result.type()).isEqualTo(DecisionType.HOT);
    }

    @Test
    void shouldPassIsFastlaneFalseWhenFastLaneRuleMatchesButBelowThreshold() {
      when(detector.addCount("hot:key", 10L)).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(eq("hot:key"), eq(true), eq(false), any(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "hot:key", null)
      );
      fastLaneEvaluator.evaluate("hot:key", 10L);
      verify(stateMachine).evaluate(eq("hot:key"), eq(true), eq(false), any(), any());
    }

    @Test
    void shouldPassIsFastlaneFalseWhenKeyDoesNotMatchAnyRule() {
      when(detector.addCount("normal:key", 10L)).thenReturn(200L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(eq("normal:key"), eq(true), eq(false), any(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "normal:key", null)
      );
      fastLaneEvaluator.evaluate("normal:key", 10L);
      verify(stateMachine).evaluate(eq("normal:key"), eq(true), eq(false), any(), any());
    }
  }

  @Nested
  class FastLaneGate {

    @Test
    void disabledGate_shouldNotConsultRuleManagerEvenWhenRuleWouldMatch() {
      FastLaneRuleManager manager = org.mockito.Mockito.mock(FastLaneRuleManager.class);
      Evaluator gated = new DefaultEvaluator(detector, stateMachine, manager, false);

      when(detector.addCount("hot:key", 10L)).thenReturn(600L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(eq("hot:key"), eq(true), eq(false), any(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "hot:key", null)
      );

      gated.evaluate("hot:key", 10L);

      org.mockito.Mockito.verifyNoInteractions(manager);
      verify(stateMachine).evaluate(eq("hot:key"), eq(true), eq(false), any(), any());
    }

    @Test
    void explicitlyEnabledGate_consultsRuleManager() {
      FastLaneRuleManager manager = org.mockito.Mockito.mock(FastLaneRuleManager.class);
      Evaluator gated = new DefaultEvaluator(detector, stateMachine, manager, true);

      when(detector.addCount("k", 1L)).thenReturn(10L);
      when(manager.match("k")).thenReturn(new FastLaneRuleManager.FastLaneRule("k", 5L));
      when(stateMachine.evaluate(eq("k"), eq(true), eq(true), any(), any())).thenReturn(
        new ZetaDecision(DecisionType.HOT, "k", null)
      );

      ZetaDecision result = gated.evaluate("k", 1L);
      assertThat(result.type()).isEqualTo(DecisionType.HOT);
    }
  }

  @Nested
  class BatchGlobalRatio {

    @Test
    void nonPositiveRatio_isNeutral_noTrendInflation() {
      when(detector.addCount(any(), anyLong())).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), ctxCaptor.capture(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );

      for (int i = 0; i < 6; i++) {
        evaluator.evaluate("key", 1L, 0.0);
      }

      // Constant window sums with a neutral ratio -> flat trend (1.0), never
      // the 10x inflation the old windowSum/0.1 clamp produced.
      assertThat(ctxCaptor.getValue().trendStrength()).isCloseTo(1.0, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void positiveRatio_normalisesTrend() {
      when(detector.addCount(any(), anyLong())).thenReturn(100L);
      when(detector.getThreshold()).thenReturn(10L);
      when(stateMachine.evaluate(any(), anyBoolean(), anyBoolean(), ctxCaptor.capture(), any())).thenReturn(
        new ZetaDecision(DecisionType.NONE, "key", null)
      );

      for (int i = 0; i < 6; i++) {
        evaluator.evaluate("key", 1L, 2.0);
      }

      // windowSum/2.0 = 50 vs preceding mean 100 -> trend 0.5.
      assertThat(ctxCaptor.getValue().trendStrength()).isCloseTo(0.5, org.assertj.core.data.Offset.offset(1e-9));
    }
  }
}
