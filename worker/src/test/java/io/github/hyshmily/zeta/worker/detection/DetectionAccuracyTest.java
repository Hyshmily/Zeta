package io.github.hyshmily.zeta.worker.detection;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.model.ZetaDecision.DecisionType;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.confidence.BayesianConfidenceEstimator;
import io.github.hyshmily.zeta.worker.confidence.ConfidenceEvaluator;
import io.github.hyshmily.zeta.worker.rule.impl.FastLaneRuleManagerImpl;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Ground-truth detection-accuracy harness (ADR-0080 standing item): drives the
 * real App→Worker decision pipeline ({@link SlidingWindowDetector} →
 * {@link DefaultEvaluator} → Bayesian state machine) with labeled synthetic
 * traffic and scores precision/recall on the emitted {@link ZetaDecision}s.
 *
 * <p>Time is fast-forwarded deterministically via
 * {@link TimeSource#setTimeOffsetForTest} on the monotonic axis (one slice per
 * step) — no sleeps, no flakiness. The state machine's wall-clock-coupled
 * paths are parked for the harness ({@code counterWindowMs=0} disables idle
 * crediting; a maximal rebroadcast interval silences ADR-0024 re-emission), so
 * every decision observed here comes from the count/confidence path under
 * test. Offsets are reset after each test; the suite runs sequentially, so no
 * other test observes the shifted clock.
 */
class DetectionAccuracyTest {

  /** 1600ms / 16 slices = exact 100ms slices (no alignment rounding). */
  private static final long WINDOW_MS = 1600L;
  private static final int SLICES = 16;
  private static final long SLICE_MS = 100L;
  /** Absolute window-sum hot threshold: cold traffic sums to ~32, hot to ~1280. */
  private static final long THRESHOLD = 100L;

  private static final int HOT_KEYS = 8;
  private static final int COLD_KEYS = 40;
  private static final int EVALS_PER_SLICE = 2;
  private static final int HOT_COUNT_PER_EVAL = 40;
  private static final int COLD_COUNT_PER_EVAL = 1;
  /** Simulated slices; decisions are scored after slice 8 (detector warmup). */
  private static final int TOTAL_SLICES = 24;
  private static final int WARMUP_SLICES = 8;

  @AfterEach
  void resetClock() {
    TimeSource.setTimeOffsetForTest(0, 0);
  }

  private static DefaultEvaluator harnessEvaluator(SlidingWindowDetector detector) {
    ConfidenceEvaluator confidence = new BayesianConfidenceEstimator(
      BayesianConfidenceEstimator.PRIOR_MEAN,
      2.0,
      0.8,
      0.95,
      0.76
    );
    io.github.hyshmily.zeta.worker.detection.impl.ZetaBayesianSM sm =
      new io.github.hyshmily.zeta.worker.detection.impl.ZetaBayesianSM(
        2,
        10,
        1,
        confidence,
        BayesianConfidenceEstimator.PRIOR_MEAN,
        Long.MAX_VALUE,
        0L
      );
    return new DefaultEvaluator(detector, sm, new FastLaneRuleManagerImpl(List.of()), false);
  }

  @Test
  @DisplayName("labeled traffic: hot keys all detected (recall), cold keys never fire (precision)")
  void labeledTraffic_precisionAndRecall() {
    SlidingWindowDetector detector = new SlidingWindowDetector(WINDOW_MS, SLICES, THRESHOLD);
    DefaultEvaluator evaluator = harnessEvaluator(detector);

    Set<String> hotFired = new HashSet<>();
    Set<String> coldFired = new HashSet<>();
    Set<String> cooled = new HashSet<>();

    for (int slice = 0; slice < TOTAL_SLICES; slice++) {
      TimeSource.setTimeOffsetForTest(0, slice * SLICE_MS);
      for (int e = 0; e < EVALS_PER_SLICE; e++) {
        for (int i = 0; i < HOT_KEYS; i++) {
          ZetaDecision d = evaluator.evaluate("hot-" + i, HOT_COUNT_PER_EVAL, 1.0);
          if (slice >= WARMUP_SLICES) {
            if (d.type() == DecisionType.HOT) {
              hotFired.add("hot-" + i);
            }
            if (d.type() == DecisionType.COOL) {
              cooled.add("hot-" + i);
            }
          }
        }
        for (int i = 0; i < COLD_KEYS; i++) {
          ZetaDecision d = evaluator.evaluate("cold-" + i, COLD_COUNT_PER_EVAL, 1.0);
          if (slice >= WARMUP_SLICES && d.type() == DecisionType.HOT) {
            coldFired.add("cold-" + i);
          }
        }
      }
    }

    double recall = (double) hotFired.size() / HOT_KEYS;
    double precisionDenom = hotFired.size() + coldFired.size();
    double precision = precisionDenom == 0 ? 1.0 : (double) hotFired.size() / precisionDenom;

    assertThat(recall)
      .as("recall: every sustained hot key must earn a HOT decision (fired=%s)", hotFired)
      .isEqualTo(1.0);
    assertThat(coldFired)
      .as("precision: cold keys must never fire HOT (precision=%.3f)", precision)
      .isEmpty();
    assertThat(cooled)
      .as("stability: continuously-hot keys must never flap to COOL")
      .isEmpty();
  }

  @Test
  @DisplayName("drifted hot key cools after traffic stops")
  void driftedHotKey_coolsAfterTrafficStops() {
    SlidingWindowDetector detector = new SlidingWindowDetector(WINDOW_MS, SLICES, THRESHOLD);
    DefaultEvaluator evaluator = harnessEvaluator(detector);

    // Heat phase: earn HOT.
    boolean everHot = false;
    for (int slice = 0; slice < 12; slice++) {
      TimeSource.setTimeOffsetForTest(0, slice * SLICE_MS);
      for (int e = 0; e < EVALS_PER_SLICE; e++) {
        if (evaluator.evaluate("drift", HOT_COUNT_PER_EVAL, 1.0).type() == DecisionType.HOT) {
          everHot = true;
        }
      }
    }
    assertThat(everHot).as("harness precondition: key must go HOT while heated").isTrue();

    // Drift phase: silence. The key must eventually discharge via COOL
    // (bounded by the cool window count, not by the hard TTL).
    boolean cooled = false;
    for (int slice = 12; slice < 12 + 40; slice++) {
      TimeSource.setTimeOffsetForTest(0, slice * SLICE_MS);
      for (int e = 0; e < EVALS_PER_SLICE; e++) {
        if (evaluator.evaluate("drift", 0, 1.0).type() == DecisionType.COOL) {
          cooled = true;
        }
      }
    }
    assertThat(cooled).as("drifted key must discharge with a COOL decision").isTrue();
  }
}
