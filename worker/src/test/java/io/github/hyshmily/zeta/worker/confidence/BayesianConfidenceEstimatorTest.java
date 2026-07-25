package io.github.hyshmily.zeta.worker.confidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BayesianConfidenceEstimator}.
 */
class BayesianConfidenceEstimatorTest {

  private static final BayesianConfidenceEstimator DEFAULT = new BayesianConfidenceEstimator(2.3026, 2.0, 0.8);

  private static final BayesianConfidenceEstimator OPTIMISTIC = new BayesianConfidenceEstimator(2.3026, 2.0, 0.5);

  @Nested
  class ConfidenceLevels_defaultParams {

    @Test
    void farBelowThreshold_shouldBeLow() {
      ProbabilityResult r = DEFAULT.evaluate(1, Math.log(10), null);
      assertThat(r.probability()).isLessThan(0.05);
      assertThat(r.level()).isEqualTo(ConfidenceLevel.LOW);
    }

    @Test
    void moderatelyAboveThreshold_shouldBeMedium() {
      ProbabilityResult r = DEFAULT.evaluate(30, Math.log(10), null);
      assertThat(r.probability()).isBetween(0.80, 0.95);
      assertThat(r.level()).isEqualTo(ConfidenceLevel.MEDIUM);
    }

    @Test
    void farAboveThreshold_shouldBeHigh() {
      ProbabilityResult r = DEFAULT.evaluate(100, Math.log(10), null);
      assertThat(r.probability()).isGreaterThan(0.95);
      assertThat(r.level()).isEqualTo(ConfidenceLevel.HIGH);
    }

    @Test
    void atThreshold_shouldBeLow() {
      ProbabilityResult r = DEFAULT.evaluate(10, Math.log(10), null);
      assertThat(r.probability()).isLessThan(0.80);
      assertThat(r.level()).isEqualTo(ConfidenceLevel.LOW);
    }
  }

  @Nested
  class ConfidenceLevels_optimisticParams {

    @Test
    void farBelowThreshold_shouldBeLow() {
      ProbabilityResult r = OPTIMISTIC.evaluate(1, Math.log(10), null);
      assertThat(r.probability()).isLessThan(0.05);
      assertThat(r.level()).isEqualTo(ConfidenceLevel.LOW);
    }

    @Test
    void moderatelyAboveThreshold_shouldBeMedium() {
      ProbabilityResult r = OPTIMISTIC.evaluate(20, Math.log(10), null);
      assertThat(r.probability()).isBetween(0.80, 0.95);
      assertThat(r.level()).isEqualTo(ConfidenceLevel.MEDIUM);
    }

    @Test
    void farAboveThreshold_shouldBeHigh() {
      ProbabilityResult r = OPTIMISTIC.evaluate(100, Math.log(10), null);
      assertThat(r.probability()).isGreaterThan(0.95);
      assertThat(r.level()).isEqualTo(ConfidenceLevel.HIGH);
    }
  }

  @Nested
  class EdgeCases {

    @Test
    void zeroObservedCount_shouldUseLogOfOne() {
      ProbabilityResult r = DEFAULT.evaluate(0, Math.log(10), null);
      assertThat(r.probability()).isLessThan(0.05);
      assertThat(r.posteriorMean()).isNotNaN();
    }

    @Test
    void zeroThreshold_shouldUseLogOfOne() {
      ProbabilityResult r = DEFAULT.evaluate(100, Math.log(1), null);
      assertThat(r.probability()).isGreaterThan(0.95);
    }

    @Test
    void largeCount_shouldSaturateHigh() {
      ProbabilityResult r = DEFAULT.evaluate(1_000_000, Math.log(10), null);
      assertThat(r.probability()).isGreaterThan(0.99);
      assertThat(r.level()).isEqualTo(ConfidenceLevel.HIGH);
    }

    @Test
    void equalPriorAndObservation_shouldYieldFiftyPercent_optimistic() {
      ProbabilityResult r = OPTIMISTIC.evaluate(10, Math.log(10), null);
      assertThat(r.posteriorMean()).isCloseTo(2.3026, within(0.01));
      assertThat(r.probability()).isCloseTo(0.5, within(0.01));
    }
  }

  @Nested
  class CvAdjustment {

    @Test
    void stableTraffic_shouldIncreaseConfidence() {
      ProbabilityResult noCv = DEFAULT.evaluate(30, Math.log(10), null);
      ProbabilityResult stableCv = DEFAULT.evaluate(30, Math.log(10), 0.1);
      assertThat(stableCv.probability()).isGreaterThan(noCv.probability());
    }

    @Test
    void burstyTraffic_shouldDecreaseConfidence() {
      ProbabilityResult noCv = DEFAULT.evaluate(30, Math.log(10), null);
      ProbabilityResult burstyCv = DEFAULT.evaluate(30, Math.log(10), 0.8);
      assertThat(burstyCv.probability()).isLessThan(noCv.probability());
    }

    @Test
    void normalCv_shouldNotChangeStd() {
      ProbabilityResult noCv = DEFAULT.evaluate(30, Math.log(10), null);
      ProbabilityResult normalCv = DEFAULT.evaluate(30, Math.log(10), 0.3);
      assertThat(noCv.probability()).isCloseTo(normalCv.probability(), within(0.01));
    }

    @Test
    void extremeBurstyCv_shouldCapAtThreefoldStd() {
      ProbabilityResult capped = DEFAULT.evaluate(30, Math.log(10), 5.0);
      ProbabilityResult atCap = DEFAULT.evaluate(30, Math.log(10), 1.5);
      assertThat(atCap.probability()).isCloseTo(capped.probability(), within(0.001));
    }

    @Test
    void cvPassedThrough_shouldBeInResult() {
      ProbabilityResult r = DEFAULT.evaluate(30, Math.log(10), 0.4);
      assertThat(r.cv()).isEqualTo(0.4);
    }
  }

  @Nested
  class PosteriorDistribution {

    @Test
    void posteriorStd_shouldBeSmallerThanPriorStd() {
      ProbabilityResult r = DEFAULT.evaluate(30, Math.log(10), null);
      assertThat(r.posteriorStd()).isLessThan(2.0);
    }

    @Test
    void posteriorMean_shouldBeBetweenPriorAndObservation_optimistic() {
      ProbabilityResult r = OPTIMISTIC.evaluate(50, Math.log(10), null);
      assertThat(r.posteriorMean()).isGreaterThan(Math.log(10)).isLessThan(Math.log(50));
    }
  }

  @Nested
  class AccumulatedPrior {

    @Test
    void firstEvaluation_accumulatedPrecisionShouldBeLikelihoodPrecision() {
      ProbabilityResult r = DEFAULT.evaluateWithAccumulatedPrior(30, Math.log(10), null, 2.3026, 0.0);
      double expectedLp = 1.0 / (0.8 * 0.8);
      assertThat(r.accumulatedPrecision()).isCloseTo(expectedLp, within(1e-12));
    }

    @Test
    void repeatedEvaluation_accumulatedPrecisionShouldIncreaseUpToCap() {
      double mean = 2.3026;
      double prec = 0.0;
      double maxPrec = BayesianConfidenceEstimator.MAX_EFFECTIVE_COUNT / (0.8 * 0.8);
      for (int i = 0; i < 10; i++) {
        ProbabilityResult r = DEFAULT.evaluateWithAccumulatedPrior(30, Math.log(10), null, mean, prec);
        mean = r.posteriorMean();
        prec = r.accumulatedPrecision();
        assertThat(prec).isLessThanOrEqualTo(maxPrec + 1e-12);
      }
      assertThat(prec).isCloseTo(maxPrec, within(1e-12));
    }

    @Test
    void accumulatedPrior_shouldIncreasePosteriorPrecision() {
      ProbabilityResult single = DEFAULT.evaluate(30, Math.log(10), null);
      ProbabilityResult accumulated = DEFAULT.evaluateWithAccumulatedPrior(
        30, Math.log(10), null, 2.3026, 3.0 / (0.8 * 0.8)
      );
      assertThat(accumulated.posteriorStd()).isLessThan(single.posteriorStd());
    }

    @Test
    void consistentlyModerateKey_shouldReachMediumWithAccumulation() {
      double mean = 2.3026;
      double prec = 0.0;
      ProbabilityResult last = null;
      for (int i = 0; i < 5; i++) {
        last = DEFAULT.evaluateWithAccumulatedPrior(15, Math.log(10), null, mean, prec);
        mean = last.posteriorMean();
        prec = last.accumulatedPrecision();
      }
      assertThat(last.level()).isIn(ConfidenceLevel.MEDIUM, ConfidenceLevel.HIGH);
    }

    @Test
    void cvAdjustment_shouldAffectAccumulatedPrecisionRate() {
      double mean = 2.3026;
      double prec = 0.0;
      ProbabilityResult stable = null;
      ProbabilityResult bursty = null;
      for (int i = 0; i < 3; i++) {
        stable = DEFAULT.evaluateWithAccumulatedPrior(30, Math.log(10), 0.1, mean, prec);
        mean = stable.posteriorMean();
        prec = stable.accumulatedPrecision();
      }
      mean = 2.3026;
      prec = 0.0;
      for (int i = 0; i < 3; i++) {
        bursty = DEFAULT.evaluateWithAccumulatedPrior(30, Math.log(10), 1.0, mean, prec);
        mean = bursty.posteriorMean();
        prec = bursty.accumulatedPrecision();
      }
      assertThat(stable.accumulatedPrecision()).isGreaterThan(bursty.accumulatedPrecision());
    }
  }
}
