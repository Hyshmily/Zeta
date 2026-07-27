package io.github.hyshmily.zeta.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EvaluationContextTest {

  @Test
  void shouldCreateWithAllFields() {
    var ctx = new EvaluationContext(100L, 200L, 10L, 0.5, 3.0, 2.5, 1.2);
    assertThat(ctx.cmsCount()).isEqualTo(100L);
    assertThat(ctx.windowSum()).isEqualTo(200L);
    assertThat(ctx.threshold()).isEqualTo(10L);
    assertThat(ctx.cv()).isEqualTo(0.5);
    assertThat(ctx.logThreshold()).isEqualTo(3.0);
    assertThat(ctx.adjustedLogThreshold()).isEqualTo(2.5);
    assertThat(ctx.trendStrength()).isEqualTo(1.2);
  }

  @Test
  void shouldCreateWithConvenienceConstructorAndThresholdAboveOne() {
    var ctx = new EvaluationContext(50L, 100L, 10L, null, 1.5);
    assertThat(ctx.threshold()).isEqualTo(10L);
    assertThat(ctx.logThreshold()).isEqualTo(Math.log(10));
    assertThat(ctx.adjustedLogThreshold()).isEqualTo(Math.log(10));
    assertThat(ctx.cv()).isNull();
  }

  @Test
  void shouldCreateWithConvenienceConstructorAndThresholdBelowOne() {
    var ctx = new EvaluationContext(50L, 100L, 0L, null, 0.8);
    assertThat(ctx.threshold()).isZero();
    assertThat(ctx.logThreshold()).isEqualTo(Math.log(1));
    assertThat(ctx.adjustedLogThreshold()).isEqualTo(Math.log(1));
  }

  @Test
  void shouldProvideFastlaneSentinel() {
    assertThat(EvaluationContext.FASTLANE).isNotNull();
    assertThat(EvaluationContext.FASTLANE.cmsCount()).isZero();
    assertThat(EvaluationContext.FASTLANE.windowSum()).isZero();
  }

  @Test
  void shouldCreateWithCvNull() {
    var ctx = new EvaluationContext(1L, 2L, 3L, null, 4L, 5L, 6.0);
    assertThat(ctx.cv()).isNull();
  }
}
