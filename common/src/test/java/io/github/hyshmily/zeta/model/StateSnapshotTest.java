package io.github.hyshmily.zeta.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StateSnapshotTest {

  @Test
  void shouldCreateSnapshotWithAllFields() {
    StateSnapshot snap = new StateSnapshot("k1", "CONFIRMED_HOT", 3, 0, 2.5, 1.0, 1, 7);
    assertThat(snap.key()).isEqualTo("k1");
    assertThat(snap.currentState()).isEqualTo("CONFIRMED_HOT");
    assertThat(snap.hotStreak()).isEqualTo(3);
    assertThat(snap.coolStreak()).isEqualTo(0);
    assertThat(snap.posteriorMean()).isEqualTo(2.5);
    assertThat(snap.accumulatedPrecision()).isEqualTo(1.0);
    assertThat(snap.lowResetCount()).isEqualTo(1);
    assertThat(snap.mutationSeq()).isEqualTo(7);
  }

  @Test
  void shouldCreateSnapshotWithDefaultValues() {
    StateSnapshot snap = new StateSnapshot("k2", "COLD", 0, 0, 2.3026, 0.0, 0, 0);
    assertThat(snap.posteriorMean()).isEqualTo(2.3026);
    assertThat(snap.accumulatedPrecision()).isZero();
  }

  @Test
  void shouldSupportBuilder() {
    StateSnapshot snap = StateSnapshot.builder()
      .key("k3")
      .currentState("PRE_COOLING")
      .hotStreak(1)
      .coolStreak(5)
      .posteriorMean(3.0)
      .accumulatedPrecision(2.0)
      .lowResetCount(0)
      .mutationSeq(9)
      .build();
    assertThat(snap.currentState()).isEqualTo("PRE_COOLING");
    assertThat(snap.hotStreak()).isEqualTo(1);
    assertThat(snap.coolStreak()).isEqualTo(5);
    assertThat(snap.mutationSeq()).isEqualTo(9);
  }

  @Test
  void shouldSupportFluentAccessors() {
    StateSnapshot snap = new StateSnapshot("k4", "CANDIDATE_HOT", 2, 1, 2.0, 0.5, 2, 4);
    assertThat(snap.key()).isEqualTo("k4");
    assertThat(snap.currentState()).isEqualTo("CANDIDATE_HOT");
    assertThat(snap.mutationSeq()).isEqualTo(4);
  }
}
