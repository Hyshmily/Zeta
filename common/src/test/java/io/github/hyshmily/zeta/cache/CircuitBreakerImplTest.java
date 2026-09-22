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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.impl.CircuitBreakerImpl;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CircuitBreakerImpl} covering all state transitions,
 * backoff scenarios, disabled path, and close lifecycle.
 */
class CircuitBreakerImplTest {

  /** Exception type exercising the direct-interface filter walk: {@code Serializable} is only on this class. */
  @SuppressWarnings("serial")
  private static final class SerializableRuntimeException extends RuntimeException implements Serializable {}

  private ZetaProperties.CircuitBreaker config;
  private CircuitBreakerImpl breaker;

  @BeforeEach
  void setUp() {
    config = new ZetaProperties.CircuitBreaker();
    config.setEnabled(true);
    config.setFailThreshold(0.5);
    config.setRequestVolumeThreshold(2);
    config.setWindowTimeMs(10_000);
    config.setWindowBuckets(10);
    config.setSingleTestIntervalMs(100);
    config.setLogEnabled(false);
    breaker = new CircuitBreakerImpl(config);
  }

  @AfterEach
  void tearDown() {
    breaker.close();
  }

  @Test
  void allowRequest_whenDisabled_shouldReturnTrue() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    assertThat(cb.allowRequest()).isTrue();
  }

  @Test
  void allowRequest_whenClosed_shouldReturnTrue() {
    assertThat(breaker.allowRequest()).isTrue();
  }

  @Test
  void allowRequest_whenOpenAndNotHalfOpen_shouldReturnFalse() throws Exception {
    triggerOpen();
    // Immediately after opening, the half-open interval has not elapsed
    assertThat(breaker.allowRequest()).isFalse();
  }

  @Test
  void allowRequest_whenOpenAndHalfOpenProbe_shouldReturnTrue() throws Exception {
    triggerOpen();
    // Advance time past the singleTestIntervalMs
    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();
  }

  @Test
  void onSuccess_whenDisabled_shouldDoNothing() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    cb.onSuccess();
    assertThat(cb.isOpen()).isFalse();
  }

  @Test
  void onSuccess_whenClosed_shouldIncrementSuccess() throws Exception {
    config.setFailThreshold(0.4);
    config.setRequestVolumeThreshold(3);
    breaker = new CircuitBreakerImpl(config);
    breaker.onSuccess();
    breaker.onSuccess();
    breaker.onFailure();
    breaker.onFailure();
    breaker.onFailure();
    // rate = 3/5 = 0.6 > 0.4, volume = 5 >= 3 → open
    assertThat(breaker.isOpen()).isTrue();
  }

  @Test
  void onSuccess_whenOpen_shouldCloseBreaker() throws Exception {
    triggerOpen();
    Thread.sleep(config.getSingleTestIntervalMs() + 50);
    assertThat(breaker.allowRequest()).isTrue();
    breaker.onSuccess();
    assertThat(breaker.isOpen()).isFalse();
  }

  /**
   * Regression: the OPEN→HALF_OPEN transitioner probe reserves its slot, so its
   * onSuccess() release is balanced and the half-open quota keeps admitting
   * exactly {@code halfOpenMaxProbes} probes. (The unreserved transitioner used
   * to leak -1 per transition, inflating the effective probe cap.)
   */
  @Test
  void halfOpenProbeQuota_notLeakedByTransitionerOnSuccess() throws Exception {
    config.setHalfOpenMaxProbes(2);
    config.setConsecutiveSuccessThreshold(100); // stay HALF_OPEN for the whole test
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();
    Thread.sleep(config.getSingleTestIntervalMs() + 50);

    assertThat(breaker.allowRequest()).isTrue(); // transitioner probe (reserved)
    breaker.onSuccess(); // releases exactly its own slot

    // Still HALF_OPEN: exactly maxProbes further probes, then refusal.
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isFalse();
  }

  /**
   * Regression: over-release floors at zero. A batch load reserves one probe
   * slot but settles per key (many onSuccess calls), and dedup-cache hits join
   * a future without ever reserving — the unbounded decrement used to drift
   * the counter negative and admit (maxProbes + |drift|) probes.
   */
  @Test
  void halfOpenQuota_floorsAtZeroUnderPerKeyOverRelease() throws Exception {
    config.setHalfOpenMaxProbes(2);
    config.setConsecutiveSuccessThreshold(100); // stay HALF_OPEN for the whole test
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();
    Thread.sleep(config.getSingleTestIntervalMs() + 50);

    assertThat(breaker.allowRequest()).isTrue(); // one reservation
    for (int i = 0; i < 5; i++) {
      breaker.onSuccess(); // per-key settle: 5 releases against 1 reservation
    }

    // Quota is capped at maxProbes (2) — not maxProbes + drift.
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isFalse();
  }

  @Test
  void onFailure_whenDisabled_shouldDoNothing() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    cb.onFailure();
  }

  @Test
  void onFailure_whenEnabled_shouldIncrementFail() throws Exception {
    triggerOpen();
    assertThat(breaker.isOpen()).isTrue();
  }

  @Test
  void isOpen_whenDisabled_shouldReturnFalse() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    cb.onFailure();
    cb.onFailure();
    assertThat(cb.isOpen()).isFalse();
  }

  @Test
  void isOpen_whenOpen_shouldReturnTrue() throws Exception {
    triggerOpen();
    assertThat(breaker.isOpen()).isTrue();
  }

  @Test
  void close_shouldCancelSlideFuture() {
    breaker.close();
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void close_shouldNotThrowWhenCalledTwice() {
    breaker.close();
    breaker.close();
  }

  @Test
  void evaluateThreshold_whenBelowVolume_shouldNotOpen() {
    // Only 1 failure, need 2 (requestVolumeThreshold)
    breaker.onFailure();
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void evaluateThreshold_whenRateBelowThreshold_shouldNotOpen() {
    // 1 success + 1 failure = 50% rate, failThreshold=0.5, strictly greater needed
    breaker.onSuccess();
    breaker.onFailure();
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void fullCycle_openHalfOpenClose() throws Exception {
    triggerOpen();
    assertThat(breaker.isOpen()).isTrue();

    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();

    breaker.onSuccess();
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void logEnabled_shouldNotThrow() {
    config.setLogEnabled(true);
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);

    cb.onFailure();
    // allowRequest on open breaker triggers log in half-open path
    cb.allowRequest();
  }

  // ── Exception filtering: include / exclude classification ──

  /**
   * {@code includeExceptions} matches by assignable type, not exact class: a
   * subclass of an included type counts as a failure (superclass walk).
   */
  @Test
  void onFailure_includeMatchesSubclass_shouldCountAsFailure() {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    config.setIncludeExceptions(List.of("java.lang.RuntimeException"));
    breaker = new CircuitBreakerImpl(config);

    breaker.onFailure(new NullPointerException("boom"));

    assertThat(breaker.isOpen()).isTrue();
  }

  /** An exception unrelated to the included type is ignorable (treated as success). */
  @Test
  void onFailure_includeUnrelated_shouldTreatAsIgnorable() {
    config.setIncludeExceptions(List.of("java.lang.IllegalStateException"));
    breaker = new CircuitBreakerImpl(config);

    breaker.onFailure(new NullPointerException("unrelated"));

    assertThat(breaker.isOpen()).isFalse();
  }

  /** A directly-declared interface of the thrown type matches the included filter. */
  @Test
  void onFailure_includeMatchesDirectInterface_shouldCountAsFailure() {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    config.setIncludeExceptions(List.of(Serializable.class.getName()));
    breaker = new CircuitBreakerImpl(config);

    // Serializable is not on RuntimeException's superclass chain — only the
    // direct-interface walk can match it.
    breaker.onFailure(new SerializableRuntimeException());

    assertThat(breaker.isOpen()).isTrue();
  }

  /** A cause-chain match counts: a wrapped exception still trips via its underlying cause. */
  @Test
  void onFailure_causeChainMatched_shouldCountAsFailure() {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    config.setIncludeExceptions(List.of("java.io.IOException"));
    breaker = new CircuitBreakerImpl(config);

    breaker.onFailure(new RuntimeException("wrapped", new IOException("root")));

    assertThat(breaker.isOpen()).isTrue();
  }

  /**
   * Cyclic cause chains (a → b → a) must not spin the classification walk
   * forever on an application thread — the depth cap terminates it.
   */
  @Test
  void onFailure_cyclicCauseChain_shouldTerminateAndClassify() {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    // Include the chain's own type so the head node matches: classification
    // must COUNT the failure, and the depth cap must terminate the walk even
    // though the cycle a → b → a never produces a new node.
    config.setIncludeExceptions(List.of("java.lang.RuntimeException"));
    breaker = new CircuitBreakerImpl(config);

    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b");
    a.initCause(b);
    b.initCause(a);

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> breaker.onFailure(a));
    // Head matches the include list → counted as a failure → OPEN.
    assertThat(breaker.isOpen()).isTrue();
  }

  /** An excluded type never trips the breaker, even when it recurs. */
  @Test
  void onFailure_excludeMatched_shouldBeIgnorable() {
    config.setExcludeExceptions(List.of("java.lang.IllegalStateException"));
    breaker = new CircuitBreakerImpl(config);

    for (int i = 0; i < 5; i++) {
      breaker.onFailure(new IllegalStateException("ignored"));
    }

    assertThat(breaker.isOpen()).isFalse();
  }

  /** {@code excludeExceptions} wins over {@code includeExceptions} for a matched type. */
  @Test
  void onFailure_excludeWinsOverInclude_shouldBeIgnorableForExcludedType() {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    config.setExcludeExceptions(List.of("java.io.IOException"));
    config.setIncludeExceptions(List.of("java.lang.Exception"));
    breaker = new CircuitBreakerImpl(config);

    // Excluded wins: IOException ⊂ Exception, but the exclude list matched first.
    breaker.onFailure(new IOException("excluded"));
    assertThat(breaker.isOpen()).isFalse();

    // A non-excluded subtype of the included type still counts.
    breaker.onFailure(new RuntimeException("included"));
    assertThat(breaker.isOpen()).isTrue();
  }

  /**
   * Unresolvable filter names are skipped without breaking resolution: the
   * remaining resolvable names still classify (and the warning fires only
   * once — resolution runs exactly once per breaker).
   */
  @Test
  void onFailure_unresolvableFilterName_stillClassifiesByRemainingNames() {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    config.setIncludeExceptions(List.of("com.missing.DoesNotExist", "java.lang.IllegalStateException"));
    breaker = new CircuitBreakerImpl(config);

    // Unrelated to the only resolvable include name → ignorable.
    breaker.onFailure(new NullPointerException("unrelated"));
    assertThat(breaker.isOpen()).isFalse();

    // Matches the resolvable include name → counted.
    breaker.onFailure(new IllegalStateException("matched"));
    assertThat(breaker.isOpen()).isTrue();
  }

  /**
   * A filter list that resolves to EMPTY is cached as resolved (no re-run):
   * the documented semantics apply — an empty include list counts every
   * failure, and resolution is never retried per classification.
   */
  @Test
  void onFailure_unresolvableOnlyInclude_countsAllFailures() {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    config.setIncludeExceptions(List.of("com.missing.DoesNotExist"));
    breaker = new CircuitBreakerImpl(config);

    breaker.onFailure(new NullPointerException("any"));
    assertThat(breaker.isOpen()).isTrue();
  }

  // ── Asymmetric probe credit (RocksDB write_controller.cc asymmetry) ──

  /**
   * A failed recovery episode tightens the next episode's probe quota
   * (×0.8), and the credit persists across episodes: with maxProbes=2 the
   * quota drops to 1 after the first failed episode and stays there (floored,
   * never zero).
   */
  @Test
  void probeCredit_failedEpisodes_tightenQuota() throws Exception {
    config.setHalfOpenMaxProbes(2);
    config.setConsecutiveSuccessThreshold(100); // stay HALF_OPEN for the whole test
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();

    // Episode 1: the transitioner probe fails → credit 1.0 → 0.8.
    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();
    breaker.onFailure();
    assertThat(breaker.isOpen()).isTrue();

    // Episode 2: quota = (int)(2 × 0.8) = 1 — only the transitioner passes.
    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isFalse();
    breaker.onFailure(); // credit → 0.64
    assertThat(breaker.isOpen()).isTrue();

    // Episode 3: quota still 1 — the tightening persists across episodes.
    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isFalse();
  }

  /**
   * Within one episode, clean probe successes recover the credit toward the
   * baseline (×1.25, capped at 1.0), widening the live quota back to the
   * configured {@code halfOpenMaxProbes} — never beyond it.
   */
  @Test
  void probeCredit_successes_recoverQuotaWithinEpisode() throws Exception {
    config.setHalfOpenMaxProbes(2);
    config.setConsecutiveSuccessThreshold(100); // stay HALF_OPEN for the whole test
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();

    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();
    breaker.onFailure(); // credit → 0.8
    Thread.sleep(150);

    // Episode 2: the transitioner exhausts the tightened quota of 1...
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isFalse();
    // ...then its success recovers the credit (0.8 → 1.0), so the live
    // quota returns to 2 for the remaining probes of this episode.
    breaker.onSuccess();
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isFalse();
  }

  /**
   * A full recovery (HALF_OPEN→CLOSED) resets the credit: the next failure
   * cycle starts from the unmodified quota again. The breaker is re-opened
   * without rebuilding — a rebuild would reset the credit trivially.
   */
  @Test
  void probeCredit_fullRecovery_resetsToBaseline() throws Exception {
    config.setHalfOpenMaxProbes(2);
    config.setConsecutiveSuccessThreshold(1);
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();

    // Episode 1 fails → credit 0.8.
    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();
    breaker.onFailure();

    // Episode 2 succeeds → CLOSED resets the credit.
    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();
    breaker.onSuccess();
    assertThat(breaker.isOpen()).isFalse();

    // Re-open and probe again: the full quota of 2 is back.
    breaker.onFailure(); // CLOSED → OPEN (volume 1, rate 1.0)
    Thread.sleep(150);
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isTrue();
    assertThat(breaker.allowRequest()).isFalse();
  }

  // ── Helpers ──

  private void triggerOpen() throws Exception {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    // Re-create with new config
    breaker = new CircuitBreakerImpl(config);
    breaker.onFailure();
    Thread.sleep(50);
  }
}
