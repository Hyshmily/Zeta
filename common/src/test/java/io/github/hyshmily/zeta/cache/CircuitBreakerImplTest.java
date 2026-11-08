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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerImpl;
import io.github.hyshmily.zeta.util.TimeSource;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CircuitBreakerImpl} covering all state transitions,
 * backoff scenarios and the disabled path.
 *
 * <p>Admission is explicit-stamp only: every {@code tryAcquireStamp} result
 * is settled exactly once with its own stamp — batch (one thread, many keys)
 * and nested calls can never cross-consume probes.
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

  @Test
  void tryAcquireStamp_whenDisabled_shouldReturnClosed() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    assertThat(cb.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_CLOSED);
  }

  @Test
  void tryAcquireStamp_whenClosed_shouldReturnClosed() {
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_CLOSED);
  }

  @Test
  void tryAcquireStamp_whenOpenAndNotHalfOpen_shouldReject() throws Exception {
    triggerOpen();
    // Immediately after opening, the half-open interval has not elapsed
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_REJECTED);
  }

  @Test
  void tryAcquireStamp_whenOpenAndHalfOpenProbe_shouldReturnGeneration() throws Exception {
    triggerOpen();
    // Advance time past the singleTestIntervalMs
    Thread.sleep(150);
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
  }

  @Test
  void onSuccess_whenDisabled_shouldDoNothing() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    cb.onSuccess(CircuitBreaker.STAMP_CLOSED);
    assertThat(cb.isOpen()).isFalse();
  }

  @Test
  void onSuccess_whenClosed_shouldIncrementSuccess() throws Exception {
    config.setFailThreshold(0.4);
    config.setRequestVolumeThreshold(3);
    breaker = new CircuitBreakerImpl(config);
    breaker.onSuccess(CircuitBreaker.STAMP_CLOSED);
    breaker.onSuccess(CircuitBreaker.STAMP_CLOSED);
    failClosedThrice(breaker);
    // rate = 3/5 = 0.6 > 0.4, volume = 5 >= 3 → open
    assertThat(breaker.isOpen()).isTrue();
  }

  @Test
  void onSuccess_whenOpen_shouldCloseBreaker() throws Exception {
    triggerOpen();
    Thread.sleep(config.getSingleTestIntervalMs() + 50);
    long stamp = breaker.tryAcquireStamp();
    assertThat(stamp).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    breaker.onSuccess(stamp);
    assertThat(breaker.isOpen()).isFalse();
  }

  /**
   * Regression: the OPEN→HALF_OPEN transitioner probe owns its stamp, so its
   * settlement releases exactly its own slot and the half-open quota keeps
   * admitting exactly {@code halfOpenMaxProbes} probes.
   */
  @Test
  void halfOpenProbeQuota_notLeakedByTransitionerOnSuccess() throws Exception {
    config.setHalfOpenMaxProbes(2);
    config.setConsecutiveSuccessThreshold(100); // stay HALF_OPEN for the whole test
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();
    Thread.sleep(config.getSingleTestIntervalMs() + 50);

    long transitioner = breaker.tryAcquireStamp();
    assertThat(transitioner).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    breaker.onSuccess(transitioner);

    // Still HALF_OPEN: exactly maxProbes further probes, then refusal.
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_REJECTED);
  }

  /**
   * Stamps isolate batch admissions on one thread: two probes admitted under
   * the same generation settle independently — settling the first never
   * consumes the second's slot.
   */
  @Test
  void halfOpenStamps_settleIndependentlyOnOneThread() throws Exception {
    config.setHalfOpenMaxProbes(2);
    config.setConsecutiveSuccessThreshold(100); // stay HALF_OPEN for the whole test
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();
    Thread.sleep(config.getSingleTestIntervalMs() + 50);

    long first = breaker.tryAcquireStamp();
    long second = breaker.tryAcquireStamp();
    assertThat(first).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(second).isGreaterThan(CircuitBreaker.STAMP_CLOSED);

    // Settling the first probe releases exactly one slot.
    breaker.onSuccess(first);
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
  }

  /**
   * A stale generation (an older HALF_OPEN episode finishing after a newer
   * one opened) is ignored — it must not decrement the live episode's quota
   * or reopen/close the breaker.
   */
  @Test
  void halfOpenStamps_staleGeneration_isIgnored() throws Exception {
    config.setHalfOpenMaxProbes(1);
    config.setConsecutiveSuccessThreshold(100);
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();
    Thread.sleep(config.getSingleTestIntervalMs() + 50);

    long stale = breaker.tryAcquireStamp();
    assertThat(stale).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    // Fail the episode → back to OPEN with a new generation on next transition.
    breaker.onFailure(new RuntimeException("probe failed"), stale);
    assertThat(breaker.isOpen()).isTrue();

    Thread.sleep(config.getSingleTestIntervalMs() + 50);
    long live = breaker.tryAcquireStamp();
    assertThat(live).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(live).isNotEqualTo(stale);

    // The stale stamp settles to nothing. The episode's single probe slot is still
    // held by the live stamp, so a fresh admission is refused — proof the breaker
    // stayed HALF_OPEN (a closed breaker would hand out STAMP_CLOSED here).
    breaker.onSuccess(stale);
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_REJECTED);
    breaker.onSuccess(live);
  }

  @Test
  void onFailure_whenDisabled_shouldDoNothing() {
    config.setEnabled(false);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
    cb.onFailure(new RuntimeException("ignored"), CircuitBreaker.STAMP_CLOSED);
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
    cb.onFailure(new RuntimeException("x"), CircuitBreaker.STAMP_CLOSED);
    cb.onFailure(new RuntimeException("x"), CircuitBreaker.STAMP_CLOSED);
    assertThat(cb.isOpen()).isFalse();
  }

  @Test
  void isOpen_whenOpen_shouldReturnTrue() throws Exception {
    triggerOpen();
    assertThat(breaker.isOpen()).isTrue();
  }

  /**
   * Any HALF_OPEN episode is reported as degraded even though it is not OPEN: an admitted probe
   * may still run, but a surplus-probe rejection or a failed probe must serve stale instead of
   * throwing.
   */
  @Test
  void isDegraded_whenHalfOpen_shouldReturnTrueWhileIsOpenStaysFalse() throws Exception {
    config.setConsecutiveSuccessThreshold(1);
    triggerOpen();
    Thread.sleep(config.getSingleTestIntervalMs() + 50);

    assertThat(breaker.isDegraded()).as("any HALF_OPEN episode is degraded").isTrue();
    long probe = breaker.tryAcquireStamp();
    assertThat(probe).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).as("HALF_OPEN is not the fast-fail state").isFalse();
    assertThat(breaker.isDegraded()).as("an admitted probe stays in the degraded state").isTrue();

    breaker.onSuccess(probe);
    assertThat(breaker.isDegraded()).isFalse();
  }

  @Test
  void evaluateThreshold_whenBelowVolume_shouldNotOpen() {
    // Only 1 failure, need 2 (requestVolumeThreshold)
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void evaluateThreshold_whenRateBelowThreshold_shouldNotOpen() {
    // 1 success + 1 failure = 50% rate, failThreshold=0.5, strictly greater needed
    breaker.onSuccess(CircuitBreaker.STAMP_CLOSED);
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void fullCycle_openHalfOpenClose() throws Exception {
    triggerOpen();
    assertThat(breaker.isOpen()).isTrue();

    Thread.sleep(150);
    long stamp = breaker.tryAcquireStamp();
    assertThat(stamp).isGreaterThan(CircuitBreaker.STAMP_CLOSED);

    breaker.onSuccess(stamp);
    assertThat(breaker.isOpen()).isFalse();
  }

  @Test
  void logEnabled_shouldNotThrow() {
    config.setLogEnabled(true);
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    CircuitBreakerImpl cb = new CircuitBreakerImpl(config);

    cb.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    // tryAcquireStamp on open breaker triggers log in half-open path
    cb.tryAcquireStamp();
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

    breaker.onFailure(new NullPointerException("boom"), CircuitBreaker.STAMP_CLOSED);

    assertThat(breaker.isOpen()).isTrue();
  }

  /** An exception unrelated to the included type is ignorable (treated as success). */
  @Test
  void onFailure_includeUnrelated_shouldTreatAsIgnorable() {
    config.setIncludeExceptions(List.of("java.lang.IllegalStateException"));
    breaker = new CircuitBreakerImpl(config);

    breaker.onFailure(new NullPointerException("unrelated"), CircuitBreaker.STAMP_CLOSED);

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
    breaker.onFailure(new SerializableRuntimeException(), CircuitBreaker.STAMP_CLOSED);

    assertThat(breaker.isOpen()).isTrue();
  }

  /** A cause-chain match counts: a wrapped exception still trips via its underlying cause. */
  @Test
  void onFailure_causeChainMatched_shouldCountAsFailure() {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    config.setIncludeExceptions(List.of("java.io.IOException"));
    breaker = new CircuitBreakerImpl(config);

    breaker.onFailure(new RuntimeException("wrapped", new IOException("root")), CircuitBreaker.STAMP_CLOSED);

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

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> breaker.onFailure(a, CircuitBreaker.STAMP_CLOSED));
    // Head matches the include list → counted as a failure → OPEN.
    assertThat(breaker.isOpen()).isTrue();
  }

  /** An excluded type never trips the breaker, even when it recurs. */
  @Test
  void onFailure_excludeMatched_shouldBeIgnorable() {
    config.setExcludeExceptions(List.of("java.lang.IllegalStateException"));
    breaker = new CircuitBreakerImpl(config);

    for (int i = 0; i < 5; i++) {
      breaker.onFailure(new IllegalStateException("ignored"), CircuitBreaker.STAMP_CLOSED);
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
    breaker.onFailure(new IOException("excluded"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isFalse();

    // A non-excluded subtype of the included type still counts.
    breaker.onFailure(new RuntimeException("included"), CircuitBreaker.STAMP_CLOSED);
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
    breaker.onFailure(new NullPointerException("unrelated"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isFalse();

    // Matches the resolvable include name → counted.
    breaker.onFailure(new IllegalStateException("matched"), CircuitBreaker.STAMP_CLOSED);
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

    breaker.onFailure(new NullPointerException("any"), CircuitBreaker.STAMP_CLOSED);
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
    long ep1 = breaker.tryAcquireStamp();
    assertThat(ep1).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    breaker.onFailure(new RuntimeException("probe failed"), ep1);
    assertThat(breaker.isOpen()).isTrue();

    // Episode 2: quota = (int)(2 × 0.8) = 1 — only the transitioner passes.
    Thread.sleep(150);
    long ep2 = breaker.tryAcquireStamp();
    assertThat(ep2).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_REJECTED);
    breaker.onFailure(new RuntimeException("probe failed"), ep2); // credit → 0.64
    assertThat(breaker.isOpen()).isTrue();

    // Episode 3: quota still 1 — the tightening persists across episodes.
    Thread.sleep(150);
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_REJECTED);
  }

  /**
   * A probe success is not an episode recovery: the credit is debited once per failed
   * episode and earned once per recovered one, so a single lucky probe can never balance
   * the penalty away. Counted per probe (×1.25 each) against one ×0.8 per failed episode,
   * a failing cycle ended up with a <em>larger</em> quota than it started with.
   */
  @Test
  void probeCredit_probeSuccessDoesNotWidenQuotaMidEpisode() throws Exception {
    config.setHalfOpenMaxProbes(3);
    config.setConsecutiveSuccessThreshold(100); // stay HALF_OPEN for the whole test
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();

    // Episode 1 fails: credit 1.0 → 0.8, so the next quota is (int) (3 × 0.8) = 2.
    Thread.sleep(150);
    long failed = breaker.tryAcquireStamp();
    assertThat(failed).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    breaker.onFailure(new RuntimeException("probe failed"), failed);

    Thread.sleep(150);
    long transitioner = breaker.tryAcquireStamp();
    assertThat(transitioner).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    breaker.onSuccess(transitioner);

    // The quota is still 2 — two more probes after the transitioner, then refusal. A
    // per-probe reward would have restored 3 here.
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_REJECTED);
  }

  /**
   * A failed episode is penalized exactly once, however many of its probes fail:
   * the first settlement retires the episode and every later one is a straggler.
   */
  @Test
  void probeCredit_failedEpisodePenalizesOnce() throws Exception {
    config.setHalfOpenMaxProbes(3);
    config.setConsecutiveSuccessThreshold(100);
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();

    Thread.sleep(150);
    long first = breaker.tryAcquireStamp();
    long second = breaker.tryAcquireStamp();
    long third = breaker.tryAcquireStamp();
    assertThat(List.of(first, second, third)).allMatch(stamp -> stamp > CircuitBreaker.STAMP_CLOSED);

    breaker.onFailure(new RuntimeException("probe failed"), first);
    breaker.onFailure(new RuntimeException("probe failed"), second);
    breaker.onFailure(new RuntimeException("probe failed"), third);
    assertThat(breaker.isOpen()).isTrue();

    // One penalty (1.0 → 0.8), not three: the next episode holds 2 probes, not 1.
    Thread.sleep(150);
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_REJECTED);
  }

  /**
   * The credit heals across clean episodes rather than snapping back: 0.8 → 0.88 → 0.968
   * → 1.0 restores the full quota after three recoveries. The reward is not reciprocal to
   * the penalty (×1.1 against ×0.8), which is what makes a source that fails more than
   * roughly one recovery episode in three keep tightening instead of oscillating.
   */
  @Test
  void probeCredit_cleanEpisodesRecoverQuotaGradually() throws Exception {
    config.setHalfOpenMaxProbes(2);
    config.setConsecutiveSuccessThreshold(1);
    config.setSingleTestIntervalMs(40);
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();

    // Episode 1 fails → credit 0.8, quota (int) (2 × 0.8) = 1.
    Thread.sleep(80);
    long failed = breaker.tryAcquireStamp();
    assertThat(failed).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    breaker.onFailure(new RuntimeException("probe failed"), failed);

    for (int episode = 0; episode < 3; episode++) {
      Thread.sleep(80);
      long probe = breaker.tryAcquireStamp();
      assertThat(probe).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
      // The transitioner holds the only slot while the credit is below 1.5/2.
      if (episode == 0) {
        assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_REJECTED);
      }
      breaker.onSuccess(probe);
      assertThat(breaker.isOpen()).isFalse();
      // Re-open from CLOSED (the window was cleared on recovery): a failed episode is the
      // only thing that penalizes, and this is not one.
      breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    }

    // The quota is whole again: two probes, then refusal.
    Thread.sleep(80);
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_REJECTED);
  }

  // ── Concurrency & lifecycle regressions ──

  /**
   * {@code STAMP_REJECTED} settled as success must not dilute the window: counted,
   * the two refused stamps below would drop the failure rate to 1/3 (below the
   * 0.4 threshold) and a breaker that should trip stays CLOSED.
   */
  @Test
  void onSuccess_withRejectedStamp_shouldNotPolluteWindow() {
    config.setFailThreshold(0.4);
    config.setRequestVolumeThreshold(1);
    breaker = new CircuitBreakerImpl(config);
    breaker.onSuccess(CircuitBreaker.STAMP_REJECTED);
    breaker.onSuccess(CircuitBreaker.STAMP_REJECTED);
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isTrue();
  }

  /**
   * A probe stamp settling after its episode already closed must not add a success
   * to the fresh window: counted, the failure below would land at rate 0.5 (above
   * the 0.4 threshold) over volume 2 and re-open a healed breaker.
   */
  @Test
  void onSuccess_staleProbeStampInClosed_shouldNotPolluteWindow() throws Exception {
    config.setFailThreshold(0.4);
    breaker = new CircuitBreakerImpl(config);
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isTrue();

    Thread.sleep(config.getSingleTestIntervalMs() + 50);
    config.setConsecutiveSuccessThreshold(1);
    long first = breaker.tryAcquireStamp();
    long second = breaker.tryAcquireStamp();
    assertThat(first).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(second).isGreaterThan(CircuitBreaker.STAMP_CLOSED);

    breaker.onSuccess(first); // closes: threshold 1 met
    // CLOSED admission is stateless: always STAMP_CLOSED (HALF_OPEN would mint a probe stamp).
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_CLOSED);

    breaker.onSuccess(second); // stale probe stamp in CLOSED
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isFalse();
  }

  /**
   * Disabling a breaker mid-incident must not flap: the forced CLOSED clears the
   * window, so a re-enabled breaker does not re-trip on the failure surplus that
   * was live when it was disabled (which would OPEN/CLOSED-flap and re-log until
   * the old buckets aged out).
   */
  @Test
  void disabledBreaker_doesNotFlapFromStaleWindow() throws Exception {
    config.setWindowTimeMs(200);
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    breaker = new CircuitBreakerImpl(config);
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isTrue();

    config.setEnabled(false);
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_CLOSED);
    Thread.sleep(300);
    assertThat(breaker.isOpen()).isFalse();
  }

  /**
   * A HALF_OPEN episode whose probe never settles (all slots held by hung stamps)
   * must not pin the breaker forever: after {@code singleTestIntervalMs} without
   * any settlement the episode is force-retired and admission opens a fresh one.
   */
  @Test
  void wedgedHalfOpenEpisode_isRetiredAfterProbeTimeout() throws Exception {
    config.setHalfOpenMaxProbes(1);
    config.setConsecutiveSuccessThreshold(100); // stay HALF_OPEN for the whole test
    config.setSingleTestIntervalMs(80);
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();

    Thread.sleep(150);
    long wedged = breaker.tryAcquireStamp();
    assertThat(wedged).isGreaterThan(CircuitBreaker.STAMP_CLOSED);

    // Never settled — the episode is wedged; the next acquire must escape to a
    // fresh episode instead of refusing forever.
    Thread.sleep(150);
    long revived = breaker.tryAcquireStamp();
    assertThat(revived).isGreaterThan(CircuitBreaker.STAMP_CLOSED);
    assertThat(revived).isNotEqualTo(wedged);
  }

  /** Bucket/window validation fails fast before any array or division happens. */
  @Test
  void constructor_rejectsInvalidWindowConfig() {
    config.setWindowBuckets(0);
    assertThatThrownBy(() -> new CircuitBreakerImpl(config)).isInstanceOf(IllegalArgumentException.class);

    config.setWindowBuckets(10);
    config.setWindowTimeMs(5); // < window-buckets
    assertThatThrownBy(() -> new CircuitBreakerImpl(config)).isInstanceOf(IllegalArgumentException.class);
  }

  // ── Window accounting regressions ──

  /**
   * The trip gate in front of the success sweep must be exact, never collapsing:
   * every (success, failure) combination must reach the verdict the rate/volume
   * rule gives.
   *
   * <p>Each row feeds the whole failure count in one burst, so a gate derived from
   * the <em>previous</em> evaluation's success total is exercised across several
   * volume/ratio shapes — including rows where a second evaluation happens at a
   * higher failure total, which is what a stale cross-window watermark used to
   * turn into a non-terminating compare-and-set.
   */
  @Test
  void evaluateThreshold_matchesRateRuleForEveryVolumeAndRatio() {
    config.setFailThreshold(0.5);
    config.setRequestVolumeThreshold(20);
    long[] successes = { 0, 1, 9, 19, 20, 100, 1_000 };
    long[] failures = { 1, 10, 11, 19, 20, 101 };

    for (long success : successes) {
      for (long failure : failures) {
        CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
        for (long i = 0; i < success; i++) {
          cb.onSuccess(CircuitBreaker.STAMP_CLOSED);
        }
        for (long i = 0; i < failure; i++) {
          cb.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
        }

        double rate = (double) failure / (success + failure);
        boolean expected = success + failure >= config.getRequestVolumeThreshold() && rate > config.getFailThreshold();
        assertThat(cb.isOpen()).as("success=%d failure=%d rate=%s", success, failure, rate).isEqualTo(expected);
      }
    }
  }

  /**
   * Regression: a window that evaluates once, stays CLOSED, then keeps failing must
   * reach a second evaluation at the higher failure total.
   *
   * <p>The gate used to be derived from a packed watermark whose low bits held the
   * previous success total; the follow-up CAS compared the <em>shifted</em> window
   * version against the raw packed value, so it never succeeded and the loop spun
   * forever on the settling thread. Every row here cross the gate twice while a
   * non-zero success total is in the window — the first row ({@code success=1},
   * {@code failure=19}) is the smallest shape that reproduces it.
   */
  @Test
  void evaluateThreshold_repeatsAtHigherFailureTotal_terminatesAndTrips() {
    config.setFailThreshold(0.5);
    config.setRequestVolumeThreshold(20);
    long[][] rows = { { 1, 19 }, { 19, 20 }, { 20, 101 }, { 100, 101 } };

    for (long[] row : rows) {
      CircuitBreakerImpl cb = new CircuitBreakerImpl(config);
      for (long i = 0; i < row[0]; i++) {
        cb.onSuccess(CircuitBreaker.STAMP_CLOSED);
      }
      for (long i = 0; i < row[1]; i++) {
        cb.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
      }

      double rate = (double) row[1] / (row[0] + row[1]);
      assertThat(cb.isOpen()).as("success=%d failure=%d rate=%s", row[0], row[1], rate).isTrue();
    }
  }

  /**
   * The window is advanced by the clock on the write path, so a breaker that goes
   * quiet and resumes after {@code windowTimeMs} must judge only the new traffic:
   * the settled failures age out instead of pinning the rate.
   */
  @Test
  void window_agesOutWithoutTraffic() throws Exception {
    config.setWindowTimeMs(200);
    config.setWindowBuckets(4);
    config.setFailThreshold(0.5);
    config.setRequestVolumeThreshold(2);
    breaker = new CircuitBreakerImpl(config);

    for (int i = 0; i < 3; i++) {
      breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    }
    assertThat(breaker.isOpen()).isTrue();

    config.setEnabled(false);
    breaker.tryAcquireStamp(); // forced CLOSED, window cleared
    config.setEnabled(true);

    Thread.sleep(300);
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isFalse();
  }

  /**
   * A bucket idle past half the 24-bit window-id range must rotate to the current
   * slice, not be mistaken for an out-of-order settlement. The old distance test
   * discarded every settlement until the id wrapped, so a cold prefix resumed with
   * a breaker that could no longer trip.
   */
  @Test
  void bucketIdlePastHalfTheWindowRange_stillCountsTheNextSettlement() {
    config.setRequestVolumeThreshold(1);
    breaker = new CircuitBreakerImpl(config);
    // Default slide is 1 s; move the monotonic clock a little past 2^23 slices.
    long offsetMs = ((1L << 23) + 1_000L) * 1_000L;
    TimeSource.setTimeOffsetForTest(0L, offsetMs);
    try {
      breaker.onFailure(new RuntimeException("resume"), CircuitBreaker.STAMP_CLOSED);
      assertThat(breaker.isOpen()).isTrue();
    } finally {
      TimeSource.setTimeOffsetForTest(0L, 0L);
    }
  }

  /**
   * Re-enabling a breaker must not inherit the window that was live when it was disabled:
   * the forced CLOSED clears the buckets before it publishes the state, so the first
   * failure afterwards evaluates a fresh window instead of the pre-disable surplus.
   */
  @Test
  void reEnabledBreaker_doesNotTripFromTheDisabledWindow() throws Exception {
    config.setFailThreshold(0.5);
    config.setRequestVolumeThreshold(20);
    breaker = new CircuitBreakerImpl(config);

    for (int i = 0; i < 30; i++) {
      breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    }
    assertThat(breaker.isOpen()).isTrue();

    config.setEnabled(false);
    assertThat(breaker.tryAcquireStamp()).isEqualTo(CircuitBreaker.STAMP_CLOSED); // forces CLOSED + clears
    config.setEnabled(true);
    assertThat(breaker.isOpen()).isFalse();

    // 31 stale failures would be rate 1.0 over the volume floor; the cleared window is
    // a volume-1 window, below requestVolumeThreshold.
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    assertThat(breaker.isOpen()).isFalse();
  }

  /**
   * Liveness under the wedged-episode escape. Half the threads hang their probes past
   * {@code singleTestIntervalMs} so the escape races the settling half on every episode;
   * when the retirement of a wedged episode and its reopening were separate steps, a
   * loser of that race could leave the breaker OPEN with a non-zero episode stamp and
   * never admit another probe. Progress is the assertion: the breaker must still close.
   */
  @Test
  void wedgedEpisodeStorm_neverStrandsTheBreaker() throws Exception {
    config.setFailThreshold(0.5);
    config.setRequestVolumeThreshold(1);
    config.setHalfOpenMaxProbes(2);
    config.setConsecutiveSuccessThreshold(2);
    config.setSingleTestIntervalMs(1);
    config.setLogEnabled(false);
    breaker = new CircuitBreakerImpl(config);
    triggerOpen();

    int threads = 4;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      final int id = t;
      pool.execute(() -> {
        try {
          start.await();
          for (int i = 0; i < 20_000; i++) {
            long stamp = breaker.tryAcquireStamp();
            if (stamp <= CircuitBreaker.STAMP_CLOSED) {
              continue;
            }
            if (id % 2 == 0 && i % 3 != 0) {
              continue; // held forever: a hung probe, which is what arms the wedged escape
            }
            if ((i & 1) == 0) {
              breaker.onSuccess(stamp);
            } else {
              breaker.onFailure(new RuntimeException("probe failed"), stamp);
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();

    assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (System.nanoTime() < deadline) {
        long stamp = breaker.tryAcquireStamp();
        if (stamp == CircuitBreaker.STAMP_CLOSED) {
          return;
        }
        if (stamp > CircuitBreaker.STAMP_CLOSED) {
          breaker.onSuccess(stamp);
        } else {
          Thread.sleep(1); // OPEN: wait out the 1 ms cooldown
        }
      }
      throw new AssertionError("breaker stopped admitting probes after the wedged-episode storm");
    });
  }

  // ── Helpers ──

  private void failClosedThrice(CircuitBreaker b) {
    b.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    b.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    b.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
  }

  private void triggerOpen() throws Exception {
    config.setFailThreshold(0.1);
    config.setRequestVolumeThreshold(1);
    // Re-create with new config
    breaker = new CircuitBreakerImpl(config);
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    Thread.sleep(50);
  }
}
