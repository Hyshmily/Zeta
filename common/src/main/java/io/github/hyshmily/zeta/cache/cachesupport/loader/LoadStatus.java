package io.github.hyshmily.zeta.cache.cachesupport.loader;

import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;

/**
 * Outcome classification for a {@link SingleFlight} load.
 *
 * <p>The members keep "source returned null", "load suppressed by protection
 * machinery" and "reader threw" distinct; every terminal state of the dedup
 * execution maps to exactly one member:
 *
 * <ul>
 *   <li>{@code LOADED} — reader completed with a non-null value.</li>
 *   <li>{@code SOURCE_EMPTY} — reader completed with {@code null}.</li>
 *   <li>{@code SUPPRESSED_BREAKER} — circuit breaker open, reader never ran.</li>
 *   <li>{@code SUPPRESSED_TIMEOUT} — reader outlived the per-load timeout
 *       (interrupted via {@code InterruptingAsync}).</li>
 *   <li>{@code SUPPRESSED_EXECUTOR} — executor saturated, task rejected before
 *       any data-source call (half-open probe reservation released).</li>
 *   <li>{@code FAILED} — reader threw; {@link LoaderOutcome#cause()} carries it.
 *       An {@code InterruptedException} cause maps here.</li>
 * </ul>
 */
public enum LoadStatus {
  /** Reader completed with a non-null value. */
  LOADED,
  /** Reader completed with {@code null} (source has no data). */
  SOURCE_EMPTY,
  /** Circuit breaker open — the reader was never executed. */
  SUPPRESSED_BREAKER,
  /** Reader exceeded the per-load timeout and was interrupted. */
  SUPPRESSED_TIMEOUT,
  /** Executor rejected the task before any data-source call. */
  SUPPRESSED_EXECUTOR,
  /** Reader threw — see {@link LoaderOutcome#cause()}. */
  FAILED;

  /**
   * Whether this status is a protection-machinery suppression: the reader
   * never produced a value, and no failure occurred.
   *
   * @return {@code true} for the three {@code SUPPRESSED_*} members
   */
  public boolean isSuppressed() {
    return this == SUPPRESSED_BREAKER || this == SUPPRESSED_TIMEOUT || this == SUPPRESSED_EXECUTOR;
  }
}
