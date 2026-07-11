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
package io.github.hyshmily.zeta.reporting;

/**
 * Periodically aggregates per-key access counts and publishes them
 * to the Worker via {@link ReportPublisher}.
 *
 * <p>Uses a {@link io.github.hyshmily.zeta.hotkeydetector.doublebuffer.BufferedCounter} double-buffer as a temporary counter store.
 * Burst absorption and backpressure are provided by a bounded
 * {@link java.util.concurrent.LinkedBlockingQueue} between the flush callback and the
 * RabbitMQ publisher.
 */
public interface KeyReporter {
  /**
   * Record one access for the given cache key.
   *
   * @param cacheKey the accessed key
   */
  @SuppressWarnings("all")
  void recordReport(String cacheKey);

  /**
   * Start the periodic flush scheduler and the report dispatcher.
   * Idempotent — subsequent calls are silently ignored.
   */
  void start();

  /**
   * Gracefully shut down the report dispatcher.
   * Idempotent — safe to call multiple times.
   */
  void stop();

  /**
   * Return the current number of batches waiting in the dispatcher work queue.
   *
   * @return queue depth, or {@code -1} if the dispatcher has not been started
   */
  int dispatcherDepth();

  /**
   * Return the maximum capacity of the dispatcher work queue.
   *
   * @return queue capacity, or {@code -1} if the dispatcher has not been started
   */
  int dispatcherCapacity();

  /**
   * Return the total number of batches discarded due to staleness in the dispatcher queue.
   *
   * @return total expired count since startup, or {@code -1} if the dispatcher has not been started
   */
  long dispatcherExpired();

  /**
   * Return the total number of batches rejected because the dispatcher queue was full.
   *
   * @return total dropped count since startup, or {@code -1} if the dispatcher has not been started
   */
  long dispatcherDropped();

  /**
   * Return the approximate number of unique keys currently buffered.
   *
   * @return estimated number of unique keys with pending access counts
   */
  long getPendingKeyCount();

  /**
   * Return the total number of flush cycles permitted by the BBR rate limiter.
   *
   * @return total passed count, or {@code -1} if BBR is disabled
   */
  long bbrPassed();

  /**
   * Return the total number of flush cycles dropped by the BBR rate limiter.
   *
   * @return total dropped count, or {@code -1} if BBR is disabled
   */
  long bbrDropped();

  /**
   * Return the current number of in-flight batches tracked by the BBR rate limiter.
   *
   * @return current in-flight count, or {@code -1} if BBR is disabled
   */
  long bbrInFlight();

  /**
   * Return the BBR-computed maximum concurrency budget (max in-flight).
   *
   * @return computed max in-flight, or {@code -1} if BBR is disabled
   */
  long bbrMaxInFlight();
}
