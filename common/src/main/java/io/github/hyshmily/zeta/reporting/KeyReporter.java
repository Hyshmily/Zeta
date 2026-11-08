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

import io.github.hyshmily.zeta.reporting.impl.ReportFeedLoop;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Publishes per-key access-add batches to the Worker via {@link ReportPublisher}.
 *
 * <p>The read path counts once in the shared {@code AccessCounter}, whose tide delivers
 * merged batches to {@link #publishBatch(Map, long)}; the reporter owns routing, batching
 * and the feed-loop cadence tuner (design decisions: reporting).
 */
public interface KeyReporter {
  /**
   * Publish one flushed counting batch: group the counts by target Worker (consistent-hash
   * routing) and enqueue one batch per target for asynchronous publishing.
   *
   * <p>The only path the cadence tuner samples, because it is the only one that knows the
   * window its counts accumulated over. Called on the access counter's tide deliverer
   * (single-threaded); the map must not be retained or mutated. Failures are logged inside
   * the implementation, never propagated.
   *
   * @param keyCounts merged per-key access counts since the last tide (never {@code null};
   *                  possibly empty)
   * @param windowMs  measured monotonic window the counts accumulated over, in ms (positive)
   */
  void publishBatch(Map<String, Long> keyCounts, long windowMs);

  /**
   * Publish a single out-of-band report that is not a tide flush
   * ({@code TagMode.REPORT_ONLY}): route and enqueue it exactly like a batch, but never
   * sample it into the cadence tuner — a one-key report says nothing about how many keys a
   * flush window would accumulate.
   *
   * @param keyCounts per-key counts to publish (never {@code null}; possibly empty)
   */
  void publishDirect(Map<String, Long> keyCounts);

  /** Start the dispatcher. Idempotent. */
  void start();

  /** Gracefully shut the dispatcher down. Idempotent. */
  void stop();

  /**
   * Dispatcher counters, or {@code null} before {@link #start()}.
   *
   * @return the current counters, or {@code null} when the dispatcher is not running
   */
  @Nullable
  DispatcherStats dispatcherStats();

  /**
   * Routing-drop counters, or {@code null} before {@link #start()}. These cover the keys that
   * never reached the dispatcher: unroutable keys, no-alive-Worker drops and batches refused
   * by the queue or the shared flush offer budget.
   *
   * @return the routing counters, or {@code null} when the reporter has not started
   */
  default @Nullable RoutingStats routingStats() {
    return null;
  }

  /**
   * A point-in-time read of the routing-drop counters.
   *
   * @param unroutableKeys keys whose target could not be resolved, or whose target had no epoch
   * @param workerDeadKeys keys dropped because no Worker was alive
   * @param refusedKeys    keys in batches the queue or shared offer budget refused
   */
  record RoutingStats(long unroutableKeys, long workerDeadKeys, long refusedKeys) {}

  /**
   * The flush-cadence tuner, or {@code null} when the tuner is off.
   *
   * @return the feed loop, or {@code null}
   */
  @Nullable
  ReportFeedLoop feedLoop();

  /**
   * Number of unique keys with pending access counts. The reservoir lives in the shared
   * access counter, so a reporter that owns none answers {@code 0}.
   *
   * @return the pending key count, or {@code 0} when not tracked here
   */
  default long pendingKeyCount() {
    return 0L;
  }

  /**
   * A point-in-time read of the dispatcher's counters.
   *
   * @param depth             batches waiting in the work queue
   * @param expiredDeadTarget batches discarded because their target Worker was no longer alive
   * @param expiredStale      batches discarded because they waited past the staleness bound
   * @param dropped           batches refused after the offer: the queue stayed full past the
   *                          offer timeout, or the batch was withdrawn during shutdown
   */
  record DispatcherStats(int depth, long expiredDeadTarget, long expiredStale, long dropped) {}
}
