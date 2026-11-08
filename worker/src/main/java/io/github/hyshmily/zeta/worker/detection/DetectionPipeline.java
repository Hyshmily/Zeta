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
package io.github.hyshmily.zeta.worker.detection;

import io.github.hyshmily.zeta.worker.detection.lifecycle.HE;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import io.github.hyshmily.zeta.worker.detection.rule.RuleBinding;
import io.github.hyshmily.zeta.worker.detection.rule.RuleResolver;
import io.github.hyshmily.zeta.worker.detection.window.SlidingWindowDetector;
import io.github.hyshmily.zeta.worker.dispatch.DecisionDispatcher;

/**
 * The detection entry point: window first, rules only when the sum is worth
 * resolving.
 *
 * <p>
 * Every report takes the same short path. Two cheap exits keep the expensive
 * work off the hot keys and the cold ones:
 *
 * <ul>
 * <li><b>Already hot</b> — one map read. The count is still added to the window
 * (cooling needs it), but the
 * window sum is not computed: the cooling check re-reads it under the key lock,
 * and no promotion decision is pending
 * for this report.</li>
 * <li><b>Below the floor</b> — one integer compare against the smallest
 * {@code hotCount} that the floor can gate. An
 * effective {@code EXACT} rule is checked by its own O(1) index first and its
 * own threshold decides, so a special key
 * with a low threshold does not lower the floor for everyone else, and a cold
 * report for one does not enter the match
 * cache.</li>
 * </ul>
 *
 * <p>
 * Only a key between the floor and its rule's threshold pays for a resolution;
 * the resolver caches a non-exact answer
 * and serves an effective exact key from its index. The pipeline owns no
 * detection state: the window, the rules, the
 * HOT set and the cool-down wheels are each owned by exactly one component.
 */
public class DetectionPipeline {

  private final SlidingWindowDetector detector;
  private final DecisionDispatcher dispatcher;
  private final HotKeyRegistry registry;
  private final RuleResolver rules;

  private final long retentionMs;
  private final boolean enabled;

  /**
   * Creates the pipeline.
   *
   * @param detector    the shared window table
   * @param rules       the published rule set and its match cache
   * @param registry    the authoritative HOT set
   * @param dispatcher  the single decision submission point
   * @param retentionMs idle-window reclamation threshold
   * @param enabled     {@code false} skips evaluation and counts the reports
   */
  public DetectionPipeline(
    SlidingWindowDetector detector,
    RuleResolver rules,
    HotKeyRegistry registry,
    DecisionDispatcher dispatcher,
    long retentionMs,
    boolean enabled
  ) {
    this.rules = rules;
    this.enabled = enabled;
    this.detector = detector;
    this.registry = registry;
    this.dispatcher = dispatcher;
    this.retentionMs = retentionMs;
  }

  /**
   * The window slice a batch timestamp belongs to, for the batch path.
   *
   * @param nowMs the batch's monotonic sample
   * @return the absolute slice number
   */
  public long sliceIndex(long nowMs) {
    return detector.sliceIndex(nowMs);
  }

  /**
   * Feeds one reported count with the batch's precomputed window slice.
   *
   * @param key      the cache key
   * @param count    the accesses reported for it; callers drop non-positive
   *                 counts before this point
   * @param nowMs    the batch's monotonic sample
   * @param nowSlice the slice derived from {@code nowMs} once for the whole batch
   */
  public void onReport(String key, long count, long nowMs, long nowSlice) {
    if (registry.isHot(key)) {
      // The count still feeds the cool-down window; the sum is re-read under the key
      // lock when the cooling check runs,
      // so computing it here would only be discarded.
      detector.recordCountAtSlice(key, count, nowSlice);
      return;
    }

    long sum = detector.addCountAtSlice(key, count, nowSlice);
    // The floor and the exact index are read from one published snapshot: a special
    // key with a low EXACT threshold
    // cannot pull every warm key into the match cache, and a concurrent rule
    // publish cannot mix the old floor with the
    // new exact index.
    RuleBinding binding = rules.resolveIfEligible(key, sum);
    if (binding == null) {
      return;
    }
    HE promoted = registry.promoteEntry(key, binding, nowMs);
    if (promoted != null) {
      // From here the dispatcher owns delivery and returns the seat if the HOT is refused or dropped
      // before it reaches the broker (design decisions: decision delivery). The entry reference *is*
      // the promotion, so the dispatcher marks that exact object rather than a second map lookup.
      dispatcher.submitPromotion(promoted);
    }
  }

  /**
   * Periodic housekeeping: reclaim idle windows, then repair overdue cool-down
   * timers.
   *
   * <p>
   * A rejected admission at the tracked-key cap arms the reclaimer's relief
   * signal, so the next scan runs with the
   * detector's window-aligned idle boundary instead of the configured retention:
   * a key is locked out for at most one
   * sweep interval, not for the full retention. The boundary is the same slice
   * predicate the read path and the capacity
   * sampler use, so a guard slice a concurrent read still covers is never
   * reclaimed. The configured retention remains
   * the caller's explicit memory bound.
   *
   * @return the number of entries the repair pass selected as overdue
   */
  public int sweep() {
    if (detector.consumeReliefRequest()) {
      detector.evictIdle();
    } else {
      detector.evictStale(retentionMs);
    }
    return registry.repairOverdue(dispatcher::submit);
  }

  /** @return whether evaluation is enabled */
  public boolean enabled() {
    return enabled;
  }
}
