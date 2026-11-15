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
 * The detection entry point: window first, rules only when the sum is worth resolving.
 *
 * <p>
 * Two cheap exits keep the expensive work off the hot and cold paths. An <b>already-hot</b> key pays
 * one map read: the count still enters the window for the cool-down, but the sum is not computed
 * because the cooling check re-reads it under the key lock. A report <b>below the floor</b> pays one
 * integer compare against the smallest {@code hotCount} the floor can gate; an effective
 * {@code EXACT} rule is checked by its own O(1) index first and its own threshold decides. Only a key
 * between the floor and its rule's threshold pays for a resolution.
 *
 * <p>
 * The pipeline owns no detection state: the window, the rules, the HOT set and the cool-down wheels
 * are each owned by exactly one component.
 *
 * <p>
 * design decisions: worker pipeline
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
   * @param enabled     the detection switch; the ingest consumer skips evaluation and counts the
   *                    reports when it is {@code false}
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
   * <p>
   * The caller owns the enablement gate: {@code ReportConsumer} calls this only while {@link #enabled()} is
   * {@code true}, so the report path pays no second switch.
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
    HE promoted = registry.promote(key, binding, nowMs);
    if (promoted != null) {
      // From here the dispatcher owns delivery and returns the seat if the HOT is refused, dropped
      // before it reaches the broker, or the submission itself fails (design decisions: decision
      // delivery). The entry carries the key and version, so the decision needs no second map lookup.
      dispatcher.submit(promoted);
    }
  }

  /**
   * Periodic housekeeping: reclaim idle windows, then re-link overdue cool-down checks.
   *
   * <p>
   * A rejected admission at the tracked-key cap arms the reclaimer's relief signal, so the next scan
   * uses the detector's window-aligned idle boundary instead of the configured retention: a key is
   * locked out for at most one sweep interval. The configured retention remains the explicit memory
   * bound.
   *
   * @return the number of overdue cool-down checks the repair pass re-linked
   */
  public int sweep() {
    if (detector.consumeReliefRequest()) {
      detector.evictIdle();
    } else {
      detector.evictStale(retentionMs);
    }
    return registry.repair();
  }

  /** @return whether evaluation is enabled */
  public boolean enabled() {
    return enabled;
  }
}
