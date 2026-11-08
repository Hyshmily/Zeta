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
package io.github.hyshmily.zeta.worker.endpoint;

import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.lifecycle.CoolingScheduler;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HE;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HotKeyRegistry;
import io.github.hyshmily.zeta.worker.detection.rule.HotKeyRule;
import io.github.hyshmily.zeta.worker.detection.rule.RuleBinding;
import io.github.hyshmily.zeta.worker.detection.rule.RuleResolver;
import io.github.hyshmily.zeta.worker.detection.window.SlidingWindowDetector;
import io.github.hyshmily.zeta.worker.detection.window.WindowGeometry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

/**
 * Actuator {@code /actuator/hotkey-worker} endpoint (id {@code hotkeyworker}): the detection
 * plane's runtime state.
 *
 * <p>Without a {@code key} it answers "is detection healthy": the window geometry actually in
 * force, which rules are loaded and what the promotion floor is, how many seats are used, and
 * what each rule has matched, promoted, aborted and cooled. With {@code key} it answers "why is
 * this key hot or not": the governing rule, the live window sum, how long it has been hot, how
 * long it has been below the band, when the next check is due and the version of its promotion.
 *
 * <p>The two time fields are separate on purpose. {@code promotedForMs} means "hot for this long"
 * under either policy; {@code belowForMs} exists only for {@code COOL_FOR_LOW_QPS} while the key is
 * actually below the band. A single {@code sinceMs} would silently change meaning with the policy.
 *
 * <p>Returns real key names and their traffic, so the shipped Worker exposes only {@code health}
 * by default. Opting in requires the endpoint id {@code hotkeyworker} in the web exposure include
 * and a path mapping (the documented public path is {@code /actuator/hotkey-worker}); protect it
 * like the App's {@code hotkey} endpoint (roles/security) in production.
 */
@Internal
@Endpoint(id = "hotkeyworker")
public class HotKeyEndpoint {

  private final RuleResolver rules;
  private final WindowGeometry window;
  private final HotKeyRegistry registry;
  private final CoolingScheduler cooling;
  private final SlidingWindowDetector detector;

  /**
   * Creates the endpoint.
   *
   * @param rules    the published rule set and its counters
   * @param window   the effective window geometry
   * @param registry the HOT set
   * @param cooling  the cool-down scheduler
   * @param detector the window table
   */
  public HotKeyEndpoint(
    RuleResolver rules,
    WindowGeometry window,
    HotKeyRegistry registry,
    CoolingScheduler cooling,
    SlidingWindowDetector detector
  ) {
    this.rules = rules;
    this.window = window;
    this.registry = registry;
    this.cooling = cooling;
    this.detector = detector;
  }

  /**
   * Detection summary, or one key's state when {@code key} is supplied.
   *
   * @param key an optional cache key
   * @return the diagnostic document
   */
  @ReadOperation
  public Map<String, Object> info(@Nullable String key) {
    return key == null ? summary() : keyState(key);
  }

  /**
   * Builds the detection-wide document from one published rule snapshot plus the registry and
   * scheduling counters.
   *
   * @return the summary view
   */
  private Map<String, Object> summary() {
    Map<String, Object> info = new LinkedHashMap<>();
    // One volatile read backs the floor, the fingerprint and every rule's counters, so a
    // concurrent rule publication cannot produce a mixed scrape.
    RuleResolver.Snapshot snapshot = rules.snapshot();

    Map<String, Object> detection = new LinkedHashMap<>();
    detection.put("window", window.windowMs() + "ms/" + window.alignedSlices());
    detection.put("minHotCount", snapshot.minHotCount());
    detection.put("rulesHash", snapshot.hash());
    info.put("detection", detection);

    Map<String, Object> hot = new LinkedHashMap<>();
    hot.put("active", registry.activeCount());
    hot.put("rejected", registry.rejectedCount());
    info.put("hot", hot);

    Map<String, Object> cool = new LinkedHashMap<>();
    cool.put("scheduled", cooling.scheduled());
    info.put("cooling", cool);

    List<Map<String, Object>> ruleViews = new ArrayList<>(snapshot.bindings().size());
    for (RuleBinding binding : snapshot.bindings()) {
      ruleViews.add(toRuleView(binding));
    }
    info.put("rules", ruleViews);
    return info;
  }

  /**
   * Renders one rule's effective threshold and cumulative counters as a side-effect-free view.
   *
   * @param binding the published binding to render
   * @return the rule view
   */
  private Map<String, Object> toRuleView(RuleBinding binding) {
    HotKeyRule rule = binding.rule();
    Map<String, Object> view = new LinkedHashMap<>();
    view.put("id", rule.id());
    view.put("effectiveHotQps", window.toEffectiveQps(rule.hotCount()));
    view.put("matches", binding.matches());
    view.put("promotions", binding.promotions());
    view.put("aborts", binding.aborts());
    view.put("cools", binding.cools());
    return view;
  }

  /**
   * Builds one key's diagnostic view: the governing rule, live window sum, elapsed HOT/below times
   * and the next scheduled check.
   *
   * @param key the cache key
   * @return the key view
   */
  private Map<String, Object> keyState(String key) {
    HE entry = registry.entry(key);
    // The snapshot resolves a non-hot key exactly the way the report path does: the exact index
    // first, then the precedence scan. A full linear scan here would disagree with the hot path
    // under a large EXACT rule set and would not share the published snapshot with the summary.
    HotKeyRule rule = entry != null ? entry.rule() : rules.snapshot().resolveRule(key);

    Map<String, Object> view = new LinkedHashMap<>();
    view.put("key", key);
    view.put("hot", entry != null);
    view.put("rule", rule.id());
    view.put("windowSum", detector.windowSum(key));
    if (entry == null) {
      view.put("promotedForMs", null);
      view.put("belowForMs", null);
      view.put("nextCheckInMs", null);
      view.put("promotedVersion", null);
      return view;
    }
    // Read the volatile dwell clock once, then sample the clock: a second read could observe a
    // concurrent recovery and turn the subtraction into a raw uptime value. belowSince() folds the
    // retired tombstone into NOT_BELOW, so this is the only read of the field the endpoint needs.
    long belowSince = entry.belowSince();
    long nextCheckAt = entry.nextCheckAt();
    long now = TimeSource.monotonicMillis();
    view.put("promotedForMs", Math.max(0L, now - entry.promotedAt()));
    view.put("belowForMs", belowSince == HE.NOT_BELOW ? null : Math.max(0L, now - belowSince));
    view.put("nextCheckInMs", nextCheckAt - now);
    view.put("promotedVersion", entry.promotedVersion());
    return view;
  }
}
