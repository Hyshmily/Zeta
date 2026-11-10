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
package io.github.hyshmily.zeta.worker.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.worker.detection.rule.CoolStrategy;
import io.github.hyshmily.zeta.worker.detection.rule.MatchType;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Startup validation: every rule that can be checked without traffic fails the context, with all
 * problems reported together.
 */
class DetectionSettingsTest {

  private static WorkerProperties.Cool lowQps(int coolQps, Duration lowQpsFor) {
    WorkerProperties.Cool cool = new WorkerProperties.Cool();
    cool.setStrategy(CoolStrategy.COOL_FOR_LOW_QPS);
    cool.setCoolQps(coolQps);
    cool.setLowQpsFor(lowQpsFor);
    return cool;
  }

  private static WorkerProperties base(int hotQps, int coolQps) {
    WorkerProperties properties = new WorkerProperties();
    WorkerProperties.Detection detection = properties.getDetection();
    detection.setEnabled(true);
    detection.getDefaults().setHotQps(hotQps);
    detection.getDefaults().setCool(lowQps(coolQps, Duration.ofSeconds(1)));
    return properties;
  }

  @Test
  void convertsQpsAgainstTheEffectiveWindow() {
    // 1s / 16 => 62ms slices, a 992ms effective window.
    DetectionSettings settings = DetectionSettings.of(base(100, 50));

    assertThat(settings.window().windowMs()).isEqualTo(992L);
    assertThat(settings.ruleSet().defaultRule().hotCount()).as("ceil(100 * 992 / 1000)").isEqualTo(100L);
    assertThat(settings.ruleSet().defaultRule().coolCount()).as("ceil(50 * 992 / 1000)").isEqualTo(50L);
    assertThat(settings.ruleSet().defaultRule().matches("any:key"))
      .as("the default rule is match-all even when called directly")
      .isTrue();
  }

  /**
   * The default rule's id keys its binding. A configured rule that reuses it would overwrite that
   * binding, so unmatched keys would be judged against the configured rule's threshold and policy.
   */
  @Test
  void rejectsAConfiguredRuleThatReusesTheDefaultRuleId() {
    WorkerProperties properties = base(100, 50);
    WorkerProperties.Rule rule = new WorkerProperties.Rule();
    rule.setId("defaults");
    WorkerProperties.Match match = new WorkerProperties.Match();
    match.setType(MatchType.PREFIX);
    match.setKey("user:");
    rule.setMatch(match);
    rule.setHotQps(10);
    rule.setCool(lowQps(5, Duration.ofSeconds(1)));
    properties.getDetection().getRules().add(rule);

    assertThatThrownBy(() -> DetectionSettings.of(properties))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("id 'defaults' is duplicated");
  }

  @Test
  void rejectsACollapsedHysteresisBand() {
    WorkerProperties properties = new WorkerProperties();
    properties.getDetection().getWindow().setDuration(Duration.ofMillis(100));
    properties.getDetection().getWindow().setSlices(4);
    properties.getDetection().getWindow().setRetention(Duration.ofSeconds(1));
    properties.getDetection().getCooling().setLowQpsCheckInterval(Duration.ofMillis(25));
    properties.getDetection().getDefaults().setHotQps(20);
    properties.getDetection().getDefaults().setCool(lowQps(11, Duration.ofMillis(100)));

    // ceil(20*100/1000) == 2 and ceil(11*100/1000) == 2: equal counts, empty band.
    assertThatThrownBy(() -> DetectionSettings.of(properties))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("coolCount")
      .hasMessageContaining("hotCount");
  }

  @Test
  void rejectsAnIncompleteDefaultsBlock() {
    WorkerProperties properties = new WorkerProperties();
    properties.getDetection().setEnabled(true);
    properties.getDetection().getDefaults().setHotQps(100);

    assertThatThrownBy(() -> DetectionSettings.of(properties))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("detection.defaults");
  }

  @Test
  void disabledDetectionDoesNotRequireDefaults() {
    WorkerProperties properties = new WorkerProperties();
    properties.getDetection().setEnabled(false);

    DetectionSettings settings = DetectionSettings.of(properties);

    assertThat(settings.ruleSet().defaultRule().id()).isEqualTo("defaults");
    assertThat(settings.ruleSet().defaultRule().hotCount()).isEqualTo(1L);
    assertThat(settings.ruleSet().defaultRule().matches("any")).isTrue();
  }

  /**
   * The LOW_QPS re-check interval is not read by COOL_FOR_EXPIRE. A short window must not force the
   * operator to shrink an unrelated interval just to pass startup validation.
   */
  @Test
  void expireOnlyRulesIgnoreTheLowQpsCheckIntervalBounds() {
    WorkerProperties properties = new WorkerProperties();
    WorkerProperties.Detection detection = properties.getDetection();
    detection.getWindow().setDuration(Duration.ofMillis(500));
    detection.getWindow().setSlices(16);
    detection.getWindow().setRetention(Duration.ofSeconds(1));
    detection.getDefaults().setHotQps(100);
    WorkerProperties.Cool cool = new WorkerProperties.Cool();
    cool.setStrategy(CoolStrategy.COOL_FOR_EXPIRE);
    cool.setCoolAfter(Duration.ofSeconds(1));
    detection.getDefaults().setCool(cool);

    DetectionSettings settings = DetectionSettings.of(properties);

    assertThat(settings.ruleSet().defaultRule().isExpire()).isTrue();
  }

  @Test
  void rejectsACheckIntervalFasterThanOneSlice() {
    WorkerProperties properties = base(100, 50);
    properties.getDetection().getCooling().setLowQpsCheckInterval(Duration.ofMillis(1));

    assertThatThrownBy(() -> DetectionSettings.of(properties))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("one slice");
  }

  /**
   * A non-positive re-check interval is a configuration error, not a record-constructor exception:
   * the gate must report it with the other violations instead of letting {@code HotKeyRule}'s own
   * invariant throw first.
   */
  @Test
  void rejectsANonPositiveCheckIntervalAsAConfigurationError() {
    WorkerProperties properties = base(100, 50);
    properties.getDetection().getCooling().setLowQpsCheckInterval(Duration.ZERO);

    assertThatThrownBy(() -> DetectionSettings.of(properties))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("detection.cooling.low-qps-check-interval");
  }

  @Test
  void rejectsAnExpireRuleThatAlsoSetsLowQpsFields() {
    WorkerProperties properties = base(100, 50);
    WorkerProperties.Rule rule = new WorkerProperties.Rule();
    rule.setId("bad");
    WorkerProperties.Match match = new WorkerProperties.Match();
    match.setType(MatchType.PREFIX);
    match.setKey("x:");
    rule.setMatch(match);
    rule.setHotQps(10);
    WorkerProperties.Cool cool = new WorkerProperties.Cool();
    cool.setStrategy(CoolStrategy.COOL_FOR_EXPIRE);
    cool.setCoolAfter(Duration.ofMinutes(1));
    cool.setCoolQps(5);
    rule.setCool(cool);
    properties.getDetection().getRules().add(rule);

    assertThatThrownBy(() -> DetectionSettings.of(properties))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("no effect");
  }

  @Test
  void rejectsAnUncompilableRegexRule() {
    WorkerProperties properties = base(100, 50);
    WorkerProperties.Rule rule = new WorkerProperties.Rule();
    rule.setId("bad-regex");
    WorkerProperties.Match match = new WorkerProperties.Match();
    match.setType(MatchType.REGEX);
    match.setKey("[unclosed");
    rule.setMatch(match);
    rule.setHotQps(10);
    rule.setCool(lowQps(5, Duration.ofSeconds(1)));
    properties.getDetection().getRules().add(rule);

    assertThatThrownBy(() -> DetectionSettings.of(properties))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("bad-regex");
  }

  @Test
  void exactRulesDoNotLowerTheGeneralPromotionFloor() {
    WorkerProperties properties = base(1_000, 500);
    WorkerProperties.Rule rule = new WorkerProperties.Rule();
    rule.setId("special-key");
    WorkerProperties.Match match = new WorkerProperties.Match();
    match.setType(MatchType.EXACT);
    match.setKey("user:42");
    rule.setMatch(match);
    rule.setHotQps(10);
    rule.setCool(lowQps(5, Duration.ofSeconds(1)));
    properties.getDetection().getRules().add(rule);

    DetectionSettings settings = DetectionSettings.of(properties);

    assertThat(settings.ruleSet().minHotCount())
      .as("the exact rule is indexed before the floor; the default rule sets the floor")
      .isEqualTo(settings.ruleSet().defaultRule().hotCount());
    assertThat(settings.ruleSet().resolve("user:42").id())
      .as("the exact rule remains the governing rule for its key")
      .isEqualTo("special-key");
  }

  @Test
  void rejectsASweepIntervalThatWouldWalkTheWholeTableEveryMillisecond() {
    WorkerProperties properties = base(100, 50);
    properties.getDetection().getCooling().setSweepInterval(Duration.ofMillis(999));

    assertThatThrownBy(() -> DetectionSettings.of(properties))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("sweep-interval");
  }

  /**
   * A cool queue smaller than the seat cap must start: a seat is released before its COOL is
   * queued and can be reused for another lifecycle, so the queue is a burst buffer, not a bound on
   * the number of demotions that can be outstanding (design decisions: decision delivery).
   */
  @Test
  void aCoolQueueSmallerThanTheSeatCapIsAllowed() {
    WorkerProperties properties = base(100, 50);
    properties.getBroadcast().setCoolQueueCapacity(10);
    properties.getDetection().getLimits().setMaxActiveHotKeys(100);

    DetectionSettings settings = DetectionSettings.of(properties);

    assertThat(settings.maxActiveHotKeys()).isEqualTo(100);
  }
}
