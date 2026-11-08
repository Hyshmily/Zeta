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

import io.github.hyshmily.zeta.worker.detection.rule.CoolStrategy;
import io.github.hyshmily.zeta.worker.detection.rule.HotKeyRule;
import io.github.hyshmily.zeta.worker.detection.rule.MatchType;
import io.github.hyshmily.zeta.worker.detection.rule.RuleSet;
import io.github.hyshmily.zeta.worker.detection.window.WindowGeometry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * The detection configuration, validated once and resolved into the objects the runtime uses.
 *
 * <p>Everything that can be checked without traffic is checked here, in one pass, so a
 * misconfiguration fails startup with every problem listed instead of surfacing as a key that
 * never cools. Thresholds are converted from QPS to window counts against the <b>effective</b>
 * window ({@link WindowGeometry#windowMs()}), never the configured duration: {@code 1000ms / 16}
 * is a 992ms window, and converting against 1000 would make every rule 0.8% stricter than its
 * author intended.
 *
 * @param enabled             whether the pipeline evaluates reports at all
 * @param window              the aligned geometry every rule is converted against
 * @param ruleSet             the resolved, ordered rule snapshot
 * @param retentionMs         idle-window reclamation threshold
 * @param sweepIntervalMs     timer-repair cadence
 * @param coolingThreads      number of striped cool-down wheels (one advancing thread each)
 * @param maxActiveHotKeys    HOT seats
 * @param maxHotBroadcastsPerSecond  HOT send budget
 * @param maxCoolBroadcastsPerSecond COOL send budget
 */
@Slf4j
public record DetectionSettings(
  boolean enabled,
  WindowGeometry window,
  RuleSet ruleSet,
  long retentionMs,
  long sweepIntervalMs,
  int coolingThreads,
  int maxActiveHotKeys,
  int maxHotBroadcastsPerSecond,
  int maxCoolBroadcastsPerSecond
) {
  /** The reserved id of the default rule; a configured rule may not reuse it. */
  private static final String DEFAULT_RULE_ID = "defaults";

  /**
   * Placeholder default used when detection is disabled and no default rule resolved. A disabled
   * pipeline never resolves a rule, but the resolver and the endpoint still need a well-formed
   * snapshot.
   */
  private static final HotKeyRule DISABLED_DEFAULT_RULE = new HotKeyRule(
    DEFAULT_RULE_ID,
    MatchType.PREFIX,
    "*",
    Integer.MIN_VALUE,
    -1,
    Long.MAX_VALUE,
    0L,
    0L,
    60_000L,
    null
  );

  /**
   * Validates the whole detection block and resolves it.
   *
   * @param properties the worker configuration
   * @return the resolved settings
   * @throws IllegalStateException listing every violation found
   */
  public static DetectionSettings of(WorkerProperties properties) {
    return new Validator(properties).validate();
  }

  /**
   * One startup pass over the detection block. The validator owns the error sink and the two
   * derived values every rule conversion needs, so neither has to travel through the whole call
   * chain.
   */
  private static final class Validator {

    private final List<String> errors = new ArrayList<>();
    private final WorkerProperties.Detection detection;
    private final WorkerProperties properties;
    private WindowGeometry window;
    private long checkIntervalMs;

    private Validator(WorkerProperties properties) {
      this.properties = properties;
      this.detection = properties.getDetection();
    }

    private DetectionSettings validate() {
      window = resolveGeometry(detection.getWindow());
      WorkerProperties.Cooling cooling = detection.getCooling();
      checkIntervalMs = millis(cooling.getLowQpsCheckInterval(), "detection.cooling.low-qps-check-interval");
      long sweepIntervalMs = millis(cooling.getSweepInterval(), "detection.cooling.sweep-interval");
      if (sweepIntervalMs > 0 && sweepIntervalMs < 1_000L) {
        errors.add("detection.cooling.sweep-interval (" + sweepIntervalMs + "ms) must be >= 1000ms");
      }

      long retentionMs = millis(detection.getWindow().getRetention(), "detection.window.retention");
      RuleSet ruleSet = null;
      if (window != null) {
        validateWindowBounds(retentionMs);
        ruleSet = resolveRuleSet();
      }

      if (!errors.isEmpty()) {
        throw new IllegalStateException(
          "Invalid zeta.worker.detection configuration:\n  - " + String.join("\n  - ", errors)
        );
      }

      WorkerProperties.Limits limits = detection.getLimits();
      DetectionSettings settings = new DetectionSettings(
        detection.isEnabled(),
        window,
        ruleSet,
        retentionMs,
        sweepIntervalMs,
        cooling.getThreads(),
        limits.getMaxActiveHotKeys(),
        limits.getMaxHotBroadcastsPerSecond(),
        limits.getMaxCoolBroadcastsPerSecond()
      );
      settings.warn(properties.getMaxTrackedKeys(), checkIntervalMs);
      return settings;
    }

    private void validateWindowBounds(long retentionMs) {
      if (retentionMs > 0 && retentionMs < window.windowMs()) {
        errors.add(
          "detection.window.retention (" +
            retentionMs +
            "ms) must be >= detection.window.duration (" +
            window.windowMs() +
            "ms)"
        );
      }
      // The upper bound is the CONFIGURED duration, not the effective window: `1s` against a
      // 992ms window is a rounding artifact of the slice alignment, not a slower re-check.
      if (checkIntervalMs > window.configuredMs()) {
        errors.add(
          "detection.cooling.low-qps-check-interval (" +
            checkIntervalMs +
            "ms) must be <= window.duration (" +
            window.configuredMs() +
            "ms)"
        );
      }
      if (checkIntervalMs > 0 && checkIntervalMs < window.sliceMs()) {
        errors.add(
          "detection.cooling.low-qps-check-interval (" +
            checkIntervalMs +
            "ms) must be >= one slice (" +
            window.sliceMs() +
            "ms)"
        );
      }
    }

    @Nullable
    private WindowGeometry resolveGeometry(WorkerProperties.Window windowConfig) {
      long durationMs = millis(windowConfig.getDuration(), "detection.window.duration");
      try {
        WindowGeometry geometry = WindowGeometry.of(durationMs, windowConfig.getSlices());
        if (!geometry.exact()) {
          log.warn(
            "detection.window.duration ({}ms) is not evenly divisible by the aligned slice count ({}); " +
              "the effective window is {}ms with {}ms slices, and every threshold is converted against {}ms",
            durationMs,
            geometry.alignedSlices(),
            geometry.windowMs(),
            geometry.sliceMs(),
            geometry.windowMs()
          );
        }
        return geometry;
      } catch (IllegalArgumentException e) {
        errors.add("detection.window: " + e.getMessage());
        return null;
      }
    }

    @Nullable
    private RuleSet resolveRuleSet() {
      HotKeyRule defaultRule = resolveDefaultRule();
      List<HotKeyRule> rules = resolveConfiguredRules(detection.getRules());
      if (defaultRule == null) {
        return detection.isEnabled() ? null : RuleSet.of(rules, DISABLED_DEFAULT_RULE);
      }
      return RuleSet.of(rules, defaultRule);
    }

    @Nullable
    private HotKeyRule resolveDefaultRule() {
      WorkerProperties.Defaults defaults = detection.getDefaults();
      if (defaults == null || defaults.getHotQps() == null || defaults.getCool() == null) {
        if (detection.isEnabled()) {
          errors.add("detection.defaults is required in full (hot-qps + cool) when detection is enabled");
        }
        return null;
      }
      return resolveRule(
        DEFAULT_RULE_ID,
        MatchType.PREFIX,
        "*",
        Integer.MIN_VALUE,
        -1,
        defaults.getHotQps(),
        defaults.getCool()
      );
    }

    private List<HotKeyRule> resolveConfiguredRules(List<WorkerProperties.Rule> configured) {
      List<HotKeyRule> rules = new ArrayList<>();
      Set<String> ids = new HashSet<>();
      // The default rule's binding is keyed by this id; a configured rule that reuses it would
      // overwrite the default's counters and make unmatched keys resolve against the wrong rule.
      ids.add(DEFAULT_RULE_ID);
      for (int i = 0; i < configured.size(); i++) {
        WorkerProperties.Rule rule = configured.get(i);
        String id = rule.getId() == null || rule.getId().isBlank() ? "rule-" + (i + 1) : rule.getId();
        if (!ids.add(id)) {
          errors.add("detection.rules[" + i + "].id '" + id + "' is duplicated");
        }
        if (rule.getMatch() == null || rule.getMatch().getType() == null) {
          errors.add("detection.rules[" + i + "].match.type is required");
          continue;
        }
        if (rule.getHotQps() == null || rule.getCool() == null) {
          errors.add("detection.rules[" + i + "] (" + id + ") is incomplete: hot-qps + cool are required");
          continue;
        }
        HotKeyRule resolved = resolveRule(
          id,
          rule.getMatch().getType(),
          rule.getMatch().getKey(),
          rule.getPriority(),
          i,
          rule.getHotQps(),
          rule.getCool()
        );
        if (resolved != null) {
          rules.add(resolved);
        }
      }
      return rules;
    }

    /**
     * Resolves one rule, appending every problem it has.
     *
     * <p>The callers have already proved {@code hotQps} and {@code cool} non-null; the default
     * rule's pattern is a placeholder because the rule set returns it directly when no other rule
     * matched, so no matcher ever consults it.
     */
    @Nullable
    private HotKeyRule resolveRule(
      String id,
      MatchType type,
      String pattern,
      int priority,
      int order,
      int hotQps,
      WorkerProperties.Cool cool
    ) {
      if (pattern == null || pattern.isBlank()) {
        errors.add("rule '" + id + "' has a blank match.key");
        return null;
      }

      if (hotQps < 1) {
        errors.add("rule '" + id + "'.hot-qps must be >= 1");
        return null;
      }

      long hotCount = window.toWindowCount(hotQps);
      if (hotCount < 1) {
        errors.add("rule '" + id + "' converts to hotCount " + hotCount + "; raise hot-qps or the window");
        return null;
      }

      CoolParameters params = resolveCoolParameters(id, cool, hotCount);
      if (params == null) {
        return null;
      }

      if (window.toEffectiveQps(hotCount) < 10) {
        log.warn(
          "rule '{}' promotes at {} accesses per {}ms window (~{} qps): at that rate a single burst is " +
            "indistinguishable from Poisson noise and the rule will flap near its threshold",
          id,
          hotCount,
          window.windowMs(),
          hotQps
        );
      }
      // Compile here, not in HotKeyRule's constructor: a bad expression is a configuration error that
      // belongs in this error list with the other violations, not an exception used as control flow.
      Pattern regex = null;
      if (type == MatchType.REGEX) {
        try {
          regex = Pattern.compile(pattern);
        } catch (PatternSyntaxException e) {
          errors.add("rule '" + id + "' has an invalid regex: " + e.getDescription());
          return null;
        }
      }
      return new HotKeyRule(
        id,
        type,
        pattern,
        priority,
        order,
        hotCount,
        params.coolCount(),
        params.lowQpsForMs(),
        params.checkIntervalMs(),
        regex
      );
    }

    /**
     * Resolves and validates one rule's cool-down block into the three primitive parameters the
     * runtime rule stores.
     *
     * <p>Each strategy is checked against the fields it actually uses: EXPIRE rejects
     * {@code cool-qps}/{@code low-qps-for}, LOW_QPS rejects {@code cool-after}, and LOW_QPS also
     * requires a non-empty hysteresis band ({@code coolCount < hotCount}) and a positive global
     * re-check interval. The returned encoding is the runtime contract: {@code coolCount == 0} means
     * EXPIRE and stores the lifetime in {@code checkIntervalMs}; a positive {@code coolCount} means
     * LOW_QPS and stores the dwell in {@code lowQpsForMs}. Returning {@code null} means at least one
     * error was appended; the caller must not build a rule.
     *
     * @param id       the rule id, for error messages
     * @param cool     the configured cool block (never {@code null})
     * @param hotCount the rule's already-converted promotion threshold
     * @return the validated parameters, or {@code null} when the block is invalid
     */
    @Nullable
    private CoolParameters resolveCoolParameters(String id, WorkerProperties.Cool cool, long hotCount) {
      CoolStrategy strategy = cool.getStrategy();
      if (strategy == null) {
        errors.add("rule '" + id + "'.cool.strategy is required");
        return null;
      }
      return switch (strategy) {
        case COOL_FOR_EXPIRE -> resolveExpire(id, cool);
        case COOL_FOR_LOW_QPS -> resolveLowQps(id, cool, hotCount);
      };
    }

    /** Validates the fixed-lifetime strategy: it owns {@code cool-after} and no low-QPS fields. */
    @Nullable
    private CoolParameters resolveExpire(String id, WorkerProperties.Cool cool) {
      if (cool.getCoolQps() != null || cool.getLowQpsFor() != null) {
        errors.add("rule '" + id + "' uses COOL_FOR_EXPIRE but sets cool-qps/low-qps-for; those fields have no effect");
        return null;
      }

      long coolAfterMs = millis(cool.getCoolAfter(), "rule '" + id + "'.cool.cool-after");
      if (coolAfterMs < 1_000L) {
        errors.add("rule '" + id + "'.cool.cool-after must be >= 1s");
        return null;
      }

      return new CoolParameters(0L, 0L, coolAfterMs);
    }

    /** Validates the low-QPS strategy: it owns {@code cool-qps}/{@code low-qps-for} and needs a non-empty band. */
    @Nullable
    private CoolParameters resolveLowQps(String id, WorkerProperties.Cool cool, long hotCount) {
      if (cool.getCoolAfter() != null) {
        errors.add("rule '" + id + "' uses COOL_FOR_LOW_QPS but sets cool-after; that field has no effect");
        return null;
      }

      if (cool.getCoolQps() == null || cool.getCoolQps() < 1) {
        errors.add("rule '" + id + "'.cool.cool-qps must be >= 1");
        return null;
      }

      long lowQpsForMs = millis(cool.getLowQpsFor(), "rule '" + id + "'.cool.low-qps-for");
      if (lowQpsForMs < window.windowMs()) {
        errors.add(
          "rule '" +
            id +
            "'.cool.low-qps-for (" +
            lowQpsForMs +
            "ms) must be >= the effective window (" +
            window.windowMs() +
            "ms)"
        );
        return null;
      }

      long coolCount = window.toWindowCount(cool.getCoolQps());
      if (coolCount < 1) {
        errors.add("rule '" + id + "'.cool.cool-qps converts to coolCount " + coolCount);
        return null;
      }

      if (coolCount >= hotCount) {
        errors.add(
          "rule '" +
            id +
            "': coolCount (" +
            coolCount +
            ") must be < hotCount (" +
            hotCount +
            "); with equal counts the hysteresis band is empty and one window flip cools the key"
        );
        return null;
      }

      if (checkIntervalMs < 1L) {
        // millis() already recorded the violation. Refusing to build here keeps the record's own
        // invariant from preempting the aggregated startup report with an IllegalArgumentException.
        return null;
      }

      return new CoolParameters(coolCount, lowQpsForMs, checkIntervalMs);
    }

    /**
     * Converts a configured duration to milliseconds, recording a missing or non-positive value in
     * {@link #errors}. The raw conversion is returned even when it is invalid (0 for a missing
     * value), so a caller must treat any appended error as fatal before using the result.
     *
     * @param value the configured duration, possibly {@code null}
     * @param name  the property path used in the error message
     * @return the duration in milliseconds, or 0 when the value is missing
     */
    private long millis(@Nullable Duration value, String name) {
      if (value == null) {
        errors.add(name + " is required");
        return 0L;
      }

      long ms = value.toMillis();
      if (ms <= 0) {
        errors.add(name + " must be positive");
      }
      return ms;
    }

    /**
     * The three primitive cool-down parameters a resolved rule carries. The strategy is encoded by
     * {@code coolCount == 0} (EXPIRE) versus {@code coolCount > 0} (LOW_QPS).
     */
    private record CoolParameters(long coolCount, long lowQpsForMs, long checkIntervalMs) {}
  }

  /**
   * Startup self-checks on the resolved settings. Called only after the validation gate, so the
   * conversions below can assume a positive check interval.
   *
   * @param maxTrackedKeys the configured window-table cap ({@code 0} disables the memory check)
   * @param checkIntervalMs the resolved LOW_QPS re-check period
   */
  private void warn(long maxTrackedKeys, long checkIntervalMs) {
    if (!enabled) {
      return;
    }

    if (maxTrackedKeys > 0 && maxActiveHotKeys > maxTrackedKeys) {
      log.warn(
        "detection.limits.max-active-hot-keys ({}) exceeds max-tracked-keys ({}); the seat cap can never bind",
        maxActiveHotKeys,
        maxTrackedKeys
      );
    }
    warnTrackedKeyMemory(maxTrackedKeys);
    long checksPerSecond = (maxActiveHotKeys * 1_000L) / checkIntervalMs;
    // A wheel thread inserts and fires in O(1) and allocates nothing; the cost per check is the
    // registry evaluation plus the wheel link. The budget is deliberately conservative: at 200k
    // checks/s one wheel still spends only a few percent of a core, so crossing it means the
    // deployment is an order of magnitude beyond the design point, not that the wheel is full.
    long checksPerWheel = 200_000L;
    if (checksPerSecond > checksPerWheel * coolingThreads) {
      log.warn(
        "max-active-hot-keys / low-qps-check-interval projects ~{}/s of checks, above {} checks/s ({} per wheel, " +
          "{} configured); add wheels (detection.cooling.threads) before the per-check evaluation dominates a " +
          "core, and expect the sweep to repair checks that fall behind",
        checksPerSecond,
        checksPerWheel * coolingThreads,
        checksPerWheel,
        coolingThreads
      );
    }
    long refreshPerSecond = 0L;
    for (HotKeyRule rule : ruleSet.allRules()) {
      if (rule.isExpire()) {
        // max-active-hot-keys is shared by every rule, so the worst-case refresh rate is the
        // fastest EXPIRE rule's 2-per-key rate, not the sum across rules.
        long ruleRefreshPerSecond = (2L * maxActiveHotKeys * 1_000L) / rule.checkIntervalMs();
        refreshPerSecond = Math.max(refreshPerSecond, ruleRefreshPerSecond);
      } else {
        long coolQps = window.toEffectiveQps(rule.coolCount());
        long hotQps = window.toEffectiveQps(rule.hotCount());
        if (coolQps * 10 > 9 * hotQps) {
          log.warn(
            "rule '{}': cool-qps (~{} qps) is within 10% of hot-qps (~{} qps); the hysteresis band is very narrow",
            rule.id(),
            coolQps,
            hotQps
          );
        }
      }
    }
    if (refreshPerSecond > maxHotBroadcastsPerSecond + maxCoolBroadcastsPerSecond) {
      log.warn(
        "COOL_FOR_EXPIRE rules can emit ~{}/s of COOL/HOT refreshes (2 per hot key per cool-after), " +
          "above the combined broadcast budget {}/s; still-hot keys will queue and be dropped",
        refreshPerSecond,
        maxHotBroadcastsPerSecond + maxCoolBroadcastsPerSecond
      );
    }
  }

  /**
   * Warns when the configured key cap projects a window table larger than half the heap.
   *
   * <p>Each tracked key holds a doubled ring of two {@code long} slots per slice plus one window
   * object, a concurrent-map node and the key string. The estimate is deliberately coarse — it is
   * a startup capacity check, not an allocation model — and it catches the common failure of
   * keeping the 1M default on a sub-gigabyte container.
   *
   * @param maxTrackedKeys the configured cap ({@code 0} disables the check)
   */
  private void warnTrackedKeyMemory(long maxTrackedKeys) {
    if (maxTrackedKeys <= 0) {
      return;
    }
    long bytesPerKey = 2L * window.alignedSlices() * Long.BYTES + 96L;
    long estimatedBytes = maxTrackedKeys > Long.MAX_VALUE / bytesPerKey ? Long.MAX_VALUE : maxTrackedKeys * bytesPerKey;
    long maxHeap = Runtime.getRuntime().maxMemory();
    if (estimatedBytes <= maxHeap / 2) {
      return;
    }
    log.warn(
      "max-tracked-keys ({}) at a {}ms/{} slices window projects ~{} MiB for the window table alone, " +
        "over half of the {} MiB max heap; lower the cap or raise -Xmx before the table drives the JVM " +
        "into GC death",
      maxTrackedKeys,
      window.windowMs(),
      window.alignedSlices(),
      estimatedBytes / (1024L * 1024L),
      maxHeap / (1024L * 1024L)
    );
  }
}
