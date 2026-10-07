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
package io.github.hyshmily.zeta.benchmark;

import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost of {@code RuleMatcher.evaluateRule} — paid on every {@code get} via
 * {@code preGuard} (and again on the load path via {@code afterGuard}).
 *
 * <p>{@code ruleSet} selects the matcher content; every rule is a BLOCK that
 * never matches the benchmark keys, so each miss pays the full ordered scan:
 * <ul>
 *   <li>{@code empty} — the zero-config fast path ({@code ALLOW} without
 *       touching the memo).</li>
 *   <li>{@code exact1} — one EXACT rule (cheapest non-empty scan).</li>
 *   <li>{@code mixed10} — 3 PREFIX + 2 WILDCARD + 4 REGEX + 1 EXACT
 *       (regex-engine cost on every miss).</li>
 * </ul>
 *
 * <p>Two access shapes separate the memo layers (10k entries / 30s TTL):
 * <ul>
 *   <li>{@code evaluate_memoHit} — one key forever: steady-state memo hit
 *       (one Caffeine probe per {@code get}).</li>
 *   <li>{@code evaluate_memoMiss} — 20k distinct keys over a 10k memo:
 *       steady-state memo miss + full scan per call.</li>
 * </ul>
 *
 * <p>Run with the shaded jar:
 *
 * <pre>
 *   java -jar benchmark/target/benchmarks.jar RuleBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class RuleBenchmark {

  /** Keys cycle through 20k entries against a 10k memo: steady-state miss. */
  private static final int MISS_KEY_SPACE = 20_000;

  @Param({"empty", "exact1", "mixed10"})
  String ruleSet;

  RuleMatcherImpl matcher;
  String[] missKeys;
  int cursor;

  @Setup(Level.Trial)
  public void setup() {
    matcher = new RuleMatcherImpl(Optional.empty(), Optional.empty());
    switch (ruleSet) {
      case "exact1" -> matcher.addRule(new Rule(Rule.RuleType.EXACT, "bench:never", Rule.RuleAction.BLOCK));
      case "mixed10" -> {
        matcher.addRule(new Rule(Rule.RuleType.PREFIX, "bench:nope:p0:", Rule.RuleAction.BLOCK));
        matcher.addRule(new Rule(Rule.RuleType.PREFIX, "bench:nope:p1:", Rule.RuleAction.BLOCK));
        matcher.addRule(new Rule(Rule.RuleType.PREFIX, "bench:nope:p2:", Rule.RuleAction.BLOCK));
        matcher.addRule(new Rule(Rule.RuleType.WILDCARD, "bench:*:nope:w0", Rule.RuleAction.BLOCK));
        matcher.addRule(new Rule(Rule.RuleType.WILDCARD, "bench:*:nope:w1", Rule.RuleAction.BLOCK));
        matcher.addRule(new Rule(Rule.RuleType.REGEX, "bench:\\d+:nope:r0", Rule.RuleAction.BLOCK));
        matcher.addRule(new Rule(Rule.RuleType.REGEX, "bench:\\d+:nope:r1", Rule.RuleAction.BLOCK));
        matcher.addRule(new Rule(Rule.RuleType.REGEX, "bench:\\d+:nope:r2", Rule.RuleAction.BLOCK));
        matcher.addRule(new Rule(Rule.RuleType.REGEX, "bench:\\d+:nope:r3", Rule.RuleAction.BLOCK));
        matcher.addRule(new Rule(Rule.RuleType.EXACT, "bench:never", Rule.RuleAction.BLOCK));
      }
      default -> {}
    }
    missKeys = new String[MISS_KEY_SPACE];
    for (int k = 0; k < MISS_KEY_SPACE; k++) {
      missKeys[k] = "bench:key:" + k;
    }
    cursor = 0;
  }

  /** One key forever: memo-hit cost (what a hot key pays per {@code get}). */
  @Benchmark
  public Rule.RuleAction evaluate_memoHit() {
    return matcher.evaluateRule("bench:key:7");
  }

  /** 20k keys over a 10k memo: miss + full ordered scan per call. */
  @Benchmark
  public Rule.RuleAction evaluate_memoMiss() {
    if (++cursor >= MISS_KEY_SPACE) {
      cursor = 0;
    }
    return matcher.evaluateRule(missKeys[cursor]);
  }
}
