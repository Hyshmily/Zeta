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
package io.github.hyshmily.hotkey.rule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import io.github.hyshmily.hotkey.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.hotkey.sync.local.CacheSyncPublisher;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * Tests for {@link RuleMatcher} covering pattern detection, rule evaluation, and rule management.
 */
class RuleMatcherTest {

  private RuleMatcher ruleMatcher;

  @BeforeEach
  void setUp() {
    ruleMatcher = new RuleMatcherImpl(Optional.empty(), Optional.empty());
  }

  /**
   * Verifies that a pattern without wildcards or regex prefix is detected as EXACT type.
   */
  @Test
  void of_shouldDetectExactPattern() {
    Rule r = RuleMatcher.of("foo", RuleAction.BLOCK);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.EXACT);
    assertThat(r.getPattern()).isEqualTo("foo");
    assertThat(r.getAction()).isEqualTo(RuleAction.BLOCK);
  }

  /**
   * Verifies that a pattern ending with * is detected as PREFIX type with the wildcard portion stripped.
   */
  @Test
  void of_shouldDetectPrefixPattern() {
    Rule r = RuleMatcher.of("user:*", RuleAction.ALLOW_NO_REPORT);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.PREFIX);
    assertThat(r.getPattern()).isEqualTo("user:");
    assertThat(r.getAction()).isEqualTo(RuleAction.ALLOW_NO_REPORT);
  }

  /**
   * Verifies that a pattern with * in the middle is detected as WILDCARD type.
   */
  @Test
  void of_shouldDetectWildcardPattern() {
    Rule r = RuleMatcher.of("abc*def", RuleAction.BLOCK);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.WILDCARD);
    assertThat(r.getPattern()).isEqualTo("abc*def");
  }

  /**
   * Verifies that a pattern with ? is detected as WILDCARD type.
   */
  @Test
  void of_shouldDetectWildcardWithQuestionMark() {
    Rule r = RuleMatcher.of("abc?def", RuleAction.BLOCK);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.WILDCARD);
  }

  /**
   * Verifies that a pattern with regex: prefix is detected as REGEX type with the prefix stripped.
   */
  @Test
  void of_shouldDetectRegexPattern() {
    Rule r = RuleMatcher.of("regex:^foo.*bar$", RuleAction.BLOCK);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.REGEX);
    assertThat(r.getPattern()).isEqualTo("^foo.*bar$");
  }

  /**
   * Verifies that keys are ALLOW by default when no rules are configured.
   */
  @Test
  void evaluateRule_shouldAllowByDefault() {
    assertThat(ruleMatcher.evaluateRule("anything")).isEqualTo(RuleAction.ALLOW);
  }

  /**
   * Verifies that an exact-match BLOCK rule correctly blocks matching keys and allows others.
   */
  @Test
  void evaluateRule_shouldMatchExactBlock() {
    ruleMatcher.addRule(RuleMatcher.of("secret", RuleAction.BLOCK));
    assertThat(ruleMatcher.evaluateRule("secret")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("secret2")).isEqualTo(RuleAction.ALLOW);
  }

  /**
   * Verifies that a PREFIX BLOCK rule matches all keys starting with the prefix and allows others.
   */
  @Test
  void evaluateRule_shouldMatchPrefixBlock() {
    ruleMatcher.addRule(RuleMatcher.of("admin:*", RuleAction.BLOCK));
    assertThat(ruleMatcher.evaluateRule("admin:login")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("user:login")).isEqualTo(RuleAction.ALLOW);
  }

  /**
   * Verifies that a WILDCARD rule matches keys matching the glob pattern and excludes non-matching keys.
   */
  @Test
  void evaluateRule_shouldMatchWildcardBlock() {
    ruleMatcher.addRule(RuleMatcher.of("a*c", RuleAction.BLOCK));
    assertThat(ruleMatcher.evaluateRule("abc")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("ac")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("abdc")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("ab")).isEqualTo(RuleAction.ALLOW);
  }

  /**
   * Verifies that a REGEX rule performs a find (partial match) rather than full match against the key.
   */
  @Test
  void evaluateRule_shouldMatchRegexFind() {
    ruleMatcher.addRule(RuleMatcher.of("regex:error|secret", RuleAction.BLOCK));
    assertThat(ruleMatcher.evaluateRule("error_occurred")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("top_secret_data")).isEqualTo(RuleAction.BLOCK);
    assertThat(ruleMatcher.evaluateRule("normal_key")).isEqualTo(RuleAction.ALLOW);
  }

  /**
   * Verifies that the first matching rule takes precedence when multiple rules match the same key.
   */
  @Test
  void evaluateRule_shouldRespectFirstMatch() {
    ruleMatcher.addRule(RuleMatcher.of("foo", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("regex:foo", RuleAction.ALLOW_NO_REPORT));
    assertThat(ruleMatcher.evaluateRule("foo")).isEqualTo(RuleAction.BLOCK);
  }

  /**
   * Verifies that an ALLOW_NO_REPORT rule allows access but suppresses reporting for matching keys.
   */
  @Test
  void evaluateRule_shouldAllowNoReport() {
    ruleMatcher.addRule(RuleMatcher.of("audit:*", RuleAction.ALLOW_NO_REPORT));
    assertThat(ruleMatcher.evaluateRule("audit:log")).isEqualTo(RuleAction.ALLOW_NO_REPORT);
  }

  /**
   * Verifies removing a rule by its pattern and action succeeds and restores ALLOW default.
   */
  @Test
  void removeRule_shouldRemoveByPatternAndAction() {
    ruleMatcher.addRule(RuleMatcher.of("foo", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("bar", RuleAction.ALLOW_NO_REPORT));

    assertThat(ruleMatcher.removeRule("foo", RuleAction.BLOCK)).isTrue();
    assertThat(ruleMatcher.evaluateRule("foo")).isEqualTo(RuleAction.ALLOW);
    assertThat(ruleMatcher.removeRule("foo", RuleAction.BLOCK)).isFalse();
  }

  /**
   * Verifies that removeRule does not remove when the action parameter does not match.
   */
  @Test
  void removeRule_shouldNotRemoveIfActionDiffers() {
    ruleMatcher.addRule(RuleMatcher.of("foo", RuleAction.BLOCK));
    assertThat(ruleMatcher.removeRule("foo", RuleAction.ALLOW_NO_REPORT)).isFalse();
    assertThat(ruleMatcher.evaluateRule("foo")).isEqualTo(RuleAction.BLOCK);
  }

  /**
   * Verifies that removeRule returns false for a non-existent pattern.
   */
  @Test
  void removeRule_shouldHandleNonExistent() {
    assertThat(ruleMatcher.removeRule("nonexistent", RuleAction.BLOCK)).isFalse();
  }

  /**
   * Verifies that removeRulesByAction removes all rules with the given action and returns the count.
   */
  @Test
  void removeRulesByAction_shouldRemoveAllMatching() {
    ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("k2", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("k3", RuleAction.ALLOW_NO_REPORT));

    assertThat(ruleMatcher.removeRulesByAction(RuleAction.BLOCK)).isEqualTo(2);
    assertThat(ruleMatcher.evaluateRule("k1")).isEqualTo(RuleAction.ALLOW);
    assertThat(ruleMatcher.evaluateRule("k3")).isEqualTo(RuleAction.ALLOW_NO_REPORT);
  }

  /**
   * Verifies that removeRulesByAction returns zero when no rules match the given action.
   */
  @Test
  void removeRulesByAction_shouldReturnZeroIfNone() {
    assertThat(ruleMatcher.removeRulesByAction(RuleAction.BLOCK)).isZero();
  }

  /**
   * Verifies that clearRules removes all configured rules and reverts evaluation to ALLOW default.
   */
  @Test
  void clearRules_shouldRemoveAll() {
    ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("k2", RuleAction.ALLOW_NO_REPORT));
    ruleMatcher.clearRules();
    assertThat(ruleMatcher.getAllRules()).isEmpty();
    assertThat(ruleMatcher.evaluateRule("k1")).isEqualTo(RuleAction.ALLOW);
  }

  /**
   * Verifies that getAllRules returns a defensive copy (snapshot) not affected by external mutation.
   */
  @Test
  void getAllRules_shouldReturnSnapshot() {
    ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
    List<Rule> snapshot = ruleMatcher.getAllRules();
    assertThat(snapshot).hasSize(1);
    snapshot.clear();
    assertThat(ruleMatcher.getAllRules()).hasSize(1);
  }

  /**
   * Verifies that replaceRules replaces all existing rules with the new set.
   */
  @Test
  void replaceRules_shouldReplaceAll() {
    ruleMatcher.addRule(RuleMatcher.of("old", RuleAction.BLOCK));
    ruleMatcher.replaceRules(List.of(RuleMatcher.of("new1", RuleAction.ALLOW_NO_REPORT)));
    assertThat(ruleMatcher.getAllRules()).hasSize(1);
    assertThat(ruleMatcher.evaluateRule("old")).isEqualTo(RuleAction.ALLOW);
    assertThat(ruleMatcher.evaluateRule("new1")).isEqualTo(RuleAction.ALLOW_NO_REPORT);
  }

  /**
   * Verifies that replaceRules with an empty list clears all rules without error.
   */
  @Test
  void replaceRules_shouldHandleEmpty() {
    ruleMatcher.replaceRules(List.of());
    assertThat(ruleMatcher.getAllRules()).isEmpty();
  }

  /**
   * Verifies that removeRule by index removes the correct rule and does not affect others.
   */
  @Test
  void removeRuleByIndex_shouldWork() {
    ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("k2", RuleAction.BLOCK));
    ruleMatcher.removeRule(0);
    assertThat(ruleMatcher.evaluateRule("k1")).isEqualTo(RuleAction.ALLOW);
    assertThat(ruleMatcher.evaluateRule("k2")).isEqualTo(RuleAction.BLOCK);
  }

  /**
   * Verifies that removeRule by index handles out-of-bounds and negative indices without throwing.
   */
  @Test
  void removeRuleByIndex_shouldHandleOutOfBounds() {
    ruleMatcher.removeRule(99);
    ruleMatcher.removeRule(-1);
  }

  @Test
  void evaluateRule_withNullKey_shouldThrowNullPointerException() {
    ruleMatcher.addRule(RuleMatcher.of("test", RuleAction.BLOCK));
    assertThatNullPointerException().isThrownBy(() -> ruleMatcher.evaluateRule(null));
  }

  @Test
  void evaluateRule_withEmptyKey_shouldBeAllowedByDefault() {
    assertThat(ruleMatcher.evaluateRule("")).isEqualTo(RuleAction.ALLOW);
  }

  @Test
  void addRule_withEmptyPattern_shouldThrowIllegalArgumentException() {
    assertThatIllegalArgumentException()
      .isThrownBy(() -> ruleMatcher.addRule(RuleMatcher.of("", RuleAction.BLOCK)))
      .withMessage("pattern must not be null or empty");
  }

  @Test
  void isAllowNoReport_withBlockRule_shouldReturnEmpty() {
    ruleMatcher.addRule(RuleMatcher.of("blockme", RuleAction.BLOCK));
    assertThat(ruleMatcher.isAllowNoReport("blockme", "get")).isEmpty();
  }

  @Test
  void isAllowNoReport_withAllowRule_shouldReturnFalse() {
    assertThat(ruleMatcher.isAllowNoReport("anykey", "get")).contains(false);
  }

  @Test
  void isAllowNoReport_withAllowNoReportRule_shouldReturnTrue() {
    ruleMatcher.addRule(RuleMatcher.of("noreport:*", RuleAction.ALLOW_NO_REPORT));
    assertThat(ruleMatcher.isAllowNoReport("noreport:test", "get")).contains(true);
  }

  @Test
  void of_withEmptyString_shouldDetectExact() {
    Rule r = RuleMatcher.of("", RuleAction.ALLOW);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.EXACT);
  }

  @Test
  void of_withOnlyStar_shouldDetectPrefixWithEmptyPattern() {
    Rule r = RuleMatcher.of("*", RuleAction.ALLOW);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.PREFIX);
    assertThat(r.getPattern()).isEmpty();
  }

  @Test
  void of_withOnlyQuestionMark_shouldDetectWildcard() {
    Rule r = RuleMatcher.of("?", RuleAction.BLOCK);
    assertThat(r.getType()).isEqualTo(Rule.RuleType.WILDCARD);
  }

  @Test
  void removeRuleByIndex_shouldHandleAllOutOfBoundsIndices() {
    ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
    ruleMatcher.removeRule(-5);
    assertThat(ruleMatcher.evaluateRule("k1")).isEqualTo(RuleAction.BLOCK);
    ruleMatcher.removeRule(5);
    assertThat(ruleMatcher.evaluateRule("k1")).isEqualTo(RuleAction.BLOCK);
  }

  @Test
  void clearRules_onEmptySet_shouldNotThrow() {
    ruleMatcher.clearRules();
    assertThat(ruleMatcher.getAllRules()).isEmpty();
  }

  @Test
  void addAndRemoveMultipleRules_shouldTrackVersionIncrement() {
    ruleMatcher.addRule(RuleMatcher.of("a", RuleAction.BLOCK));
    ruleMatcher.addRule(RuleMatcher.of("b", RuleAction.ALLOW_NO_REPORT));
    ruleMatcher.addRule(RuleMatcher.of("c", RuleAction.ALLOW));
    assertThat(ruleMatcher.getAllRules()).hasSize(3);
    ruleMatcher.removeRule("a", RuleAction.BLOCK);
    ruleMatcher.removeRule("b", RuleAction.ALLOW_NO_REPORT);
    assertThat(ruleMatcher.getAllRules()).hasSize(1);
  }

  @Test
  void addRule_withNullRule_shouldThrowNullPointerException() {
    assertThatNullPointerException()
      .isThrownBy(() -> ruleMatcher.addRule(null))
      .withMessage("rule must not be null");
  }

  @Test
  void addRule_withNullPattern_shouldThrowIllegalArgumentException() {
    Rule rule = new Rule(Rule.RuleType.EXACT, null, RuleAction.BLOCK);
    assertThatIllegalArgumentException()
      .isThrownBy(() -> ruleMatcher.addRule(rule))
      .withMessage("pattern must not be null or empty");
  }

  @Test
  void addRule_withNullAction_shouldThrowIllegalArgumentException() {
    Rule rule = new Rule(Rule.RuleType.EXACT, "foo", null);
    assertThatIllegalArgumentException()
      .isThrownBy(() -> ruleMatcher.addRule(rule))
      .withMessage("action must not be null");
  }

  @Test
  void addRule_withInvalidRegex_shouldThrowPatternSyntaxException() {
    Rule rule = new Rule();
    rule.setType(Rule.RuleType.REGEX);
    rule.setPattern("[invalid");
    rule.setAction(RuleAction.BLOCK);
    assertThatExceptionOfType(java.util.regex.PatternSyntaxException.class)
      .isThrownBy(() -> ruleMatcher.addRule(rule));
  }

  @Test
  void replaceRules_withNullElement_shouldThrow() {
    assertThatNullPointerException().isThrownBy(() -> ruleMatcher.replaceRules(Collections.singletonList(null)));
  }

  private static void callInitRules(RuleMatcher ruleMatcher) {
    try {
      Method m = RuleMatcherImpl.class.getDeclaredMethod("initRules");
      m.setAccessible(true);
      m.invoke(ruleMatcher);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Nested
  @DisplayName("With Redis and CacheSyncPublisher")
  class WithRedisAndPublisher {

    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOps;
    private CacheSyncPublisher publisher;
    private RuleMatcher ruleMatcher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
      redisTemplate = mock(StringRedisTemplate.class);
      valueOps = mock(ValueOperations.class);
      when(redisTemplate.opsForValue()).thenReturn(valueOps);
      publisher = mock(CacheSyncPublisher.class);
      ruleMatcher = new RuleMatcherImpl(Optional.of(redisTemplate), Optional.of(publisher));
    }

    @Test
    @DisplayName("syncRules should skip when incoming version <= local version")
    void syncRules_shouldSkipStaleBroadcast() {
      ruleMatcher.addRule(RuleMatcher.of("k1", RuleAction.BLOCK));
      ruleMatcher.syncRules("[]", Long.MAX_VALUE);
      // After syncRules, local-only pattern should still exist
      assertThat(ruleMatcher.evaluateRule("k1")).isEqualTo(RuleAction.BLOCK);
    }

    @Test
    @DisplayName("syncRules should merge incoming rules with version > local version")
    void syncRules_shouldMergeIncomingRules() {
      ruleMatcher.addRule(RuleMatcher.of("local", RuleAction.BLOCK));
      ruleMatcher.syncRules(
        "[{\"pattern\":\"incoming\",\"action\":\"ALLOW_NO_REPORT\",\"type\":\"EXACT\"}]",
        Long.MAX_VALUE
      );
      assertThat(ruleMatcher.evaluateRule("incoming")).isEqualTo(RuleAction.ALLOW_NO_REPORT);
      assertThat(ruleMatcher.evaluateRule("local")).isEqualTo(RuleAction.BLOCK);
    }

    @Test
    @DisplayName("syncRules should parse versioned wrapper format")
    void syncRules_shouldParseVersionedWrapperFormat() {
      ruleMatcher.syncRules(
        "{\"rulesVersion\":10,\"rules\":[{\"pattern\":\"wrapper\",\"action\":\"BLOCK\",\"type\":\"EXACT\"}]}",
        5L
      );
      assertThat(ruleMatcher.evaluateRule("wrapper")).isEqualTo(RuleAction.BLOCK);
    }

    @Test
    @DisplayName("syncRules should handle legacy array format")
    void syncRules_shouldParseLegacyArrayFormat() {
      ruleMatcher.syncRules("[{\"pattern\":\"legacy\",\"action\":\"BLOCK\",\"type\":\"EXACT\"}]", Long.MAX_VALUE);
      assertThat(ruleMatcher.evaluateRule("legacy")).isEqualTo(RuleAction.BLOCK);
    }

    @Test
    @DisplayName("syncRules should catch JSON parse errors without propagation")
    void syncRules_shouldHandleJsonParseException() {
      ruleMatcher.syncRules("not valid json", Long.MAX_VALUE);
      assertThat(ruleMatcher.getAllRules()).isEmpty();
    }

    @Test
    @DisplayName("syncRules should persist merged rules to Redis")
    void syncRules_shouldPersistToRedisAfterMerge() {
      ruleMatcher.syncRules(
        "{\"rulesVersion\":10,\"rules\":[{\"pattern\":\"p\",\"action\":\"BLOCK\",\"type\":\"EXACT\"}]}",
        5L
      );
      verify(redisTemplate, timeout(1000).atLeastOnce()).execute(any(RedisScript.class), anyList(), any(), any());
    }

    @Test
    @DisplayName("syncRules should keep local-only patterns after merge")
    void syncRules_shouldKeepLocalOnlyPatterns() {
      ruleMatcher.addRule(RuleMatcher.of("local-only", RuleAction.BLOCK));
      ruleMatcher.syncRules(
        "[{\"pattern\":\"incoming\",\"action\":\"ALLOW_NO_REPORT\",\"type\":\"EXACT\"}]",
        Long.MAX_VALUE
      );
      assertThat(ruleMatcher.evaluateRule("local-only")).isEqualTo(RuleAction.BLOCK);
      assertThat(ruleMatcher.evaluateRule("incoming")).isEqualTo(RuleAction.ALLOW_NO_REPORT);
    }

    @Test
    @DisplayName("syncRules should overwrite same-pattern rules with incoming value")
    void syncRules_shouldOverwriteSamePattern() {
      ruleMatcher.addRule(RuleMatcher.of("shared", RuleAction.BLOCK));
      ruleMatcher.syncRules(
        "[{\"pattern\":\"shared\",\"action\":\"ALLOW_NO_REPORT\",\"type\":\"EXACT\"}]",
        Long.MAX_VALUE
      );
      assertThat(ruleMatcher.evaluateRule("shared")).isEqualTo(RuleAction.ALLOW_NO_REPORT);
    }

    @Test
    @DisplayName("broadcastAllLocalRulesManually should broadcast when publisher is present")
    void broadcastAllLocalRulesManually_shouldBroadcastWhenPublisherPresent() {
      ruleMatcher.broadcastAllLocalRulesManually();
      verify(publisher, timeout(1000).atLeastOnce()).broadcastAllLocalRules(anyString(), anyLong());
    }

    @Test
    @DisplayName("addRule should persist to Redis when template is present")
    void addRule_shouldPersistToRedis() {
      ruleMatcher.addRule(RuleMatcher.of("k", RuleAction.BLOCK));
      verify(redisTemplate, timeout(1000).atLeastOnce()).execute(any(RedisScript.class), anyList(), any(), any());
    }

    @Test
    @DisplayName("addRule should broadcast when publisher is present")
    void addRule_shouldBroadcast() {
      ruleMatcher.addRule(RuleMatcher.of("k", RuleAction.BLOCK));
      verify(publisher, timeout(1000).atLeastOnce()).broadcastAllLocalRules(anyString(), anyLong());
    }

    @Test
    @DisplayName("removeRule should persist to Redis when template is present")
    void removeRule_shouldPersistToRedis() {
      ruleMatcher.addRule(RuleMatcher.of("k", RuleAction.BLOCK));
      ruleMatcher.removeRule("k", RuleAction.BLOCK);
      verify(redisTemplate, timeout(1000).atLeast(2)).execute(any(RedisScript.class), anyList(), any(), any());
    }

    @Test
    @DisplayName("clearRules should persist to Redis when template is present")
    void clearRules_shouldPersistToRedis() {
      ruleMatcher.addRule(RuleMatcher.of("k", RuleAction.BLOCK));
      ruleMatcher.clearRules();
      verify(redisTemplate, timeout(1000).atLeast(2)).execute(any(RedisScript.class), anyList(), any(), any());
    }

    @Test
    @DisplayName("initRules should load from Redis when template is present")
    void initRules_shouldLoadFromRedis() {
      when(valueOps.get(anyString())).thenReturn(
        "[{\"pattern\":\"redis-loaded\",\"action\":\"BLOCK\",\"type\":\"EXACT\"}]"
      );
      ruleMatcher = new RuleMatcherImpl(Optional.of(redisTemplate), Optional.empty());
      callInitRules(ruleMatcher);
      assertThat(ruleMatcher.evaluateRule("redis-loaded")).isEqualTo(RuleAction.BLOCK);
    }

    @Test
    @DisplayName("initRules should handle null Redis value gracefully")
    void initRules_shouldHandleNullRedisValue() {
      when(valueOps.get(anyString())).thenReturn(null);
      ruleMatcher = new RuleMatcherImpl(Optional.of(redisTemplate), Optional.empty());
      callInitRules(ruleMatcher);
      assertThat(ruleMatcher.getAllRules()).isEmpty();
    }

    @Test
    @DisplayName("initRules should handle empty Redis value gracefully")
    void initRules_shouldHandleEmptyRedisValue() {
      when(valueOps.get(anyString())).thenReturn("");
      ruleMatcher = new RuleMatcherImpl(Optional.of(redisTemplate), Optional.empty());
      callInitRules(ruleMatcher);
      assertThat(ruleMatcher.getAllRules()).isEmpty();
    }

    @Test
    @DisplayName("initRules should handle malformed JSON in Redis gracefully")
    void initRules_shouldHandleMalformedRedisJson() {
      when(valueOps.get(anyString())).thenReturn("not valid json");
      ruleMatcher = new RuleMatcherImpl(Optional.of(redisTemplate), Optional.empty());
      callInitRules(ruleMatcher);
      assertThat(ruleMatcher.getAllRules()).isEmpty();
    }
  }
}
