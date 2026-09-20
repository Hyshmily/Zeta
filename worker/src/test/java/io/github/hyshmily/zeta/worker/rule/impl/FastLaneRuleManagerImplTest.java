package io.github.hyshmily.zeta.worker.rule.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FastLaneRuleManagerImplTest {

  private FastLaneRuleManager manager;

  @BeforeEach
  void setUp() {
    manager = new FastLaneRuleManagerImpl(List.of(
      new FastLaneRuleManager.FastLaneRule("product:*", 500),
      new FastLaneRuleManager.FastLaneRule("news:*", 1000)
    ));
  }

  @Nested
  class Match {

    @Test
    void shouldMatchExactKey() {
      assertThat(manager.match("product:123")).isNotNull();
      assertThat(manager.match("product:123").threshold()).isEqualTo(500);
    }

    @Test
    void shouldMatchWildcardSuffix() {
      assertThat(manager.match("news:breaking")).isNotNull();
      assertThat(manager.match("news:breaking").threshold()).isEqualTo(1000);
    }

    @Test
    void shouldReturnNullWhenNoRuleMatches() {
      assertThat(manager.match("unknown:key")).isNull();
    }

    @Test
    void shouldMatchFirstRuleWhenMultipleMatch() {
      manager.addRule("*", 999);
      assertThat(manager.match("anything").threshold()).isEqualTo(999);
    }

    @Test
    void shouldCacheResultForRepeatedKey() {
      String key = "product:abc";
      assertThat(manager.match(key)).isNotNull();
      assertThat(manager.match(key).threshold()).isEqualTo(500);
    }
  }

  @Nested
  class AddRule {

    @Test
    void shouldAddNewRule() {
      manager.addRule("flash:*", 200);
      assertThat(manager.match("flash:deal")).isNotNull();
      assertThat(manager.match("flash:deal").threshold()).isEqualTo(200);
    }

    @Test
    void shouldOverwriteExistingRule() {
      manager.addRule("product:*", 999);
      assertThat(manager.match("product:x").threshold()).isEqualTo(999);
    }

    @Test
    void shouldInvalidateCacheOnAdd() {
      manager.match("product:x");
      manager.addRule("product:*", 999);
      assertThat(manager.match("product:x").threshold()).isEqualTo(999);
    }
  }

  @Nested
  class RemoveRule {

    @Test
    void shouldRemoveExistingRule() {
      assertThat(manager.removeRule("product:*")).isTrue();
      assertThat(manager.match("product:x")).isNull();
    }

    @Test
    void shouldReturnFalseForNonExistentRule() {
      assertThat(manager.removeRule("nonexistent:*")).isFalse();
    }

    @Test
    void shouldInvalidateCacheOnRemove() {
      manager.match("product:x");
      manager.removeRule("product:*");
      assertThat(manager.match("product:x")).isNull();
    }
  }

  @Nested
  class UpdateRule {

    @Test
    void shouldUpdateExistingRule() {
      assertThat(manager.updateRule("product:*", 999)).isTrue();
      assertThat(manager.match("product:x").threshold()).isEqualTo(999);
    }

    @Test
    void shouldReturnFalseForNonExistentRule() {
      assertThat(manager.updateRule("nonexistent:*", 100)).isFalse();
    }

    @Test
    void shouldInvalidateCacheOnUpdate() {
      manager.match("product:x");
      manager.updateRule("product:*", 999);
      assertThat(manager.match("product:x").threshold()).isEqualTo(999);
    }
  }

  @Nested
  class GetRules {

    @Test
    void shouldReturnAllRules() {
      List<FastLaneRuleManager.FastLaneRule> rules = manager.getRules();
      assertThat(rules).hasSize(2);
    }

    @Test
    void shouldReturnDefensiveCopy() {
      List<FastLaneRuleManager.FastLaneRule> rules = manager.getRules();
      manager.addRule("extra:*", 100);
      assertThat(rules).hasSize(2);
    }

    @Test
    void shouldReturnEmptyForNoRules() {
      manager = new FastLaneRuleManagerImpl(List.of());
      assertThat(manager.getRules()).isEmpty();
    }

    @Test
    void shouldReturnEmptyForNullInitialRules() {
      manager = new FastLaneRuleManagerImpl(null);
      assertThat(manager.getRules()).isEmpty();
    }
  }

  @Nested
  class GlobMatching {

    @Test
    void shouldMatchWildcardOnlyPattern() {
      manager.addRule("*", 1);
      assertThat(manager.match("anything")).isNotNull();
    }

    @Test
    void shouldMatchQuestionMark() {
      manager.addRule("???", 1);
      assertThat(manager.match("abc")).isNotNull();
      assertThat(manager.match("ab")).isNull();
      assertThat(manager.match("abcd")).isNull();
    }

    @Test
    void shouldMatchEmptyPattern() {
      manager.addRule("", 1);
      assertThat(manager.match("")).isNotNull();
      assertThat(manager.match("a")).isNull();
    }
  }

  /**
   * The rule-set version is the only convergence key for a replicated rule set
   * (ADR-0025 last-writer-wins), so it must be strictly increasing. A bare
   * wall-clock stamp collapses to a tie when two mutations land in the same
   * millisecond, and {@code replaceAll} accepts equal versions — so peers could
   * settle on whichever snapshot arrived last and diverge. A backward wall-clock
   * step is the other failure: the edit's version is not newer than what peers
   * already hold, so the broadcast is rejected as stale and never propagates.
   */
  @Nested
  class VersionMonotonicity {

    @Test
    void rapidMutations_shouldProduceStrictlyIncreasingVersions() {
      long previous = manager.getRulesVersion();
      // 200 back-to-back mutations. Pre-fix this loop hits same-millisecond ties
      // almost immediately, and a tie makes two consecutive versions equal.
      for (int i = 0; i < 200; i++) {
        manager.addRule("burst:" + i + ":*", i + 1);
        long current = manager.getRulesVersion();
        assertThat(current).as("version after mutation %d", i).isGreaterThan(previous);
        previous = current;
      }
    }

    @Test
    void everyMutationKind_shouldAdvanceTheVersion() {
      long v0 = manager.getRulesVersion();
      manager.addRule("versioned:*", 10);
      long v1 = manager.getRulesVersion();
      manager.updateRule("versioned:*", 20);
      long v2 = manager.getRulesVersion();
      manager.removeRule("versioned:*");
      long v3 = manager.getRulesVersion();

      assertThat(v1).isGreaterThan(v0);
      assertThat(v2).isGreaterThan(v1);
      assertThat(v3).isGreaterThan(v2);
    }

    @Test
    void replaceAll_withOlderVersion_shouldBeIgnoredAndNotLowerTheVersion() {
      manager.addRule("versioned:*", 10);
      long published = manager.getRulesVersion();

      manager.replaceAll(List.of(new FastLaneRuleManager.FastLaneRule("other:*", 20)), published - 1);

      assertThat(manager.getRulesVersion()).isEqualTo(published);
      assertThat(manager.match("versioned:x")).isNotNull();
      assertThat(manager.match("other:x")).isNull();
    }
  }
}
