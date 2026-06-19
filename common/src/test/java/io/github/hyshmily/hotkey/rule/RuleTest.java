package io.github.hyshmily.hotkey.rule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import io.github.hyshmily.hotkey.rule.Rule.RuleType;
import java.util.regex.PatternSyntaxException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RuleTest {

  @Test
  void exactMatchMatchesEqualKeys() {
    var rule = new Rule(RuleType.EXACT, "foo", RuleAction.BLOCK);
    assertThat(rule.match("foo")).isTrue();
  }

  @Test
  void exactMatchRejectsDifferentKeys() {
    var rule = new Rule(RuleType.EXACT, "foo", RuleAction.BLOCK);
    assertThat(rule.match("bar")).isFalse();
  }

  @Test
  void prefixMatchMatchesStartsWith() {
    var rule = new Rule(RuleType.PREFIX, "foo", RuleAction.ALLOW);
    assertThat(rule.match("foobar")).isTrue();
  }

  @Test
  void prefixMatchRejectsNonPrefix() {
    var rule = new Rule(RuleType.PREFIX, "foo", RuleAction.ALLOW);
    assertThat(rule.match("barfoo")).isFalse();
  }

  @Test
  void wildcardStarMatchesAnyString() {
    var rule = new Rule(RuleType.WILDCARD, "*", RuleAction.BLOCK);
    assertThat(rule.match("anything")).isTrue();
    assertThat(rule.match("")).isTrue();
  }

  @Test
  void wildcardQuestionMarkMatchesSingleChar() {
    var rule = new Rule(RuleType.WILDCARD, "f?o", RuleAction.BLOCK);
    assertThat(rule.match("foo")).isTrue();
    assertThat(rule.match("fao")).isTrue();
    assertThat(rule.match("fooo")).isFalse();
  }

  @Test
  void wildcardFooStarBarMatchesCorrectRange() {
    var rule = new Rule(RuleType.WILDCARD, "foo*bar", RuleAction.BLOCK);
    assertThat(rule.match("foobar")).isTrue();
    assertThat(rule.match("fooXYZbar")).isTrue();
    assertThat(rule.match("foobaz")).isFalse();
  }

  @Test
  void wildcardEscapesRegexSpecialChars() {
    var rule = new Rule(RuleType.WILDCARD, "test.data*", RuleAction.BLOCK);
    assertThat(rule.match("test.dataX")).isTrue();
    assertThat(rule.match("testXdataX")).isFalse();
  }

  @Test
  void wildcardEscapesPlusSign() {
    var rule = new Rule(RuleType.WILDCARD, "a+b*", RuleAction.BLOCK);
    assertThat(rule.match("a+bX")).isTrue();
    assertThat(rule.match("aXXbX")).isFalse();
  }

  @Test
  void wildcardEscapesDollarSign() {
    var rule = new Rule(RuleType.WILDCARD, "price$*", RuleAction.BLOCK);
    assertThat(rule.match("price$10")).isTrue();
    assertThat(rule.match("price10")).isFalse();
  }

  @Test
  void wildcardEmptyPattern() {
    var rule = new Rule(RuleType.WILDCARD, "", RuleAction.BLOCK);
    assertThat(rule.match("")).isTrue();
    assertThat(rule.match("x")).isFalse();
  }

  @Test
  void regexFindSemanticsPartialMatch() {
    var rule = new Rule(RuleType.REGEX, "\\d+", RuleAction.BLOCK);
    assertThat(rule.match("abc123xyz")).isTrue();
  }

  @Test
  void regexFullMatch() {
    var rule = new Rule(RuleType.REGEX, "^hello$", RuleAction.BLOCK);
    assertThat(rule.match("hello")).isTrue();
  }

  @Test
  void regexNoMatch() {
    var rule = new Rule(RuleType.REGEX, "^hello$", RuleAction.BLOCK);
    assertThat(rule.match("world")).isFalse();
  }

  @Test
  void matchWithNullKeyThrowsNullPointerException() {
    var rule = new Rule(RuleType.EXACT, "foo", RuleAction.BLOCK);
    assertThatNullPointerException().isThrownBy(() -> rule.match(null));
  }

  @Test
  void nullKeyThrowsForPrefix() {
    var rule = new Rule(RuleType.PREFIX, "foo", RuleAction.BLOCK);
    assertThatNullPointerException().isThrownBy(() -> rule.match(null));
  }

  @Test
  void nullKeyThrowsForWildcard() {
    var rule = new Rule(RuleType.WILDCARD, "*", RuleAction.BLOCK);
    assertThatNullPointerException().isThrownBy(() -> rule.match(null));
  }

  @Test
  void nullKeyThrowsForRegex() {
    var rule = new Rule(RuleType.REGEX, ".*", RuleAction.BLOCK);
    assertThatNullPointerException().isThrownBy(() -> rule.match(null));
  }

  @Test
  void prepareOnWildcardCompilesLazily() {
    var rule = new Rule(RuleType.WILDCARD, "foo*", RuleAction.BLOCK);
    assertThat(rule.getCompiledPattern()).isNull();
    rule.prepare();
    assertThat(rule.getCompiledPattern()).isNotNull();
  }

  @Test
  void repeatedPrepareIsIdempotent() {
    var rule = new Rule(RuleType.WILDCARD, "foo*", RuleAction.BLOCK);
    rule.prepare();
    assertThat(rule.match("foobar")).isTrue();
    rule.prepare();
    assertThat(rule.match("foobar")).isTrue();
    assertThat(rule.match("bar")).isFalse();
  }

  @Test
  void regexConstructorPrecompilesPattern() {
    var rule = new Rule(RuleType.REGEX, "\\w+", RuleAction.BLOCK);
    assertThat(rule.getCompiledPattern()).isNotNull();
  }

  @Test
  void wildcardConstructorDoesNotPrecompile() {
    var rule = new Rule(RuleType.WILDCARD, "foo*", RuleAction.BLOCK);
    assertThat(rule.getCompiledPattern()).isNull();
  }

  @Test
  void exactConstructorDoesNotPrecompile() {
    var rule = new Rule(RuleType.EXACT, "foo", RuleAction.BLOCK);
    assertThat(rule.getCompiledPattern()).isNull();
  }

  @Test
  void prefixConstructorDoesNotPrecompile() {
    var rule = new Rule(RuleType.PREFIX, "foo", RuleAction.BLOCK);
    assertThat(rule.getCompiledPattern()).isNull();
  }

  @Test
  void noArgConstructorDefaults() {
    var rule = new Rule();
    assertThat(rule.getId()).isNull();
    assertThat(rule.getType()).isEqualTo(RuleType.EXACT);
    assertThat(rule.getPattern()).isNull();
    assertThat(rule.getAction()).isNull();
    assertThat(rule.getCompiledPattern()).isNull();
  }

  @Test
  void regularConstructorGeneratesUuid() {
    var rule = new Rule(RuleType.EXACT, "foo", RuleAction.BLOCK);
    assertNotNull(rule.getId());
  }

  @Test
  void regularConstructorSetsCreatedAt() {
    var rule = new Rule(RuleType.EXACT, "foo", RuleAction.BLOCK);
    assertThat(rule.getCreatedAt()).isPositive();
  }

  @Test
  void prepareThreadSafety() throws Exception {
    var rule = new Rule(RuleType.WILDCARD, "test*", RuleAction.BLOCK);
    var barrier = new CyclicBarrier(10);
    var ref = new AtomicReference<Throwable>();
    var threads = new Thread[10];
    for (int i = 0; i < 10; i++) {
      threads[i] = new Thread(() -> {
        try {
          barrier.await();
          rule.prepare();
        } catch (Throwable t) {
          ref.set(t);
        }
      });
      threads[i].start();
    }
    for (var t : threads) {
      t.join();
    }
    assertThat(ref).hasValue(null);
    assertThat(rule.getCompiledPattern()).isNotNull();
  }

  @Test
  void prepareOnRegexCompilesLazilyWhenNotPrecompiled() {
    var rule = new Rule();
    rule.setType(RuleType.REGEX);
    rule.setPattern("\\d+");
    rule.prepare();
    assertThat(rule.getCompiledPattern()).isNotNull();
  }

  @Test
  void invalidRegexPattern_shouldThrowPatternSyntaxException() {
    assertThrows(PatternSyntaxException.class, () -> new Rule(RuleType.REGEX, "[invalid", RuleAction.BLOCK));
  }

  @Test
  void wildcardEscapesBracket_shouldNotThrow() {
    var rule = new Rule(RuleType.WILDCARD, "[invalid", RuleAction.BLOCK);
    assertThat(rule.match("[invalid")).isTrue();
  }

  @Test
  void emptyPatternForExact_shouldMatchOnlyEmptyString() {
    var rule = new Rule(RuleType.EXACT, "", RuleAction.BLOCK);
    assertThat(rule.match("")).isTrue();
    assertThat(rule.match("a")).isFalse();
  }

  @Test
  void emptyPatternForPrefix_shouldMatchAll() {
    var rule = new Rule(RuleType.PREFIX, "", RuleAction.BLOCK);
    assertThat(rule.match("")).isTrue();
    assertThat(rule.match("anything")).isTrue();
  }

  @Test
  void emptyPatternForWildcard_shouldMatchOnlyEmptyString() {
    var rule = new Rule(RuleType.WILDCARD, "", RuleAction.BLOCK);
    assertThat(rule.match("")).isTrue();
    assertThat(rule.match("x")).isFalse();
  }

  @Test
  void wildcardOnlyQuestionMarks_shouldMatchExactLength() {
    var rule = new Rule(RuleType.WILDCARD, "???", RuleAction.BLOCK);
    assertThat(rule.match("abc")).isTrue();
    assertThat(rule.match("ab")).isFalse();
    assertThat(rule.match("abcd")).isFalse();
  }

  @Test
  void wildcardEscapesBracketsAndParens() {
    var rule = new Rule(RuleType.WILDCARD, "test[0]*", RuleAction.BLOCK);
    assertThat(rule.match("test[0]xyz")).isTrue();
    assertThat(rule.match("test0xyz")).isFalse();
  }

  @Test
  void wildcardEscapesAllRegexSpecials() {
    var rule = new Rule(RuleType.WILDCARD, "a.+b", RuleAction.BLOCK);
    assertThat(rule.match("a.+b")).isTrue();
    assertThat(rule.match("aXb")).isFalse();
    assertThat(rule.match("aXXb")).isFalse();
  }

  @Test
  void matchEmptyKeyForRegex_shouldRespectFindSemantics() {
    var rule = new Rule(RuleType.REGEX, "", RuleAction.BLOCK);
    assertThat(rule.match("")).isTrue();
    assertThat(rule.match("x")).isTrue();
  }

  @Test
  void matchWithEmptyKeyForExact_shouldWork() {
    var rule = new Rule(RuleType.EXACT, "test", RuleAction.BLOCK);
    assertThat(rule.match("")).isFalse();
  }

  @Test
  void regexWithBackslash_shouldMatchCorrectly() {
    var rule = new Rule(RuleType.REGEX, "\\d{2,4}", RuleAction.BLOCK);
    assertThat(rule.match("123")).isTrue();
    assertThat(rule.match("12")).isTrue();
    assertThat(rule.match("1")).isFalse();
  }
}
