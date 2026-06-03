package io.github.hyshmily.hotkey.rule;

import java.util.regex.Pattern;
import lombok.Data;

@Data
public class Rule {

  public enum RuleType {
    EXACT,
    PREFIX,
    WILDCARD,
    REGEX,
  }

  public enum RuleAction {
    BLOCK,
    ALLOW_NO_REPORT,
    ALLOW,
  }

  private RuleType type;
  private String pattern;
  private RuleAction action;
  private transient volatile Pattern compiledPattern;

  public Rule() {}

  public Rule(RuleType type, String pattern, RuleAction action) {
    this.type = type;
    this.pattern = pattern;
    this.action = action;
    if (type == RuleType.REGEX) {
      this.compiledPattern = Pattern.compile(pattern);
    }
  }

  public boolean match(String key) {
    return switch (type) {
      case EXACT -> key.equals(pattern);
      case PREFIX -> key.startsWith(pattern);
      case WILDCARD -> {
        if (compiledPattern == null) {
          prepare();
        }
        yield compiledPattern.matcher(key).matches();
      }
      case REGEX -> {
        if (compiledPattern == null) {
          prepare();
        }
        yield compiledPattern.matcher(key).find();
      }
    };
  }

  public void prepare() {
    if (type == RuleType.REGEX) {
      compiledPattern = Pattern.compile(pattern);
    } else if (type == RuleType.WILDCARD) {
      String regex = pattern.replaceAll("([.+^$\\[\\]\\\\(){}|])", "\\\\$1").replace("*", ".*").replace("?", ".");
      compiledPattern = Pattern.compile(regex);
    }
  }
}
