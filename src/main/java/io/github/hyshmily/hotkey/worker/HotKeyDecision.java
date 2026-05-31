package io.github.hyshmily.hotkey.worker;

/**
 * A decision emitted by the Worker's sliding-window / state-machine pipeline.
 *
 * <p>Indicates whether a key should be treated as {@link DecisionType#HOT},
 * {@link DecisionType#COOL}, or that no action is required
 * ({@link DecisionType#NONE}).
 *
 * @param type     the decision type
 * @param cacheKey the affected key
 */
public record HotKeyDecision(DecisionType type, String cacheKey) {
  /** Possible decision outcomes. */
  public enum DecisionType {
    HOT,
    COOL,
    NONE,
  }

  /** Create a HOT decision for the given key. */
  public static HotKeyDecision hot(String cacheKey) {
    return new HotKeyDecision(DecisionType.HOT, cacheKey);
  }

  /** Create a COOL decision for the given key. */
  public static HotKeyDecision cool(String cacheKey) {
    return new HotKeyDecision(DecisionType.COOL, cacheKey);
  }

  /** Create a no-op decision for the given key. */
  public static HotKeyDecision none(String cacheKey) {
    return new HotKeyDecision(DecisionType.NONE, cacheKey);
  }
}
