package io.github.hyshmily.hotkey.exception;

/**
 * Thrown when a cache get operation is blocked by a blacklist rule.
 * <p>
 * Unlike returning {@link java.util.Optional#empty()}, throwing prevents
 * callers from silently bypassing the block via
 * {@link java.util.Optional#orElseGet}.  The calling code must either
 * catch this exception or let it propagate.
 */
public class HotKeyBlockedException extends RuntimeException {

  private final String cacheKey;

  public HotKeyBlockedException(String cacheKey) {
    super("Cache key blocked by rule: " + cacheKey);
    this.cacheKey = cacheKey;
  }

  /**
   * Return the key that was blocked.
   *
   * @return the blocked cache key
   */
  public String getCacheKey() {
    return cacheKey;
  }
}
