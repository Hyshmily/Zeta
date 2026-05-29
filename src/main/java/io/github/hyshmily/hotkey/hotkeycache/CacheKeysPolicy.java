package io.github.hyshmily.hotkey.hotkeycache;

public final class CacheKeysPolicy {

  public static boolean invalidCacheKey(String cacheKey) {
    return cacheKey == null || cacheKey.isBlank();
  }

  public static boolean invalidTypeKey(String cacheKey) {
    return cacheKey == null || cacheKey.isBlank();
  }

  private CacheKeysPolicy() {}
}
