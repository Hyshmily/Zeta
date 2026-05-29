package io.github.hyshmily.hotkey.broadcast;

import static io.github.hyshmily.hotkey.hotkeycache.CacheKeysPolicy.invalidCacheKey;

import java.nio.charset.StandardCharsets;
import org.springframework.amqp.core.Message;

public record BroadcastMessage(String cacheKey, String type, long version, boolean isVersionDegraded) {
  public static BroadcastMessage from(Message msg) {
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }
    String cacheKey = new String(body, StandardCharsets.UTF_8);
    if (invalidCacheKey(cacheKey)) {
      return null;
    }
    String type = msg.getMessageProperties().getHeader("type");
    long version = msg.getMessageProperties().getHeader("version") instanceof Number n ? n.longValue() : 0L;
    boolean isVersionDegraded =
      msg.getMessageProperties().getHeader("isVersionDegraded") instanceof Boolean b ? b : true;
    return new BroadcastMessage(cacheKey, type, version, isVersionDegraded);
  }
}
