package io.github.hyshmily.hotkey.broadcast;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "hotkey.broadcast")
public class BroadcastProperties {

  private boolean enabled = false;
  private String exchangeName = "hotkey.broadcast.exchange";
  private String queuePrefix = "hotkey.broadcast";
  @Value("${server.port:instance}-${HOSTNAME:${random.uuid}}")
  private String instanceId;

  private int dedupWindowSeconds = 10;
  private int dedupMaxSize = 10_000;
  private int warmupJitterMs = 1_000;
  public static final String TYPE_INVALIDATE = "INVALIDATE";
  public static final String TYPE_HOT = "HOT";
  public static final Long VERSIONED_HOT_KEY_DEFAULT_VERSION = 0L;

  public String getQueueName() {
    return queuePrefix + ":" + instanceId;
  }
}
