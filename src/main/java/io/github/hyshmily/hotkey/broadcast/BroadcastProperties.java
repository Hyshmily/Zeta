package io.github.hyshmily.hotkey.broadcast;

import java.util.UUID;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "hotkey.broadcast")
public class BroadcastProperties {

  private boolean enabled = false;
  private String exchangeName = "hotkey.broadcast.exchange";
  private String queuePrefix = "hotkey.broadcast";

  @Setter(AccessLevel.NONE)
  private String instanceId;

  private int dedupWindowSeconds = 10;
  private int dedupMaxSize = 10_000;
  private int warmupJitterMs = 100;
  private int concurrentConsumers = 3;
  private int schedulerPoolSize = 4;
  public static final String TYPE_INVALIDATE = "INVALIDATE";
  public static final String TYPE_HOT = "HOT";
  public static final Long VERSIONED_HOT_KEY_DEFAULT_VERSION = 0L;

  public String getInstanceId() {
    if (instanceId == null) {
      String port = System.getProperty("server.port", "instance");
      String hostname = System.getenv("HOSTNAME");
      String uniquePart = (hostname != null && !hostname.isBlank())
                            ? hostname
                            : UUID.randomUUID().toString();
      instanceId = port + "-" + uniquePart;
    }
    return instanceId;
  }

  public String getQueueName() {
    return queuePrefix + ":" + getInstanceId();
  }
}
