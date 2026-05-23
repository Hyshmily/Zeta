package io.github.hyshmily.hotkey.broadcast;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "hotkey.broadcast")
public class BroadcastProperties {

  private boolean enabled = false;
  private String exchangeName = "hotkey.broadcast.exchange";
  private String queuePrefix = "hotkey.broadcast";
  private String instanceId = "${server.port:instance}";
  private int dedupWindowSeconds = 10;

  public String getQueueName() {
    return queuePrefix + ":" + instanceId;
  }
}
