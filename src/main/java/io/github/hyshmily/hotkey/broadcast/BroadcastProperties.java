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
  @Value("${server.port:instance}")
  private String instanceId;
  private int dedupWindowSeconds = 10;
  private int dedupMaxSize = 10_000;

  public String getQueueName() {
    return queuePrefix + ":" + instanceId;
  }
}
