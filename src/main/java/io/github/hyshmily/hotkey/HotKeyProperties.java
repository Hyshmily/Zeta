package io.github.hyshmily.hotkey;

import jakarta.validation.constraints.Min;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "hotkey")
public class HotKeyProperties {

  @Min(1)
  private int topK = 100;
  private int width = 100_000;
  private int depth = 5;
  private double decay = 0.92;
  private int minCount = 10;
  private int localCacheMaxSize = 1000;
  private int localCacheTtlMinutes = 5;
  private int decayPeriod = 20;
}
