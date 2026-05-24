package io.github.hyshmily.hotkey;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "hotkey")
public class HotKeyProperties {

  @Min(1)
  private int topK = 100;

  @Min(1)
  @Max(1_000_000)
  private int width = 100_000;

  @Min(1)
  @Max(10)
  private int depth = 5;

  @Positive
  private double decay = 0.92;

  @Min(1)
  private int minCount = 10;

  @Min(1)
  private int localCacheMaxSize = 1000;

  @Min(1)
  private int localCacheTtlMinutes = 5;

  @Deprecated
  private int decayPeriod = 20;
}
