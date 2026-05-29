/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.hyshmily.hotkey.hotkeycache;

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
  @Max(200_000)
  private int width = 50_000;

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

  @Min(0)
  private int localCacheAccessTtlMinutes = 0;

  @Min(1)
  private int inflightMaxSize = 50_000;

  @Min(1)
  private int inflightTtlSeconds = 5;

  @Min(1)
  private int inflightTimeoutSeconds = 3;

  @Min(1)
  private int executorCorePoolSize = 8;

  @Min(1)
  private int executorMaxPoolSize = 32;

  @Min(1)
  private int executorQueueCapacity = 2000;

  @Deprecated
  private int decayPeriod = 20;

  @Min(0)
  private int softTtlMs = 0;

  @Min(1)
  private int softExpireMaxSize = 50_000;

  @Min(1)
  private int softExpireTtlMinutes = 60;

  @Min(1)
  private int refreshConcurrency = 100;

  @Min(0)
  private int versionKeyTtlMinutes = 60;

}
