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
package io.github.hyshmily.hotkey.worker;

import jakarta.validation.constraints.Min;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "hotkey.worker")
public class WorkerProperties {

  @Data
  public static class Routing {

    private String appName = "default";
    private int shardCount = 1;
    private int shardIndex = 0;
  }

  @Data
  public static class Messaging {

    private String reportExchange = "hotkey.report.exchange";
    private String broadcastExchange = "hotkey.broadcast.exchange";
  }

  @Data
  public static class SlidingWindow {

    private long durationMs = 1000;

    @Min(1)
    private int slices = 10;
  }

  @Data
  public static class Threshold {

    private long hotThreshold = 1000;
    private double hotThresholdRatio = 0.01;
  }

  @Data
  public static class StateMachine {

    private long confirmDurationMs = 2000;
    private long coolDurationMs = 15000;
    private long preCoolGraceMs = 5000;
  }

  @Data
  public static class GlobalQpsDynamicThreshold {

    private double qpsChangeTolerance = 0.5;
    private double learningPeriodMs = 30_000;
    private double hotThresholdRatio = 0.01;
    private long recalculateIntervalMs = 60_000;
  }

  @Data
  public static class TopKValidation {

    private long validateIntervalMs = 60000;
    private int preWarmCount = 5;
    private int preWarmMinAppearances = 2;
  }

  @Data
  public static class HeavyKeeper {

    private int topK = 100;
    private int width = 20000;
    private int depth = 10;
    private double decay = 0.9;
    private int minCount = 10;
  }

  private boolean enabled = false;
  private Routing routing = new Routing();
  private Messaging messaging = new Messaging();
  private SlidingWindow slidingWindow = new SlidingWindow();
  private Threshold threshold = new Threshold();
  private StateMachine stateMachine = new StateMachine();
  private GlobalQpsDynamicThreshold globalQpsDynamicThreshold = new GlobalQpsDynamicThreshold();
  private TopKValidation topKValidation = new TopKValidation();
  private HeavyKeeper heavyKeeper = new HeavyKeeper();

  public int getConfirmWindows() {
    double sliceMs = (double) slidingWindow.getDurationMs() / slidingWindow.getSlices();
    return (int) Math.ceil(stateMachine.getConfirmDurationMs() / sliceMs);
  }

  public int getCoolWindows() {
    double sliceMs = (double) slidingWindow.getDurationMs() / slidingWindow.getSlices();
    return (int) Math.ceil(stateMachine.getCoolDurationMs() / sliceMs);
  }

  public int getPreCoolGraceWindows() {
    double sliceMs = (double) slidingWindow.getDurationMs() / slidingWindow.getSlices();
    return (int) Math.ceil(stateMachine.getPreCoolGraceMs() / sliceMs);
  }
}
