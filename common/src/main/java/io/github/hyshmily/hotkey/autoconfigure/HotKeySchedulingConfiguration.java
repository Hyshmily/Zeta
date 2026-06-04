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
package io.github.hyshmily.hotkey.autoconfigure;

import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Scheduled tasks for periodic TopK maintenance.
 *
 * <p>Handles decaying (fading) the HeavyKeeper counters and draining
 * expelled hot-key entries.  Uses {@link List List&#60;TopK&#62;} injection
 * to support both the app-side and Worker-side TopK instances when they
 * coexist in the same JVM.
 *
 * <p>Enabled by default; controlled via {@code hotkey.scheduling.enabled}.
 */
@Slf4j
@AutoConfiguration(after = HotKeyAutoConfiguration.class)
@ConditionalOnProperty(name = "hotkey.scheduling.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnBean(TopK.class)
@EnableScheduling
public class HotKeySchedulingConfiguration {

  private final List<TopK> topKInstances;

  /**
   * Creates the scheduling configuration with all available TopK instances.
   *
   * @param topKInstances list of TopK beans (app-side and/or Worker-side)
   */
  public HotKeySchedulingConfiguration(List<TopK> topKInstances) {
    this.topKInstances = topKInstances;
  }

  /**
   * Periodically decay the HeavyKeeper counters for all registered TopK instances.
   * Controlled by {@code hotkey.decay-period} (default 20 seconds).
   */
  @Scheduled(fixedDelayString = "${hotkey.decay-period:20}", timeUnit = java.util.concurrent.TimeUnit.SECONDS)
  public void cleanHotKeys() {
    topKInstances.forEach(TopK::fading);
    log.debug("HeavyKeeper count has decayed for {} TopK instance(s)", topKInstances.size());
  }

  /**
   * Periodically drain expelled hot keys from all registered TopK instances.
   * Logs a truncated summary of up to 20 sample keys.  Runs every 60 seconds.
   */
  @Scheduled(fixedDelay = 10_000)
  public void drainExpelled() {
    int totalDrained = 0;
    List<String> sampleKeys = new ArrayList<>();
    for (TopK topK : topKInstances) {
      List<Item> items = new ArrayList<>();
      topK.expelled().drainTo(items, 100_000);
      totalDrained += items.size();
      items.stream().map(Item::key).limit(20).forEach(sampleKeys::add);
    }
    if (totalDrained > 0) {
      String keys = sampleKeys.stream().limit(20).collect(Collectors.joining(","));
      boolean truncated = totalDrained > 20;
      log.info(
        "Drained {} expelled hot keys from {} TopK instance(s): {}{}",
        totalDrained,
        topKInstances.size(),
        keys,
        truncated ? "..." : ""
      );
    }
  }
}
