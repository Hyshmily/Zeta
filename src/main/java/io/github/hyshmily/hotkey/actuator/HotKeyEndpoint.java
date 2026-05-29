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
package io.github.hyshmily.hotkey.actuator;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import io.github.hyshmily.hotkey.hotkeycache.SingleFlight;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

@Endpoint(id = "hotkey")
public class HotKeyEndpoint {

  private final TopK hotKeyDetector;
  private final Cache<String, Object> caffeineCache;
  private final SingleFlight singleFlight;
  private final int l1MaxSize;

  public HotKeyEndpoint(
      TopK hotKeyDetector,
      Cache<String, Object> caffeineCache,
      SingleFlight singleFlight,
      HotKeyProperties properties) {
    this.hotKeyDetector = hotKeyDetector;
    this.caffeineCache = caffeineCache;
    this.singleFlight = singleFlight;
    this.l1MaxSize = properties.getLocalCacheMaxSize();
  }

  @ReadOperation
  public Map<String, Object> hotKeyInfo() {
    Map<String, Object> info = new LinkedHashMap<>();

    List<Item> topKList = hotKeyDetector.list();
    List<Map<String, Object>> topKKeys = new ArrayList<>(topKList.size());
    for (Item item : topKList) {
      topKKeys.add(Map.of("key", item.key(), "count", item.count()));
    }
    info.put("topK", topKKeys);
    info.put("topKCount", topKList.size());

    info.put("totalRequests", hotKeyDetector.total());

    info.put("l1CacheSize", caffeineCache.estimatedSize());
    info.put("l1MaxSize", l1MaxSize);

    info.put("inflightSize", singleFlight.estimatedInflightSize());

    info.put("recentlyExpelled",
      hotKeyDetector.expelled().stream()
        .map(Item::key)
        .limit(10)
        .toList());

    return info;
  }
}
