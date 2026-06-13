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
package io.github.hyshmily.hotkey.worker.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import io.github.hyshmily.hotkey.worker.config.WorkerProperties.Persistence;
import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Persists the Worker's TopK hot keys to Redis so that after a restart the
 * HeavyKeeper sketch can be warmed with historical data, drastically
 * reducing the time needed to re-accumulate the hot-key set.
 */
public class TopKPersistService {

  private static final HotKeyLogger log = new DefaultLogger(TopKPersistService.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final TopK topK;
  private final StringRedisTemplate redisTemplate;
  private final String redisKey;
  private final Persistence config;

  /**
   * Creates a TopK persistence service.
   *
   * @param topK          the Worker-scoped HeavyKeeper sketch to persist
   * @param redisTemplate the Redis template used for reading/writing the snapshot
   * @param appName       the application name used to build the Redis key
   * @param nodeId        the unique Worker node identifier used to build the Redis key
   * @param config        the persistence configuration (key prefix, TTL, etc.)
   */
  public TopKPersistService(
    TopK topK,
    StringRedisTemplate redisTemplate,
    String appName,
    String nodeId,
    Persistence config
  ) {
    this.topK = topK;
    this.redisTemplate = redisTemplate;
    this.redisKey = config.getRedisKeyPrefix() + appName + ":" + nodeId;
    this.config = config;
  }

  /**
   * Restores the last persisted TopK snapshot from Redis into the HeavyKeeper sketch.
   * Called once on Worker startup via {@link PostConstruct}.
   */
  @PostConstruct
  public void restoreFromRedis() {
    try {
      String json = redisTemplate.opsForValue().get(redisKey);
      if (json == null || json.isEmpty()) {
        log.info("No persisted TopK data found at key: {}", redisKey);
        return;
      }

      List<Item> items = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
      if (items == null || items.isEmpty()) {
        return;
      }

      topK.add(items.stream().collect(Collectors.toMap(Item::key, Item::count)));
      log.info("Restored {} hot keys from Redis for Worker [{}]", items.size(), redisKey);
    } catch (Exception e) {
      log.error("Failed to restore TopK from Redis at key: {}", redisKey, e);
    }
  }

  /**
   * Snapshots the current TopK list to Redis. Called periodically by a scheduler.
   */
  public void persistToRedis() {
    try {
      List<Item> topKeys = topK.listTopN(config.getTopKCount());
      if (topKeys.isEmpty()) {
        return;
      }

      String json = OBJECT_MAPPER.writeValueAsString(topKeys);
      redisTemplate.opsForValue().set(redisKey, json, config.getTtlDays(), TimeUnit.DAYS);
      log.debug("Persisted {} hot keys to Redis at key: {}", topKeys.size(), redisKey);
    } catch (Exception e) {
      log.error("Failed to persist TopK to Redis at key: {}", redisKey, e);
    }
  }
}
