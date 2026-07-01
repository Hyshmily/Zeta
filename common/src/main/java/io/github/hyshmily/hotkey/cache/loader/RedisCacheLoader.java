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
package io.github.hyshmily.hotkey.cache.loader;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Default {@link CacheLoader} that reads values from Redis via
 * {@link StringRedisTemplate#opsForValue()}.
 *
 * <p>Returns the raw string value from Redis, or {@code null} if the
 * key does not exist. The caller is responsible for deserialization.
 */
@RequiredArgsConstructor
public class RedisCacheLoader implements CacheLoader {

  private final StringRedisTemplate redisTemplate;

  @Override
  public Object load(String cacheKey) {
    return redisTemplate.opsForValue().get(cacheKey);
  }
}
