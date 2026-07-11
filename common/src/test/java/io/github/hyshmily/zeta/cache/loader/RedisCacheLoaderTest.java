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
package io.github.hyshmily.zeta.cache.loader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

class RedisCacheLoaderTest {

  private StringRedisTemplate redisTemplate;
  private ValueOperations<String, String> valueOps;
  private RedisCacheLoader loader;

  @BeforeEach
  void setUp() {
    redisTemplate = mock(StringRedisTemplate.class);
    valueOps = mock(ValueOperations.class);
    when(redisTemplate.opsForValue()).thenReturn(valueOps);
    loader = new RedisCacheLoader(redisTemplate);
  }

  @Test
  void load_whenKeyExists_shouldReturnValue() {
    when(valueOps.get("myKey")).thenReturn("myValue");
    assertThat(loader.load("myKey")).isEqualTo("myValue");
  }

  @Test
  void load_whenKeyAbsent_shouldReturnNull() {
    when(valueOps.get("missingKey")).thenReturn(null);
    assertThat(loader.load("missingKey")).isNull();
  }

  @Test
  void load_shouldDelegateToOpsForValue() {
    loader.load("someKey");
    verify(redisTemplate).opsForValue();
    verify(valueOps).get("someKey");
  }
}
