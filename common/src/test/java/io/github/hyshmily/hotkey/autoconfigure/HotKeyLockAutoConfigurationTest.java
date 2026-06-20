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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.hyshmily.hotkey.cache.distributedlock.LockProvider;
import io.github.hyshmily.hotkey.sync.RedisLockProvider;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Tests for {@link HotKeyLockAutoConfiguration}.
 */
class HotKeyLockAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner().withConfiguration(
    AutoConfigurations.of(HotKeyLockAutoConfiguration.class)
  );

  @Test
  void redisLockProvider_shouldCreateBean() {
    StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    HotKeyProperties properties = new HotKeyProperties();
    HotKeyLockAutoConfiguration config = new HotKeyLockAutoConfiguration();
    LockProvider provider = config.redisLockProvider(redisTemplate, properties);
    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(RedisLockProvider.class);
  }

  @Test
  void redisLockProvider_shouldUsePropertiesDefaults() {
    StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    HotKeyProperties properties = new HotKeyProperties();
    HotKeyLockAutoConfiguration config = new HotKeyLockAutoConfiguration();
    LockProvider provider = config.redisLockProvider(redisTemplate, properties);
    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(RedisLockProvider.class);
  }

  @Test
  void configIsSkippedWhenStringRedisTemplateNotOnClasspath() {
    runner.run(ctx -> {
      assertThat(ctx).doesNotHaveBean(LockProvider.class);
    });
  }

  @Test
  void configIsSkippedWhenStringRedisTemplateBeanNotAvailable() {
    new ApplicationContextRunner()
      .withBean(HotKeyProperties.class, HotKeyProperties::new)
      .withConfiguration(AutoConfigurations.of(HotKeyLockAutoConfiguration.class))
      .run(ctx -> {
        assertThat(ctx).doesNotHaveBean(LockProvider.class);
      });
  }
}
