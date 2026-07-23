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
package io.github.hyshmily.zeta.autoconfigure;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.sync.distributedlock.LockProvider;
import io.github.hyshmily.zeta.sync.distributedlock.impl.RedisLockProvider;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Auto-configuration for the Redis-backed {@link LockProvider}.
 *
 * <p>Activates when a {@link StringRedisTemplate} bean is present in the
 * application context (i.e. when {@code spring-boot-starter-data-redis}
 * is on the classpath and Redis is configured).  The bean is
 * {@link ConditionalOnMissingBean}, so consumers can override it with a
 * custom {@link LockProvider}.
 *
 * <p>Graceful degradation: when no {@link StringRedisTemplate} is
 * available, no {@link LockProvider} bean is created and HotKey's
 * {@code tryLock()} / {@code tryLockAndRun()} simply return
 * {@code null} / {@code false}.
 */
@Internal
@AutoConfiguration(after = ZetaFacadeAutoConfiguration.class)
@ConditionalOnClass(name = "org.springframework.data.redis.core.StringRedisTemplate")
public class ZetaLockAutoConfiguration {

  /**
   * Create the Redis-backed lock provider with retry counts from properties.
   *
   * @param redisTemplate the Redis template for SET / GET / EVAL operations
   * @param properties    the HotKey configuration properties for retry settings
   * @param scheduler     the shared scheduler for watchdog renewal
   * @return a new {@link RedisLockProvider} instance
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(StringRedisTemplate.class)
  public LockProvider redisLockProvider(
    StringRedisTemplate redisTemplate,
    ZetaProperties properties,
    @Qualifier("hotKeyScheduler") ScheduledExecutorService scheduler
  ) {
    return new RedisLockProvider(
      redisTemplate,
      properties.getTryLockLockCount(),
      properties.getTryLockInquiryCount(),
      properties.getTryLockUnlockCount(),
      scheduler
    );
  }
}
