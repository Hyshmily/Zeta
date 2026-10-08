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
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.sync.distributedlock.LockProvider;
import io.github.hyshmily.zeta.sync.distributedlock.impl.RedisLockProvider;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
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
@AutoConfiguration(
  after = ZetaFacadeAutoConfiguration.class,
  afterName = "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration"
)
@ConditionalOnClass(name = "org.springframework.data.redis.core.StringRedisTemplate")
public class ZetaLockAutoConfiguration {

  /**
   * Fixed pool size for the lock-renewal scheduler. Deliberately not
   * configurable: renewal cadence is derived from each lock's TTL, so thread
   * count does not change throughput — a small pool only bounds how much
   * renewal can be in flight against Redis at once.
   */
  private static final int LOCK_RENEW_SCHEDULER_POOL_SIZE = 2;

  /**
   * Create the Redis-backed lock provider with retry counts from properties.
   *
   * @param redisTemplate the Redis template for SET / GET / EVAL operations
   * @param properties    the HotKey configuration properties for retry settings
   * @param scheduler     the dedicated lock-renewal scheduler for watchdog tasks
   * @return a new {@link RedisLockProvider} instance
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(StringRedisTemplate.class)
  public LockProvider redisLockProvider(
    StringRedisTemplate redisTemplate,
    ZetaProperties properties,
    @Qualifier("hotKeyLockRenewScheduler") ScheduledExecutorService scheduler
  ) {
    return new RedisLockProvider(
      redisTemplate,
      properties.getTryLockLockCount(),
      properties.getTryLockInquiryCount(),
      properties.getTryLockUnlockCount(),
      scheduler
    );
  }

  /**
   * Dedicated scheduler for lock watchdog renewal.
   *
   * <p>A watchdog tick is one synchronous Redis round-trip (a Lua {@code GET +
   * PEXPIRE}), so it blocks its thread for a full RTT — or for the whole Redis
   * command timeout during a failover. Renewal is scheduled per held lock, so
   * {@code heldLocks} concurrent acquisitions occupy {@code heldLocks} threads.
   *
   * <p>Isolated from {@code hotKeyScheduler} for the same reason
   * {@code hotKeySyncScheduler} and {@code hotKeyWorkerSchedScheduler} are:
   * a blocking control-plane call must never starve the latency-sensitive
   * periodic tasks sharing the general pool — the reporter's 50 ms flush tide,
   * HeavyKeeper's decay window rotation, and the deferred broadcast flush.
   * Eight blocked renewal threads on that pool would delay report delivery in
   * whole tide intervals and slip the sketch window.
   *
   * <p>Pool size is fixed at 2 rather than configurable: renewal is a periodic
   * task with a cadence derived from the lock TTL, not a throughput workload,
   * so more threads buy nothing and a small pool is a natural back-pressure
   * bound — a backlogged renewal simply runs late, and the lease TTL (not this
   * pool) is the real constraint.
   *
   * @return a scheduled executor dedicated to lock renewal
   */
  @Bean(name = "hotKeyLockRenewScheduler", destroyMethod = "shutdownNow")
  @ConditionalOnMissingBean(name = "hotKeyLockRenewScheduler")
  public ScheduledExecutorService hotKeyLockRenewScheduler() {
    return new SafeScheduledExecutorService(
      LOCK_RENEW_SCHEDULER_POOL_SIZE,
      new ZetaThreadFactory(ZetaConstants.Thread.PREFIX_SCHEDULER)
    );
  }
}
