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
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.CacheExtensionAspect;
import io.github.hyshmily.zeta.annotation.annotationsupporter.ZetaCacheManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for Spring Cache integration via HotKey.
 *
 * <p>When {@code zeta.spring-cache.enabled=true} and {@code spring-boot-starter-cache}
 * is on the classpath, creates a {@link ZetaCacheManager} that delegates all cache
 * operations through HotKey's L1 Caffeine cache with hot-key detection.
 *
 * <p>Creates a {@link CacheManager} that delegates to the HotKey data path,
 * enabling {@code @Cacheable} / {@code @CachePut} / {@code @CacheEvict} annotations
 * to type hot-key detection, soft-expire, and cross-instance send.
 */
@Internal
@AutoConfiguration
@ConditionalOnProperty(prefix = "zeta.spring-cache", name = "enabled", havingValue = "true")
@ConditionalOnClass(name = "org.springframework.cache.support.AbstractValueAdaptingCache")
@ConditionalOnBean(Zeta.class)
@EnableConfigurationProperties(ZetaProperties.class)
public class ZetaSpringCacheAutoConfiguration {

  /**
   * Create the HotKey-backed {@link CacheManager} for Spring Cache integration.
   *
   * @param zeta     the HotKey facade
   * @param properties the HotKey configuration properties
   * @return a new {@link ZetaCacheManager} instance
   */
  @Bean
  @ConditionalOnMissingBean(CacheManager.class)
  public ZetaCacheManager hotKeyCacheManager(Zeta zeta, ZetaProperties properties) {
    return new ZetaCacheManager(zeta, properties);
  }

  /**
   * Create the companion aspect that enables {@code @Intercept}, {@code @Fallback},
   * and {@code @CacheTTL} on {@code @Cacheable} methods.
   *
   * @param zeta     the HotKey facade
   * @param properties the HotKey configuration properties
   * @return a new {@link CacheExtensionAspect} instance
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnClass(name = "org.aspectj.lang.ProceedingJoinPoint")
  public CacheExtensionAspect hotKeyCacheExtensionAspect(Zeta zeta, ZetaProperties properties) {
    return new CacheExtensionAspect(zeta, properties);
  }
}
