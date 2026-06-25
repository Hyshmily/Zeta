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

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.annotation.HotKeyCacheExtensionAspect;
import io.github.hyshmily.hotkey.cache.annotationsupporter.HotKeyCacheManager;
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
 * <p>When {@code hotkey.spring-cache.enabled=true} and {@code spring-boot-starter-cache}
 * is on the classpath, creates a {@link HotKeyCacheManager} that delegates all cache
 * operations through HotKey's L1 Caffeine cache with hot-key detection.
 *
 * <p>Creates a {@link CacheManager} that delegates to the HotKey data path,
 * enabling {@code @Cacheable} / {@code @CachePut} / {@code @CacheEvict} annotations
 * to trigger hot-key detection, soft-expire, and cross-instance broadcast.
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "hotkey.spring-cache", name = "enabled", havingValue = "true")
@ConditionalOnClass(name = "org.springframework.cache.support.AbstractValueAdaptingCache")
@ConditionalOnBean(HotKey.class)
@EnableConfigurationProperties(HotKeyProperties.class)
public class HotKeySpringCacheAutoConfiguration {

  /**
   * Create the HotKey-backed {@link CacheManager} for Spring Cache integration.
   *
   * @param hotKey     the HotKey facade
   * @param properties the HotKey configuration properties
   * @return a new {@link HotKeyCacheManager} instance
   */
  @Bean
  @ConditionalOnMissingBean(CacheManager.class)
  public HotKeyCacheManager hotKeyCacheManager(HotKey hotKey, HotKeyProperties properties) {
    return new HotKeyCacheManager(hotKey, properties);
  }

  /**
   * Create the companion aspect that enables {@code @Intercept}, {@code @Fallback},
   * and {@code @HotKeyCacheTTL} on {@code @Cacheable} methods.
   *
   * @param hotKey     the HotKey facade
   * @param properties the HotKey configuration properties
   * @return a new {@link HotKeyCacheExtensionAspect} instance
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnClass(name = "org.aspectj.lang.ProceedingJoinPoint")
  public HotKeyCacheExtensionAspect hotKeyCacheExtensionAspect(HotKey hotKey, HotKeyProperties properties) {
    return new HotKeyCacheExtensionAspect(hotKey, properties);
  }
}
