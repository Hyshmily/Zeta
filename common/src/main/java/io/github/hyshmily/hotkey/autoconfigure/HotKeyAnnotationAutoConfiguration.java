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
import io.github.hyshmily.hotkey.annotation.HotKeyAspect;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration for {@link io.github.hyshmily.hotkey.annotation.HotKey @HotKey} annotation support.
 * <p>
 * Activates when:
 * <ul>
 *   <li>{@code hotkey.annotation.enabled=true} (required, prefix {@code hotkey.annotation})</li>
 *   <li>{@link org.aspectj.lang.annotation.Aspect Aspect} is on the classpath (AOP enabled)</li>
 *   <li>A {@link HotKey} facade bean is present</li>
 * </ul>
 * <p>
 * Runs after {@link HotKeyFacadeAutoConfiguration} to ensure the HotKey facade
 * is already registered before the aspect is created.
 *
 * @see io.github.hyshmily.hotkey.annotation.HotKey
 * @see HotKeyAspect
 */
@AutoConfiguration(after = HotKeyFacadeAutoConfiguration.class)
@ConditionalOnClass(Aspect.class)
@ConditionalOnBean(HotKey.class)
@ConditionalOnProperty(prefix = "hotkey.annotation", name = "enabled", havingValue = "true")
public class HotKeyAnnotationAutoConfiguration {

  /**
   * Create the {@link HotKeyAspect} that intercepts {@code @HotKey} methods.
   *
   * @param hotKey the HotKey facade (injected automatically)
   * @return the aspect instance
   */
  @Bean
  @ConditionalOnMissingBean
  public HotKeyAspect hotKeyAspect(HotKey hotKey) {
    return new HotKeyAspect(hotKey);
  }
}
