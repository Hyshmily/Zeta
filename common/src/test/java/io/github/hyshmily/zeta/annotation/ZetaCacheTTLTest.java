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
package io.github.hyshmily.zeta.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("CacheTTL annotation tests")
class ZetaCacheTTLTest {

  @Test
  @DisplayName("default hardTtlMs is 0")
  void defaultHardTtlMsIsZero() {
    CacheTTL annotation = FakeClass.class.getAnnotation(CacheTTL.class);
    assertThat(annotation.hardTtlMs()).isZero();
  }

  @Test
  @DisplayName("default softTtlMs is 0")
  void defaultSoftTtlMsIsZero() {
    CacheTTL annotation = FakeClass.class.getAnnotation(CacheTTL.class);
    assertThat(annotation.softTtlMs()).isZero();
  }

  @Test
  @DisplayName("target is METHOD and TYPE")
  void targetIsMethod() {
    Target target = CacheTTL.class.getAnnotation(Target.class);
    assertThat(target).isNotNull();
    assertThat(target.value()).contains(ElementType.METHOD, ElementType.TYPE);
  }

  @Test
  @DisplayName("retention is RUNTIME")
  void retentionIsRuntime() {
    Retention retention = CacheTTL.class.getAnnotation(Retention.class);
    assertThat(retention).isNotNull();
    assertThat(retention.value()).isEqualTo(RetentionPolicy.RUNTIME);
  }

  @CacheTTL
  private static class FakeClass {}
}
