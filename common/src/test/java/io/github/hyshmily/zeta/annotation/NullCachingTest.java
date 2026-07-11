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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("NullCaching annotation tests")
class NullCachingTest {

  @Test
  @DisplayName("default value is true")
  void defaultValueIsTrue() throws Exception {
    NullCaching nc = FakeClass.class.getMethod("method").getAnnotation(NullCaching.class);
    assertThat(nc.value()).isTrue();
  }

  @Test
  @DisplayName("target is METHOD")
  void targetIsMethod() {
    Target target = NullCaching.class.getAnnotation(Target.class);
    assertThat(target).isNotNull();
    assertThat(target.value()).containsExactly(java.lang.annotation.ElementType.METHOD);
  }

  @Test
  @DisplayName("retention is RUNTIME")
  void retentionIsRuntime() {
    Retention retention = NullCaching.class.getAnnotation(Retention.class);
    assertThat(retention).isNotNull();
    assertThat(retention.value()).isEqualTo(RetentionPolicy.RUNTIME);
  }

  private static class FakeClass {

    @NullCaching
    public void method() {}
  }
}
