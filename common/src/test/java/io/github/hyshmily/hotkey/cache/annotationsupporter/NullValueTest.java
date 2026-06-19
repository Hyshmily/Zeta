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
package io.github.hyshmily.hotkey.cache.annotationsupporter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("NullValue sentinel tests")
class NullValueTest {

  @Test
  @DisplayName("INSTANCE is a singleton")
  void instance_isSingleton() {
    assertThat(NullValue.INSTANCE).isSameAs(NullValue.INSTANCE);
  }

  @Test
  @DisplayName("INSTANCE is the same reference across multiple access")
  void instance_isSameReference() {
    NullValue a = NullValue.INSTANCE;
    NullValue b = NullValue.INSTANCE;
    assertThat(a).isSameAs(b);
  }

  @Test
  @DisplayName("constructor is private")
  void constructor_isPrivate() {
    Constructor<?>[] constructors = NullValue.class.getDeclaredConstructors();
    assertThat(constructors).hasSize(1);
    assertThat(Modifier.isPrivate(constructors[0].getModifiers())).isTrue();
  }

  @Test
  @DisplayName("cannot instantiate via reflection")
  void cannotInstantiateViaReflection() {
    assertThat(NullValue.class.getDeclaredConstructors()[0].canAccess(null)).isFalse();
  }
}
