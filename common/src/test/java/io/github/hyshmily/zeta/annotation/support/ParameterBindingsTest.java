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
package io.github.hyshmily.zeta.annotation.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Method;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;

@DisplayName("ParameterBindings")
class ParameterBindingsTest {

  private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();

  @Test
  @DisplayName("a parameter named for its own positional slot is legal")
  void ownPositionalNameIsLegal() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("ownPosition", String.class);

    ParameterBindings bindings = ParameterBindings.of(method, names(method));

    assertThat(bindings.slotOf("p0")).isZero();
    assertThat(bindings.resolve("p0", new Object[] { "v" })).isEqualTo("v");
  }

  @Test
  @DisplayName("a parameter that would overwrite another argument's positional slot fails resolution")
  void conflictingPositionalNameIsRejected() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("conflictingPosition", String.class);

    assertThatThrownBy(() -> ParameterBindings.of(method, names(method)))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("reserved positional parameter 'p1'");
  }

  @Test
  @DisplayName("reserved context names never enter the slot table")
  void contextNamesAreNotSlots() throws NoSuchMethodException {
    Method method = ParameterBindingsTest.class.getDeclaredMethod("reservedNames", String.class, String.class);
    Object[] args = { "root-value", "result-value" };

    ParameterBindings bindings = ParameterBindings.of(method, names(method));

    assertThat(bindings.slotOf("root")).isEqualTo(-1);
    assertThat(bindings.slotOf("result")).isEqualTo(-1);
    assertThat(bindings.resolve("root", args)).isNull();
    assertThat(bindings.resolve("result", args)).isNull();
  }

  @SuppressWarnings("unused")
  private String ownPosition(String p0) {
    return p0;
  }

  @SuppressWarnings("unused")
  private String conflictingPosition(String p1) {
    return p1;
  }

  @SuppressWarnings("unused")
  private String reservedNames(String root, String result) {
    return root + result;
  }

  private String@Nullable [] names(Method method) {
    return parameterNames.getParameterNames(method);
  }
}
