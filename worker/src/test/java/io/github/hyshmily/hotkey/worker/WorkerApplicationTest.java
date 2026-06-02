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
package io.github.hyshmily.hotkey.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;

/**
 * Tests for {@link WorkerApplication}.
 */
class WorkerApplicationTest {

  @Test
  void shouldHaveMainMethod() throws Exception {
    Method main = WorkerApplication.class.getMethod("main", String[].class);
    assertThat(main).isNotNull();
  }

  @Test
  void shouldBeAnnotatedWithSpringBootApplication() {
    org.springframework.boot.autoconfigure.SpringBootApplication annotation =
      WorkerApplication.class.getAnnotation(org.springframework.boot.autoconfigure.SpringBootApplication.class);
    assertThat(annotation).isNotNull();
  }

  @Test
  void classShouldLoad() {
    assertThat(WorkerApplication.class).isNotNull();
  }
}
