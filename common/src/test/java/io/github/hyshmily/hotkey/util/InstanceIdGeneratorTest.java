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
package io.github.hyshmily.hotkey.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.util.InstanceIdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link InstanceIdGenerator}, verifying node ID generation, caching, and override behavior.
 */
class InstanceIdGeneratorTest {

  @AfterEach
  void tearDown() {
    InstanceIdGenerator.setOverride("");
  }

  @Test
  void getNodeId_shouldReturnNonNegative() {
    assertThat(InstanceIdGenerator.getNodeId()).isNotNegative();
  }

  @Test
  void get_shouldReturnCachedValueOnSecondCall() {
    String first = InstanceIdGenerator.get();
    String second = InstanceIdGenerator.get();
    assertThat(second).isEqualTo(first);
  }

  @Test
  void setOverride_shouldTakePrecedence() {
    InstanceIdGenerator.setOverride("custom-instance");
    assertThat(InstanceIdGenerator.get()).isEqualTo("custom-instance");
  }
}
