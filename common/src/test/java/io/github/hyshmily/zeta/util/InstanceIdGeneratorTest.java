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
package io.github.hyshmily.zeta.util;

import static org.assertj.core.api.Assertions.assertThat;

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

  /**
   * Verifies that getNodeId returns a non-negative node ID.
   */
  @Test
  void getNodeId_shouldReturnNonNegative() {
    assertThat(InstanceIdGenerator.getNodeId()).isNotNegative();
  }

  /**
   * Verifies that get() returns the same cached value on the second call.
   */
  @Test
  void get_shouldReturnCachedValueOnSecondCall() {
    String first = InstanceIdGenerator.get();
    String second = InstanceIdGenerator.get();
    assertThat(second).isEqualTo(first);
  }

  /**
   * Verifies that setOverride overrides the default instance ID with a custom value.
   */
  @Test
  void setOverride_shouldTakePrecedence() {
    InstanceIdGenerator.setOverride("custom-instance");
    assertThat(InstanceIdGenerator.get()).isEqualTo("custom-instance");
  }

  /**
   * Verifies that a blank string override does not take precedence.
   */
  @Test
  void setOverride_blankString_shouldNotOverride() {
    InstanceIdGenerator.setOverride("  ");
    var result = InstanceIdGenerator.get();
    assertThat(result).isNotNull();
    assertThat(result).isNotEqualTo("  ");
  }

  /**
   * Verifies that setting override to null clears it and falls back to auto-detection.
   */
  @Test
  void setOverride_null_shouldClearAndFallback() {
    InstanceIdGenerator.setOverride("temp-override");
    assertThat(InstanceIdGenerator.get()).isEqualTo("temp-override");
    InstanceIdGenerator.setOverride(null);
    var result = InstanceIdGenerator.get();
    assertThat(result).isNotNull();
    assertThat(result).isNotEqualTo("temp-override");
  }

  /**
   * Verifies that getNodeId returns a stable value across multiple calls.
   */
  @Test
  void getNodeId_shouldBeStable() {
    assertThat(InstanceIdGenerator.getNodeId()).isEqualTo(InstanceIdGenerator.getNodeId());
  }

  /**
   * Verifies that get() is thread-safe under concurrent access.
   */
  @Test
  void get_shouldBeThreadSafe() throws Exception {
    InstanceIdGenerator.setOverride(null);
    var first = InstanceIdGenerator.get();
    java.util.concurrent.ExecutorService exec = java.util.concurrent.Executors.newFixedThreadPool(4);
    var futures = new java.util.ArrayList<java.util.concurrent.Future<String>>();
    for (int i = 0; i < 10; i++) {
      futures.add(exec.submit(InstanceIdGenerator::get));
    }
    for (var f : futures) {
      assertThat(f.get()).isEqualTo(first);
    }
    exec.shutdown();
  }
}
