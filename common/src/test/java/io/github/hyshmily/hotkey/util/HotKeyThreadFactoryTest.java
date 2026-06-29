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

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyThreadFactory} verifying thread naming, daemon mode,
 * priority normalization, and counter increment behaviour.
 */
class HotKeyThreadFactoryTest {

  @Test
  void newThread_shouldHaveCorrectNamePrefix() {
    HotKeyThreadFactory factory = new HotKeyThreadFactory("test-pool-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.getName()).startsWith("test-pool-");
  }

  @Test
  void newThread_shouldIncrementCounter() {
    HotKeyThreadFactory factory = new HotKeyThreadFactory("pool-");
    Thread t1 = factory.newThread(() -> {});
    Thread t2 = factory.newThread(() -> {});
    assertThat(t2.getName()).isEqualTo("pool-2");
  }

  @Test
  void newThread_shouldBeDaemon() {
    HotKeyThreadFactory factory = new HotKeyThreadFactory("daemon-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.isDaemon()).isTrue();
  }

  @Test
  void newThread_shouldHaveNormalPriority() {
    HotKeyThreadFactory factory = new HotKeyThreadFactory("normal-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.getPriority()).isEqualTo(Thread.NORM_PRIORITY);
  }

  @Test
  void newThread_shouldBelongToCurrentThreadGroup() {
    HotKeyThreadFactory factory = new HotKeyThreadFactory("group-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.getThreadGroup()).isEqualTo(Thread.currentThread().getThreadGroup());
  }

  @Test
  void newThread_shouldStartWithCounterOne() {
    HotKeyThreadFactory factory = new HotKeyThreadFactory("start-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.getName()).isEqualTo("start-1");
  }

  @Test
  void multipleFactories_shouldEachCountIndependently() {
    HotKeyThreadFactory f1 = new HotKeyThreadFactory("f1-");
    HotKeyThreadFactory f2 = new HotKeyThreadFactory("f2-");
    assertThat(f1.newThread(() -> {}).getName()).isEqualTo("f1-1");
    assertThat(f2.newThread(() -> {}).getName()).isEqualTo("f2-1");
    assertThat(f1.newThread(() -> {}).getName()).isEqualTo("f1-2");
  }
}
