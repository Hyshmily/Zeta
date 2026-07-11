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

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ZetaThreadFactory} verifying thread naming, daemon mode,
 * priority normalization, and counter increment behaviour.
 */
class ZetaThreadFactoryTest {

  @Test
  void newThread_shouldHaveCorrectNamePrefix() {
    ZetaThreadFactory factory = new ZetaThreadFactory("test-pool-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.getName()).startsWith("test-pool-");
  }

  @Test
  void newThread_shouldIncrementCounter() {
    ZetaThreadFactory factory = new ZetaThreadFactory("pool-");
    Thread t1 = factory.newThread(() -> {});
    Thread t2 = factory.newThread(() -> {});
    assertThat(t2.getName()).isEqualTo("pool-2");
  }

  @Test
  void newThread_shouldBeDaemon() {
    ZetaThreadFactory factory = new ZetaThreadFactory("daemon-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.isDaemon()).isTrue();
  }

  @Test
  void newThread_shouldHaveNormalPriority() {
    ZetaThreadFactory factory = new ZetaThreadFactory("normal-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.getPriority()).isEqualTo(Thread.NORM_PRIORITY);
  }

  @Test
  void newThread_shouldBelongToCurrentThreadGroup() {
    ZetaThreadFactory factory = new ZetaThreadFactory("group-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.getThreadGroup()).isEqualTo(Thread.currentThread().getThreadGroup());
  }

  @Test
  void newThread_shouldStartWithCounterOne() {
    ZetaThreadFactory factory = new ZetaThreadFactory("start-");
    Thread t = factory.newThread(() -> {});
    assertThat(t.getName()).isEqualTo("start-1");
  }

  @Test
  void multipleFactories_shouldEachCountIndependently() {
    ZetaThreadFactory f1 = new ZetaThreadFactory("f1-");
    ZetaThreadFactory f2 = new ZetaThreadFactory("f2-");
    assertThat(f1.newThread(() -> {}).getName()).isEqualTo("f1-1");
    assertThat(f2.newThread(() -> {}).getName()).isEqualTo("f2-1");
    assertThat(f1.newThread(() -> {}).getName()).isEqualTo("f1-2");
  }
}
