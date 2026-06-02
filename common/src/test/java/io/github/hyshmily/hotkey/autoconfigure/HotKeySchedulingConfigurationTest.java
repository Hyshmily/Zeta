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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;

/**
 * Tests for {@link HotKeySchedulingConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class HotKeySchedulingConfigurationTest {

  @Test
  void cleanHotKeysCallsFadingOnAllInstances() {
    TopK topK1 = mock(TopK.class);
    TopK topK2 = mock(TopK.class);
    HotKeySchedulingConfiguration config = new HotKeySchedulingConfiguration(List.of(topK1, topK2));

    config.cleanHotKeys();

    verify(topK1).fading();
    verify(topK2).fading();
  }

  @Test
  void cleanHotKeysHandlesSingleInstance() {
    TopK topK = mock(TopK.class);
    HotKeySchedulingConfiguration config = new HotKeySchedulingConfiguration(List.of(topK));

    config.cleanHotKeys();

    verify(topK).fading();
  }

  @Test
  void cleanHotKeysHandlesEmptyList() {
    HotKeySchedulingConfiguration config = new HotKeySchedulingConfiguration(List.of());

    config.cleanHotKeys();
    // No exception should be thrown
  }

  @Test
  void drainExpelledDrainsItemsFromAllInstances() {
    TopK topK1 = mock(TopK.class);
    TopK topK2 = mock(TopK.class);
    LinkedBlockingQueue<Item> queue1 = new LinkedBlockingQueue<>();
    queue1.add(new Item("k1", 5));
    queue1.add(new Item("k2", 3));
    LinkedBlockingQueue<Item> queue2 = new LinkedBlockingQueue<>();
    queue2.add(new Item("k3", 7));
    when(topK1.expelled()).thenReturn(queue1);
    when(topK2.expelled()).thenReturn(queue2);

    HotKeySchedulingConfiguration config = new HotKeySchedulingConfiguration(List.of(topK1, topK2));
    config.drainExpelled();

    assertThat(queue1).isEmpty();
    assertThat(queue2).isEmpty();
  }

  @Test
  void drainExpelledHandlesEmptyQueues() {
    TopK topK = mock(TopK.class);
    when(topK.expelled()).thenReturn(new LinkedBlockingQueue<>());

    HotKeySchedulingConfiguration config = new HotKeySchedulingConfiguration(List.of(topK));
    config.drainExpelled();
    // No exception should be thrown
  }

  @Test
  void drainExpelledLimitsDrainTo1000ItemsPerInstance() {
    TopK topK = mock(TopK.class);
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    for (int i = 0; i < 1500; i++) {
      queue.add(new Item("k" + i, i));
    }
    when(topK.expelled()).thenReturn(queue);

    HotKeySchedulingConfiguration config = new HotKeySchedulingConfiguration(List.of(topK));
    config.drainExpelled();

    // drainTo(collection, 1000) should have drained only 1000
    assertThat(queue).hasSize(500);
  }

  @Test
  void configIsActiveByDefaultWhenTopKBeanExists() {
    new ApplicationContextRunner()
      .withBean(TopK.class, () -> mock(TopK.class))
      .withConfiguration(AutoConfigurations.of(HotKeySchedulingConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(HotKeySchedulingConfiguration.class));
  }

  @Test
  void configIsInactiveWhenPropertyIsDisabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeySchedulingConfiguration.class))
      .withPropertyValues("hotkey.scheduling.enabled=false")
      .run(ctx -> assertThat(ctx).doesNotHaveBean(HotKeySchedulingConfiguration.class));
  }

  @Test
  void configIsInactiveWhenNoTopKBeanExists() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(HotKeySchedulingConfiguration.class))
      .run(ctx -> assertThat(ctx).doesNotHaveBean(HotKeySchedulingConfiguration.class));
  }
}
