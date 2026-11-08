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
package io.github.hyshmily.zeta.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link ZetaSchedulingConfiguration}.
 */
@ExtendWith(MockitoExtension.class)
class ZetaSchedulingConfigurationTest {

  private ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

  /**
   * Verifies that cleanHotKeys calls fading on all registered TopK instances.
   */
  @Test
  void cleanHotKeysCallsFadingOnAllInstances() {
    TopK topK1 = mock(TopK.class);
    TopK topK2 = mock(TopK.class);
    ZetaSchedulingConfiguration config = new ZetaSchedulingConfiguration(
      List.of(topK1, topK2),
      provider(null),
      scheduler,
      provider(null)
    );

    config.cleanHotKeys();

    verify(topK1).fading();
    verify(topK2).fading();
  }

  /**
   * Verifies that cleanHotKeys works correctly with a single TopK instance.
   */
  @Test
  void cleanHotKeysHandlesSingleInstance() {
    TopK topK = mock(TopK.class);
    ZetaSchedulingConfiguration config = new ZetaSchedulingConfiguration(
      List.of(topK),
      provider(null),
      scheduler,
      provider(null)
    );

    config.cleanHotKeys();

    verify(topK).fading();
  }

  /**
   * Verifies that cleanHotKeys handles an empty TopK list without throwing.
   */
  @Test
  void cleanHotKeysHandlesEmptyList() {
    ZetaSchedulingConfiguration config = new ZetaSchedulingConfiguration(
      List.of(),
      provider(null),
      scheduler,
      provider(null)
    );

    config.cleanHotKeys();
    // No exception should be thrown
  }

  /**
   * Verifies that drainExpelled drains expelled items from all registered TopK instances.
   */
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

    ZetaSchedulingConfiguration config = new ZetaSchedulingConfiguration(
      List.of(topK1, topK2),
      provider(null),
      scheduler,
      provider(null)
    );
    config.drainExpelled();

    assertThat(queue1).isEmpty();
    assertThat(queue2).isEmpty();
  }

  /**
   * Verifies that drainExpelled handles empty expelled queues without throwing.
   */
  @Test
  void drainExpelledHandlesEmptyQueues() {
    TopK topK = mock(TopK.class);
    when(topK.expelled()).thenReturn(new LinkedBlockingQueue<>());

    ZetaSchedulingConfiguration config = new ZetaSchedulingConfiguration(
      List.of(topK),
      provider(null),
      scheduler,
      provider(null)
    );
    config.drainExpelled();
    // No exception should be thrown
  }

  /**
   * The drain is a memory-protection pass: every queue is emptied, not just a bounded batch.
   */
  @Test
  void drainExpelledEmptiesTheQueue() {
    TopK topK = mock(TopK.class);
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    for (int i = 0; i < 1500; i++) {
      queue.add(new Item("k" + i, i));
    }
    when(topK.expelled()).thenReturn(queue);

    ZetaSchedulingConfiguration config = new ZetaSchedulingConfiguration(
      List.of(topK),
      provider(null),
      scheduler,
      provider(null)
    );
    config.drainExpelled();

    assertThat(queue).isEmpty();
  }

  /**
   * The app-side detector is not a {@link TopK} bean, so the maintenance pass decays the
   * sketch exactly once per tick.
   */
  @Test
  void maintenanceDecaysTheSketchExactlyOncePerTick() {
    TopK heavyKeeper = mock(TopK.class);
    when(heavyKeeper.expelled()).thenReturn(new LinkedBlockingQueue<>());

    ZetaSchedulingConfiguration config = new ZetaSchedulingConfiguration(
      List.of(heavyKeeper),
      provider(null),
      scheduler,
      provider(null)
    );

    config.cleanHotKeys();
    config.drainExpelled();

    verify(heavyKeeper).fading();
    verify(heavyKeeper).expelled();
  }

  /**
   * Verifies that cleanUpExpiredEntries runs the L1 cache maintenance.
   */
  @Test
  void cleanUpExpiredEntries_shouldCleanUpL1Cache() {
    @SuppressWarnings("unchecked")
    com.github.benmanes.caffeine.cache.Cache<String, CacheEntry> l1Cache = mock(
      com.github.benmanes.caffeine.cache.Cache.class
    );
    ZetaSchedulingConfiguration config = new ZetaSchedulingConfiguration(
      List.of(),
      provider(null),
      scheduler,
      provider(l1Cache)
    );

    config.cleanUpExpiredEntries();

    verify(l1Cache).cleanUp();
  }

  /**
   * A tick whose collaborator is absent is not registered at all, so the shared
   * scheduler does not run an empty pass every interval.
   */
  @Test
  void scheduleTasksRegistersOnlyPresentCollaborators() {
    HotKeyCache hotKeyCache = mock(HotKeyCache.class);
    ZetaSchedulingConfiguration config = new ZetaSchedulingConfiguration(
      List.of(mock(TopK.class)),
      provider(hotKeyCache),
      scheduler,
      provider(null)
    );

    config.scheduleTasks();

    // cleanHotKeys + drainExpelled + promoteAdmittedKeys; no L1 cleanup tick.
    verify(scheduler, times(3)).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
  }

  /**
   * Verifies that the scheduling configuration is active by default when a TopK bean exists.
   */
  @Test
  void configIsActiveByDefaultWhenTopKBeanExists() {
    new ApplicationContextRunner()
      .withBean(TopK.class, () -> mock(TopK.class))
      .withBean("hotKeyScheduler", ScheduledExecutorService.class, () -> mock(ScheduledExecutorService.class))
      .withConfiguration(AutoConfigurations.of(ZetaSchedulingConfiguration.class))
      .run(ctx -> assertThat(ctx).hasSingleBean(ZetaSchedulingConfiguration.class));
  }

  /**
   * Verifies that the scheduling configuration is inactive when the property zeta.scheduling.enabled is false.
   */
  @Test
  void configIsInactiveWhenPropertyIsDisabled() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaSchedulingConfiguration.class))
      .withPropertyValues("zeta.scheduling.enabled=false")
      .run(ctx -> assertThat(ctx).doesNotHaveBean(ZetaSchedulingConfiguration.class));
  }

  /**
   * Verifies that the scheduling configuration is inactive when no TopK bean exists.
   */
  @Test
  void configIsInactiveWhenNoTopKBeanExists() {
    new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(ZetaSchedulingConfiguration.class))
      .run(ctx -> assertThat(ctx).doesNotHaveBean(ZetaSchedulingConfiguration.class));
  }

  private static <T> ObjectProvider<T> provider(@Nullable T value) {
    @SuppressWarnings("unchecked")
    ObjectProvider<T> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(value);
    return provider;
  }
}
