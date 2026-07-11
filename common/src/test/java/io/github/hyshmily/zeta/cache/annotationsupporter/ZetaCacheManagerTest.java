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
package io.github.hyshmily.zeta.cache.annotationsupporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache;

@DisplayName("ZetaCacheManager tests")
class ZetaCacheManagerTest {

  private Zeta zeta;
  private ZetaProperties properties;
  private ZetaCacheManager manager;

  @BeforeEach
  void setUp() {
    zeta = mock(Zeta.class);
    properties = mock(ZetaProperties.class);
    manager = new ZetaCacheManager(zeta, properties);
  }

  @Test
  @DisplayName("constructor stores dependencies")
  void constructor_storesDependencies() {
    assertThat(manager).isNotNull();
  }

  @Test
  @DisplayName("getCache creates new ZetaSpringCache on first access")
  void getCache_createsCacheOnFirstAccess() {
    Cache cache = manager.getCache("test");
    assertThat(cache).isNotNull();
    assertThat(cache.getName()).isEqualTo("test");
  }

  @Test
  @DisplayName("getCache returns same instance on second access")
  void getCache_returnsSameInstanceOnSecondAccess() {
    Cache first = manager.getCache("test");
    Cache second = manager.getCache("test");
    assertThat(second).isSameAs(first);
  }

  @Test
  @DisplayName("getCache creates distinct caches for different names")
  void getCache_createsDistinctCachesForDifferentNames() {
    Cache first = manager.getCache("cache-a");
    Cache second = manager.getCache("cache-b");
    assertThat(first).isNotSameAs(second);
    assertThat(first.getName()).isEqualTo("cache-a");
    assertThat(second.getName()).isEqualTo("cache-b");
  }

  @Test
  @DisplayName("getCache concurrent access produces same instance")
  void getCache_concurrentAccess_producesSameInstance() throws Exception {
    int threadCount = 10;
    CountDownLatch startLatch = new CountDownLatch(1);
    Cache[] results = new Cache[threadCount];
    Thread[] threads = new Thread[threadCount];

    for (int i = 0; i < threadCount; i++) {
      int idx = i;
      threads[idx] = new Thread(() -> {
        try {
          startLatch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        results[idx] = manager.getCache("concurrent");
      });
      threads[idx].start();
    }

    startLatch.countDown();
    for (Thread thread : threads) {
      thread.join();
    }

    Cache first = results[0];
    for (Cache result : results) {
      assertThat(result).isSameAs(first);
    }
  }

  @Test
  @DisplayName("getCacheNames returns registered names")
  void getCacheNames_returnsRegisteredNames() {
    manager.getCache("alpha");
    manager.getCache("beta");
    manager.getCache("gamma");

    Collection<String> names = manager.getCacheNames();
    assertThat(names).containsExactlyInAnyOrder("alpha", "beta", "gamma");
  }

  @Test
  @DisplayName("getCacheNames is unmodifiable")
  void getCacheNames_isUnmodifiable() {
    manager.getCache("test");
    Collection<String> names = manager.getCacheNames();
    assertThat(names).isUnmodifiable();
  }

  @Test
  @DisplayName("getCacheNames returns empty initially")
  void getCacheNames_returnsEmptyInitially() {
    assertThat(manager.getCacheNames()).isEmpty();
  }

  @Test
  @DisplayName("getMissingCache creates non-null cache")
  void getMissingCache_createsNonNullCache() {
    Cache cache = manager.getMissingCache("custom");
    assertThat(cache).isNotNull();
    assertThat(cache.getName()).isEqualTo("custom");
  }

  @Test
  @DisplayName("getCache returns null when getMissingCache returns null")
  void getCache_whenGetMissingCacheNull_returnsNull() {
    ZetaCacheManager nullManager = new ZetaCacheManager(zeta, properties) {
      @Override
      protected Cache getMissingCache(String name) {
        return null;
      }
    };
    Cache cache = nullManager.getCache("nonExistent");
    assertThat(cache).isNull();
  }
}
