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
package io.github.hyshmily.zeta.cache;

import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * design decisions: version stamps invariant I1 for invalidation: after a local invalidate the next
 * read must reload from the source instead of replaying a cached value.
 */
class NoRedisInvalidateTest {

  @Test
  @DisplayName("get -> invalidate -> get reloads from the source")
  void invalidate_thenReadReloads() {
    try (StampTestSupport support = new StampTestSupport()) {
      GetPolicy options = GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH);
      AtomicInteger reads = new AtomicInteger();

      assertThat((Object) support.cache.get("k", () -> "v" + reads.incrementAndGet(), options)).isEqualTo("v1");
      support.cache.invalidate("k", new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, false));
      assertThat((Object) support.cache.get("k", () -> "v" + reads.incrementAndGet(), options)).isEqualTo("v2");
      assertThat(reads.get()).isEqualTo(2);
    }
  }
}
