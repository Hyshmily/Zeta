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
package io.github.hyshmily.zeta.cache.cachesupport.raw;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReadContextTest {

  @Test
  void lazyTtl_shouldEvaluateOnceAndOnlyWhenAsked() {
    AtomicInteger evaluations = new AtomicInteger();
    GetPolicy policy = new GetPolicy(
      () -> {
        evaluations.incrementAndGet();
        return 10L;
      },
      () -> {
        evaluations.incrementAndGet();
        return 5L;
      },
      StalePolicy.SOFT_REFRESH,
      true,
      false
    );
    ReadContext ctx = ReadContext.of(null, policy);

    assertThat(evaluations).as("a context allocates without consuming the suppliers").hasValue(0);
    assertThat(ctx.hardTtlMs()).isEqualTo(10L);
    assertThat(ctx.hardTtlMs()).as("memoized per call").isEqualTo(10L);
    assertThat(ctx.softTtlMs()).isEqualTo(5L);
    assertThat(evaluations).hasValue(2);
  }

  @Test
  void lazyNegativeTtl_shouldBeRejectedAtEvaluation() {
    AtomicInteger evaluations = new AtomicInteger();
    GetPolicy policy = new GetPolicy(
      () -> {
        evaluations.incrementAndGet();
        return -1L;
      },
      () -> 0L,
      StalePolicy.SOFT_REFRESH,
      true,
      false
    );
    ReadContext ctx = ReadContext.of(null, policy);

    assertThatThrownBy(ctx::hardTtlMs)
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("hardTtlMs must not be negative");
    assertThat(evaluations).as("the supplier is evaluated exactly once before rejection").hasValue(1);
  }
}
