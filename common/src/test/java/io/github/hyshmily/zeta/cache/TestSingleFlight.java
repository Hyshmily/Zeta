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

import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlightImpl;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@link SingleFlightImpl} with the convenience overloads the tests read best
 * with: load without a commit sink, load on the caller's thread, and a batch
 * whose commit writes into a caller-supplied map. Every method delegates to
 * {@link #load(String, Supplier, boolean, Consumer)}; no behaviour lives here.
 */
final class TestSingleFlight extends SingleFlightImpl {

  TestSingleFlight(
    int maxSize,
    int ttlSec,
    int timeoutSeconds,
    java.util.concurrent.Executor executor,
    CircuitBreakerRegistry registry
  ) {
    super(maxSize, ttlSec, timeoutSeconds, executor, registry);
  }

  <T> LoaderOutcome<T> loadOutcome(String cacheKey, Supplier<T> reader) {
    return load(cacheKey, reader, false, v -> {});
  }

  <T> LoaderOutcome<T> loadOutcomeCallerRuns(String cacheKey, Supplier<T> reader, Consumer<? super T> commit) {
    return load(cacheKey, reader, true, commit != null ? commit : v -> {});
  }

  <T> Map<String, LoaderOutcome<T>> loadOutcome(Iterable<String> cacheKeys, Function<String, T> reader) {
    return load(cacheKeys, reader, (key, value) -> {});
  }

  <T> Map<String, LoaderOutcome<T>> loadOutcome(
    Iterable<String> cacheKeys,
    Function<String, T> reader,
    Map<String, T> commitTarget
  ) {
    return load(cacheKeys, reader, commitTarget::put);
  }
}
