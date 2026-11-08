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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.cache.cachesupport.loader.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import java.util.function.Consumer;

/**
 * Stubbing helper for the mocked {@link SingleFlight} in cache tests: answers
 * the production {@code load(key, reader, callerRuns, leaderCommit)} shape with
 * a fixed outcome and, for a {@code LOADED} outcome, runs the leader commit the
 * real implementation would run before returning. Without the commit the
 * harness would report a loaded value that never reached L1.
 */
final class SingleFlightStubs {

  private SingleFlightStubs() {}

  static void stubLoad(SingleFlight singleFlight, LoaderOutcome<?> outcome) {
    when(singleFlight.load(anyString(), any(), anyBoolean(), any())).thenAnswer(inv -> {
      if (outcome.status() == LoadStatus.LOADED) {
        @SuppressWarnings("unchecked")
        Consumer<Object> commit = inv.getArgument(3);
        commit.accept(outcome.value());
      }
      return outcome;
    });
  }

  /** Batch form: answers every key with its scripted outcome, running the per-key commit for loaded values. */
  static void stubBatchLoad(SingleFlight singleFlight, java.util.Map<String, LoaderOutcome<?>> outcomes) {
    when(singleFlight.load(anyIterable(), any(), any())).thenAnswer(inv -> {
      Iterable<String> keys = inv.getArgument(0);
      @SuppressWarnings("unchecked")
      java.util.function.BiConsumer<String, Object> commit = inv.getArgument(2);
      java.util.Map<String, LoaderOutcome<Object>> out = new java.util.LinkedHashMap<>();
      for (String key : keys) {
        LoaderOutcome<?> outcome = outcomes.get(key);
        if (outcome == null) {
          continue;
        }
        if (outcome.status() == LoadStatus.LOADED) {
          // The batch read path wraps every loaded value in a VersionedValue before
          // invoking the leader commit; already-wrapped scripted values are passed through.
          io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue wrapped =
            outcome.value() instanceof io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue existing
              ? existing
              : new io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue(outcome.value(), 1L);
          commit.accept(key, wrapped);
          @SuppressWarnings("unchecked")
          LoaderOutcome<Object> loaded = (LoaderOutcome<Object>) (LoaderOutcome<?>) LoaderOutcome.loaded(wrapped);
          out.put(key, loaded);
        } else {
          @SuppressWarnings("unchecked")
          LoaderOutcome<Object> typed = (LoaderOutcome<Object>) outcome;
          out.put(key, typed);
        }
      }
      return out;
    });
  }
}
