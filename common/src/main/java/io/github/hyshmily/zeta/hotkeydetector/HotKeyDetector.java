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
package io.github.hyshmily.zeta.hotkeydetector;

import static io.github.hyshmily.zeta.cache.CacheKeysPolicy.invalidCacheKey;

import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.AddResult;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.util.Internal;
import java.util.Map;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/**
 * The inbound edge of the local sketch: the only place that turns read-path
 * traffic into sketch writes.
 *
 * <p>It does <b>not</b> implement {@code TopK}: query-side callers (actuator,
 * Micrometer, the facade's ranking) inject the
 * {@link io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK} bean and talk
 * to the {@link HeavyKeeper} bean directly, while this class owns the write
 * edge and the lenient membership probe below (design decisions: access
 * counting):
 *
 * <ul>
 *   <li>{@link #addDirect} — one synchronous single-key write
 *       ({@code DETECT_ONLY} tag mode).</li>
 *   <li>{@link #ingest} — the batched flush the shared {@code AccessCounter}
 *       tide drives every {@code detect-interval-ms}.</li>
 *   <li>{@link #contains} — the lenient membership probe the read path and
 *       local promotion use; an invalid key answers {@code false} instead of
 *       reaching the strict sketch kernel.</li>
 * </ul>
 *
 * <p>The admission sink is forwarded from the sketch so the cache layer can
 * learn about newly admitted keys without depending on {@link HeavyKeeper}.
 * The sink runs on the admitting thread under the sketch's admission lock and
 * must not block.
 *
 * <p>Thread-safe: every method delegates to the thread-safe sketch.
 */
@Internal
public class HotKeyDetector {

  private final HeavyKeeper heavyKeeper;

  /**
   * Receives keys that newly enter the TopK set. Registered by the cache layer;
   * the detector itself stays independent of the cache.
   */
  private volatile Consumer<String> admittedSink;

  /**
   * Creates the detector over the given sketch.
   *
   * @param heavyKeeper the underlying sketch-based TopK implementation
   */
  public HotKeyDetector(HeavyKeeper heavyKeeper) {
    this.heavyKeeper = heavyKeeper;
    heavyKeeper.setAdmissionListener(this::publishAdmitted);
  }

  /**
   * Register the sink for TopK admission events.
   *
   * @param sink the admission sink, or {@code null} to detach
   */
  public void setAdmittedSink(@Nullable Consumer<String> sink) {
    this.admittedSink = sink;
  }

  private void publishAdmitted(String key) {
    Consumer<String> sink = admittedSink;
    if (sink != null) {
      sink.accept(key);
    }
  }

  /**
   * Record an access directly to the sketch and TopK heap. Invalid keys are
   * mapped to {@link AddResult#cold()} here — the sketch kernel is strict and
   * throws on {@code null}, while the read path may hand over either.
   *
   * @param key       the accessed key
   * @param increment the frequency increment
   * @return result indicating whether the key became hot
   */
  public AddResult addDirect(String key, long increment) {
    if (invalidCacheKey(key)) {
      return AddResult.cold();
    }
    return heavyKeeper.addDirect(key, increment);
  }

  /**
   * Ingest one flushed counting batch from the shared access counter's tide —
   * the buffered path's entry point.
   *
   * <p>No copy and no per-key pre-scan: the kernel's batch ingest skips a null
   * key rather than failing the flush, so the tide's snapshot is handed over
   * unexamined and must not be mutated (the counter's contract).
   *
   * @param keyCounts the batch's snapshot (never mutated, may be empty)
   */
  public void ingest(Map<String, Long> keyCounts) {
    heavyKeeper.ingestBatch(keyCounts);
  }

  /**
   * Check whether the given key is currently in the TopK set.
   *
   * @param key the key to check
   * @return {@code true} if the key is present in the current TopK ranking
   */
  public boolean contains(String key) {
    return !invalidCacheKey(key) && heavyKeeper.contains(key);
  }
}
