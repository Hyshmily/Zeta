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
package io.github.hyshmily.zeta.worker.detection.window;

import org.springframework.util.Assert;

/**
 * The aligned geometry of the one shared window, computed once at startup.
 *
 * <p>One home for the arithmetic both the detector and the threshold conversion need. The
 * configured duration and the effective window differ whenever the slice count is not a power of
 * two (and, after rounding, whenever the duration is not divisible by the aligned count):
 * {@code 1000ms / 10} becomes 16 slices of {@code 62ms}, i.e. a 992ms window. Every QPS-to-count
 * conversion uses {@link #windowMs()}, never the configured value, so a threshold means the same
 * thing to the person who wrote it and to the detector that enforces it.
 *
 * @param alignedSlices power-of-two slice count the ring is built with
 * @param sliceMs       per-slice duration after integer division
 * @param windowMs      effective window length, {@code alignedSlices * sliceMs}
 * @param configuredMs  the configured duration, kept for the alignment warning and the re-check upper bound
 */
public record WindowGeometry(int alignedSlices, long sliceMs, long windowMs, long configuredMs) {
  /**
   * Upper bound on the aligned slice count. The ring doubles the slice count, so 65,536 slices is
   * already a 131,072-slot buffer per tracked key; no real window needs more, and the bound keeps
   * the power-of-two round-up from overflowing into a negative window.
   */
  private static final int MAX_ALIGNED_SLICES = 1 << 16;

  /**
   * Validates the mask arithmetic at the canonical-constructor boundary: {@link #of} is the only
   * in-module caller, but the public constructor must not be able to publish a geometry the ring
   * cannot index.
   */
  public WindowGeometry {
    Assert.isTrue(alignedSlices > 0, "alignedSlices must be positive, got " + alignedSlices);
    Assert.isTrue(
      (alignedSlices & (alignedSlices - 1)) == 0,
      "alignedSlices must be a power of two, got " + alignedSlices
    );
    Assert.isTrue(
      alignedSlices <= MAX_ALIGNED_SLICES,
      "alignedSlices must be <= " + MAX_ALIGNED_SLICES + ", got " + alignedSlices
    );
    Assert.isTrue(sliceMs > 0L, "sliceMs must be positive, got " + sliceMs);
    Assert.isTrue(
      sliceMs <= Long.MAX_VALUE / alignedSlices,
      "alignedSlices * sliceMs must fit in a long, got alignedSlices=" + alignedSlices + ", sliceMs=" + sliceMs
    );
    Assert.isTrue(windowMs == alignedSlices * sliceMs, "windowMs must equal alignedSlices * sliceMs, got " + windowMs);
  }

  /**
   * Derives the geometry from the raw configuration.
   *
   * @param configuredMs  configured window length in milliseconds
   * @param requestedSlices requested slice count
   * @return the aligned geometry
   * @throws IllegalArgumentException when the slice count is non-positive, above
   *                                  {@value #MAX_ALIGNED_SLICES}, or the configured duration
   *                                  cannot hold one slice per aligned slot
   */
  public static WindowGeometry of(long configuredMs, int requestedSlices) {
    Assert.isTrue(requestedSlices > 0, "slices must be positive, got " + requestedSlices);
    Assert.isTrue(
      requestedSlices <= MAX_ALIGNED_SLICES,
      "slices must be <= " + MAX_ALIGNED_SLICES + ", got " + requestedSlices
    );

    int aligned = requestedSlices;
    if ((aligned & (aligned - 1)) != 0) {
      aligned = Integer.highestOneBit(aligned - 1) << 1;
    }
    // Rounding up can push configuredMs / aligned to 0 (15ms over 10 slices becomes 16, so 0),
    // which would divide by zero on every add and window read.
    Assert.isTrue(
      configuredMs >= aligned,
      "windowDurationMs (" + configuredMs + ") must be >= aligned slices (" + aligned + ") to avoid division by zero"
    );
    long sliceMs = configuredMs / aligned;
    return new WindowGeometry(aligned, sliceMs, aligned * sliceMs, configuredMs);
  }

  /**
   * Converts a QPS threshold to the number of accesses in one effective window. The single home of
   * the conversion: the detector and the startup validator never repeat the rounding.
   *
   * @param qps accesses per second
   * @return the window count, rounded up so a threshold is never stricter than its author wrote
   */
  public long toWindowCount(long qps) {
    if (qps <= 0L) {
      return 0L;
    }
    // A valid-looking qps and window can still overflow the product; saturate instead of returning
    // a negative threshold that would reject every report.
    if (qps > (Long.MAX_VALUE - 999L) / windowMs) {
      return Long.MAX_VALUE;
    }
    return (qps * windowMs + 999L) / 1_000L;
  }

  /**
   * Converts a window count back to the QPS it represents, rounded to the nearest whole QPS for
   * display and startup warnings.
   *
   * @param windowCount accesses in one effective window
   * @return the equivalent QPS
   */
  public long toEffectiveQps(long windowCount) {
    return Math.round(((double) windowCount * 1_000L) / windowMs);
  }

  /**
   * Whether the configured duration equals the effective one.
   *
   * @return {@code false} when integer division shortened the window
   */
  public boolean exact() {
    return configuredMs == windowMs;
  }
}
