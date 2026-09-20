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

import io.github.hyshmily.zeta.Internal;

/**
 * FastRange bucket mapping, adapted from RocksDB {@code util/fastrange.h}.
 *
 * <p>Maps a hash into {@code [0, range)} via the high half of the
 * {@code range × hash} product instead of a modulo: the product consumes the
 * full hash width (a modulo only sees the low bits — the part a quality hash
 * exercises least) and replaces division with a single multiplication.
 *
 * <p><b>Precondition</b> (the {@code fastrange.h} L92-93 warning): the hash
 * must be the natural output width of a quality hash function — a 32-bit
 * value goes through {@link #fastRange32}; feeding a 32-bit hash to a 64-bit
 * variant "gives extremely bad results, mostly zero". The 64-bit variant
 * (128-bit product via {@code Math.multiplyHigh}) is deliberately not
 * provided until a 64-bit-hash caller exists.
 */
@Internal
public final class FastRangeUtil {

  private FastRangeUtil() {}

  /**
   * Map a 32-bit hash into {@code [0, range)}:
   * {@code (range × uint32(hash)) >>> 32}.
   *
   * <p>Distribution is equivalent to unsigned modulo for a perfectly
   * distributed hash, but consumes all 32 bits of entropy and costs one
   * multiply instead of one division.
   *
   * @param hash  the hash value (all 32 bits are consumed)
   * @param range the bucket count, must be positive
   * @return the bucket index in {@code [0, range)}
   */
  public static int fastRange32(int hash, int range) {
    return (int) (((hash & 0xFFFFFFFFL) * range) >>> 32);
  }
}
