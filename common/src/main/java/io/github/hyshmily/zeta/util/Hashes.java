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

/**
 * 32-bit hash mixing shared by the routing and detection paths.
 *
 * <p>{@link String#hashCode()} is cheap and cached but weakly distributed for
 * patterned keys. Applying the Murmur3 finalizer ({@code fmix32}) gives full
 * avalanche with two multiplies and three shifts, turning it into a
 * well-spread index for the consistent-hash ring, sketch fingerprints and
 * lock stripes.
 *
 * <p>Deterministic across JVMs: the same input always yields the same mixed
 * value, so App instances agree on consistent-hash ring placement.
 */
@Internal
public final class Hashes {

  /**
   * Avalanche-mix a 32-bit value (Murmur3 x86_32 finalizer).
   *
   * @param hash the raw value, typically {@link String#hashCode()}
   * @return the mixed value
   */
  public static int mix32(int hash) {
    hash ^= hash >>> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >>> 13;
    hash *= 0xc2b2ae35;
    hash ^= hash >>> 16;
    return hash;
  }

  /**
   * Avalanche-mix a 64-bit value (Murmur3 x86_64 finalizer).
   *
   * @param value the raw value
   * @return the mixed value
   */
  public static long mix64(long value) {
    value ^= value >>> 33;
    value *= 0xff51afd7ed558ccdL;
    value ^= value >>> 33;
    value *= 0xc4ceb9fe1a85ec53L;
    value ^= value >>> 33;
    return value;
  }

  /**
   * Hash a string over its full character sequence with a 64-bit FNV-1a seed, then avalanche
   * it. Unlike {@link String#hashCode()}, two different strings that collide in the 32-bit
   * polynomial hash still get independent 64-bit values; the consistent-hash ring uses this
   * for its physical-node seed so one collision cannot erase a node's whole virtual set.
   *
   * @param value the string to hash
   * @return the mixed 64-bit hash
   */
  public static long mix64(String value) {
    long hash = 0xcbf29ce484222325L;
    for (int i = 0; i < value.length(); i++) {
      hash ^= value.charAt(i);
      hash *= 0x100000001b3L;
    }
    return mix64(hash);
  }

  /** Non-instantiable. */
  private Hashes() {}
}
