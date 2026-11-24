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
 * Constructor capacities for the JDK hash containers.
 *
 * <p>One rule for every pre-sized map and set: a table sized at
 * {@code ceil(expectedEntries / 0.75)} holds that many entries without rehashing at the default
 * load factor. The ceiling (rather than a division plus one) keeps an exact power-of-two quotient
 * from crossing into the next table size. The JDK 19 factory methods
 * ({@code HashMap.newHashMap}) encode the same rule, but the library targets Java 17, so the
 * arithmetic lives here instead of being repeated at each call site.
 */
@Internal
public final class CollectionSizing {

  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  /**
   * @param expectedEntries the number of entries the container must hold
   * @return a constructor capacity that avoids a rehash at the default load factor
   */
  public static int capacity(int expectedEntries) {
    return (int) Math.ceil(expectedEntries / (double) DEFAULT_LOAD_FACTOR);
  }

  private CollectionSizing() {}
}
