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
package io.github.hyshmily.zeta.annotation.support;

/**
 * The canonical non-negative decimal integers both annotation formats carry: a SpEL argument
 * reference ({@code p0}, {@code a12}) and a physical key's length field. One parser keeps the two
 * from drifting on the rules that matter — no empty field, no multi-digit leading zero, digits
 * only, and a value that fits an {@code int}.
 */
final class CanonicalInt {

  private CanonicalInt() {}

  /**
   * Parses {@code text[start, end)} as a canonical decimal integer.
   *
   * @param text  the containing text
   * @param start the first digit
   * @param end   the exclusive end
   * @return the value, or {@code -1} when the field is empty, has a leading zero, is non-numeric,
   *         or overflows an {@code int}
   */
  static int parse(String text, int start, int end) {
    if (start >= end || (end - start > 1 && text.charAt(start) == '0')) {
      return -1;
    }
    for (int i = start; i < end; i++) {
      char ch = text.charAt(i);
      if (ch < '0' || ch > '9') {
        return -1;
      }
    }
    try {
      return Integer.parseInt(text, start, end, 10);
    } catch (NumberFormatException overflow) {
      return -1;
    }
  }
}
