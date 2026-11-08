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
package io.github.hyshmily.zeta.worker.detection.rule;

/** The two COOL strategies a rule can select (design decisions: worker pipeline). */
public enum CoolStrategy {
  /**
   * Cool once the sliding-window sum has stayed strictly below {@code cool-count} for
   * {@code low-qps-for}. Hysteresis: a key in the band between {@code cool-qps} and
   * {@code hot-qps} keeps its HOT seat indefinitely.
   */
  COOL_FOR_LOW_QPS,
  /**
   * Cool exactly {@code cool-after} after promotion, regardless of traffic. A still-hot key
   * re-earns its seat on the next report; a banded or cold key does not.
   */
  COOL_FOR_EXPIRE,
}
