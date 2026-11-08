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
package io.github.hyshmily.zeta.hotkeydetector.heavykeeper;

/**
 * A hot-key item with its estimated access count.
 *
 * <p>Returned by {@link TopK#list()} and {@link TopK#listTopN(int)} to
 * represent a single entry in the current TopK ranking. The {@code count}
 * is the <b>estimate</b> the implementation holds for this key: for
 * {@code HeavyKeeper} it is the maximum over the sketch rows the key actually
 * owns (a row held by another key's fingerprint contributes nothing, design decisions: sketch),
 * subject to sampled decay, counter saturation, and the periodic
 * re-synchronisation of membership counts against the sketch (halving only
 * when the sketch no longer holds the key).
 *
 * @param key   the hot key (unique identifier, e.g. a cache key)
 * @param count the estimated access count (approximate, subject to sketch
 *              decay and periodic re-synchronisation)
 */
public record Item(String key, long count) {}
