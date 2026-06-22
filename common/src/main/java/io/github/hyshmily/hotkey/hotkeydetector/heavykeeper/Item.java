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
package io.github.hyshmily.hotkey.hotkeydetector.heavykeeper;

/**
 * A hot-key item with its estimated access count.
 *
 * <p>Returned by {@link TopK#list()} and {@link TopK#listTopN(int)} to
 * represent a single entry in the current TopK ranking. The {@code count}
 * is an <b>estimate</b> produced by the Count-Min Sketch algorithm — it
 * may over-count due to hash collisions in the sketch and is periodically
 * halved by {@link TopK#fading()} to decay historical data.
 *
 * @param key   the hot key (unique identifier, e.g. a cache key)
 * @param count the estimated access count (approximate, subject to sketch
 *              error and periodic decay)
 */
public record Item(String key, long count) {}
