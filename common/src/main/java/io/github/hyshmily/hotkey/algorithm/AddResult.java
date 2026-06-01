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
package io.github.hyshmily.hotkey.algorithm;

/**
 * Result of a single {@link TopK#add} operation.
 *
 * @param expelledKey the key evicted from the TopK set by this operation, or {@code null} if none
 * @param isHotKey    whether the current key entered the TopK hot set
 * @param currentKey  the key that was added
 */
public record AddResult(String expelledKey, boolean isHotKey, String currentKey) {}
