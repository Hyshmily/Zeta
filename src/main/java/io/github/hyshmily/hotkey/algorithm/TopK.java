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

import java.util.List;
import java.util.concurrent.BlockingQueue;

// Hot key detection
public interface TopK {
  // Add one access and return the result (whether it becomes hot, whether old hot key is evicted)
  AddResult add(String key, int increment);

  // Get the current TopK list (descending by count)
  List<Item> list();

  // Get the queue of items evicted from TopK for external async processing
  BlockingQueue<Item> expelled();

  // Decay all counts (for aging historical data)
  void fading();

  // Return the total number of data streams
  long total();

  // Check whether the key is in the TopK set
  default boolean contains(String key) {
    return list().stream().anyMatch(item -> item.key().equals(key));
  }
}
