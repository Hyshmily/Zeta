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
package io.github.hyshmily.zeta.hotkeydetector;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.AddResult;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.Test;

class TopKDefaultMethodTest {

  @Test
  void contains_shouldReturnTrueWhenKeyIsInList() {
    TopK topK = new TopK() {
      @Override
      public AddResult addDirect(String key, long increment) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<AddResult> addDirect(Map<String, Long> keyCounts) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<Item> list() {
        return List.of(new Item("a", 10), new Item("b", 5));
      }

      @Override
      public List<Item> listTopN(int n) {
        throw new UnsupportedOperationException();
      }

      @Override
      public BlockingQueue<Item> expelled() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void fading() {}

      @Override
      public long total() {
        return 0;
      }
    };
    assertThat(topK.contains("a")).isTrue();
    assertThat(topK.contains("b")).isTrue();
  }

  @Test
  void contains_shouldReturnFalseWhenKeyIsNotInList() {
    TopK topK = new TopK() {
      @Override
      public AddResult addDirect(String key, long increment) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<AddResult> addDirect(Map<String, Long> keyCounts) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<Item> list() {
        return List.of(new Item("a", 10));
      }

      @Override
      public List<Item> listTopN(int n) {
        throw new UnsupportedOperationException();
      }

      @Override
      public BlockingQueue<Item> expelled() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void fading() {}

      @Override
      public long total() {
        return 0;
      }
    };
    assertThat(topK.contains("c")).isFalse();
  }

  @Test
  void warm_shouldDelegateToAddDirect() {
    List<AddResult> results = new ArrayList<>();
    TopK topK = new TopK() {
      @Override
      public AddResult addDirect(String key, long increment) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<AddResult> addDirect(Map<String, Long> keyCounts) {
        results.addAll(
          keyCounts
            .entrySet()
            .stream()
            .map(e -> new AddResult(null, true, e.getKey()))
            .toList()
        );
        return results;
      }

      @Override
      public List<Item> list() {
        return List.of();
      }

      @Override
      public List<Item> listTopN(int n) {
        throw new UnsupportedOperationException();
      }

      @Override
      public BlockingQueue<Item> expelled() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void fading() {}

      @Override
      public long total() {
        return 0;
      }
    };
    topK.warm(Map.of("a", 10L, "b", 20L));
    assertThat(results).hasSize(2);
  }

  @Test
  void estimatedSize_shouldReturnListSize() {
    TopK topK = new TopK() {
      @Override
      public AddResult addDirect(String key, long increment) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<AddResult> addDirect(Map<String, Long> keyCounts) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<Item> list() {
        return List.of(new Item("a", 10), new Item("b", 5));
      }

      @Override
      public List<Item> listTopN(int n) {
        throw new UnsupportedOperationException();
      }

      @Override
      public BlockingQueue<Item> expelled() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void fading() {}

      @Override
      public long total() {
        return 0;
      }
    };
    assertThat(topK.estimatedSize()).isEqualTo(2);
  }

  @Test
  void estimatedSize_whenEmpty_shouldReturnZero() {
    TopK topK = new TopK() {
      @Override
      public AddResult addDirect(String key, long increment) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<AddResult> addDirect(Map<String, Long> keyCounts) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<Item> list() {
        return List.of();
      }

      @Override
      public List<Item> listTopN(int n) {
        throw new UnsupportedOperationException();
      }

      @Override
      public BlockingQueue<Item> expelled() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void fading() {}

      @Override
      public long total() {
        return 0;
      }
    };
    assertThat(topK.estimatedSize()).isZero();
  }

  @Test
  void contains_shouldReturnFalseWhenListIsEmpty() {
    TopK topK = new TopK() {
      @Override
      public AddResult addDirect(String key, long increment) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<AddResult> addDirect(Map<String, Long> keyCounts) {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<Item> list() {
        return List.of();
      }

      @Override
      public List<Item> listTopN(int n) {
        throw new UnsupportedOperationException();
      }

      @Override
      public BlockingQueue<Item> expelled() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void fading() {}

      @Override
      public long total() {
        return 0;
      }
    };
    assertThat(topK.contains("x")).isFalse();
  }
}
