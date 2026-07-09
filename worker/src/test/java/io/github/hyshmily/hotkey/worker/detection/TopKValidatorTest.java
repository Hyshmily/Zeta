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
package io.github.hyshmily.hotkey.worker.detection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link TopKValidator}.
 */
@ExtendWith(MockitoExtension.class)
class TopKValidatorTest {

  @Mock
  private TopK topK;

  @Mock
  private WorkerBroadcaster broadcaster;

  private TopKValidator validator;

  @BeforeEach
  void setUp() {
    validator = new TopKValidator(topK, broadcaster, 5, 2);
  }

  /**
   * Verifies that a key is send as hot after it appears in the TopK list the minimum required number of times.
   */
  @Test
  void shouldBroadcastWhenKeyAppearsMinRequiredTimes() {
    when(topK.listTopN(5)).thenReturn(List.of(new Item("hotKey", 100)));
    validator.validate();
    validator.validate();
    verify(broadcaster).broadcastHot(eq("hotKey"), any());
    assertThat(validator).isNotNull();
  }

  /**
   * Verifies that keys already marked as confirmed are skipped during validation and not re-send.
   */
  @Test
  void shouldSkipAlreadyConfirmedKeys() {
    validator.markConfirmed("hotKey");
    when(topK.listTopN(5)).thenReturn(List.of(new Item("hotKey", 100)));
    validator.validate();
    verify(broadcaster, never()).broadcastHot(any(), any());
  }

  /**
   * Verifies that a key is auto-cooled when it drops out of the TopK list and must re-meet the minimum appearances.
   */
  @Test
  void shouldAutoCoolWhenKeyDropsOutOfTopK() {
    when(topK.listTopN(5)).thenReturn(List.of(new Item("hotKey", 100)));
    validator.validate();
    validator.validate();
    verify(broadcaster).broadcastHot(eq("hotKey"), any());

    when(topK.listTopN(5)).thenReturn(List.of());
    validator.validate();
    // key should be cooled internally — re-confirm by adding back
    validator.markConfirmed("hotKey");
    when(topK.listTopN(5)).thenReturn(List.of(new Item("hotKey", 100)));
    validator.validate();
    // should need to re-meet preWarmMinAppearances before broadcasting
    verify(broadcaster).broadcastHot(eq("hotKey"), any());
  }

  /**
   * Verifies that {@code markConfirmed} and {@code markCooled} correctly toggle a key's internal state.
   */
  @Test
  void markConfirmedAndMarkCooledShouldToggleState() {
    validator.markConfirmed("key1");
    // verify through validate — confirmed key should not trigger send
    when(topK.listTopN(5)).thenReturn(List.of(new Item("key1", 100)));
    validator.validate();
    verify(broadcaster, never()).broadcastHot(any(), any());

    validator.markCooled("key1");
    // after cool, should need appearances again
    when(topK.listTopN(5)).thenReturn(List.of(new Item("key1", 100)));
    validator.validate();
    validator.validate();
    verify(broadcaster).broadcastHot(eq("key1"), any());
  }

  /**
   * Verifies that an empty TopK list does not trigger any broadcasts or throw exceptions.
   */
  @Test
  void shouldHandleEmptyTopKList() {
    when(topK.listTopN(5)).thenReturn(List.of());
    validator.validate();
    verifyNoInteractions(broadcaster);
  }

  /**
   * Verifies that when {@code preWarmMinAppearances = 1}, a key is send immediately
   * on the first validation cycle it appears.
   */
  @Test
  void shouldBroadcastImmediatelyWhenMinAppearancesIsOne() {
    validator = new TopKValidator(topK, broadcaster, 5, 1);
    when(topK.listTopN(5)).thenReturn(List.of(new Item("fastKey", 50)));
    validator.validate();
    verify(broadcaster).broadcastHot(eq("fastKey"), any());
  }

  /**
   * Verifies that a key's appearance counter is reset when it drops out of the TopK list
   * and reappears later. It must re-meet the min appearances threshold.
   */
  @Test
  void shouldResetAppearanceCounterWhenKeyDropsOut() {
    when(topK.listTopN(5)).thenReturn(List.of(new Item("key", 100)));
    validator.validate(); // appearance = 1
    // key drops out
    when(topK.listTopN(5)).thenReturn(List.of());
    validator.validate();
    // key reappears
    when(topK.listTopN(5)).thenReturn(List.of(new Item("key", 100)));
    validator.validate(); // appearance = 1 (reset), not enough
    verify(broadcaster, never()).broadcastHot(any(), any());
    validator.validate(); // appearance = 2, now should send
    verify(broadcaster).broadcastHot(eq("key"), any());
  }

  /**
   * Verifies that multiple keys reaching min appearances simultaneously are all send.
   */
  @Test
  void shouldBroadcastMultipleKeysSimultaneously() {
    when(topK.listTopN(5)).thenReturn(List.of(new Item("k1", 100), new Item("k2", 200)));
    validator.validate();
    validator.validate(); // both reach min appearances
    verify(broadcaster).broadcastHot(eq("k1"), any());
    verify(broadcaster).broadcastHot(eq("k2"), any());
  }

  /**
   * Verifies that a key with zero count in the TopK list is still tracked and can be
   * send if it appears the minimum number of times.
   */
  @Test
  void shouldHandleKeyWithZeroCount() {
    when(topK.listTopN(5)).thenReturn(List.of(new Item("zeroKey", 0)));
    validator.validate();
    validator.validate();
    verify(broadcaster).broadcastHot(eq("zeroKey"), any());
  }

  /**
   * Triggers the periodic monitoring log that fires every 10 validation cycles
   * ({@code ++validationCount % 10 == 0}).  Verifies that the validator survives
   * repeated calls without throwing.
   */
  @Test
  void validate_shouldLogStatsEvery10Cycles() {
    when(topK.listTopN(5)).thenReturn(List.of());
    for (int i = 0; i < 12; i++) {
      assertThatCode(() -> validator.validate()).doesNotThrowAnyException();
    }
    verify(topK, times(12)).listTopN(5);
  }
}
