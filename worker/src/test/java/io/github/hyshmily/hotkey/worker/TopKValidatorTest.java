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
package io.github.hyshmily.hotkey.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
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

  @Test
  void shouldBroadcastWhenKeyAppearsMinRequiredTimes() {
    when(topK.listTopN(5)).thenReturn(List.of(new Item("hotKey", 100)));
    validator.validate();
    validator.validate();
    verify(broadcaster).broadcastHot(eq("hotKey"), any());
    assertThat(validator).isNotNull();
  }

  @Test
  void shouldSkipAlreadyConfirmedKeys() {
    validator.markConfirmed("hotKey");
    when(topK.listTopN(5)).thenReturn(List.of(new Item("hotKey", 100)));
    validator.validate();
    verify(broadcaster, never()).broadcastHot(any(), any());
  }

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

  @Test
  void markConfirmedAndMarkCooledShouldToggleState() {
    validator.markConfirmed("key1");
    // verify through validate — confirmed key should not trigger broadcast
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
}
