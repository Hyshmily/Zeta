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
package io.github.hyshmily.zeta.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.sync.local.Batch;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Batch")
class BatchTest {

  @Test
  @DisplayName("a clear batch keeps every clear and carries no winner")
  void clearBatchKeepsEveryClear() {
    SyncMessage prefix = new SyncMessage(1L, "users\u0000", SyncType.INVALIDATE_PREFIX, 9L);
    SyncMessage all = new SyncMessage(2L, SyncMessage.CLEAR_ALL_BODY, SyncType.INVALIDATE_ALL, 5L);

    Batch batch = Batch.reduce(List.of(prefix, all));

    assertThat(batch).isInstanceOf(Batch.Clear.class);
    assertThat(batch.messages()).containsExactly(prefix, all);
  }

  @Test
  @DisplayName("a clear batch rejects a non-clear message")
  void clearBatchRejectsNonClear() {
    SyncMessage all = new SyncMessage(1L, SyncMessage.CLEAR_ALL_BODY, SyncType.INVALIDATE_ALL, 5L);
    SyncMessage put = new SyncMessage(2L, "key", SyncType.PUT, 6L);

    assertThatThrownBy(() -> Batch.reduce(List.of(all, put)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("clear batch");
  }

  @Test
  @DisplayName("a covered INVALIDATE forbids a soft-expire fallback")
  void coveredInvalidateForbidsSoftExpire() {
    SyncMessage invalidate = new SyncMessage(1L, "key", SyncType.INVALIDATE, 1L);
    SyncMessage refresh = new SyncMessage(2L, "key", SyncType.REFRESH, 2L);

    Batch.Value batch = (Batch.Value) Batch.reduce(List.of(invalidate, refresh));

    assertThat(batch.winner().type()).isEqualTo(SyncType.REFRESH);
    assertThat(batch.coveredInvalidate()).isSameAs(invalidate);
    assertThat(batch.allowSoftExpire()).isFalse();
  }

  @Test
  @DisplayName("a single-message batch carries no covered work")
  void singleMessageBatch() {
    SyncMessage message = new SyncMessage(1L, "key", SyncType.PUT, 3L);

    Batch.Value batch = (Batch.Value) Batch.reduce(List.of(message));

    assertThat(batch.messages()).containsExactly(message);
    assertThat(batch.winner()).isSameAs(message);
    assertThat(batch.covered()).isEmpty();
    assertThat(batch.coveredInvalidate()).isNull();
    assertThat(batch.allowSoftExpire()).isTrue();
  }
}
