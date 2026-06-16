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
package io.github.hyshmily.hotkey.reporting;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.reporting.ReportMessage;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ReportMessage} record creation and field access.
 */
class ReportMessageTest {

  /**
   * Verifies creating a ReportMessage with app name, timestamp, and key counts.
   */
  @Test
  void shouldCreateReportMessage() {
    ReportMessage msg = new ReportMessage("app1", 1000L, Map.of("key1", 5L, "key2", 3L));
    assertThat(msg.appName()).isEqualTo("app1");
    assertThat(msg.timestamp()).isEqualTo(1000L);
    assertThat(msg.counts()).hasSize(2);
  }

  @Test
  void emptyCountsMap_shouldBeAccepted() {
    ReportMessage msg = new ReportMessage("app1", 1000L, Map.of());
    assertThat(msg.counts()).isEmpty();
  }

  @Test
  void nullAppName_shouldBeAccepted() {
    ReportMessage msg = new ReportMessage(null, 1000L, Map.of("k", 1L));
    assertThat(msg.appName()).isNull();
  }

  @Test
  void nullCountsMap_shouldBeAccepted() {
    ReportMessage msg = new ReportMessage("app1", 1000L, null);
    assertThat(msg.counts()).isNull();
  }

  @Test
  void negativeTimestamp_shouldBeAccepted() {
    ReportMessage msg = new ReportMessage("app1", -1L, Map.of("k", 1L));
    assertThat(msg.timestamp()).isNegative();
  }

  @Test
  void equalsAndHashCode_shouldWork() {
    ReportMessage a = new ReportMessage("app", 1L, Map.of("k", 1L));
    ReportMessage b = new ReportMessage("app", 1L, Map.of("k", 1L));
    ReportMessage c = new ReportMessage("app", 1L, Map.of("k", 2L));
    assertThat(a).isEqualTo(b);
    assertThat(a).hasSameHashCodeAs(b);
    assertThat(a).isNotEqualTo(c);
  }

  @Test
  void toString_shouldNotBeNull() {
    ReportMessage msg = new ReportMessage("app", 1L, Map.of("k", 1L));
    assertThat(msg.toString()).isNotNull();
  }

  @Test
  void veryLongAppName_shouldBeAccepted() {
    String longName = "a".repeat(10_000);
    ReportMessage msg = new ReportMessage(longName, 1L, Map.of("k", 1L));
    assertThat(msg.appName()).isEqualTo(longName);
  }
}
