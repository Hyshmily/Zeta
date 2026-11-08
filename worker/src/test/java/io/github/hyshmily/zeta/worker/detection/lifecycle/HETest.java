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
package io.github.hyshmily.zeta.worker.detection.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** The public dwell accessor hides the internal retirement tombstone. */
class HETest {

  @Test
  void aRetiredEntryReportsNotBelowInsteadOfTheInternalSentinel() {
    HE entry = HE.sentinel();

    entry.retire();

    assertThat(entry.retired()).isTrue();
    assertThat(entry.belowSince()).as("a reader must not see Long.MIN_VALUE as a dwell start").isEqualTo(HE.NOT_BELOW);
  }
}
