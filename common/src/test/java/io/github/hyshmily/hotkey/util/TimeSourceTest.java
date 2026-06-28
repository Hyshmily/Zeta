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
package io.github.hyshmily.hotkey.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TimeSource} verifying idempotent start and correct time retrieval.
 */
class TimeSourceTest {

  @Test
  void start_shouldBeIdempotent() {
    assertThatCode(TimeSource::start).doesNotThrowAnyException();
    assertThatCode(TimeSource::start).doesNotThrowAnyException();
  }

  @Test
  void currentTimeMillis_withStart_shouldReturnPositiveValue() {
    TimeSource.start();
    assertThat(TimeSource.currentTimeMillis()).isPositive();
  }

  @Test
  void currentTimeMillis_shouldBeCloseToSystemTime() {
    TimeSource.start();
    long ts = TimeSource.currentTimeMillis();
    long sys = System.currentTimeMillis();
    assertThat(Math.abs(ts - sys)).isLessThan(10_000);
  }

  @Test
  void currentTimeMillis_withoutStart_shouldWork() {
    long ts = TimeSource.currentTimeMillis();
    assertThat(ts).isPositive();
  }
}
