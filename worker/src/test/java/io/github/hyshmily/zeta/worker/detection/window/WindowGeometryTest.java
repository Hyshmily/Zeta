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
package io.github.hyshmily.zeta.worker.detection.window;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** The public constructor must uphold the mask invariants the detector relies on. */
class WindowGeometryTest {

  @Test
  void theCanonicalConstructorRejectsAValueTheFactoryNeverProduces() {
    assertThatThrownBy(() -> new WindowGeometry(3, 1, 3, 3))
      .as("a non-power-of-two slice count breaks the ring mask")
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new WindowGeometry(0, 1, 0, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new WindowGeometry(4, 0, 0, 4))
      .as("a zero slice would divide by zero")
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new WindowGeometry(4, 1, 5, 5))
      .as("the effective window is derived from the aligned slices")
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new WindowGeometry(2, Long.MAX_VALUE, -2L, 0L))
      .as("an overflowing effective window cannot be published")
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aConsistentGeometryIsAccepted() {
    WindowGeometry geometry = new WindowGeometry(4, 2, 8, 8);

    assertThat(geometry.alignedSlices()).isEqualTo(4);
    assertThat(geometry.windowMs()).isEqualTo(8L);
  }
}
