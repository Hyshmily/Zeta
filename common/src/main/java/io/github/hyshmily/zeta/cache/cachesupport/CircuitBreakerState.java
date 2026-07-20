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
package io.github.hyshmily.zeta.cache.cachesupport;

import lombok.Getter;

/**
 * Circuit breaker lifecycle states.
 *
 * <p>CLOSED — normal operation, requests pass through.
 * OPEN — fast-fail, requests are blocked.
 * HALF_OPEN — probe state, limited requests allowed to test recovery.
 */
@Getter
public enum CircuitBreakerState {
  CLOSED("close"),
  HALF_OPEN("half-open"),
  OPEN("open");

  private final String state;

  CircuitBreakerState(String state) {
    this.state = state;
  }
}
