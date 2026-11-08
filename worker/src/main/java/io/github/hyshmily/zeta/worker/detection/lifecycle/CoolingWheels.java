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

/**
 * The striped set of cool-down wheels: one independent wheel and one advancing thread per
 * configured cooling thread.
 *
 * <p>A key is assigned to a wheel by its hash, so scheduling and evaluation of one key are always
 * serialized by that wheel while different wheels advance in parallel. Each wheel owns its share of
 * the links, locks and wakeups; there is no shared queue (design decisions: worker cooling).
 */
public final class CoolingWheels {

  private final TimerWheel[] wheels;

  /**
   * Creates the striped wheels.
   *
   * @param count number of advancing threads (one wheel each), at least 1
   */
  public CoolingWheels(int count) {
    int wheelCount = Math.max(1, count);
    this.wheels = new TimerWheel[wheelCount];
    for (int i = 0; i < wheelCount; i++) {
      wheels[i] = new TimerWheel();
    }
  }

  /**
   * Selects the wheel that owns a key.
   *
   * @param spreadHash the key's spread hash, shared with the registry's stripe selection
   * @return the owning wheel
   */
  TimerWheel select(int spreadHash) {
    return wheels[(spreadHash & 0x7fffffff) % wheels.length];
  }

  /**
   * The wheel a thread advances.
   *
   * @param index the wheel index, below {@link #size()}
   * @return the wheel
   */
  TimerWheel wheel(int index) {
    return wheels[index];
  }

  /** @return the number of wheels (and advancing threads) */
  public int size() {
    return wheels.length;
  }

  /** @return checks currently linked across every wheel */
  public int scheduledCount() {
    int total = 0;
    for (TimerWheel wheel : wheels) {
      total += wheel.scheduledCount();
    }
    return total;
  }

  /** Wakes every advancing thread; used by shutdown. */
  void signalAll() {
    for (TimerWheel wheel : wheels) {
      wheel.signal();
    }
  }
}
