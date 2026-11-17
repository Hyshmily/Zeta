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
package io.github.hyshmily.zeta.sync.local;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One granted sync batch, classified by the operation family it carries.
 *
 * <p>The ordered dispatcher hands every queued message of one key (or of the clear
 * channel) to the framework together. A value batch is reduced so only the
 * highest-version message can be observed after the batch: a superseded PUT never pays
 * its payload deserialization, a superseded REFRESH never runs the loader, and a
 * superseded INVALIDATE never takes the bin lock. A clear batch keeps every message:
 * each table-wide or prefix watermark affects a different key set.
 *
 * <p>{@link Value#covered()} lists every single-key message the winner superseded — PUT,
 * REFRESH and INVALIDATE; the framework reports each through {@link SyncHook#onSkipped} with
 * {@link SyncSkipReason#COVERED_BY_NEWER_EVENT}, so every message still receives one terminal
 * callback.
 * {@link Value#coveredInvalidate()} is the highest-version INVALIDATE the winner covered:
 * a value fallback that has to evict still hard-removes a Worker-managed entry, because the
 * covered removal already condemned that value, and a winning value message the policy refuses
 * still leaves the covered removal to apply.
 */
public sealed interface Batch permits Batch.Value, Batch.Clear {
  /** The granted messages, in submission order; never empty. */
  List<SyncMessage> messages();

  /**
   * A single-key batch: PUT, REFRESH or INVALIDATE, reduced to one effective message.
   *
   * @param messages          the granted batch, in submission order; never empty
   * @param winner            the highest-version message; never null and never a clear
   * @param covered           the superseded single-key messages; never null
   * @param coveredInvalidate the highest superseded INVALIDATE, or {@code null}
   */
  record Value(
    List<SyncMessage> messages,
    SyncMessage winner,
    List<SyncMessage> covered,
    @Nullable SyncMessage coveredInvalidate
  ) implements Batch {
    public Value {
      messages = List.copyOf(messages);
      covered = List.copyOf(covered);
      if (messages.isEmpty()) {
        throw new IllegalArgumentException("a sync batch must not be empty");
      }
      Objects.requireNonNull(winner, "winner must not be null");
      if (winner.type().isClear()) {
        throw new IllegalArgumentException("a value batch cannot carry a clear winner");
      }
      for (SyncMessage message : covered) {
        if (message.type().isClear()) {
          throw new IllegalArgumentException("only single-key messages can be covered: " + message.type());
        }
      }
    }

    /**
     * Whether a load-failure fallback may soft-expire a Worker-managed entry. A batch
     * that carried an explicit INVALIDATE — whether the winner or a covered message —
     * must hard-remove it instead.
     *
     * @return {@code true} when no INVALIDATE participated in the batch
     */
    public boolean allowSoftExpire() {
      return coveredInvalidate == null && winner.type() != SyncType.INVALIDATE;
    }
  }

  /**
   * A clear batch: every message is a table-wide or prefix clear. The context applies
   * the whole list in one sweep, so the batch carries no winner.
   *
   * @param messages the granted clear messages, in submission order; never empty
   */
  record Clear(List<SyncMessage> messages) implements Batch {
    public Clear {
      messages = List.copyOf(messages);
      if (messages.isEmpty()) {
        throw new IllegalArgumentException("a clear batch must not be empty");
      }
      for (SyncMessage message : messages) {
        if (!message.type().isClear()) {
          throw new IllegalArgumentException("a clear batch cannot carry " + message.type());
        }
      }
    }
  }

  /**
   * Reduces a granted batch to its effective shape.
   *
   * <p>A higher version always wins. At the same version INVALIDATE outranks PUT/REFRESH,
   * and PUT outranks REFRESH: the value-carrying PUT avoids the loader read a REFRESH would
   * trigger at the same data stamp. A clear batch keeps every message: each table-wide or
   * prefix watermark affects a different key set, and the context applies them in one sweep.
   *
   * @param messages the granted batch, in submission order; never empty
   * @return the reduced batch
   */
  static Batch reduce(List<SyncMessage> messages) {
    if (messages.isEmpty()) {
      throw new IllegalArgumentException("a sync batch must not be empty");
    }

    SyncMessage first = messages.get(0);
    if (first.type().isClear()) {
      return new Clear(messages);
    }

    int size = messages.size();
    if (size == 1) {
      return new Value(messages, first, List.of(), null);
    }

    SyncMessage winner = first;
    SyncMessage coveredInvalidate = null;
    List<SyncMessage> covered = new ArrayList<>(size - 1);
    for (int i = 1; i < size; i++) {
      SyncMessage candidate = messages.get(i);
      SyncMessage loser;
      if (outranks(candidate, winner)) {
        loser = winner;
        winner = candidate;
      } else {
        loser = candidate;
      }
      if (loser.type() == SyncType.INVALIDATE) {
        // Every covered INVALIDATE keeps its own terminal callback; the highest one also
        // supplies the value-path fallback removal and forbids soft expiry.
        covered.add(loser);
        if (coveredInvalidate == null || loser.version() > coveredInvalidate.version()) {
          coveredInvalidate = loser;
        }
      } else if (loser.type() == SyncType.REFRESH || loser.type() == SyncType.PUT) {
        covered.add(loser);
      }
    }
    return new Value(messages, winner, covered, coveredInvalidate);
  }

  /** Whether {@code candidate} replaces {@code current} as the batch winner. */
  private static boolean outranks(SyncMessage candidate, SyncMessage current) {
    if (candidate.version() != current.version()) {
      return candidate.version() > current.version();
    }
    if (candidate.type() == SyncType.INVALIDATE) {
      return current.type() != SyncType.INVALIDATE;
    }
    return candidate.type() == SyncType.PUT && current.type() == SyncType.REFRESH;
  }
}
