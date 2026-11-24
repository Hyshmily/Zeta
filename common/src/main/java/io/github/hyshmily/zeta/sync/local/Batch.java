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

import io.github.hyshmily.zeta.util.Internal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

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
 * a value fallback that has to evict still hard-removes a cluster-hot entry, because the
 * covered removal already condemned that value, and a winning value message the policy refuses
 * still leaves the covered removal to apply.
 */
@Internal
public sealed interface Batch permits Batch.Value, Batch.Clear {
  /**
   * A single-key batch: PUT, REFRESH or INVALIDATE, reduced to one effective message. The winner and
   * its covered messages already account for every granted message, so the batch keeps no third
   * copy of the list.
   *
   * @param winner            the highest-version message; never null and never a clear
   * @param covered           the superseded single-key messages; never null
   * @param coveredInvalidate the highest superseded INVALIDATE, or {@code null}
   */
  record Value(
    SyncMessage winner,
    List<SyncMessage> covered,
    @Nullable SyncMessage coveredInvalidate
  ) implements Batch {
    public Value {
      covered = List.copyOf(covered);
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
      return new Value(first, List.of(), null);
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
      // Every covered message keeps its own terminal callback; the highest covered INVALIDATE
      // also supplies the value-path fallback removal and forbids soft expiry.
      covered.add(loser);
      if (
        loser.type() == SyncType.INVALIDATE &&
        (coveredInvalidate == null || loser.version() > coveredInvalidate.version())
      ) {
        coveredInvalidate = loser;
      }
    }
    return new Value(winner, covered, coveredInvalidate);
  }

  /**
   * Whether {@code candidate} replaces {@code current} as the batch winner. Version dominates; an
   * equal version is broken by sync-type priority so a control message with the same stamp wins
   * deterministically rather than by arrival order. Strict comparison on priority keeps the first
   * of two equal-priority messages as the winner.
   */
  private static boolean outranks(SyncMessage candidate, SyncMessage current) {
    return candidate.version() != current.version()
      ? candidate.version() > current.version()
      : candidate.type().priority() > current.type().priority();
  }
}
