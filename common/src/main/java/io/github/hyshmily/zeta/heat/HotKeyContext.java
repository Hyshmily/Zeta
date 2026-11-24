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
package io.github.hyshmily.zeta.heat;

import io.github.hyshmily.zeta.cache.EntryView;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.Nullable;

/**
 * The framework-guarded operations a {@link HotKeyHandler} may request for one applied decision,
 * plus the read-only facts that decision settled.
 *
 * <p><b>Lifecycle.</b> One instance belongs to one applied HOT or COOL and is created on the
 * ordered decision dispatcher thread; it is never reused or cached across decisions. A handler may
 * keep the reference, and the facts it exposes stay frozen at that decision, while the entry and
 * heat queries answer for the moment they are called.
 *
 * <p><b>What it cannot do.</b> The context has no write handle: it cannot put, evict or rewrite a
 * TTL, and no operation records an access, so a background load or a renewal never manufactures
 * heat on the node that spent it. Every action re-checks the namespace policy itself; a caller
 * cannot bypass a {@code keepWarm = OFF} declaration through this interface.
 *
 * <p><b>Where the policy lives.</b> Eligibility is answered by the action, not by a companion
 * predicate: {@link #armKeepWarm()} returns whether a task was armed and {@link #prewarmIfAbsent()}
 * completes with whether a value was stored, so a caller branches on the outcome instead of
 * re-deriving the framework's gates and drifting from them.
 *
 * <p><b>Threading.</b> Every method runs on the decision dispatcher thread (or on the thread that
 * performs the cancellation, for {@link #cancelKeepWarmIfCold()}). The loads are asynchronous and
 * return a future; a handler must not block on I/O here.
 */
public interface HotKeyContext {
  /**
   * The physical key this decision is about: namespace, delimiter, logical key. Use
   * {@link #namespace()} for a name comparison instead of parsing this string.
   *
   * @return the physical key
   */
  String key();

  /**
   * The decision that produced this context, as applied: its kind, decision version, epoch and
   * issuing Worker node. The decision version orders Worker broadcasts and is orthogonal to
   * {@link EntryView#dataVersion()}, which the receiving node allocates for the entry itself.
   * Read-only; it carries no payload and no write capability.
   *
   * @return the applied decision
   */
  WorkerDecision decision();

  /**
   * Whether this decision changed the key from cold to hot. {@code false} for a COOL (which grants
   * nothing) and for a repeated HOT over a live lease, so a handler uses it to avoid spending a
   * load on a rebroadcast.
   *
   * @return whether this grant is the key's first
   */
  boolean isNewlyHot();

  /**
   * A snapshot of the resident entry, or {@code null} when none is resident. A resident past its
   * hard deadline is still returned, with {@code logicallyExpired} set, so a handler can tell
   * "absent" from "present but unusable"; {@link #hasLiveEntry()} is the servable-only form.
   *
   * <p>The snapshot is advisory: the entry may change before an action applies. {@code value()} is
   * the entry's own reference, not a copy — treat it as read-only.
   *
   * @return the entry snapshot, or {@code null}
   */
  @Nullable
  EntryView currentEntry();

  /**
   * Whether a servable entry is resident: present and not past its hard deadline. This is the
   * precondition for {@link #armKeepWarm()}; a logically expired entry is not live, even though
   * {@link #currentEntry()} still returns it.
   *
   * @return whether the key has a live resident
   */
  boolean hasLiveEntry();

  /**
   * The logical name of the key's namespace with its encoding delimiter removed, so a TYPED key
   * ({@code users\u0000u-1}) and a PLAIN key ({@code users:u-1}) both answer {@code users}. The
   * name is compared as a plain string; a caller never has to know which layer produced the key.
   *
   * @return the namespace name, or {@code null} when the key carries none
   */
  @Nullable
  String namespace();

  /**
   * Whether this key belongs to {@code namespace}, as {@link #namespace()} reports it. The
   * null-safe form of {@code namespace().equals(...)}: a key without a namespace answers
   * {@code false} instead of throwing.
   *
   * @param namespace the logical namespace name, exactly as {@link #namespace()} returns it
   *                  (non-null, non-blank)
   * @return whether the key's namespace equals {@code namespace}
   * @throws IllegalArgumentException when {@code namespace} is blank
   */
  default boolean inNamespace(String namespace) {
    Objects.requireNonNull(namespace, "namespace must not be null");
    if (namespace.isBlank()) {
      throw new IllegalArgumentException("namespace must not be blank");
    }
    return namespace.equals(namespace());
  }

  /**
   * Whether this instance's local TopK currently holds the key. Cheap: it reads the local sketch
   * and never touches the lease table. Local membership is its own renewal evidence.
   *
   * @return whether the local TopK holds the key
   */
  boolean isLocalHot();

  /**
   * Whether a valid Worker lease currently holds the key. Reads the lease table, so it is more
   * expensive than {@link #isLocalHot()} and can be {@code false} for a genuinely hot key when the
   * deployment has no Worker plane at all — heat is then local-only, and a caller must not read
   * this as "the cluster says cold".
   *
   * @return whether a live cluster lease holds the key
   */
  boolean isClusterHot();

  /**
   * Whether either heat source holds the key. The order is the contract: the local sketch is
   * checked first and short-circuits, so a locally hot key never pays for a lease-table lookup.
   *
   * @return whether the key is locally or cluster hot
   */
  default boolean isHot() {
    return isLocalHot() || isClusterHot();
  }

  /**
   * Load a missing entry through the namespace's declared loader and static TTL, and store it. The
   * framework allocates the pre-read stamp, applies the invalidation and version guards, and never
   * records an access for the load.
   *
   * <p>The call is refused (the future completes with {@code false}) when the key already has a
   * live resident, the namespace declares no loader, its TTL is dynamic (no static baseline for a
   * guessed lifetime) or its prewarm trigger does not accept the key. It never arms keep-warm: a
   * resident key that should be renewed is {@link #armKeepWarm()}'s job.
   *
   * @return a future completing with whether a value was stored
   */
  CompletableFuture<Boolean> prewarmIfAbsent();

  /**
   * Load a missing entry through a caller-supplied warmable loader and store it under the
   * namespace's static TTL. The namespace's prewarm trigger is deliberately not consulted: a
   * handler that reaches this method has already decided to spend the load.
   *
   * <p>The loader must be safe to call without the original invocation (no request-scoped state)
   * and must not block the dispatcher thread. It receives the <b>physical</b> key — namespace,
   * delimiter and logical key ({@code users\u0000u-1}) — not the decoded argument of the original
   * call, so a loader that needs the logical id must decode it or capture it from the invocation.
   * The call is refused when the key already has a live resident or the namespace's TTL is dynamic.
   *
   * <p>A refusal, a loader failure and a circuit-breaker rejection all complete the future with
   * {@code false}; the future is not the error channel, so a handler needs no exception handling.
   *
   * @param loader the warmable loader; it is invoked with the physical key
   * @return a future completing with whether a value was stored
   */
  CompletableFuture<Boolean> prewarmWith(CacheLoader<String, ?> loader);

  /**
   * Arm this key's local renewal when it is resident, the namespace has a loader, and its
   * keep-warm trigger accepts the key. Arming spends no load and records no access: it only
   * schedules the renewal that will later reload the value before its deadline.
   *
   * <p>This is the explicit form of what a HOT used to do behind {@code prewarmIfAbsent()}: call it
   * on a resident key to start the renewal cycle, and skip it for a key this instance does not
   * serve. A key that is already armed stays armed; the call returns {@code true} without
   * resetting the renewal deadline.
   *
   * @return {@code true} when a renewal task is armed for the key after the call; {@code false} when
   *         the key is not resident, the namespace declares no loader, its keep-warm trigger is
   *         absent or does not accept the key at this moment, the global warming switch is off, or
   *         the task budget is exhausted
   */
  boolean armKeepWarm();

  /**
   * Drop this key's scheduled renewal when the namespace's keep-warm trigger no longer accepts it.
   * A key that is still locally hot, or still covered by a live cluster lease the trigger accepts,
   * keeps its task.
   *
   * <p>The judgment is the framework's, not the caller's: it resolves the namespace policy and
   * re-probes only the heat sources that policy opted into. The cancellation is one-shot — the next
   * read, write or admission that satisfies the trigger arms a fresh task — so this is not a way to
   * disable renewal for a key; that is {@code keepWarm = OFF} or the runtime warm switch.
   */
  void cancelKeepWarmIfCold();
}
