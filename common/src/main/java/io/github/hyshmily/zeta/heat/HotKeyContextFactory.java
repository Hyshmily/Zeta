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

import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import io.github.hyshmily.zeta.cache.EntryView;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.QuietCache;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.policy.ResolvedNamespacePolicy;
import io.github.hyshmily.zeta.scheduler.WarmScheduler;
import io.github.hyshmily.zeta.scheduler.WarmSignal;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.util.Internal;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.Nullable;

/**
 * Creates the {@link HotKeyContext} for one applied decision.
 *
 * <p>Container wiring, not an extension point: autoconfiguration binds the framework's
 * collaborators once, and the decision listener calls {@link #create} per handler invocation. A
 * context belongs to exactly one decision and is never reused, so its event facts need no
 * synchronization.
 *
 * <p>It lives beside {@link HotKeyContext} rather than inside it so the public extension interface
 * exposes no internal collaborator.
 */
@Internal
@FunctionalInterface
public interface HotKeyContextFactory {
  /**
   * Creates the context for one applied decision, bound to the collaborators this factory captured.
   * Every call builds a new instance; a context is never cached or shared across decisions.
   *
   * @param decision the applied decision
   * @param newlyHot whether the decision changed the key from cold to hot
   * @return the context to hand to {@link HotKeyHandler}
   */
  HotKeyContext create(WorkerDecision decision, boolean newlyHot);

  /**
   * Binds the factory to the framework's collaborators.
   *
   * @param cache         the L1 engine
   * @param heatView      the heat query
   * @param policies      the namespace policy registry
   * @param warmScheduler the local renewal scheduler, or {@code null} when warming is unavailable
   * @param ttlPolicy     the shared TTL arithmetic for expiry judgments
   * @return the bound factory
   */
  static HotKeyContextFactory of(
    HotKeyCache cache,
    HeatView heatView,
    NamespacePolicyRegistry policies,
    @Nullable WarmScheduler warmScheduler,
    TtlPolicy ttlPolicy
  ) {
    return (decision, newlyHot) ->
      new DefaultContext(decision, newlyHot, cache, heatView, policies, warmScheduler, ttlPolicy);
  }
}

/**
 * The default guarded context handed to {@link HotKeyHandler}. Constructed only by
 * {@link HotKeyContextFactory#of} with the framework's collaborators.
 */
@Internal
record DefaultContext(
  WorkerDecision decision,
  boolean newlyHot,
  HotKeyCache cache,
  HeatView heatView,
  NamespacePolicyRegistry policies,
  @Nullable WarmScheduler warmScheduler,
  TtlPolicy ttlPolicy
) implements HotKeyContext {
  private static final CompletableFuture<Boolean> FALSE = CompletableFuture.completedFuture(false);

  /**
   * {@inheritDoc}
   *
   * <p>Read from the frozen decision, so it still answers after the entry is gone.
   */
  @Override
  public String key() {
    return decision.cacheKey();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Frozen with the decision, so a context a handler retained keeps answering for it.
   */
  @Override
  public boolean isNewlyHot() {
    return newlyHot;
  }

  /**
   * {@inheritDoc}
   *
   * <p>One clock sample drives the snapshot and both expiry judgments, and the read is quiet: a
   * handler's probe never counts as an access or reaches the frequency sketch.
   */
  @Override
  public @Nullable EntryView currentEntry() {
    return EntryView.snapshot(cache.table(), key(), ttlPolicy);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The same quiet table probe, judged against the hard deadline only; a soft-expired but
   * servable entry still answers {@code true}.
   */
  @Override
  public boolean hasLiveEntry() {
    return liveResident(key()) != null;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Decoded through {@link CacheKeysPolicy#logicalNamespaceOf(String)}, so a TYPED and a PLAIN
   * encoding of the same namespace answer the same name.
   */
  @Override
  public @Nullable String namespace() {
    return CacheKeysPolicy.logicalNamespaceOf(key());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Delegates to {@link HeatView}, which reads the local sketch and never the lease table.
   */
  @Override
  public boolean isLocalHot() {
    return heatView.isLocalHot(key());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Delegates to {@link HeatView}: without a Worker plane there is no lease table, and the
   * answer is {@code false} without a lookup.
   */
  @Override
  public boolean isClusterHot() {
    return heatView.isClusterHot(key());
  }

  /**
   * {@inheritDoc}
   *
   * <p>A namespace with no loader or with {@code prewarm = OFF} is refused before the TTL and heat
   * gates run, and every refusal shares one completed {@code false} future, so a refusal allocates
   * nothing. The trigger probe reads only the heat sources its mask allows.
   */
  @Override
  public CompletableFuture<Boolean> prewarmIfAbsent() {
    String key = key();
    ResolvedNamespacePolicy policy = policies.resolve(key);
    CacheLoader<String, ?> loader = policy.loader();
    if (loader == null || policy.prewarm() == HeatTrigger.OFF) {
      return FALSE;
    }
    return policy.ttl().prewarmable() && liveResident(key) == null && heatView.matches(policy.prewarm(), key)
      ? warmAsync(policy, loader)
      : FALSE;
  }

  /**
   * {@inheritDoc}
   *
   * <p>The residency read is a quiet pre-check; {@code warmAsync} re-checks it before starting the
   * load and reports what is resident when the load settles. The namespace's prewarm trigger is
   * never consulted.
   */
  @Override
  public CompletableFuture<Boolean> prewarmWith(CacheLoader<String, ?> loader) {
    Objects.requireNonNull(loader, "loader");
    String key = key();
    ResolvedNamespacePolicy policy = policies.resolve(key);
    return policy.ttl().prewarmable() && liveResident(key) == null ? warmAsync(policy, loader) : FALSE;
  }

  /**
   * {@inheritDoc}
   *
   * <p>A deployment without a scheduler answers {@code false} instead of throwing. The eligibility
   * judgment stays inside the scheduler, which is offered the resident entry with
   * {@link WarmSignal#HEAT}.
   */
  @Override
  public boolean armKeepWarm() {
    WarmScheduler scheduler = warmScheduler;
    if (scheduler == null) {
      return false;
    }
    CacheEntry resident = liveResident(key());
    return resident != null && scheduler.scheduleIfEligible(key(), resident, WarmSignal.HEAT);
  }

  /**
   * {@inheritDoc}
   *
   * <p>A no-op without a scheduler. The cold judgment stays inside the scheduler, so this method
   * cannot disagree with the policy that armed the task.
   */
  @Override
  public void cancelKeepWarmIfCold() {
    if (warmScheduler != null) {
      warmScheduler.cancelIfCold(key());
    }
  }

  /**
   * The resident entry when still servable, read quietly so an internal probe never counts as an
   * access; {@code null} when absent or past its hard deadline.
   */
  private @Nullable CacheEntry liveResident(String key) {
    CacheEntry resident = QuietCache.getIfPresent(cache.table(), key);
    return ttlPolicy.isLive(resident) ? resident : null;
  }

  /**
   * Joins the single flight that reloads the key under the namespace's resolved hard and soft TTL.
   * The key comes from the frozen decision, so this helper cannot warm a key other than the one the
   * context describes.
   *
   * <p>The cast narrows the loader to the {@code Object} the cache stores; it does not change what
   * the loader produces.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Boolean> warmAsync(ResolvedNamespacePolicy policy, CacheLoader<String, ?> loader) {
    return cache.warmAsync(
      key(),
      (CacheLoader<String, Object>) loader,
      policy.ttl().hardTtlMs(),
      policy.ttl().softTtlMs()
    );
  }
}
