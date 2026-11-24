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
package io.github.hyshmily.zeta.scheduler;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.ReadObserver;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.QuietCache;
import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.heat.HeatView;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.policy.ResolvedNamespacePolicy;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.Set;
import java.util.concurrent.*;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;

/**
 * Local hard-renewal scheduler.
 *
 * <p>Heat only decides whether a background load may be spent. A renewal replaces the value with
 * a freshly loaded one and re-arms the entry's own declared hard/soft durations; it never extends
 * the old value's life. A failed renewal leaves every deadline untouched and waits for the normal
 * expiry path.
 *
 * <p><b>Local-read gate.</b> A cluster lease alone is not enough to keep renewing: a node may
 * receive HOT for a key it never serves, and a lost COOL never arrives. A task scheduled by a
 * cluster signal therefore requires a real read observed since that warm cycle began. Local heat
 * is its own evidence.
 */
@Internal
public final class KeepWarm implements WarmScheduler, ReadObserver, DisposableBean {

  /** Never schedule a renewal after this point: it would only re-load a dead key. */
  private static final long MIN_INTERVAL_MS = 200L;

  /** Earliest lead taken off the deadline when the configured lead is smaller. */
  private static final long LEAD_FLOOR_MS = 1_000L;

  private final HeatView heatView;
  private final TtlPolicy ttlPolicy;
  private final StampClock stampClock;
  private final ZetaProperties.Warm config;
  private final BackgroundRefresher refresher;
  private final Cache<String, CacheEntry> cache;
  private final NamespacePolicyRegistry policies;
  private final ScheduledExecutorService scheduler;
  private final ConcurrentHashMap<String, WarmTask> tasks = new ConcurrentHashMap<>();
  private volatile boolean closed;

  /**
   * @param cache      the L1 table
   * @param heatView   the heat query
   * @param policies   the namespace policy registry
   * @param refresher  the shared refresh executor
   * @param scheduler  the scheduler for due tasks
   * @param stampClock the data-stamp allocator for renewal loads
   * @param ttlPolicy  the shared TTL arithmetic
   * @param properties the live configuration
   */
  public KeepWarm(
    Cache<String, CacheEntry> cache,
    HeatView heatView,
    NamespacePolicyRegistry policies,
    BackgroundRefresher refresher,
    ScheduledExecutorService scheduler,
    StampClock stampClock,
    TtlPolicy ttlPolicy,
    ZetaProperties properties
  ) {
    this.cache = cache;
    this.heatView = heatView;
    this.policies = policies;
    this.refresher = refresher;
    this.scheduler = scheduler;
    this.stampClock = stampClock;
    this.ttlPolicy = ttlPolicy;
    this.config = properties.getWarm();
    if (scheduler instanceof ScheduledThreadPoolExecutor stpe) {
      // A write, a peer PUT, a soft refresh or an invalidation replaces the task; without this the
      // cancelled future, its task and its key stay in the delay queue until the original deadline.
      stpe.setRemoveOnCancelPolicy(true);
    }
  }

  /**
   * Offers one resident entry to the renewal scheduler. The key is coalesced to one task; a
   * newer data version replaces it, an equal version only refreshes the read evidence.
   *
   * @param key    the physical key
   * @param entry  the resident entry
   * @param signal why the event was raised
   * @return whether a renewal task is armed for the key after this call
   */
  @Override
  public boolean scheduleIfEligible(String key, CacheEntry entry, WarmSignal signal) {
    return scheduleIfEligible(key, entry, signal, TimeSource.monotonicMillis());
  }

  private boolean scheduleIfEligible(String key, CacheEntry entry, WarmSignal signal, long nowMono) {
    if (inactive()) {
      return false;
    }
    ResolvedNamespacePolicy policy = policies.resolve(key);
    // The loader check comes first: most namespaces never declare one, and the heat query must
    // not run for a namespace that cannot spend a load at all.
    if (policy.loader() == null || !matchesKeepWarm(policy, key)) {
      return false;
    }
    return scheduleWithPolicy(key, entry, signal, nowMono);
  }

  /**
   * Arm or refresh the key's renewal task. The caller has validated that the namespace has a
   * loader and that its trigger accepts the key, so the heat query does not run twice. The
   * already-armed check is lock-free: a read tide touches many keys, and an equal or newer task
   * only needs its read evidence refreshed.
   *
   * @param key     the physical key
   * @param entry   the resident entry
   * @param signal  why the event was raised
   * @param nowMono one monotonic sample for this event
   * @return whether a renewal task is armed for the key after this call
   */
  private boolean scheduleWithPolicy(String key, CacheEntry entry, WarmSignal signal, long nowMono) {
    long version = entry.getDataVersion();
    WarmTask current = tasks.get(key);
    if (current != null && current.dataVersion >= version) {
      if (current.dataVersion == version && signal.isReadEvidence()) {
        current.lastReadMonoMs = nowMono;
      }
      return true;
    }
    if (current == null && tasks.size() >= config.getKeepWarm().getMaxTasks()) {
      return false;
    }
    long delayMs = computeDelayMs(entry);
    if (delayMs < 0L) {
      // The new version declares a lifetime that must not be renewed (permanent, or a window
      // below the floor). Keeping the older task would renew a regime that no longer exists.
      if (current != null && tasks.remove(key, current)) {
        current.cancel();
      }
      return false;
    }

    // The compute result is the task stored for the key, so a non-null result means the key is
    // armed: either the existing task was kept or a fresh one replaced it.
    WarmTask armed = tasks.compute(key, (k, existing) -> {
      // destroy() may have run between the entry check and this compute: never arm a task on a
      // scheduler that is shutting down.
      if (closed) {
        if (existing != null) {
          existing.cancel();
        }
        return null;
      }

      if (existing == null) {
        if (tasks.size() >= config.getKeepWarm().getMaxTasks()) {
          return null;
        }
      } else if (existing.dataVersion > version) {
        return existing;
      } else if (existing.dataVersion == version) {
        if (signal.isReadEvidence()) {
          existing.lastReadMonoMs = nowMono;
        }
        return existing;
      }
      // A write replaces the resident value but is not evidence that this instance serves it. Carry
      // the previous cycle's observation and its start forward only while that observation is still
      // fresh: a long-lived write stream must not keep a read from hours ago alive.
      long inheritedRead = Long.MIN_VALUE;
      long armedAt = nowMono;
      if (
        existing != null &&
        signal == WarmSignal.WRITE &&
        existing.lastReadMonoMs >= existing.armedAtMonoMs &&
        nowMono - existing.lastReadMonoMs <= delayMs
      ) {
        inheritedRead = existing.lastReadMonoMs;
        armedAt = existing.armedAtMonoMs;
      }
      if (existing != null) {
        existing.cancel();
      }
      WarmTask task = new WarmTask(k, version, armedAt, signal.isReadEvidence() ? nowMono : inheritedRead);
      try {
        task.future = scheduler.schedule(() -> fire(task), delayMs, TimeUnit.MILLISECONDS);
        return task;
      } catch (RejectedExecutionException e) {
        return null;
      }
    });
    // destroy() may have cleared the map while this compute was running; a task armed after that
    // clear would otherwise sit in the delay queue and in tasks until it expires.
    if (closed && armed != null && tasks.remove(key, armed)) {
      armed.cancel();
      return false;
    }
    return armed != null;
  }

  /**
   * Whether every entry point can answer without touching its key: shut down, disabled, or no
   * namespace in the application keeps warm. The last check is the frozen registry's aggregate, so
   * a deployment that never opts in pays one boolean read per tide instead of one policy lookup
   * and heat probe per read key.
   */
  private boolean inactive() {
    return closed || !config.isEnabled() || !policies.hasActiveKeepWarm();
  }

  /**
   * Called by the detector when a key newly enters the local TopK.
   *
   * @param key the admitted key
   */
  public void onLocalAdmitted(String key) {
    if (inactive()) {
      return;
    }
    ResolvedNamespacePolicy policy = policies.resolve(key);
    if (policy.loader() == null || !policy.keepWarm().allowsLocal()) {
      return;
    }
    CacheEntry resident = liveResident(key);
    if (resident != null) {
      scheduleWithPolicy(key, resident, WarmSignal.LOCAL_ADMISSION, TimeSource.monotonicMillis());
    }
  }

  /**
   * Refreshes read evidence for scheduled tasks and closes the "resident before cluster HOT"
   * gap: a cluster-hot resident that has just been read gets a task without touching the read
   * path.
   *
   * <p>A task bound to an older data version is re-armed against the resident. A soft refresh or a
   * peer write can replace the entry with a shorter TTL while the old task still waits for the old
   * deadline; re-arming is the only point that sees the new version before that deadline.
   *
   * @param keys      the keys read in the tide
   * @param nowMonoMs one monotonic sample for the tide
   */
  @Override
  public void onReads(Set<String> keys, long nowMonoMs) {
    if (inactive() || keys.isEmpty()) {
      return;
    }
    // Do not return on an empty task map or a missing cluster lease: a resident key can be read
    // for a while before either signal arrives, and this tide is the only place that can close
    // that gap without a read-path probe.
    int maxTasks = config.getKeepWarm().getMaxTasks();
    // One size sample per tide; the loop maintains an approximation and every schedule still
    // re-checks the real bound under the map's bin lock.
    int approxSize = tasks.size();
    for (String key : keys) {
      WarmTask task = tasks.get(key);
      if (task != null) {
        task.lastReadMonoMs = nowMonoMs;
        CacheEntry raw = QuietCache.getIfPresent(cache, key);
        if (raw == null) {
          // The resident is gone; a task bound to it has nothing left to renew.
          if (tasks.remove(key, task)) {
            approxSize--;
            task.cancel();
          }
          continue;
        }
        // A logically expired resident may be the subject of a renewal already in flight, so only
        // a newer live version re-arms or retires the task from the tide.
        if (raw.getDataVersion() > task.dataVersion && !ttlPolicy.isLogicallyExpired(raw)) {
          ResolvedNamespacePolicy policy = policies.resolve(key);
          if (policy.loader() != null && matchesKeepWarm(policy, key)) {
            scheduleWithPolicy(key, raw, WarmSignal.READ, nowMonoMs);
          } else if (tasks.remove(key, task)) {
            approxSize--;
            task.cancel();
          }
        }
        continue;
      }
      if (approxSize >= maxTasks) {
        continue;
      }
      // The policy is cheaper than the heat probe and rejects almost every key: its namespace
      // either declares no loader or no keep-warm trigger. Probing heat first inverted that cost
      // and queried the lease table for namespaces that could never spend a renewal.
      ResolvedNamespacePolicy policy = policies.resolve(key);
      if (policy.loader() == null || !matchesKeepWarm(policy, key)) {
        continue;
      }
      CacheEntry resident = liveResident(key);
      if (resident != null && scheduleWithPolicy(key, resident, WarmSignal.READ, nowMonoMs)) {
        approxSize++;
      }
    }
  }

  /**
   * Drops any scheduled renewal for the key.
   *
   * @param key the physical key
   */
  @Override
  public void cancel(String key) {
    WarmTask removed = tasks.remove(key);
    if (removed != null) {
      removed.cancel();
    }
  }

  /**
   * Drops the key's renewal unless the namespace's keep-warm trigger still accepts a live heat
   * signal. The trigger is re-judged here so a caller never has to know which sources the
   * namespace opted into.
   *
   * @param key the physical key
   */
  @Override
  public void cancelIfCold(String key) {
    if (!matchesKeepWarm(policies.resolve(key), key)) {
      cancel(key);
    }
  }

  /**
   * Re-judges heat, the local-read gate and the resident version, then submits one hard renewal.
   * Every early exit releases the task slot through the one {@code finally}; the renewal branch
   * hands the slot to its completion callback.
   *
   * @param task the due task
   */
  private void fire(WarmTask task) {
    boolean handedOff = false;
    try {
      if (task.cancelled || closed || !config.isEnabled()) {
        return;
      }
      ResolvedNamespacePolicy policy = policies.resolve(task.key);
      CacheLoader<String, Object> loader = policy.loader();
      if (loader == null) {
        return;
      }
      // Query only the signal this trigger can accept: OFF accepts neither, LOCAL never touches
      // the lease table, CLUSTER never touches the local sketch, and ANY stops at the first hit.
      HeatTrigger trigger = policy.keepWarm();
      boolean localHot = trigger.allowsLocal() && heatView.isLocalHot(task.key);
      if (!localHot && (!trigger.allowsCluster() || !heatView.isClusterHot(task.key))) {
        return;
      }
      if (!localHot && task.lastReadMonoMs < task.armedAtMonoMs) {
        return;
      }
      CacheEntry resident = liveResident(task.key);
      if (resident == null) {
        return;
      }
      if (resident.getDataVersion() != task.dataVersion) {
        // A peer write or a soft refresh replaced the value without going through this scheduler.
        // The task is bound to a dead version, so it re-arms against the resident; a newer task
        // that already replaced it owns the key, and this one must not touch the map.
        handedOff = true;
        if (tasks.remove(task.key, task)) {
          // A re-arm is a new cycle, not a read: the cluster gate still needs fresh read evidence.
          scheduleWithPolicy(task.key, resident, WarmSignal.RENEW, TimeSource.monotonicMillis());
        }
        return;
      }
      CompletableFuture<BackgroundRefresher.RenewOutcome> renewal = refresher.renewHard(
        task.key,
        VersionedValue.stampedReload(task.key, loader, stampClock)
      );
      handedOff = true;
      renewal.whenComplete((outcome, error) -> {
        // A cancelled or replaced task no longer owns the key; rescheduling from its outcome would
        // resurrect a renewal the application already removed.
        if (!tasks.remove(task.key, task)) {
          return;
        }
        if (
          outcome instanceof BackgroundRefresher.RenewOutcome.Applied ||
          outcome == BackgroundRefresher.RenewOutcome.Skipped.SOFT_REFRESH ||
          outcome == BackgroundRefresher.RenewOutcome.Skipped.NEWER_VALUE
        ) {
          // The renewal settled without extending this task's cycle: re-arm from the resident. The
          // signal is not read evidence, so a cluster-only key still needs a real read per cycle.
          CacheEntry current = liveResident(task.key);
          if (current != null) {
            scheduleIfEligible(task.key, current, WarmSignal.RENEW);
          }
        }
      });
    } finally {
      if (!handedOff) {
        tasks.remove(task.key, task);
      }
    }
  }

  private boolean matchesKeepWarm(ResolvedNamespacePolicy policy, String key) {
    return heatView.matches(policy.keepWarm(), key);
  }

  /**
   * The resident entry when it is still servable, or {@code null} when absent or logically
   * expired.
   */
  private @Nullable CacheEntry liveResident(String key) {
    CacheEntry resident = QuietCache.getIfPresent(cache, key);
    return ttlPolicy.isLive(resident) ? resident : null;
  }

  /**
   * The delay before this entry's renewal, or {@code -1} when it must not be scheduled.
   *
   * <p>The lead is capped at a quarter of the window and never below 1 s unless the window itself
   * is smaller, so the lead plus its jitter always consumes less than 3/8 of the window.
   */
  private long computeDelayMs(CacheEntry entry) {
    long hardDeadline = entry.getHardExpireAtMs();
    long softDeadline = entry.getSoftExpireAtMs();
    if (hardDeadline == Long.MAX_VALUE && softDeadline <= 0L) {
      return -1L;
    }
    long now = TimeSource.currentTimeMillis();
    long minWindow = config.getKeepWarm().getMinWindowMs();
    // The soft deadline is the earlier renewal point only while it can still hold the minimum
    // window. A short soft TTL (2 s against a 5 s floor) must not make a long hard TTL
    // unrenewable: fall back to the hard deadline, which is the expiry that would actually miss.
    long deadline =
      softDeadline > 0L && softDeadline < hardDeadline && softDeadline - now >= minWindow ? softDeadline : hardDeadline;
    if (deadline == Long.MAX_VALUE) {
      return -1L;
    }
    long window = deadline - now;
    if (window < minWindow) {
      return -1L;
    }
    long leadMax = Math.min(config.getKeepWarm().getLeadMaxMs(), window / 4L);
    long leadFloor = Math.min(LEAD_FLOOR_MS, leadMax);
    long lead = Math.max(leadFloor, Math.min(leadMax, window / 8L));
    long jitter = lead > 1L ? ThreadLocalRandom.current().nextLong(lead / 2L) : 0L;
    return Math.max(MIN_INTERVAL_MS, window - lead - jitter);
  }

  /** Stops all scheduled renewals at context shutdown. */
  @Override
  public void destroy() {
    closed = true;
    tasks.values().forEach(WarmTask::cancel);
    tasks.clear();
  }

  /** One per-key warm cycle, bound to one data version. */
  private static final class WarmTask {

    private final String key;
    private final long dataVersion;
    private final long armedAtMonoMs;
    private volatile long lastReadMonoMs;
    private volatile boolean cancelled;
    private volatile @Nullable ScheduledFuture<?> future;

    /**
     * @param key               the physical key
     * @param dataVersion       the resident version this task is bound to
     * @param armedAtMonoMs     when this warm cycle began, or the previous cycle's start when a write
     *                          replaced it
     * @param initialReadMonoMs the read evidence carried into this cycle, or {@link Long#MIN_VALUE}
     */
    private WarmTask(String key, long dataVersion, long armedAtMonoMs, long initialReadMonoMs) {
      this.key = key;
      this.dataVersion = dataVersion;
      this.armedAtMonoMs = armedAtMonoMs;
      this.lastReadMonoMs = initialReadMonoMs;
    }

    /**
     * Cancels the pending scheduled fire, if any. A task already dequeued by the scheduler cannot
     * be stopped here; {@link #cancelled} is what keeps it from spending a renewal.
     */
    private void cancel() {
      cancelled = true;
      ScheduledFuture<?> scheduled = future;
      if (scheduled != null) {
        scheduled.cancel(false);
      }
    }
  }
}
