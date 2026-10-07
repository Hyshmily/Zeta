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
package io.github.hyshmily.zeta.worker.dispatch;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.KEY_BROADCAST;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes HOT and COOL decisions to all application instances via the
 * configured RabbitMQ {@code broadcastExchange}.
 *
 * <p>Each send message carries a monotonically increasing
 * <b>decision version</b> (worker‑local counter) that is used by receivers to
 * discard stale or out‑of‑order messages.
 *
 * <p>Messages are delivered to every instance's dedicated queue through a
 * fanout exchange ({@code zeta.send.exchange}) — the receiver
 * differentiates message type via the {@code HEADER_TYPE} header.
 *
 * <p>This class is the <b>single funnel</b> for decisions leaving the Worker: the
 * state-machine path ({@code ReportConsumer}) and the idle-eviction path
 * ({@code EvictStaleTask}) both publish through {@link #broadcastHot} /
 * {@link #broadcastCool}, and nothing else writes to the broadcast exchange. The
 * {@code zeta.worker.decisions.hot} / {@code .cool} counters are therefore
 * incremented here — the only place that knows whether a decision actually reached
 * the cluster — rather than at the call sites, which would count one decision twice
 * (the retry after a failed send).
 */
@Slf4j
public class WorkerBroadcaster {

  /** RabbitMQ template used to publish HOT/COOL decisions and heartbeats. */
  private final RabbitTemplate rabbitTemplate;
  /** Target exchange name for all send messages (typically {@code zeta.send.exchange}). */
  private final String broadcastExchange;
  /** Application name used in the send routing key. */
  private final String appName;

  /** Originating Worker's node identity, set on every send header for cross-Worker
   * version tracking in {@link VersionGuard}. */
  private final String nodeId;

  /** Monotonically increasing epoch counter for this Worker instance, incremented on every
   * restart. Transmitted in send headers so receivers detect Worker restarts and
   * unconditionally accept decisions from a higher epoch (see ADR-0010). */
  private final AtomicLong epochCounter;

  private final SnowflakeIdGenerator snowflakeIdGenerator;

  /**
   * Optional detection-plane meters (ADR-0080). {@code null} — the legacy
   * constructor — leaves the emission counters unregistered, matching the
   * ADR-0037/0061 "null keeps the legacy path" convention for optional
   * collaborators.
   */
  private final WorkerDetectionMetrics metrics;

  /**
   * Bounded COOL re-emission count (ADR-0087): repeats scheduled per
   * successful {@link #broadcastCool}, spaced one {@code coolRepeatIntervalMs}
   * apart with the original decision version. {@code 0} disables repeats.
   */
  private final int coolRepeatTimes;

  /** Spacing between COOL repeats (ms); reuses the HOT rebroadcast interval. */
  private final long coolRepeatIntervalMs;

  /**
   * Scheduler for COOL repeats; {@code null} disables repeats (legacy path).
   * Never blocked on: {@code schedule} is non-blocking, and a saturated
   * scheduler drops the repeat batch with a DEBUG log.
   */
  private final ScheduledExecutorService repeatScheduler;

  /**
   * Constructs a broadcaster.
   *
   * @param rabbitTemplate     the template used to publish messages
   * @param broadcastExchange  the fanout exchange name
   * @param appName            the application name carried in the send header
   * @param nodeId             this Worker's node identity
   * @param epochCounter       this Worker's epoch counter
   * @param snowflakeIdGenerator the trace-ID generator
   * @param metrics            the emission counters, or {@code null} to leave them off
   */
  public WorkerBroadcaster(
    RabbitTemplate rabbitTemplate,
    String broadcastExchange,
    String appName,
    String nodeId,
    AtomicLong epochCounter,
    SnowflakeIdGenerator snowflakeIdGenerator,
    WorkerDetectionMetrics metrics
  ) {
    this(rabbitTemplate, broadcastExchange, appName, nodeId, epochCounter, snowflakeIdGenerator, metrics, 0, 0, null);
  }

  /**
   * Constructs a broadcaster with bounded COOL re-emission (ADR-0087).
   *
   * @param rabbitTemplate     the template used to publish messages
   * @param broadcastExchange  the fanout exchange name
   * @param appName            the application name carried in the send header
   * @param nodeId             this Worker's node identity
   * @param epochCounter       this Worker's epoch counter
   * @param snowflakeIdGenerator the trace-ID generator
   * @param metrics            the emission counters, or {@code null} to leave them off
   * @param coolRepeatTimes    COOL repeats per successful broadcast (0 disables)
   * @param coolRepeatIntervalMs spacing between repeats in milliseconds
   * @param repeatScheduler    scheduler for repeats, or {@code null} to disable
   */
  public WorkerBroadcaster(
    RabbitTemplate rabbitTemplate,
    String broadcastExchange,
    String appName,
    String nodeId,
    AtomicLong epochCounter,
    SnowflakeIdGenerator snowflakeIdGenerator,
    WorkerDetectionMetrics metrics,
    int coolRepeatTimes,
    long coolRepeatIntervalMs,
    ScheduledExecutorService repeatScheduler
  ) {
    this.rabbitTemplate = rabbitTemplate;
    this.broadcastExchange = broadcastExchange;
    this.appName = appName;
    this.nodeId = nodeId;
    this.epochCounter = epochCounter;
    this.snowflakeIdGenerator = snowflakeIdGenerator;
    this.metrics = metrics;
    this.coolRepeatTimes = Math.max(0, coolRepeatTimes);
    this.coolRepeatIntervalMs = coolRepeatIntervalMs;
    this.repeatScheduler = repeatScheduler;
  }

  /**
   * Worker‑local decision version counter.
   * Allocated atomically for every broadcast attempt to provide strict ordering
   * for this worker.  Combined with consistent‑hash sharding (one key → one
   * worker) this guarantees a total order of decisions per key.
   *
   * <p><b>Watermark semantics:</b> The version is allocated {@code before}
   * the send ({@link #nextDecisionVersion()}), so a failed broadcast burns a
   * version and the sequence contains holes. It is a monotone <em>high-water
   * mark of broadcast attempts</em>, not a count of delivered messages.
   * Receivers must treat it as an ordering watermark only (compare with
   * {@code >=}) and never assume every intermediate version exists.
   *
   * <p>Initialised at zero — the epoch counter handles cross‑restart ordering,
   * so we neither need nor want a wall‑clock seed (see ADR-0010).
   */
  private final AtomicLong decisionVersionCounter = new AtomicLong(0L);

  /**
   * Per‑instance 100 ms debounce cache; prevents the same key+type from being
   * broadcast twice by <em>this</em> Worker within the expiry window.  The
   * original intent was cross‑Worker dedup during ring convergence, but a
   * per‑JVM Caffeine cache cannot see broadcasts from other Workers.  Actual
   * cross‑Worker convergence is provided by consistent‑hash sharding (one
   * owner per key) plus the {@code decisionVersion >=} guard on the App side.
   *
   * <p>The real value is local debounce: the state‑machine path and the
   * FastLane / TopKValidator path may both evaluate the same key within
   * 100 ms, and this cache silently elides the duplicate.  See ADR-0013.
   *
   * <p>Note: {@code put} happens <em>after</em> a successful
   * {@link #sendBroadcast}, so a failed send leaves the cache clean and the
   * next cycle will retry.
   *
   * <p>The check-then-put pair is not atomic: two concurrent consumers can
   * both miss the cache and send the same key+type within the window. This is
   * accepted, not fixed — same-key decisions are serialized by the state
   * machine's striped lock, so the overlap window is a rebroadcast-interval
   * boundary race that requires two decisions for one key from two consumer
   * threads within 100 ms; the receiver's {@code decisionVersion >=} guard
   * makes the duplicate harmless (the second send is simply a no-op there),
   * and closing the gap with a putIfAbsent reservation would add a
   * failed-send cleanup path for no observable gain.
   */
  private final Cache<String, Boolean> broadcastDedupCache = Caffeine.newBuilder()
    .expireAfterWrite(100, TimeUnit.MILLISECONDS)
    .maximumSize(1_000)
    .build();

  /**
   * Atomically allocates the next decision version for this Worker.
   *
   * <p>Sole allocation point for broadcast ordering, called internally by
   * {@link #broadcastHot} and {@link #broadcastCool} immediately before each
   * send (sends are synchronous on the consumer thread — historically
   * {@code ReportConsumer} pre-allocated versions before an async send, which
   * no longer exists).
   *
   * @return the next monotonically increasing decision version
   */
  public long nextDecisionVersion() {
    return decisionVersionCounter.incrementAndGet();
  }

  /**
   * Broadcasts a HOT decision for the given key.
   *
   * <p>A successful send is counted into {@code zeta.worker.decisions.hot}
   * (ADR-0080). The counter sits at this funnel rather than at the call sites
   * because a failed send returns {@code false} and the caller rolls the state
   * machine back — the decision is then re-emitted by the next evaluation or by the
   * periodic HOT rebroadcast (ADR-0024), so counting at the call site would count
   * one decision twice.
   *
   * @param cacheKey the key that has been confirmed as hot
   */
  public boolean broadcastHot(String cacheKey) {
    String dedupKey = cacheKey + ":" + WorkerMessage.TYPE_HOT;
    if (broadcastDedupCache.getIfPresent(dedupKey) != null) {
      // Duplicate elided inside the 100 ms debounce window: nothing is emitted, so
      // nothing is counted — the decision being deduplicated was already counted
      // when it was sent. The caller still sees success and must not roll back.
      return true;
    }
    long dv = nextDecisionVersion();
    try {
      sendBroadcast(cacheKey, WorkerMessage.TYPE_HOT, dv);
      if (metrics != null) {
        metrics.countHotDecision();
      }

      broadcastDedupCache.put(dedupKey, Boolean.TRUE);
      log.debug("Broadcast HOT: key={}, dv={}", cacheKey, dv);
    } catch (Exception e) {
      // ADR-0037 log discipline: the send site stays quiet (DEBUG, no stack) —
      // WorkerBroadcastBuffer aggregates send failures into one rate-limited
      // WARN per window with counts, which is the signal operators read. A
      // per-decision ERROR with a full stack would flood the log exactly when
      // a broker outage collides with a mass-heat wave (ADR-0061's own
      // motivation), drowning the aggregated warning that matters.
      log.debug("Failed to broadcast HOT decision for key={}: {}", cacheKey, e.toString());
      return false;
    }
    return true;
  }

  /**
   * Broadcasts a COOL decision for the given key.
   *
   * <p>A successful send is counted into {@code zeta.worker.decisions.cool}
   * (ADR-0080), on the same emission-only rule as {@link #broadcastHot(String)}.
   * On success, {@link #coolRepeatTimes} idempotent repeats are scheduled
   * (ADR-0087) — a lost COOL has no other self-healing (HOT has the periodic
   * rebroadcast, ADR-0024).
   *
   * @param cacheKey the key that has been confirmed as fully cooled
   */
  public boolean broadcastCool(String cacheKey) {
    String dedupKey = cacheKey + ":" + WorkerMessage.TYPE_COOL;
    if (broadcastDedupCache.getIfPresent(dedupKey) != null) {
      // Elided duplicate: see broadcastHot — nothing emitted, nothing counted.
      return true;
    }
    long dv = nextDecisionVersion();
    try {
      sendBroadcast(cacheKey, WorkerMessage.TYPE_COOL, dv);
      if (metrics != null) {
        metrics.countCoolDecision();
      }
      broadcastDedupCache.put(dedupKey, Boolean.TRUE);
      // ADR-0064: per-key state transitions are DEBUG — a mass-heat wave emits
      // one line per key, which is exactly the flood the transition-log
      // throttling was introduced to stop.
      log.debug("Broadcast COOL: key={}, dv={}", cacheKey, dv);
      scheduleCoolRepeats(cacheKey, dv);
    } catch (Exception e) {
      // Same ADR-0037 discipline as broadcastHot: aggregate WARN lives in
      // WorkerBroadcastBuffer, the send site stays at DEBUG without a stack.
      log.debug("Failed to broadcast COOL decision for key={}: {}", cacheKey, e.toString());
      return false;
      // no throw — ADR-0007 fire-and-forget
    }
    return true;
  }

  /**
   * Schedules the bounded COOL repeats for a just-sent decision (ADR-0087).
   * Each repeat re-sends the <em>same</em> decision version — never a fresh
   * one: a fresh version could overtake a subsequent HOT (cool → reheat inside
   * the repeat window) and demote a legitimately hot entry, while the same
   * version is skipped exactly where it should be (already applied → equal
   * skip; superseded → newer wins) and applied exactly where it is needed
   * (first send lost). Repeats bypass the 100ms dedup cache entirely (neither
   * checked nor populated): the spacing (≥1s by config bound) already exceeds
   * the debounce window, and a dedup-populated repeat could elide a genuine
   * new decision.
   *
   * @param cacheKey the cooled key
   * @param dv       the decision version of the initial broadcast to repeat
   */
  private void scheduleCoolRepeats(String cacheKey, long dv) {
    if (repeatScheduler == null || coolRepeatTimes <= 0 || coolRepeatIntervalMs <= 0) {
      return;
    }

    for (int i = 1; i <= coolRepeatTimes; i++) {
      long delayMs = i * coolRepeatIntervalMs;
      try {
        repeatScheduler.schedule(() -> sendCoolRepeat(cacheKey, dv), delayMs, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        // Scheduler saturated or shutting down: the remaining repeats are
        // dropped (fire-and-forget, ADR-0007) — the entry still converges via
        // its hard TTL, exactly the pre-0087 behaviour.
        log.debug("COOL repeat scheduling rejected (saturated/shutdown), key={}", cacheKey);
        break;
      }
    }
  }

  /**
   * Sends one idempotent COOL repeat with the original decision version.
   *
   * @param cacheKey the cooled key
   * @param dv       the original decision version to re-send
   */
  private void sendCoolRepeat(String cacheKey, long dv) {
    try {
      sendBroadcast(cacheKey, WorkerMessage.TYPE_COOL, dv);
      if (metrics != null) {
        metrics.countCoolDecision();
      }
      log.debug("Rebroadcast COOL (repeat): key={}, dv={}", cacheKey, dv);
    } catch (Exception e) {
      // Fire-and-forget: a lost repeat just narrows the healing window.
      log.debug("Failed to rebroadcast COOL repeat for key={}: {}", cacheKey, e.toString());
    }
  }

  /**
   * Common send helper for HOT/COOL decisions.
   *
   * <p>Builds {@link MessageProperties} with type, version, and degraded flag,
   * creates a {@link Message}, and publishes via {@link #rabbitTemplate}.
   *
   * @param cacheKey the key being send
   * @param type     the message type ({@link WorkerMessage#TYPE_HOT} or {@link WorkerMessage#TYPE_COOL})
   * @param version  the monotonically increasing decision version for ordering
   */
  private void sendBroadcast(String cacheKey, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_IS_VERSION_DEGRADED, false);
    props.setHeader(HEADER_NODE_ID, nodeId);
    props.setHeader(HEADER_EPOCH, epochCounter.get());
    // ADR-0068: the fanout exchange ignores the routing key, so appName travels as a
    // header and receivers drop decisions addressed to a different app (shared-broker
    // isolation). Pre-0068 receivers ignore the unknown header.
    props.setHeader(HEADER_APP_NAME, appName);
    props.setHeader(HEADER_MESSAGE_ID, snowflakeIdGenerator.nextId());

    Message msg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send(broadcastExchange, KEY_BROADCAST + appName, msg);
  }
}
