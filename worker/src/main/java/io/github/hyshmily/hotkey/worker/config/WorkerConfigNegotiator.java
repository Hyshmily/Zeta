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
package io.github.hyshmily.hotkey.worker.config;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.sync.worker.WorkerHeartbeatMessage;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

/**
 * Listens for heartbeat-based config updates from peer Workers and applies them
 * if the received config timestamp is newer than the local one.
 *
 * <p>On startup, waits up to 3 seconds for the first heartbeat to arrive.
 * If none is received, the Worker continues with the values from
 * {@link WorkerProperties} — this can happen when all other Workers are down.
 */
@RequiredArgsConstructor
@Slf4j
public class WorkerConfigNegotiator {

  /** State machine whose config (confirm/cool/grace counts) is updated from heartbeat messages. */
  private final HotKeyStateMachine stateMachine;
  /** Monotonically increasing counter tracking the latest config-change timestamp. */
  private final AtomicLong configTimestampCounter;
  /** Unique identifier for this Worker node, used in queue names and heartbeat identification. */
  private final String nodeId;
  private final CountDownLatch startupLatch = new CountDownLatch(1);

  /**
   * Waits for the first heartbeat from a peer Worker.
   *
   * <p>If a heartbeat with a valid config arrives within 3 seconds the
   * config is applied synchronously.  Otherwise, a warning is logged and
   * the Worker proceeds with configured defaults.
   */
  @PostConstruct
  void syncOnStartup() {
    Thread waitThread = Thread.ofVirtual()
      .name("config-sync-startup")
      .unstarted(() -> {
        try {
          boolean received = startupLatch.await(3000, TimeUnit.MILLISECONDS);
          if (!received) {
            log.warn("No config heartbeat received within 3s, using WorkerProperties defaults");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    waitThread.start();
  }

  /**
   * Processes an incoming heartbeat message from a peer Worker and applies
   * its state-machine config if the embedded timestamp is newer.
   *
   * @param msg the raw AMQP heartbeat message
   */
  @RabbitListener(queues = "#{@workerConfigQueue.name}")
  public void onHeartbeat(Message msg) {
    try {
      doOnHeartbeat(msg);
    } catch (Exception e) {
      log.warn("Uncaught exception in onHeartbeat config negotiation, discarding message to prevent requeue loop", e);
    }
  }

  private void doOnHeartbeat(Message msg) {
    WorkerHeartbeatMessage hb = WorkerHeartbeatMessage.from(msg);
    if (hb == null) {
      return;
    }

    if (hb.workerId().equals(nodeId)) {
      return;
    }

    long remoteTs = hb.configTimestamp();
    long localTs = configTimestampCounter.get();
    if (remoteTs <= localTs) {
      return;
    }

    stateMachine.setConfirmCount(hb.configConfirmCount());
    stateMachine.setCoolCount(hb.configCoolCount());
    stateMachine.setPreCoolGraceCount(hb.configGraceCount());

    configTimestampCounter.set(remoteTs);

    log.debug(
      "Applied newer config from {}: confirmCount={}, coolCount={}, preCoolGraceCount={}",
      hb.workerId(),
      hb.configConfirmCount(),
      hb.configCoolCount(),
      hb.configGraceCount()
    );

    if (startupLatch.getCount() > 0) {
      startupLatch.countDown();
    }
  }
}
