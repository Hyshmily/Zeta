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

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
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

  private final HotKeyStateMachine stateMachine;
  private final AtomicLong configTimestampCounter;
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
    Thread waitThread = new Thread(
      () -> {
        try {
          boolean received = startupLatch.await(3000, TimeUnit.MILLISECONDS);
          if (!received) {
            log.warn("No config heartbeat received within 3s, using WorkerProperties defaults");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      },
      "config-sync-startup"
    );
    waitThread.setDaemon(true);
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
    var props = msg.getMessageProperties();
    if (props == null) {
      return;
    }

    if (!AMQP_MESSAGE_PING.equals(props.getHeader(AMQP_HEADER_TYPE))) {
      return;
    }

    String fromNode = props.getHeader(AMQP_HEADER_NODE_ID);
    if (fromNode == null || fromNode.equals(nodeId)) {
      return;
    }

    Number remoteTsObj = props.getHeader(AMQP_HEADER_CONFIG_TIMESTAMP);
    if (remoteTsObj == null) {
      return;
    }
    long remoteTs = remoteTsObj.longValue();
    long localTs = configTimestampCounter.get();
    if (remoteTs <= localTs) {
      return;
    }

    Number confirm = props.getHeader(AMQP_HEADER_CONFIG_CONFIRM_COUNT);
    Number cool = props.getHeader(AMQP_HEADER_CONFIG_COOL_COUNT);
    Number grace = props.getHeader(AMQP_HEADER_CONFIG_GRACE_COUNT);

    boolean updated = false;
    if (confirm != null) {
      stateMachine.setConfirmCount(confirm.intValue());
      updated = true;
    }
    if (cool != null) {
      stateMachine.setCoolCount(cool.intValue());
      updated = true;
    }
    if (grace != null) {
      stateMachine.setPreCoolGraceCount(grace.intValue());
      updated = true;
    }

    if (updated) {
      configTimestampCounter.set(remoteTs);
      log.info("Applied newer config from {}: confirm={}, cool={}, grace={}", fromNode, confirm, cool, grace);
    }

    if (startupLatch.getCount() > 0) {
      startupLatch.countDown();
    }
  }
}
