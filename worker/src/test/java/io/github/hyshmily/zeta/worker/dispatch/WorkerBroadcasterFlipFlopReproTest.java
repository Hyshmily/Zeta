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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Minimal reproduction for P0-5: the 100ms debounce cache keys by {@code type + ":" + key},
 * so a {@code HOT -> COOL -> HOT} flip inside the window silently drops the second HOT.
 */
class WorkerBroadcasterFlipFlopReproTest {

  /**
   * Exercises a fast flip-flop inside the debounce window.
   *
   * <p>Expected (fixed): 3 AMQP sends. Actual (buggy): 2 sends, the last HOT is elided.
   */
  @Test
  void hotCoolHot_withinDebounce_shouldSendThreeTimes() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    WorkerBroadcaster broadcaster = new WorkerBroadcaster(
      rabbitTemplate,
      "zeta.send.exchange",
      "testApp",
      "test-node",
      new AtomicLong(0),
      mock(SnowflakeIdGenerator.class),
      null
    );

    broadcaster.broadcastHot("k");
    broadcaster.broadcastCool("k");
    broadcaster.broadcastHot("k");

    verify(rabbitTemplate, times(3)).send(any(String.class), any(String.class), any(Message.class));
  }
}
