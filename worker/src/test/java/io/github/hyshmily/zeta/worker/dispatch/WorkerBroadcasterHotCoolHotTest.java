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

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * The 100 ms debounce is keyed by decision type: a {@code HOT -> COOL -> HOT} flip inside the
 * window is a type change, not a duplicate, so all three decisions travel.
 */
class WorkerBroadcasterHotCoolHotTest {

  /** A type change inside the debounce window is not a duplicate. */
  @Test
  void hotCoolHotInsideTheDebounceWindowIsSentThreeTimes() {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    WorkerBroadcaster broadcaster = new WorkerBroadcaster(
      rabbitTemplate,
      "zeta.send.exchange",
      "testApp",
      new WorkerIdentity("test-node", 0L),
      new WorkerDetectionMetrics(null),
      30_000L
    );

    broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE);
    broadcaster.send(ZetaDecision.cool("k", 2L), Long.MAX_VALUE);
    broadcaster.send(ZetaDecision.hot("k", 3L), Long.MAX_VALUE);

    verify(rabbitTemplate, times(3)).send(any(String.class), any(String.class), any(Message.class));
  }
}
