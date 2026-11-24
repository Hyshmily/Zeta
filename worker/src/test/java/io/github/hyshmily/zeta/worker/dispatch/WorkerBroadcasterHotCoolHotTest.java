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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * The 100 ms debounce is keyed by decision type: a {@code HOT -> COOL -> HOT} flip inside the
 * window is a type change, not a duplicate, so all three decisions travel. An elided flip still
 * has to keep the accepted record's retention honest.
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
      30_000L,
      65_536L
    );

    broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE);
    broadcaster.send(ZetaDecision.cool("k", 2L), Long.MAX_VALUE);
    broadcaster.send(ZetaDecision.hot("k", 3L), Long.MAX_VALUE);

    verify(rabbitTemplate, times(3)).send(any(String.class), any(String.class), any(Message.class));
  }

  /**
   * An expired COOL gives up the century retention; a re-promotion inside the debounce window is
   * elided (no wire message) but the Worker holds an active HOT seat again, so the settled record
   * must take the retention back. Otherwise it expires after {@code 2 * maxDecisionAgeMs} while the
   * cluster still believes the key is hot.
   *
   * <p>The elision is driven by a 100 ms wall-clock window, so the terminal outcome is settled
   * directly instead of trying to race a publish inside it.
   */
  @Test
  void anElidedHotRestoresTheRetentionAnExpiredCoolGaveUp() throws Exception {
    RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
    WorkerBroadcaster broadcaster = new WorkerBroadcaster(
      rabbitTemplate,
      "zeta.send.exchange",
      "testApp",
      new WorkerIdentity("test-node", 0L),
      new WorkerDetectionMetrics(null),
      90L,
      65_536L
    );

    broadcaster.send(ZetaDecision.hot("k", 1L), Long.MAX_VALUE);
    // The COOL is expired the moment it is classified: the record drops the long retention.
    broadcaster.send(ZetaDecision.cool("k", 2L), 0L);
    Object cleared = wireRecord(broadcaster, "k");
    assertThat(hotRetained(cleared)).as("an expired COOL gives up the long retention").isFalse();

    settle(broadcaster, "ELIDED", cleared, ZetaDecision.hot("k", 3L));

    assertThat(broadcaster.acceptedVersion("k")).isEqualTo(3L);
    assertThat(hotRetained(wireRecord(broadcaster, "k"))).as("an elided HOT still holds the seat").isTrue();
    verify(rabbitTemplate, times(1)).send(any(String.class), any(String.class), any(Message.class));
  }

  /** Applies one terminal outcome through the private funnel, as {@code send} would. */
  private static void settle(WorkerBroadcaster broadcaster, String outcome, Object prev, ZetaDecision decision)
    throws Exception {
    Class<?> delivery = Class.forName(WorkerBroadcaster.class.getName() + "$Delivery");
    Class<?> accepted = Class.forName(WorkerBroadcaster.class.getName() + "$AcceptedDecision");
    Method settle = WorkerBroadcaster.class.getDeclaredMethod("settle", delivery, accepted, ZetaDecision.class);
    settle.setAccessible(true);
    settle.invoke(broadcaster, Enum.valueOf((Class<Enum>) delivery, outcome), prev, decision);
  }

  /** Reads the private wire record that the funnel keeps per key. */
  private static Object wireRecord(WorkerBroadcaster broadcaster, String key) throws Exception {
    Field acceptedField = WorkerBroadcaster.class.getDeclaredField("accepted");
    acceptedField.setAccessible(true);
    Cache<String, ?> accepted = (Cache<String, ?>) acceptedField.get(broadcaster);
    return accepted.getIfPresent(key);
  }

  /** Reads the private wire record's retention bit. */
  private static boolean hotRetained(Object record) throws Exception {
    Method hotRetained = record.getClass().getDeclaredMethod("hotRetained");
    hotRetained.setAccessible(true);
    return (boolean) hotRetained.invoke(record);
  }
}
