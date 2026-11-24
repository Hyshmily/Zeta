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
package io.github.hyshmily.zeta.sync.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.heat.HeatDirectory;
import io.github.hyshmily.zeta.heat.HeatLease;
import io.github.hyshmily.zeta.heat.HotKeyContext;
import io.github.hyshmily.zeta.heat.HotKeyContextFactory;
import io.github.hyshmily.zeta.heat.HotKeyHandler;
import io.github.hyshmily.zeta.heat.HotKeyListener;
import io.github.hyshmily.zeta.model.DecisionStamp;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the same-node batch fold and the callback order: the effective run is at most one COOL
 * followed by one HOT, and a grant over a dead lease ends that lease before the new {@code onHot}.
 */
@DisplayName("worker batch fold")
class WorkerListenerBatchFoldTest {

  private static final String KEY = "users\u0000u-1";
  private static final String NODE = "worker-1";

  private static WorkerDecision decision(long version, boolean hot) {
    return new WorkerDecision(
      version,
      KEY,
      hot ? DecisionStamp.hot(version, NODE, 1L) : DecisionStamp.cool(version, NODE, 1L)
    );
  }

  private static WorkerDecision decision(long version, boolean hot, long epoch) {
    return new WorkerDecision(
      version,
      KEY,
      hot ? DecisionStamp.hot(version, NODE, epoch) : DecisionStamp.cool(version, NODE, epoch)
    );
  }

  private static WorkerListener listener(HotKeyHandler handler, List<HotKeyListener> observers) {
    HotKeyContextFactory contexts = (decision, newlyHot) -> {
      HotKeyContext context = mock(HotKeyContext.class);
      when(context.decision()).thenReturn(decision);
      when(context.isNewlyHot()).thenReturn(newlyHot);
      return context;
    };
    return new WorkerListener(
      new WorkerListenerProperties(),
      mock(ScheduledExecutorService.class),
      new HeatDirectory(1_000, 60_000L, Runnable::run, null, null),
      handler,
      observers,
      contexts,
      null,
      "app"
    );
  }

  /**
   * The fold is exercised through the production dispatcher callback; the method stays private so
   * the listener exposes no test-only seam.
   */
  private static void dispatch(WorkerListener listener, List<WorkerDecision> batch) throws Exception {
    Method method = WorkerListener.class.getDeclaredMethod("dispatchBatch", List.class);
    method.setAccessible(true);
    method.invoke(listener, batch);
  }

  private static final class RecordingHandler implements HotKeyHandler {

    private final List<Long> versions = new ArrayList<>();

    @Override
    public void onHot(HotKeyContext context) {
      versions.add(context.decision().stamp().decisionVersion());
    }

    @Override
    public void onCool(HotKeyContext context) {
      versions.add(context.decision().stamp().decisionVersion());
    }
  }

  @Test
  void aCoolAfterAHotAfterACool_shouldApplyOneCool() throws Exception {
    RecordingHandler handler = new RecordingHandler();
    WorkerListener listener = listener(handler, List.of());

    dispatch(listener, List.of(decision(2L, false), decision(3L, true), decision(4L, false)));

    assertThat(handler.versions).containsExactly(4L);
  }

  @Test
  void aHotFollowedByACool_shouldApplyTheCoolWithoutAHot() throws Exception {
    RecordingHandler handler = new RecordingHandler();
    WorkerListener listener = listener(handler, List.of());

    dispatch(listener, List.of(decision(1L, true), decision(2L, false)));

    assertThat(handler.versions).containsExactly(2L);
  }

  @Test
  void aCoolFollowedByAHot_shouldApplyBothInOrder() throws Exception {
    RecordingHandler handler = new RecordingHandler();
    WorkerListener listener = listener(handler, List.of());

    dispatch(listener, List.of(decision(1L, false), decision(2L, true)));

    assertThat(handler.versions).containsExactly(1L, 2L);
  }

  @Test
  void aRunOfHot_shouldApplyTheNewestAndItsFollowingCool() throws Exception {
    RecordingHandler handler = new RecordingHandler();
    WorkerListener listener = listener(handler, List.of());

    dispatch(listener, List.of(decision(1L, true), decision(2L, true), decision(3L, false)));

    assertThat(handler.versions).containsExactly(3L);
  }

  @Test
  void aBatchMixingTwoIncarnationsOfOneNode_shouldNotFoldAcrossEpochs() throws Exception {
    RecordingHandler handler = new RecordingHandler();
    WorkerListener listener = listener(handler, List.of());

    // A restart resets the version counter, so v1 of epoch 2 is newer than v2 of epoch 1. Folding
    // the pair would keep the COOL and drop the new incarnation's HOT; the batch runs verbatim.
    dispatch(listener, List.of(decision(2L, true, 1L), decision(1L, false, 2L)));

    assertThat(handler.versions).containsExactly(2L, 1L);
  }

  @Test
  void aGrantOverADeadLease_shouldEndItBeforeTheNewHot() throws Exception {
    List<String> events = new ArrayList<>();
    HotKeyListener observer = new HotKeyListener() {
      @Override
      public void onHot(WorkerDecision decision, boolean newlyHot) {
        events.add("HOT:" + decision.stamp().decisionEpoch());
      }

      @Override
      public void onCool(HeatLease lease, HeatLease.EndReason reason) {
        events.add("COOL:" + lease.stamp().decisionEpoch() + ":" + reason);
      }
    };
    WorkerListener listener = listener(new RecordingHandler(), List.of(observer));

    dispatch(listener, List.of(decision(1L, true, 1L)));
    dispatch(listener, List.of(decision(2L, true, 2L)));

    assertThat(events).containsExactly("HOT:1", "COOL:1:INCARNATION_DEAD", "HOT:2");
  }
}
