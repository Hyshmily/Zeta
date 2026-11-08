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

import java.util.List;

/**
 * Strategy interface for processing Worker HOT/COOL decisions.
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to fully
 * replace the default HOT/COOL processing logic. The default implementation
 * ({@link DefaultWorkerDecisionHandler}) performs loader-backed promotion and
 * downgrade with version guarding and hook dispatch.
 *
 * <p>Use {@code @ConditionalOnMissingBean(WorkerDecisionHandler.class)} on your
 * custom bean to replace the default.
 */
public interface WorkerDecisionHandler {
  /**
   * Process a HOT decision from the Worker.
   *
   * @param workerMessage the Worker message containing the HOT decision; must not be null
   */
  void handleHot(WorkerMessage workerMessage);

  /**
   * Process a COOL decision from the Worker.
   *
   * @param workerMessage the Worker message containing the COOL decision; must not be null
   */
  void handleCool(WorkerMessage workerMessage);

  /**
   * Process a run of consecutive COOL decisions for one key, in submission order.
   *
   * <p>COOL is pure in-memory (no cluster-loader read, no entry creation or removal), so
   * the default implementation ({@link DefaultWorkerDecisionHandler}) may coalesce the run
   * to the decision that would survive the version guard and report the covered ones as
   * skipped. This default method preserves the per-message contract and applies every
   * message even if an earlier one fails; the first failure is rethrown after the run.
   *
   * @param batch the COOL run, never empty, all messages for the same key
   */
  default void handleCoolBatch(List<WorkerMessage> batch) {
    RuntimeException failure = null;
    for (WorkerMessage workerMessage : batch) {
      try {
        handleCool(workerMessage);
      } catch (RuntimeException e) {
        if (failure == null) {
          failure = e;
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Synchronous COOL application for the ordered-dispatcher rejection path: the
   * per-key ordered dispatcher refused to enqueue the decision, and nothing re-sends a COOL, so
   * the receiver applies it inline on the AMQP consumer thread instead of dropping it.
   *
   * <p>An override must limit the work to the in-memory compute (version guards + TTL
   * rewrite — only the HOT path performs cluster-loader I/O) and must not dispatch
   * {@link WorkerDecisionHook} callbacks: hooks are user code that may block, and the
   * consumer thread must never wait on them. This default delegates to
   * {@link #handleCool}, so it does dispatch hooks; the default handler overrides it with
   * the hook-free compute.
   *
   * @param workerMessage the Worker message containing the COOL decision; must not be null
   */
  default void handleCoolInline(WorkerMessage workerMessage) {
    handleCool(workerMessage);
  }
}
