package io.github.hyshmily.hotkey.hotkeycache;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
public final class TransactionSupport {

  public static void runAfterCommit(Runnable task, Executor fallback) {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
          @Override
          public void afterCommit() {
            task.run();
          }
        }
      );
      return;
    }
    log.debug("Called outside transaction, submitting to async executor");
    CompletableFuture.runAsync(task, fallback).exceptionally(e -> {
      log.error("Async task failed after non-transactional call", e);
      return null;
    });
  }

  public static void runNowOrAfterCommit(Runnable task) {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
          @Override
          public void afterCommit() {
            task.run();
          }
        }
      );
      return;
    }
    task.run();
  }

  private TransactionSupport() {}
}
