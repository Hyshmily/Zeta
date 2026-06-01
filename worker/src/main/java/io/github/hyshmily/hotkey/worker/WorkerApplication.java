package io.github.hyshmily.hotkey.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the standalone HotKey Worker server.
 *
 * <p>Listens for batched reports from application instances, runs
 * sliding-window detection and state-machine analysis, and broadcasts
 * HOT/COOL decisions back to all connected apps via RabbitMQ.
 */
@SpringBootApplication
@EnableScheduling
public class WorkerApplication {

  public static void main(String[] args) {
    SpringApplication.run(WorkerApplication.class, args);
  }
}
