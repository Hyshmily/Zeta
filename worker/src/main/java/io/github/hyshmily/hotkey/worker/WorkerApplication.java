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

  /**
   * Starts the standalone HotKey Worker server.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    SpringApplication.run(WorkerApplication.class, args);
  }
}
