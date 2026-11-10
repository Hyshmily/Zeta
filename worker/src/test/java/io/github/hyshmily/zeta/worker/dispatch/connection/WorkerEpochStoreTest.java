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
package io.github.hyshmily.zeta.worker.dispatch.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The default epoch file keeps a deployment-supplied worker id a valid file name. */
class WorkerEpochStoreTest {

  @Test
  void sanitizesFilesystemSignificantCharacters() {
    Path path = WorkerEpochStore.defaultEpochFile("pod:1/a b");

    assertThat(path.getFileName().toString()).isEqualTo("zeta-epoch-pod_1_a_b");
  }

  @Test
  void fallsBackToAFixedNameForAnEmptyId() {
    Path path = WorkerEpochStore.defaultEpochFile("  ");

    assertThat(path.getFileName().toString()).isEqualTo("zeta-epoch-unknown");
  }

  @Test
  void fallsBackToAFixedNameForANullId() {
    Path path = WorkerEpochStore.defaultEpochFile(null);

    assertThat(path.getFileName().toString()).isEqualTo("zeta-epoch-unknown");
  }

  @Test
  void fallsBackToAFixedNameForAnIdMadeOnlyOfSeparators() {
    Path path = WorkerEpochStore.defaultEpochFile("///");

    assertThat(path.getFileName().toString()).isEqualTo("zeta-epoch-unknown");
  }

  /** An underscore is a whitelisted identity character, not a sanitized separator. */
  @Test
  void keepsAnIdMadeOnlyOfUnderscores() {
    Path path = WorkerEpochStore.defaultEpochFile("__");

    assertThat(path.getFileName().toString()).isEqualTo("zeta-epoch-__");
  }

  /**
   * A corrupt file holding {@link Long#MAX_VALUE} must saturate: incrementing it wraps to the
   * minimum and silently steps the epoch back to the wall-clock floor.
   */
  @Test
  void saturatesACorruptMaximumEpochInsteadOfWrappingIt(@TempDir Path dir) throws Exception {
    Path path = dir.resolve("epoch");
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).putLong(Long.MAX_VALUE);
      buffer.flip();
      channel.write(buffer);
    }

    assertThat(WorkerEpochStore.initEpoch("worker-1", path)).isEqualTo(Long.MAX_VALUE);
    assertThat(readEpoch(path)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * The JVM rejects a second lock on a file this process already holds with
   * {@link java.nio.channels.OverlappingFileLockException}; that is not an I/O failure, but it
   * must degrade to the in-JVM monitor instead of escaping and forcing the timestamp fallback.
   */
  @Test
  void anOverlappingInJvmLockDegradesInsteadOfEscaping(@TempDir Path dir) throws Exception {
    Path path = dir.resolve("epoch");
    Path lockFile = dir.resolve("epoch.lock");
    try (
      FileChannel holder = FileChannel.open(
        lockFile,
        StandardOpenOption.CREATE,
        StandardOpenOption.READ,
        StandardOpenOption.WRITE
      );
      FileLock ignored = holder.lock()
    ) {
      assertThat(WorkerEpochStore.initEpoch("worker-1", path)).isPositive();
    }
  }

  @Test
  void rewritesTheEpochAsOneValueWithoutLeavingTemporaryFiles(@TempDir Path dir) throws Exception {
    Path path = dir.resolve("epoch");

    long first = WorkerEpochStore.initEpoch("worker-1", path);
    long second = WorkerEpochStore.initEpoch("worker-1", path);

    assertThat(second).isGreaterThan(first);
    assertThat(Files.size(path)).isEqualTo(Long.BYTES);
    assertThat(readEpoch(path)).isEqualTo(second);
    try (Stream<Path> files = Files.list(dir)) {
      assertThat(files.map(file -> file.getFileName().toString()).toList()).containsExactlyInAnyOrder(
        "epoch",
        "epoch.lock"
      );
    }
  }

  private static long readEpoch(Path path) throws Exception {
    byte[] bytes = Files.readAllBytes(path);
    return ByteBuffer.wrap(bytes).getLong();
  }
}
