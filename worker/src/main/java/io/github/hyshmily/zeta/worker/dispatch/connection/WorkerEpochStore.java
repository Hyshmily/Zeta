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

import io.github.hyshmily.zeta.util.Internal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Persists the Worker's restart epoch as one binary {@code long} in a lock-guarded local file.
 *
 * <p>The file is machine-owned state, not a document: eight bytes, no text formatting, no
 * comments, no parsing. The read-modify-write runs under an OS file lock when the platform provides
 * one, and always under the in-JVM monitor, so two Pods briefly sharing the same {@code hostPath}
 * during a rolling update cannot mint the same epoch.
 *
 * <p>A file whose size is not exactly eight bytes (an empty file, a torn write, or a file left by
 * a text-format build) reads as {@code 0}: the wall-clock floor then supplies a fresh epoch.
 *
 * <p>The value is {@code max(previous + 1, System.currentTimeMillis() * 1000)}, so a healthy
 * clock dominates the increment and the file only matters when the clock steps backwards. If the
 * file cannot be read or written, the fallback is the wall clock plus up to 1 s of jitter.
 */
@Internal
@Slf4j
public final class WorkerEpochStore {

  /** Characters a worker id may keep in a file name; everything else becomes {@code _}. */
  private static final Pattern UNSAFE_FILE_CHARS = Pattern.compile("[^A-Za-z0-9._-]");

  /** The epoch is one binary long. */
  private static final int EPOCH_BYTES = Long.BYTES;

  private WorkerEpochStore() {}

  /**
   * Default epoch-file location for a Worker that configures none:
   * {@code ${java.io.tmpdir}/zeta-epoch-<workerId>}, with every character outside
   * {@code [A-Za-z0-9._-]} replaced by {@code _} so a deployment-supplied instance id is always a
   * valid file name.
   *
   * @param workerId unique identity of this Worker node
   * @return the default epoch file path
   */
  public static Path defaultEpochFile(String workerId) {
    // INSTANCE_ID is deployment-supplied and may contain ':', '/' or other characters that are
    // invalid or path-significant on some filesystems. The whitelist keeps the name a name.
    String safeId = UNSAFE_FILE_CHARS.matcher(workerId.trim()).replaceAll("_");
    if (safeId.isBlank()) {
      safeId = "unknown";
    }
    return Path.of(System.getProperty("java.io.tmpdir"), "zeta-epoch-" + safeId);
  }

  /**
   * Initializes this Worker incarnation's epoch from the lock-guarded local file.
   *
   * @param workerId  unique identity of this Worker node
   * @param epochFile the epoch file; {@code null} selects the default under
   *                  {@code java.io.tmpdir}
   * @return the new epoch value, or a timestamp-based value on complete failure
   */
  public static long initEpoch(String workerId, @Nullable Path epochFile) {
    long floor = System.currentTimeMillis() * 1000L;
    Path path = epochFile != null ? epochFile : defaultEpochFile(workerId);
    synchronized (WorkerEpochStore.class) {
      Path parent = path.getParent();
      if (parent != null) {
        try {
          Files.createDirectories(parent);
        } catch (Exception e) {
          return fallbackEpoch(floor, path, "directory creation", e);
        }
      }
      try (
        FileChannel channel = FileChannel.open(
          path,
          StandardOpenOption.CREATE,
          StandardOpenOption.READ,
          StandardOpenOption.WRITE
        )
      ) {
        FileLock fileLock = tryLock(channel, path);
        try {
          long next = Math.max(readEpoch(channel) + 1L, floor);
          writeEpoch(channel, next);
          return next;
        } finally {
          releaseQuietly(fileLock);
        }
      } catch (Exception e) {
        return fallbackEpoch(floor, path, "read/write", e);
      }
    }
  }

  /**
   * Reads the previous epoch. Anything other than exactly one long is treated as absent, which
   * repairs a truncated or torn file on the next write.
   *
   * @param channel the open channel
   * @return the stored epoch, or {@code 0} when the file does not hold exactly one long
   * @throws IOException when the channel fails
   */
  private static long readEpoch(FileChannel channel) throws IOException {
    if (channel.size() != EPOCH_BYTES) {
      return 0L;
    }

    ByteBuffer buffer = ByteBuffer.allocate(EPOCH_BYTES);
    while (buffer.hasRemaining()) {
      if (channel.read(buffer, buffer.position()) < 0) {
        return 0L;
      }
    }
    return buffer.flip().getLong();
  }

  /**
   * Writes the epoch as one long at offset zero and truncates any trailing bytes.
   *
   * @param channel the open channel
   * @param epoch   the value to store
   * @throws IOException when the channel fails
   */
  private static void writeEpoch(FileChannel channel, long epoch) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(EPOCH_BYTES).putLong(epoch);
    buffer.flip();
    // Positional writes: the channel position stays at zero, so the offset is the buffer's own.
    while (buffer.hasRemaining()) {
      channel.write(buffer, buffer.position());
    }
    channel.truncate(EPOCH_BYTES);
    // The truncate above changes the file length, which is metadata: force(true) flushes both.
    channel.force(true);
  }

  /**
   * Takes the inter-process lock on the epoch file, degrading to the in-JVM monitor when the
   * platform or filesystem does not support advisory locks, or when this JVM already holds the
   * file through another channel (the class monitor still serializes this class's own calls).
   *
   * @param channel the open channel
   * @param path    the epoch file, for the warning
   * @return the lock, or {@code null} when locking is unsupported
   */
  private static @Nullable FileLock tryLock(FileChannel channel, Path path) {
    try {
      return channel.lock();
    } catch (IOException | UnsupportedOperationException | OverlappingFileLockException e) {
      log.warn("Epoch file lock unavailable ({}); concurrent processes sharing {} may mint the same epoch", e, path);
      return null;
    }
  }

  /**
   * Releases a lock that may not have been acquired.
   *
   * @param fileLock the lock, or {@code null}
   */
  private static void releaseQuietly(@Nullable FileLock fileLock) {
    if (fileLock == null || !fileLock.isValid()) {
      return;
    }
    try {
      fileLock.release();
    } catch (IOException e) {
      log.debug("Failed to release epoch file lock", e);
    }
  }

  /**
   * Last-resort epoch when the file cannot be used: wall clock plus jitter.
   *
   * @param floor wall-clock floor in the epoch domain
   * @param path  the epoch file, so one ERROR names the path an operator must fix
   * @param stage what failed
   * @param cause the failure
   * @return {@code floor} plus 0..999 ms
   */
  private static long fallbackEpoch(long floor, Path path, String stage, Exception cause) {
    long epoch = floor + ThreadLocalRandom.current().nextInt(1000);
    log.error("Epoch {} failed, using timestamp with jitter: epoch={}, path={}", stage, epoch, path, cause);
    return epoch;
  }
}
