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
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
 * <p>The epoch file is replaced atomically: the new eight bytes are written and forced to a
 * same-directory temporary file, then renamed over the target. A crash therefore leaves either the
 * old value or the new one, never a half-overwritten eight-byte file whose length still looks
 * valid. A file whose size is not exactly eight bytes reads as {@code 0}: the wall-clock floor then
 * supplies a fresh epoch.
 *
 * <p>The value is {@code max(previous + 1, System.currentTimeMillis() * 1000)}, so a healthy
 * clock dominates the increment and the file only matters when the clock steps backwards. If the
 * file cannot be read or written, the fallback is that floor plus up to 999 of its sub-millisecond
 * units.
 */
@Internal
@Slf4j
public final class WorkerEpochStore {

  /** Characters a worker id may keep in a file name; everything else becomes {@code _}. */
  private static final Pattern UNSAFE_FILE_CHARS = Pattern.compile("[^A-Za-z0-9._-]");

  /** The whitelist itself, used to tell a separator-only id from an id that is all underscores. */
  private static final Pattern SAFE_FILE_CHARS = Pattern.compile("[A-Za-z0-9._-]");

  /** The epoch is one binary long. */
  private static final int EPOCH_BYTES = Long.BYTES;

  /**
   * The stable sidecar file the OS lock is taken on. The epoch file itself is replaced by rename,
   * so a lock held on its old inode would not serialize a process that opens the new file.
   */
  private static final String LOCK_SUFFIX = ".lock";

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
    String trimmed = workerId == null ? "" : workerId.trim();
    String safeId = UNSAFE_FILE_CHARS.matcher(trimmed).replaceAll("_");
    if (!SAFE_FILE_CHARS.matcher(trimmed).find()) {
      // An id made only of separators (for example "///") sanitizes to a name that carries no
      // identity; the fixed name keeps the file findable instead of silently colliding.
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
    Path path = (epochFile != null ? epochFile : defaultEpochFile(workerId)).toAbsolutePath().normalize();
    synchronized (WorkerEpochStore.class) {
      Path parent = path.getParent();
      try {
        Files.createDirectories(parent);
      } catch (Exception e) {
        return fallbackEpoch(floor, path, "directory creation", e);
      }
      Path lockFile = path.resolveSibling(path.getFileName() + LOCK_SUFFIX);
      try (
        FileChannel lockChannel = FileChannel.open(
          lockFile,
          StandardOpenOption.CREATE,
          StandardOpenOption.READ,
          StandardOpenOption.WRITE
        )
      ) {
        FileLock fileLock = tryLock(lockChannel, lockFile);
        try {
          // Re-sample after the inter-process lock: waiting for another incarnation must not mint an
          // epoch below the wall clock the file was already written with.
          floor = Math.max(floor, System.currentTimeMillis() * 1000L);
          long prev = readEpoch(path);
          // Saturate instead of wrapping: a corrupt Long.MAX_VALUE must not step the epoch back to the
          // wall-clock floor.
          long next = prev >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(prev + 1L, floor);
          // A failed write falls back from this floor. It must include the value just read, or a
          // restart whose clock stepped backwards could mint an epoch at or below its predecessor.
          floor = next;
          writeEpoch(path, next);
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
   * repairs a truncated or legacy file on the next write.
   *
   * @param path the epoch file
   * @return the stored epoch, or {@code 0} when the file does not hold exactly one long
   * @throws IOException when the file cannot be read
   */
  private static long readEpoch(Path path) throws IOException {
    if (!Files.exists(path) || Files.size(path) != EPOCH_BYTES) {
      return 0L;
    }

    byte[] bytes = Files.readAllBytes(path);
    return bytes.length == EPOCH_BYTES ? ByteBuffer.wrap(bytes).getLong() : 0L;
  }

  /**
   * Replaces the epoch file atomically with one long: write and force a same-directory temporary
   * file, then rename it over the target. A crash during the write leaves the target untouched
   * instead of producing an eight-byte file with mixed old and new bytes.
   *
   * @param path  the epoch file
   * @param epoch the value to store
   * @throws IOException when the temporary file or the rename fails
   */
  private static void writeEpoch(Path path, long epoch) throws IOException {
    boolean replaced = false;
    Path parent = path.getParent();
    Path temp = Files.createTempFile(parent, "zeta-epoch-", ".tmp");
    try {
      try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.WRITE)) {
        ByteBuffer buffer = ByteBuffer.allocate(EPOCH_BYTES).putLong(epoch);
        buffer.flip();
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
        channel.force(true);
      }
      try {
        Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        // The file system cannot promise an atomic replacement; a same-directory move is still the
        // closest available shape, and a damaged target reads as absent on the next start.
        Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
        log.warn("Atomic epoch-file replacement is unsupported for {}; the rename was not atomic", path);
      }
      replaced = true;
    } finally {
      if (!replaced) {
        try {
          Files.deleteIfExists(temp);
        } catch (IOException e) {
          log.debug("Failed to delete temporary epoch file {}", temp, e);
        }
      }
    }
  }

  /**
   * Takes the inter-process lock on the sidecar lock file, degrading to the in-JVM monitor when
   * the platform or filesystem does not support advisory locks, or when this JVM already holds the
   * file through another channel (the class monitor still serializes this class's own calls).
   *
   * @param channel the open lock-file channel
   * @param path    the lock file, for the warning
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
   * @return {@code floor} plus 0..999 of its sub-millisecond units, saturated
   */
  private static long fallbackEpoch(long floor, Path path, String stage, Exception cause) {
    int jitter = ThreadLocalRandom.current().nextInt(1000);
    long epoch = floor > Long.MAX_VALUE - jitter ? Long.MAX_VALUE : floor + jitter;
    log.error("Epoch {} failed, using timestamp with jitter: epoch={}, path={}", stage, epoch, path, cause);
    return epoch;
  }
}
