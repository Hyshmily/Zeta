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
package io.github.hyshmily.zeta.util.id;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.InstanceIdGenerator;
import io.github.hyshmily.zeta.util.TimeSource;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Twitter-Snowflake style ID generator producing 64-bit, time-sortable, cluster-unique IDs.
 *
 * <p>Bit layout (MSB first):
 * <pre>
 *   0 | 41 bits timestamp delta (ms) | 2 bits datacenter | 8 bits worker | 12 bits sequence
 * </pre>
 *
 * <ul>
 *   <li><b>Sign (1 bit):</b> always 0 – IDs are always positive</li>
 *   <li><b>Timestamp (41 bits):</b> milliseconds since {@link #EPOCH} (~69 years)</li>
 *   <li><b>Datacenter (2 bits):</b> {@code 0..3}, configured explicitly</li>
 *   <li><b>Worker (8 bits):</b> {@code 0..255}, derived from IP last octet or
 *       {@link InstanceIdGenerator#getNodeId()}</li>
 *   <li><b>Sequence (12 bits):</b> {@code 0..4095}, increments per-millisecond per-node</li>
 * </ul>
 *
 * <p>Zeta-specific adaptations over the canonical reference:
 * <ul>
 *   <li>Clock source: {@link TimeSource#currentTimeMillis()} – cached, avoids native JNI overhead</li>
 *   <li>Worker seed: {@link InstanceIdGenerator#getNodeId()} – already unique per JVM</li>
 *   <li>Small clock rewinds (up to {@code timeOffset} ms) are tolerated by busy-wait</li>
 *   <li>Optionally randomises the per-millisecond sequence start to avoid even-number bias
 *       in the lowest 12 bits (useful when IDs are used for hash-based partitioning)</li>
 * </ul>
 *
 * <p>Thread-safe (synchronized).</p>
 */
@Internal
public class SnowflakeIdGenerator {

  /**
   * Custom epoch: 2025-01-01T00:00:00Z.
   * Chosen so that the 41-bit timestamp space lasts until ~2094.
   */
  private static final long EPOCH = 1735689600000L;

  private static final long DATA_CENTER_ID_BITS = 2L;
  private static final long WORKER_ID_BITS = 8L;
  private static final long SEQUENCE_BITS = 12L;

  private static final long MAX_DATA_CENTER_ID = ~(-1L << DATA_CENTER_ID_BITS);
  private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);
  private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

  private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
  private static final long DATA_CENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
  private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATA_CENTER_ID_BITS;

  private final long dataCenterId;
  private final long workerId;
  private final long timeOffset;
  private final boolean randomSequence;

  private long sequence;
  private long lastTimestamp = -1L;

  /**
   * Auto-configuring constructor: datacenter=0, worker derived from IP last octet.
   */
  public SnowflakeIdGenerator() {
    this(0, resolveWorkerId(), 5L, false);
  }

  /**
   * Explicit datacenter + worker, default 5ms clock rewind tolerance.
   */
  public SnowflakeIdGenerator(long dataCenterId, long workerId) {
    this(dataCenterId, workerId, 5L, false);
  }

  /**
   * Full constructor.
   *
   * @param dataCenterId   datacenter ID (0..3)
   * @param workerId       worker ID (0..255)
   * @param timeOffset     max allowed clock rewind in ms (0 disables tolerance)
   * @param randomSequence if true, randomise per-millisec sequence start to avoid even bias
   */
  public SnowflakeIdGenerator(
      long dataCenterId, long workerId, long timeOffset, boolean randomSequence) {
    if (dataCenterId < 0 || dataCenterId > MAX_DATA_CENTER_ID) {
      throw new IllegalArgumentException(
          "dataCenterId must be 0.." + MAX_DATA_CENTER_ID + ", got " + dataCenterId);
    }
    if (workerId < 0 || workerId > MAX_WORKER_ID) {
      throw new IllegalArgumentException(
          "workerId must be 0.." + MAX_WORKER_ID + ", got " + workerId);
    }
    this.dataCenterId = dataCenterId;
    this.workerId = workerId;
    this.timeOffset = timeOffset;
    this.randomSequence = randomSequence;
    TimeSource.start();
  }

  /**
   * Generate the next unique ID.
   *
   * @return a 64-bit, time-sortable, cluster-unique ID (always positive)
   * @throws RuntimeException if the system clock has moved backwards by more than {@code timeOffset} ms
   */
  public synchronized long nextId() {
    long current = TimeSource.currentTimeMillis();

    if (current < lastTimestamp) {
      long offset = lastTimestamp - current;
      if (offset > timeOffset) {
        throw new RuntimeException(
            "Clock moved backwards by " + offset + "ms (max allowed: " + timeOffset + "ms)");
      }
      current = waitForNextMillis(lastTimestamp);
    }

    if (lastTimestamp == current) {
      sequence = (sequence + 1) & SEQUENCE_MASK;
      if (sequence == 0) {
        current = waitForNextMillis(lastTimestamp);
      }
    } else {
      sequence = randomSequence
          ? (long) (Math.random() * (SEQUENCE_MASK + 1))
          : 0L;
    }

    lastTimestamp = current;
    return ((current - EPOCH) << TIMESTAMP_SHIFT)
        | (dataCenterId << DATA_CENTER_ID_SHIFT)
        | (workerId << WORKER_ID_SHIFT)
        | sequence;
  }

  private static long waitForNextMillis(long last) {
    long timestamp = TimeSource.currentTimeMillis();
    while (timestamp <= last) {
      timestamp = TimeSource.currentTimeMillis();
    }
    return timestamp;
  }

  /**
   * Resolve worker ID from the last octet of the local IP address, falling back
   * to the lower 8 bits of {@link InstanceIdGenerator#getNodeId()}.
   */
  private static long resolveWorkerId() {
    try {
      InetAddress addr = InetAddress.getLocalHost();
      byte[] bytes = addr.getAddress();
      return bytes[bytes.length - 1] & 0xFFL;
    } catch (UnknownHostException e) {
      return InstanceIdGenerator.getNodeId() & 0xFFL;
    }
  }
}
