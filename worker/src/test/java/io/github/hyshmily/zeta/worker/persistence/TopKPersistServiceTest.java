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
package io.github.hyshmily.zeta.worker.persistence;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.worker.config.WorkerProperties.Persistence;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

/**
 * Tests for {@link TopKPersistService}.
 *
 * <p>Covers Redis null/empty/malformed data, empty TopK lists, and Redis
 * connection failures for both restore and persist paths.
 */
@ExtendWith(MockitoExtension.class)
class TopKPersistServiceTest {

  @Mock
  private TopK topK;

  @Mock
  private StringRedisTemplate redisTemplate;

  @Mock
  private ValueOperations<String, String> valueOps;

  private Persistence config;
  private TopKPersistService service;

  private static final String APP_NAME = "testApp";
  private static final String NODE_ID = "worker-1";
  private static final String REDIS_KEY = "zeta:topk:worker:testApp:worker-1";

  @BeforeEach
  void setUp() {
    config = new Persistence();
    config.setEnabled(true);
    config.setRedisKeyPrefix("zeta:topk:worker:");
    config.setTopKCount(100);
    config.setTtlDays(3);
    lenient().when(redisTemplate.opsForValue()).thenReturn(valueOps);
    service = new TopKPersistService(topK, redisTemplate, APP_NAME, NODE_ID, config);
  }

  // ── restoreFromRedis ──

  /**
   * Verifies that restoreFromRedis handles a null Redis value gracefully (no error).
   */
  @Test
  void restoreFromRedis_shouldHandleNullJson() {
    when(valueOps.get(REDIS_KEY)).thenReturn(null);
    assertThatCode(() -> service.restoreFromRedis()).doesNotThrowAnyException();
    verifyNoInteractions(topK);
  }

  /**
   * Verifies that restoreFromRedis handles the JSON literal {@code "null"}
   * gracefully.  Jackson's {@code readValue("null", List.class)} returns
   * {@code null}, which exercises the {@code items == null} branch inside
   * {@code restoreFromRedis}.
   */
  @Test
  void restoreFromRedis_shouldHandleNullJsonLiteral() {
    when(valueOps.get(REDIS_KEY)).thenReturn("null");
    assertThatCode(() -> service.restoreFromRedis()).doesNotThrowAnyException();
    verifyNoInteractions(topK);
  }

  /**
   * Verifies that restoreFromRedis handles an empty Redis value gracefully.
   */
  @Test
  void restoreFromRedis_shouldHandleEmptyJson() {
    when(valueOps.get(REDIS_KEY)).thenReturn("");
    assertThatCode(() -> service.restoreFromRedis()).doesNotThrowAnyException();
    verifyNoInteractions(topK);
  }

  /**
   * Verifies that restoreFromRedis handles malformed JSON without propagating exceptions.
   */
  @Test
  void restoreFromRedis_shouldHandleMalformedJson() {
    when(valueOps.get(REDIS_KEY)).thenReturn("not valid json");
    assertThatCode(() -> service.restoreFromRedis()).doesNotThrowAnyException();
    verifyNoInteractions(topK);
  }

  /**
   * Verifies that restoreFromRedis handles an empty JSON array gracefully.
   */
  @Test
  void restoreFromRedis_shouldHandleEmptyList() {
    when(valueOps.get(REDIS_KEY)).thenReturn("[]");
    assertThatCode(() -> service.restoreFromRedis()).doesNotThrowAnyException();
    verifyNoInteractions(topK);
  }

  /**
   * Verifies that restoreFromRedis handles a Redis connection failure gracefully.
   */
  @Test
  void restoreFromRedis_shouldHandleRedisFailure() {
    when(valueOps.get(REDIS_KEY)).thenThrow(new RuntimeException("Redis unavailable"));
    assertThatCode(() -> service.restoreFromRedis()).doesNotThrowAnyException();
  }

  // ── persistToRedis ──

  /**
   * Verifies that persistToRedis does nothing when the TopK list is empty.
   */
  @Test
  void persistToRedis_shouldSkipWhenTopKIsEmpty() {
    when(topK.listTopN(100)).thenReturn(List.of());
    service.persistToRedis();
    verify(valueOps, never()).set(anyString(), anyString(), anyLong(), any());
  }

  /**
   * Verifies that persistToRedis serialises and stores the TopK list.
   */
  @Test
  void persistToRedis_shouldPersistItems() {
    when(topK.listTopN(100)).thenReturn(List.of(new Item("hotKey", 100)));
    service.persistToRedis();
    verify(valueOps).set(eq(REDIS_KEY), anyString(), eq(3L), any());
  }

  /**
   * Verifies that persistToRedis handles Redis connection failure gracefully
   * without propagating exceptions.
   */
  @Test
  void persistToRedis_shouldHandleRedisFailure() {
    when(topK.listTopN(100)).thenReturn(List.of(new Item("hotKey", 100)));
    doThrow(new RuntimeException("Redis unavailable")).when(valueOps).set(anyString(), anyString(), anyLong(), any());
    assertThatCode(() -> service.persistToRedis()).doesNotThrowAnyException();
  }

  /**
   * Verifies that persistToRedis handles serialization failure gracefully
   * (e.g., TopK item with null key or null count).
   */
  @Test
  void persistToRedis_shouldHandleSerializationFailure() {
    when(topK.listTopN(100)).thenReturn(List.of(new Item(null, 0)));
    assertThatCode(() -> service.persistToRedis()).doesNotThrowAnyException();
  }

  /**
   * Verifies that restoreFromRedis parses valid JSON and feeds the extracted
   * items into {@link TopK#addDirect(Map)} for warm-up of the HeavyKeeper sketch.
   */
  @Test
  void restoreFromRedis_shouldRestoreItemsWhenValidJson() {
    when(valueOps.get(REDIS_KEY)).thenReturn("[{\"key\":\"hotKey\",\"count\":100}]");
    service.restoreFromRedis();
    verify(topK).warm(argThat((Map<String, Long> m) -> m.size() == 1 && m.get("hotKey") == 100L));
  }
}
