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
package io.github.hyshmily.zeta.reporting;

import io.github.hyshmily.zeta.Internal;
import org.jspecify.annotations.NonNull;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

/**
 * Report-path {@link MessageConverter} that can emit either the classic
 * Jackson JSON body or the compact binary body ({@link ReportMessageCodec},
 * ADR-0074), and decodes both.
 *
 * <p><b>Encode side</b> follows the configured {@code compactEncoding} flag:
 * when set, {@link ReportMessage} payloads are written as the compact body
 * (content type {@code application/octet-stream}); everything else — and
 * every payload when the flag is unset — goes through the Jackson delegate
 * unchanged.
 *
 * <p><b>Decode side is always dual-format</b>: a body starting with the
 * compact magic byte is decoded by {@link ReportMessageCodec}, anything else
 * is handed to the Jackson delegate. This is what makes the JSON→COMPACT
 * rolling upgrade safe (ADR-0074): upgrade Workers first (they decode both),
 * then flip Apps to compact. Old Apps never see a compact body, and a mixed
 * cluster loses no reports. A malformed compact body surfaces as a
 * {@link MessageConversionException} — the same poison-message handling the
 * JSON path already gets (log + discard, no requeue).
 */
@Internal
public class CompactAwareReportMessageConverter implements MessageConverter {

  private final Jackson2JsonMessageConverter jsonDelegate;
  private final boolean compactEncoding;

  /**
   * @param jsonDelegate    the Jackson converter used for the JSON format
   * @param compactEncoding whether {@link ReportMessage} payloads are
   *                        <em>sent</em> in the compact format (decode is
   *                        always dual-format regardless of this flag)
   */
  public CompactAwareReportMessageConverter(Jackson2JsonMessageConverter jsonDelegate, boolean compactEncoding) {
    this.jsonDelegate = jsonDelegate;
    this.compactEncoding = compactEncoding;
  }

  @Override
  @NonNull
  public Message toMessage(@NonNull Object object, @NonNull MessageProperties messageProperties)
    throws MessageConversionException {
    if (compactEncoding && object instanceof ReportMessage message) {
      messageProperties.setContentType(MessageProperties.CONTENT_TYPE_BYTES);
      return new Message(ReportMessageCodec.encode(message), messageProperties);
    }

    return jsonDelegate.toMessage(object, messageProperties);
  }

  @Override
  @NonNull
  public Object fromMessage(Message message) throws MessageConversionException {
    byte[] body = message.getBody();
    if (ReportMessageCodec.isCompact(body)) {
      try {
        return ReportMessageCodec.decode(body);
      } catch (IllegalArgumentException e) {
        throw new MessageConversionException("Malformed compact report body", e);
      }
    }
    return jsonDelegate.fromMessage(message);
  }
}
