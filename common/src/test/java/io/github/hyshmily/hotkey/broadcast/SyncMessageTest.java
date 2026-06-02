package io.github.hyshmily.hotkey.broadcast;

import static io.github.hyshmily.hotkey.constant.HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED;
import static io.github.hyshmily.hotkey.constant.HotKeyConstants.AMQP_HEADER_TYPE;
import static io.github.hyshmily.hotkey.constant.HotKeyConstants.AMQP_HEADER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class SyncMessageTest {

  @Test
  void from_shouldParseValidMessage() {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, SyncMessage.TYPE_REFRESH);
    props.setHeader(AMQP_HEADER_VERSION, 42L);
    props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, true);
    Message msg = new Message("cacheKey".getBytes(StandardCharsets.UTF_8), props);
    SyncMessage sm = SyncMessage.from(msg);
    assertThat(sm).isNotNull();
    assertThat(sm.cacheKey()).isEqualTo("cacheKey");
    assertThat(sm.type()).isEqualTo(SyncMessage.TYPE_REFRESH);
    assertThat(sm.version()).isEqualTo(42L);
    assertThat(sm.isVersionDegraded()).isTrue();
  }

  @Test
  void from_shouldReturnNullForEmptyBody() {
    Message msg = new Message(new byte[0], new MessageProperties());
    assertThat(SyncMessage.from(msg)).isNull();
  }

  @Test
  void from_shouldReturnNullForBlankKey() {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, SyncMessage.TYPE_REFRESH);
    Message msg = new Message("   ".getBytes(StandardCharsets.UTF_8), props);
    assertThat(SyncMessage.from(msg)).isNull();
  }

  @Test
  void from_shouldUseDefaultsForMissingHeaders() {
    Message msg = new Message("key".getBytes(StandardCharsets.UTF_8), new MessageProperties());
    SyncMessage sm = SyncMessage.from(msg);
    assertThat(sm.version()).isZero();
    assertThat(sm.isVersionDegraded()).isFalse();
  }

  @Test
  void shouldHaveExpectedTypeConstants() {
    assertThat(SyncMessage.TYPE_REFRESH).isEqualTo("REFRESH");
    assertThat(SyncMessage.TYPE_INVALIDATE).isEqualTo("INVALIDATE");
  }
}
