package io.github.hyshmily.hotkey.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ReportMessage} record creation and field access.
 */
class ReportMessageTest {

  @Test
  void shouldCreateReportMessage() {
    ReportMessage msg = new ReportMessage("app1", 1000L, Map.of("key1", 5L, "key2", 3L));
    assertThat(msg.appName()).isEqualTo("app1");
    assertThat(msg.timestamp()).isEqualTo(1000L);
    assertThat(msg.counts()).hasSize(2);
  }
}
