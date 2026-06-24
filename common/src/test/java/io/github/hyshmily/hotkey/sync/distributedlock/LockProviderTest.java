package io.github.hyshmily.hotkey.sync.distributedlock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("LockProvider tests")
class LockProviderTest {

  @Test
  @DisplayName("default tryLock delegates to base method")
  void defaultTryLock_delegatesToBaseMethod() {
    AtomicReference<String> capturedKey = new AtomicReference<>();
    LockProvider provider = (key, expire, unit) -> {
      capturedKey.set(key);
      return () -> {};
    };
    AutoReleaseLock lock = provider.tryLock("myLock", 5, TimeUnit.SECONDS, 3, 2, 1);
    assertThat(lock).isNotNull();
    assertThat(capturedKey.get()).isEqualTo("myLock");
  }

  @Test
  @DisplayName("default tryLock returns null when base returns null")
  void defaultTryLock_returnsNullWhenBaseReturnsNull() {
    LockProvider provider = (key, expire, unit) -> null;
    AutoReleaseLock lock = provider.tryLock("myLock", 5, TimeUnit.SECONDS, 3, 2, 1);
    assertThat(lock).isNull();
  }
}
