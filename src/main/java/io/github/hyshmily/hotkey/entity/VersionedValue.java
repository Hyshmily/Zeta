package io.github.hyshmily.hotkey.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class VersionedValue {

  private final Object value;
  private final long version;
}
