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
package io.github.hyshmily.zeta.cache.codec;

import io.github.hyshmily.zeta.Internal;
import jakarta.annotation.Nullable;
import java.io.IOException;

@Internal
public interface CacheCompressor {
  int MIN_COMPRESS_LENGTH = 256;

  @Nullable
  Object wrap(@Nullable Object value);

  @Nullable
  Object unwrap(@Nullable Object stored) throws IOException;

  CacheCompressor NONE = new CacheCompressor() {
    @Override
    public Object wrap(Object value) {
      return value;
    }

    @Override
    public Object unwrap(Object stored) {
      return stored;
    }
  };
}
