/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.source.split;

import com.bytedance.bitsail.base.serializer.BinarySerializer;

import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.util.Objects;

public abstract class BaseSplitSerializer implements BinarySerializer<BaseCDCSplit> {
  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(BaseCDCSplit split) {
    if (Objects.nonNull(split)) {
      return SerializationUtils.serialize(split);
    }
    return null;
  }

  @Override
  public BaseCDCSplit deserialize(int version, byte[] serialized) throws IOException {
    if (Objects.isNull(serialized)) {
      return null;
    }
    switch (version) {
      case 1:
        return SerializationUtils.deserialize(serialized);
      default:
        throw new IOException("Invalid BaseSplitSerializer version: " + version);
    }
  }
}
