/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.core.flink.bridge.serializer;

import com.bytedance.bitsail.base.serializer.BinarySerializer;

import com.google.common.base.Preconditions;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created 2022/6/14
 */
public class DelegateSimpleVersionedSerializer<T> implements SimpleVersionedSerializer<T>, Serializable {

  private final BinarySerializer<T> serializer;

  private DelegateSimpleVersionedSerializer(BinarySerializer<T> serializer) {
    this.serializer = serializer;
  }

  public static <T> DelegateSimpleVersionedSerializer<T> delegate(BinarySerializer<T> serializer) {
    Preconditions.checkNotNull(serializer, "Serializer should not be null.");
    return new DelegateSimpleVersionedSerializer<T>(serializer);
  }

  @Override
  public int getVersion() {
    return this.serializer.getVersion();
  }

  @Override
  public byte[] serialize(T obj) throws IOException {
    return serializer.serialize(obj);
  }

  @Override
  public T deserialize(int version, byte[] serialized) throws IOException {
    return serializer.deserialize(version, serialized);
  }
}
