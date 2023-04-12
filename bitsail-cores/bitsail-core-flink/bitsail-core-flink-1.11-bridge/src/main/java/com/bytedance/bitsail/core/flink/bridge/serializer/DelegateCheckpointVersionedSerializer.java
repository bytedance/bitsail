/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.core.flink.bridge.serializer;

import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;

public class DelegateCheckpointVersionedSerializer<T> implements SimpleVersionedSerializer<Tuple2<Long, T>>, Serializable {

  private final BinarySerializer<T> serializer;

  public DelegateCheckpointVersionedSerializer(BinarySerializer<T> serializer) {
    this.serializer = serializer;
  }

  @Override
  public int getVersion() {
    return this.serializer.getVersion();
  }

  @Override
  public byte[] serialize(Tuple2<Long, T> obj) throws IOException {
    Preconditions.checkNotNull(obj);
    //fixed 8 bit
    byte[] checkpoints = Longs.toByteArray(obj.f0);
    byte[] serialize = serializer.serialize(obj.f1);
    byte[] bytes = new byte[checkpoints.length + serialize.length];
    System.arraycopy(checkpoints, 0, bytes, 0, checkpoints.length);
    System.arraycopy(serialize, 0, bytes, checkpoints.length, serialize.length);
    return bytes;
  }

  @Override
  public Tuple2<Long, T> deserialize(int version, byte[] serialized) throws IOException {
    if (serialized.length < Long.BYTES) {
      throw BitSailException.asBitSailException(CommonErrorCode.INTERNAL_ERROR, "checkpoint's size should bigger than " + Long.BYTES);
    }
    byte[] checkpoints = new byte[Long.BYTES];
    byte[] serialize = new byte[serialized.length - checkpoints.length];
    System.arraycopy(serialized, 0, checkpoints, 0, checkpoints.length);
    System.arraycopy(serialized, checkpoints.length, serialize, 0, serialize.length);

    return Tuple2.of(Longs.fromByteArray(checkpoints), serializer.deserialize(version, serialize));
  }
}