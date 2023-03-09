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

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.core.flink.bridge.reader.delegate.DelegateFlinkSourceSplit;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;

public class DelegateFlinkSourceSplitSerializer<SplitT extends SourceSplit>
    implements SimpleVersionedSerializer<DelegateFlinkSourceSplit<SplitT>>, Serializable {

  private BinarySerializer<SplitT> splitBinarySerializer;

  public DelegateFlinkSourceSplitSerializer(BinarySerializer<SplitT> splitBinarySerializer) {
    this.splitBinarySerializer = splitBinarySerializer;
  }

  @Override
  public int getVersion() {
    return this.splitBinarySerializer.getVersion();
  }

  @Override
  public byte[] serialize(DelegateFlinkSourceSplit<SplitT> obj) throws IOException {
    return splitBinarySerializer.serialize(obj.getSourceSplit());
  }

  @Override
  public DelegateFlinkSourceSplit<SplitT> deserialize(int version, byte[] serialized) throws IOException {
    SplitT deserialize = splitBinarySerializer.deserialize(version, serialized);
    return new DelegateFlinkSourceSplit<>(deserialize);
  }
}
