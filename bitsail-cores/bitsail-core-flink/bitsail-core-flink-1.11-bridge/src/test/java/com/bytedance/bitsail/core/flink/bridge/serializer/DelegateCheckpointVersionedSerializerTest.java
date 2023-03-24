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

import com.bytedance.bitsail.base.serializer.SimpleVersionedBinarySerializer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DelegateCheckpointVersionedSerializerTest {

  @Test
  public void testReadAndWrite() throws IOException {
    SimpleVersionedBinarySerializer<String> binarySerializer = new SimpleVersionedBinarySerializer<>();
    DelegateCheckpointVersionedSerializer<String> delegate =
        new DelegateCheckpointVersionedSerializer<>(binarySerializer);

    Tuple2<Long, String> tuple2 = new Tuple2<Long, String>(1L, "bitsail");
    byte[] serialize = delegate.serialize(tuple2);
    Tuple2<Long, String> after = delegate.deserialize(1, serialize);

    Assert.assertEquals(tuple2.f0, after.f0);
    Assert.assertEquals(tuple2.f1, after.f1);
  }

}