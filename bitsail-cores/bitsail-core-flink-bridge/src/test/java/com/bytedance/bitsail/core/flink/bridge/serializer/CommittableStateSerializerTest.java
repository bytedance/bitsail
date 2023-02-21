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

import com.bytedance.bitsail.base.connector.writer.v1.comittable.CommittableState;
import com.bytedance.bitsail.base.serializer.BinarySerializer;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CommittableStateSerializerTest {

  private CommittableStateSerializer<String> committableStateSerializer;
  private DelegateSimpleVersionedSerializer<String> delegateSimpleVersionedSerializer;

  @Before
  public void init() {
    delegateSimpleVersionedSerializer = DelegateSimpleVersionedSerializer.delegate(new StringBinarySerializer());
    committableStateSerializer = new CommittableStateSerializer<>(
        delegateSimpleVersionedSerializer);
  }

  @Test
  public void testSerializeAndDeserialize() throws IOException {
    CommittableState<String> state = new CommittableState<>(
        1L, ImmutableList.of("test1", "test2"));
    byte[] serializeResult = committableStateSerializer.serialize(state);
    CommittableState<String> deserializeResult = committableStateSerializer.deserialize(
        delegateSimpleVersionedSerializer.getVersion(),
        serializeResult);
    assertEquals(deserializeResult.getCheckpointId(), 1L);
    assertEquals(deserializeResult.getCommittables().size(), 2);
    assertEquals(deserializeResult.getCommittables().get(0), "test1");
  }

  public static class StringBinarySerializer implements BinarySerializer<String> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
      return VERSION;
    }

    @Override
    public byte[] serialize(String obj) throws IOException {
      return obj.getBytes();
    }

    @Override
    public String deserialize(int version, byte[] serialized) throws IOException {
      return new String(serialized);
    }
  }
}