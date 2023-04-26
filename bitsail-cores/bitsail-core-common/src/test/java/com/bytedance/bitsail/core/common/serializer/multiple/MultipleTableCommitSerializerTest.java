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

package com.bytedance.bitsail.core.common.serializer.multiple;

import com.bytedance.bitsail.base.serializer.SimpleVersionedBinarySerializer;
import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.core.common.sink.multiple.comittable.MultipleTableCommit;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MultipleTableCommitSerializerTest {

  private MultipleTableCommitSerializer<String> multipleTableCommitSerializer;

  @Before
  public void before() {
    multipleTableCommitSerializer = new MultipleTableCommitSerializer<>(new SimpleVersionedBinarySerializer<String>());
  }

  @Test
  public void testSerializer() throws IOException {
    MultipleTableCommit<String> multipleTableCommit = new MultipleTableCommit<String>();

    multipleTableCommit.setTableId(TableId.of("test", "name"));
    multipleTableCommit.setCommits(Lists.newArrayList("a", "b", "c"));

    byte[] serialize = multipleTableCommitSerializer.serialize(multipleTableCommit);

    MultipleTableCommit<String> deserialize = multipleTableCommitSerializer
        .deserialize(multipleTableCommitSerializer.getVersion(), serialize);

    Assert.assertEquals(multipleTableCommit.getTableId(), deserialize.getTableId());
    Assert.assertEquals(multipleTableCommit.getCommits(), deserialize.getCommits());
  }

}