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

import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.core.common.sink.multiple.comittable.MultipleTableCommit;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.SerializationUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MultipleTableCommitSerializer<CommitT> implements BinarySerializer<MultipleTableCommit<CommitT>> {

  private BinarySerializer<CommitT> original;

  public MultipleTableCommitSerializer(BinarySerializer<CommitT> original) {
    this.original = original;
  }

  @Override
  public int getVersion() {
    return original.getVersion();
  }

  @Override
  public byte[] serialize(MultipleTableCommit<CommitT> multipleTableCommit) throws IOException {
    if (Objects.isNull(multipleTableCommit)) {
      return null;
    }

    byte[] buffer;
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      TableId tableId = multipleTableCommit.getTableId();
      List<CommitT> commits = multipleTableCommit.getCommits();
      buffer = SerializationUtils.serialize(tableId);
      outputStream.write(buffer.length);
      outputStream.write(buffer);
      for (CommitT commitT : commits) {
        buffer = original.serialize(commitT);
        outputStream.write(buffer.length);
        outputStream.write(buffer);
      }
      buffer = outputStream.toByteArray();
    }

    return buffer;
  }

  @Override
  public MultipleTableCommit<CommitT> deserialize(int version, byte[] serialized) throws IOException {
    if (serialized == null) {
      return null;
    }
    MultipleTableCommit.MultipleTableCommitBuilder<CommitT> builder = MultipleTableCommit.<CommitT>builder();
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized)) {
      int length = inputStream.read();
      byte[] buffer = new byte[length];

      inputStream.read(buffer, 0, length);
      TableId tableId = SerializationUtils.deserialize(buffer);
      builder.tableId(tableId);

      List<CommitT> commits = Lists.newArrayList();
      while ((length = inputStream.read()) != -1) {
        buffer = new byte[length];
        inputStream.read(buffer, 0, length);
        CommitT deserialize = original.deserialize(version, buffer);
        commits.add(deserialize);
      }

      builder.commits(commits);

    }
    return builder.build();
  }
}
