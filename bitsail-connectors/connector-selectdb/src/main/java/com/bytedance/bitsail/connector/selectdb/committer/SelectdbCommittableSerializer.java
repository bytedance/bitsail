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

package com.bytedance.bitsail.connector.selectdb.committer;

import com.bytedance.bitsail.base.serializer.BinarySerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SelectdbCommittableSerializer implements BinarySerializer<SelectdbCommittable> {

  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(SelectdbCommittable selectdbCommittable) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
         final DataOutputStream out = new DataOutputStream(baos)) {
      out.writeUTF(selectdbCommittable.getHostPort());
      out.writeUTF(selectdbCommittable.getClusterName());
      out.writeUTF(selectdbCommittable.getCopySQL());
      out.flush();
      return baos.toByteArray();
    }
  }

  @Override
  public SelectdbCommittable deserialize(int version, byte[] bytes) throws IOException {
    try (final ByteArrayInputStream inputBytes = new ByteArrayInputStream(bytes);
         final DataInputStream in = new DataInputStream(inputBytes)) {
      final String hostPort = in.readUTF();
      final String clusterName = in.readUTF();
      final String copySQL = in.readUTF();
      return new SelectdbCommittable(hostPort, clusterName, copySQL);
    }
  }
}