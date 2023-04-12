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

package com.bytedance.bitsail.connector.cdc.source.coordinator.state;

import com.bytedance.bitsail.connector.cdc.source.split.SplitType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AssignmentStateSerializerTest {
  @Test
  public void testSerializeBinlogState() throws IOException {
    AssignmentStateSerializer serializer = new AssignmentStateSerializer();
    BinlogAssignmentState binlogAssignmentState = new BinlogAssignmentState(true);
    byte[] obj = serializer.serialize(binlogAssignmentState);
    BaseAssignmentState desObj = serializer.deserialize(serializer.getVersion(), obj);
    Assert.assertEquals(SplitType.BINLOG_SPLIT_TYPE, desObj.getType());
    Assert.assertTrue(((BinlogAssignmentState) desObj).isAssigned());
  }

  @Test
  public void testSerializeSnapshotState() throws IOException {
    AssignmentStateSerializer serializer = new AssignmentStateSerializer();
    SnapshotAssignmentState snapshotAssignmentState = new SnapshotAssignmentState();
    byte[] obj = serializer.serialize(snapshotAssignmentState);
    BaseAssignmentState desObj = serializer.deserialize(serializer.getVersion(), obj);
    Assert.assertEquals(SplitType.SNAPSHOT_SPLIT_TYPE, desObj.getType());
  }
}
