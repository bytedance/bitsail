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

package com.bytedance.bitsail.connector.rocketmq.sink;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HashQueueSelectorTest {

  private HashQueueSelector selector;

  @Before
  public void setUp() {
    selector = new HashQueueSelector();
  }

  @Test
  public void testSelectWithNonNullPartitionKey() {
    List<MessageQueue> mqList = createMessageQueueList(3);
    Message message = new Message("TestTopic", "TestTag", "TestBody".getBytes());
    Object partitionKey = "nonNullKey";

    MessageQueue selectedQueue = selector.select(mqList, message, partitionKey);

    int expectedQueueId = Math.abs(partitionKey.hashCode()) % mqList.size();
    Assert.assertEquals(mqList.get(expectedQueueId), selectedQueue);
  }

  @Test
  public void testSelectWithNullPartitionKey() {
    List<MessageQueue> mqList = createMessageQueueList(3);
    Message message = new Message("TestTopic", "TestTag", "TestBody".getBytes());

    MessageQueue selectedQueue1 = selector.select(mqList, message, null);
    MessageQueue selectedQueue2 = selector.select(mqList, message, null);

    Assert.assertEquals(mqList.get(0), selectedQueue1);
    Assert.assertEquals(mqList.get(1), selectedQueue2);
  }

  private List<MessageQueue> createMessageQueueList(int size) {
    List<MessageQueue> mqList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      mqList.add(new MessageQueue("TestTopic", "TestBroker", i));
    }
    return mqList;
  }

}