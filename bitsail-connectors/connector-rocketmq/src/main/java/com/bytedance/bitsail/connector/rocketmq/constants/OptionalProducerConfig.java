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

package com.bytedance.bitsail.connector.rocketmq.constants;

public class OptionalProducerConfig {
  public static final String INSTANCE_NAME = "instanceName";
  public static final String VIP_CHANNEL = "vipChannelEnabled";
  public static final String DEFAULT_TOPIC_QUEUE_NUMS = "defaultTopicQueueNums";
  public static final String COMPRESS_SG_BODY_OVER = "compressMsgBodyOverHowmuch";
  public static final String HEARTBEAT_BROKER_INTERVAL = "heartbeatBrokerInterval";
}
