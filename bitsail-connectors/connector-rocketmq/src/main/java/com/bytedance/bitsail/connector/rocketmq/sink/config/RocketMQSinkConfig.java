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

package com.bytedance.bitsail.connector.rocketmq.sink.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.rocketmq.error.RocketMQErrorCode;
import com.bytedance.bitsail.connector.rocketmq.option.RocketMQWriterOptions;

import lombok.Data;

import java.io.Serializable;
import java.util.UUID;

@Data
public class RocketMQSinkConfig implements Serializable {
  private static final long serialVersionUID = 2L;

  private String nameServerAddress;
  private String producerGroup;
  private String topic;
  private String tag;

  private boolean enableBatchFlush;
  private int batchSize;

  private String accessKey;
  private String secretKey;

  private boolean logFailuresOnly;
  private boolean enableSyncSend;

  private int failureRetryTimes;
  private int sendMsgTimeout;
  private int maxMessageSize;
  private boolean vipChannelEnabled;
  private int defaultTopicQueueNums;
  private int compressMsgBodyOverHowmuch;
  private int heartbeatBrokerInterval;

  public RocketMQSinkConfig(BitSailConfiguration outputSliceConfig) {
    this.nameServerAddress = outputSliceConfig.getNecessaryOption(RocketMQWriterOptions.NAME_SERVER_ADDRESS,
        RocketMQErrorCode.REQUIRED_VALUE);
    this.producerGroup = outputSliceConfig.getUnNecessaryOption(RocketMQWriterOptions.PRODUCER_GROUP,
        UUID.randomUUID().toString());
    this.topic = outputSliceConfig.getNecessaryOption(RocketMQWriterOptions.TOPIC,
        RocketMQErrorCode.REQUIRED_VALUE);
    this.tag = outputSliceConfig.get(RocketMQWriterOptions.TAG);

    this.enableBatchFlush = outputSliceConfig.get(RocketMQWriterOptions.ENABLE_BATCH_FLUSH);
    this.batchSize = outputSliceConfig.get(RocketMQWriterOptions.BATCH_SIZE);

    this.accessKey = outputSliceConfig.get(RocketMQWriterOptions.ACCESS_KEY);
    this.secretKey = outputSliceConfig.get(RocketMQWriterOptions.SECRET_KEY);

    this.logFailuresOnly = outputSliceConfig.get(RocketMQWriterOptions.LOG_FAILURES_ONLY);
    this.enableSyncSend = outputSliceConfig.get(RocketMQWriterOptions.ENABLE_SYNC_SEND);

    this.failureRetryTimes = outputSliceConfig.get(RocketMQWriterOptions.SEND_FAILURE_RETRY_TIMES);
    this.sendMsgTimeout = outputSliceConfig.get(RocketMQWriterOptions.SEND_MESSAGE_TIMEOUT);
    this.maxMessageSize = outputSliceConfig.get(RocketMQWriterOptions.MAX_MESSAGE_SIZE);
    this.vipChannelEnabled = outputSliceConfig.get(RocketMQWriterOptions.VIP_CHANNEL_ENABLED);
    this.defaultTopicQueueNums = outputSliceConfig.get(RocketMQWriterOptions.DEFAULT_TOPIC_QUEUE_NUMS);
    this.compressMsgBodyOverHowmuch = outputSliceConfig.get(RocketMQWriterOptions.COMPRESS_MSG_BODY_SIZE);
    this.heartbeatBrokerInterval = outputSliceConfig.get(RocketMQWriterOptions.HEART_BEAT_BROKER_INTERVAL);
  }

  @Override
  public String toString() {
    return "RocketMQSinkConfig{" +
        "nameServerAddress='" + nameServerAddress + '\'' +
        ", producerGroup='" + producerGroup + '\'' +
        ", topic='" + topic + '\'' +
        ", tag='" + tag + '\'' +
        ", enableBatchFlush=" + enableBatchFlush +
        ", batchSize=" + batchSize +
        ", accessKey='" + accessKey + '\'' +
        ", secretKey='" + secretKey + '\'' +
        ", logFailuresOnly=" + logFailuresOnly +
        ", enableSyncSend=" + enableSyncSend +
        ", failureRetryTimes=" + failureRetryTimes +
        ", sendMsgTimeout=" + sendMsgTimeout +
        ", maxMessageSize=" + maxMessageSize +
        ", vipChannelEnabled=" + vipChannelEnabled +
        ", defaultTopicQueueNums=" + defaultTopicQueueNums +
        ", compressMsgBodyOverHowmuch=" + compressMsgBodyOverHowmuch +
        ", heartbeatBrokerInterval=" + heartbeatBrokerInterval +
        '}';
  }
}

