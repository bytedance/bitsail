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

package com.bytedance.bitsail.connector.kafka.sink.callback;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.kafka.option.KafkaOptions;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class CallbackWrapper implements Callback {
  private static final Logger LOG = LoggerFactory.getLogger(CallbackWrapper.class);
  private final BitSailConfiguration configuration;
  private final Writer.Context<?> context;
  private final boolean skipException;

  private transient IOException throwable;

  public CallbackWrapper(BitSailConfiguration configuration,
                         Writer.Context<?> context) {
    this.configuration = configuration;
    this.skipException = configuration.get(KafkaOptions.LOG_FAILURES_ONLY);
    this.context = context;
  }

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (Objects.nonNull(exception)) {
      throwable = new IOException(exception);
    }
  }

  public void checkErroneous() throws IOException {
    if (Objects.isNull(throwable)) {
      return;
    }
    if (skipException) {
      LOG.error("Subtask {} flush data to kafka failed.", context.getIndexOfSubTaskId(), throwable);
      return;
    }
    throw throwable;
  }
}
