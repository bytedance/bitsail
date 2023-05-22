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

package com.bytedance.bitsail.core.flink.bridge.reader.delegate;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.dirty.AbstractDirtyCollector;
import com.bytedance.bitsail.base.messenger.Messenger;
import com.bytedance.bitsail.base.messenger.common.MessageType;
import com.bytedance.bitsail.base.metrics.MetricManager;
import com.bytedance.bitsail.base.metrics.manager.CallTracer;
import com.bytedance.bitsail.base.ratelimit.Channel;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.flink.core.delagate.converter.FlinkRowConverter;
import com.bytedance.bitsail.flink.core.util.RowUtil;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.bytedance.bitsail.base.constants.BaseMetricsNames.RECORD_CHANNEL_FLOW_CONTROL;

public class DelegateSourcePipeline<T> implements SourcePipeline<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DelegateSourcePipeline.class);

  private final ReaderOutput<T> readerOutput;

  //todo flink row converter and watermark.
  private final FlinkRowConverter flinkRowConvertSerializer;

  private final AbstractDirtyCollector dirtyCollector;

  private final Messenger messenger;

  private final MetricManager metricManager;

  private final BitSailConfiguration commonConfiguration;

  private Channel trafficLimiter;

  public DelegateSourcePipeline(ReaderOutput<T> readerOutput,
                                FlinkRowConverter flinkRowConvertSerializer,
                                MetricManager metricManager,
                                Messenger messenger,
                                AbstractDirtyCollector dirtyCollectorFactory,
                                BitSailConfiguration commonConfiguration) {
    this.readerOutput = readerOutput;
    this.flinkRowConvertSerializer = flinkRowConvertSerializer;
    this.metricManager = metricManager;
    this.messenger = messenger;
    this.dirtyCollector = dirtyCollectorFactory;
    this.commonConfiguration = commonConfiguration;
    preparePipeline();
  }

  private void preparePipeline() {
    long recordSpeed = commonConfiguration.get(CommonOptions.READER_TRANSPORT_CHANNEL_SPEED_RECORD);
    long byteSpeed = commonConfiguration.get(CommonOptions.READER_TRANSPORT_CHANNEL_SPEED_BYTE);
    trafficLimiter = new Channel(recordSpeed, byteSpeed);
  }

  @Override
  public void output(T record) throws IOException {
    org.apache.flink.types.Row serialize;
    try {
      serialize = flinkRowConvertSerializer.to((Row) record);
      long rowBytesSize = RowUtil.getRowBytesSize(serialize);
      messenger.addSuccessRecord(rowBytesSize);
      metricManager.reportRecord(rowBytesSize, MessageType.SUCCESS);
    } catch (BitSailException e) {
      LOG.debug("Failed to write one record. - {}", record, e);
      messenger.addFailedRecord(e);
      dirtyCollector.collectDirty(record, e, System.currentTimeMillis());
      metricManager.reportRecord(0, MessageType.FAILED);
      return;
    }
    try (CallTracer ignore = metricManager.recordTimer(RECORD_CHANNEL_FLOW_CONTROL).get()) {
      trafficLimiter.checkFlowControl(
          messenger.getSuccessRecords(),
          messenger.getSuccessRecordBytes(),
          messenger.getFailedRecords()
      );
    }
    readerOutput.collect((T) serialize);
  }

  @Override
  public void output(T record, long timestamp) {
    readerOutput.collect(record, timestamp);
  }
}
