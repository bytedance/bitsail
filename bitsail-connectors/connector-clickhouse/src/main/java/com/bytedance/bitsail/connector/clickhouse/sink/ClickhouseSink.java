package com.bytedance.bitsail.connector.clickhouse.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;
import com.bytedance.bitsail.connector.clickhouse.constant.ClickhouseConstants;

import java.io.IOException;
import java.io.Serializable;

public class ClickhouseSink<CommitT extends Serializable> implements Sink<Row, CommitT, EmptyState> {
  private BitSailConfiguration jobConf;

  @Override
  public String getWriterName() {
    return ClickhouseConstants.CLICKHOUSE_CONNECTOR_NAME;
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) throws Exception {
    this.jobConf = writerConfiguration;
  }

  @Override
  public Writer<Row, CommitT, EmptyState> createWriter(Writer.Context<EmptyState> context) throws IOException {
    return new ClickhouseWriter<>(this.jobConf, context);
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new FileMappingTypeInfoConverter(getWriterName());
  }
}

