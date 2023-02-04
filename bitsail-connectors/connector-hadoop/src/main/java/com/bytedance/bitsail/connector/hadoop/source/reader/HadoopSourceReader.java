/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.hadoop.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.hadoop.format.HiveInputFormatDeserializationSchema;
import com.bytedance.bitsail.connector.hadoop.format.TextInputFormatDeserializationSchema;
import com.bytedance.bitsail.connector.hadoop.option.HadoopReaderOptions;
import com.bytedance.bitsail.connector.hadoop.source.split.HadoopSourceSplit;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class HadoopSourceReader<K, V> implements SourceReader<Row, HadoopSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopSourceReader.class);
  private static final long serialVersionUID = 1L;
  private final BitSailConfiguration readerConfiguration;
  private final Context readerContext;
  private final HashSet<HadoopSourceSplit> assignedHadoopSplits;
  private final HashSet<HadoopSourceSplit> finishedHadoopSplits;
  private boolean noMoreSplits;
  protected JobConf jobConf;
  private InputFormat<K, V> mapredInputFormat;
  private K key;
  private V value;
  private DeserializationSchema<Writable, Row> deserializationSchema;
  private RecordReader<K, V> recordReader;

  public HadoopSourceReader(BitSailConfiguration readerConfiguration, Context readerContext, List<String> hadoopPathList) {
    this.readerConfiguration = readerConfiguration;
    this.readerContext = readerContext;
    this.assignedHadoopSplits = Sets.newHashSet();
    this.finishedHadoopSplits = Sets.newHashSet();
    this.jobConf = new JobConf();
    for (String path : hadoopPathList) {
      FileInputFormat.addInputPath(this.jobConf, new Path(path));
    }

    String inputClassName = readerConfiguration.get(HadoopReaderOptions.HADOOP_INPUT_FORMAT_CLASS);
    Class<?> inputClass;
    try {
      inputClass = Class.forName(inputClassName);
      this.mapredInputFormat = (InputFormat<K, V>) inputClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ReflectionUtils.setConf(mapredInputFormat, jobConf);
    this.noMoreSplits = false;

    if (this.mapredInputFormat instanceof TextInputFormat) {
      deserializationSchema = new TextInputFormatDeserializationSchema(
          readerConfiguration,
          readerContext.getTypeInfos(),
          readerContext.getFieldNames());
    } else if (this.mapredInputFormat instanceof MapredParquetInputFormat
        || this.mapredInputFormat instanceof OrcInputFormat) {
      deserializationSchema = new HiveInputFormatDeserializationSchema(
          readerConfiguration,
          readerContext.getTypeInfos(),
          readerContext.getFieldNames());
    }
  }

  @Override
  public void start() {

  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    for (HadoopSourceSplit sourceSplit : assignedHadoopSplits) {

      sourceSplit.initInputSplit(jobConf);
      LOG.info("Start to process split: {}", sourceSplit.uniqSplitId());
      this.recordReader = this.mapredInputFormat.getRecordReader(sourceSplit.getHadoopInputSplit(), jobConf, Reporter.NULL);
      if (this.recordReader instanceof Configurable) {
        ((Configurable) this.recordReader).setConf(jobConf);
      }
      key = this.recordReader.createKey();
      value = this.recordReader.createValue();
      while (this.recordReader.next(key, value)) {
        Row row = deserializationSchema.deserialize((Writable) value);
        pipeline.output(row);
      }
      finishedHadoopSplits.add(sourceSplit);
    }
    assignedHadoopSplits.removeAll(finishedHadoopSplits);
  }

  @Override
  public void notifyNoMoreSplits() {
    LOG.info("Subtask {} received no more split signal.", readerContext.getIndexOfSubtask());
    noMoreSplits = true;
  }

  @Override
  public void addSplits(List<HadoopSourceSplit> splitList) {
    assignedHadoopSplits.addAll(splitList);
  }

  @Override
  public boolean hasMoreElements() {
    if (noMoreSplits) {
      return CollectionUtils.size(assignedHadoopSplits) != 0;
    }
    return true;
  }

  @Override
  public List<HadoopSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void close() throws Exception {
    recordReader.close();
  }
}
