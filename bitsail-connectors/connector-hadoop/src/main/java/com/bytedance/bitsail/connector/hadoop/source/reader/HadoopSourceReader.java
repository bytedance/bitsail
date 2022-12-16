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
import com.bytedance.bitsail.base.enumerate.ContentType;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.component.format.csv.CsvDeserializationSchema;
import com.bytedance.bitsail.component.format.json.JsonDeserializationSchema;
import com.bytedance.bitsail.connector.hadoop.error.HadoopErrorCode;
import com.bytedance.bitsail.connector.hadoop.option.HadoopReaderOptions;
import com.bytedance.bitsail.connector.hadoop.source.config.HadoopConf;
import com.bytedance.bitsail.connector.hadoop.source.split.HadoopSourceSplit;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.bytedance.bitsail.common.option.ReaderOptions.BaseReaderOptions.CONTENT_TYPE;

public class HadoopSourceReader implements SourceReader<Row, HadoopSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopSourceReader.class);
  private static final long serialVersionUID = 1L;
  private final BitSailConfiguration readerConfiguration;
  private final DeserializationSchema<byte[], Row> deserializationSchema;
  private final Context readerContext;
  private final HadoopConf hadoopConf;
  private final HashSet<HadoopSourceSplit> assignedHadoopSplits;
  private final HashSet<HadoopSourceSplit> finishedHadoopSplits;
  private boolean noMoreSplits;

  public HadoopSourceReader(BitSailConfiguration readerConfiguration, Context readerContext) {
    this.readerConfiguration = readerConfiguration;
    this.readerContext = readerContext;
    this.assignedHadoopSplits = Sets.newHashSet();
    this.finishedHadoopSplits = Sets.newHashSet();
    this.noMoreSplits = false;

    String defaultFS = readerConfiguration.getNecessaryOption(HadoopReaderOptions.DEFAULT_FS, HadoopErrorCode.REQUIRED_VALUE);
    this.hadoopConf = new HadoopConf(defaultFS);
    ContentType contentType = ContentType.valueOf(readerConfiguration.getNecessaryOption(CONTENT_TYPE, HadoopErrorCode.REQUIRED_VALUE).toUpperCase());
    switch (contentType) {
      case CSV:
        deserializationSchema =
            new CsvDeserializationSchema(readerConfiguration, readerContext.getTypeInfos(), readerContext.getFieldNames());
        break;
      case JSON:
        deserializationSchema =
            new JsonDeserializationSchema(readerConfiguration, readerContext.getTypeInfos(), readerContext.getFieldNames());
        ;
        break;
      default:
        throw BitSailException.asBitSailException(HadoopErrorCode.UNSUPPORTED_ENCODING, "unsupported parser type: " + contentType);
    }
  }

  @Override
  public void start() {

  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    for (HadoopSourceSplit sourceSplit : assignedHadoopSplits) {
      String hadoopPath = sourceSplit.uniqSplitId();
      LOG.info("Start to process split: {}", hadoopPath);
      Configuration conf = getConfiguration(hadoopConf);
      FileSystem fs = FileSystem.get(conf);
      Path filePath = new Path(hadoopPath);
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath), StandardCharsets.UTF_8))) {
        reader.lines().forEach(line -> {
          try {
            Row row = deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8));
            pipeline.output(row);
          } catch (IOException e) {
            throw BitSailException.asBitSailException(HadoopErrorCode.HDFS_IO, e);
          }
        });
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

  private Configuration getConfiguration(HadoopConf hadoopConf) {
    Configuration configuration = new Configuration();
    configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hadoopConf.getHdfsNameKey());
    configuration.set(String.format("fs.%s.impl", hadoopConf.getSchema()), hadoopConf.getFsHdfsImpl());
    hadoopConf.setExtraOptionsForConfiguration(configuration);
    return configuration;
  }

  @Override
  public List<HadoopSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void close() throws Exception {

  }
}
