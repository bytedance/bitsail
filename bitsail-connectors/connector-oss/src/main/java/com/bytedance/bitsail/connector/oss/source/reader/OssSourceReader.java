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

package com.bytedance.bitsail.connector.oss.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.oss.config.OssConf;
import com.bytedance.bitsail.connector.oss.config.OssConfig;
import com.bytedance.bitsail.connector.oss.constant.OssConstants;
import com.bytedance.bitsail.connector.oss.exception.OssConnectorErrorCode;
import com.bytedance.bitsail.connector.oss.source.split.OssSourceSplit;

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
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class OssSourceReader implements SourceReader<Row, OssSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(OssSourceReader.class);
  protected OssConf ossConf;
  private final transient DeserializationSchema<byte[], Row> deserializationSchema;
  private final OssConfig ossConfig;
  private final transient Context context;
  private final Deque<OssSourceSplit> splits;
  private boolean skipFirstLine = false;
  private boolean hasNoMoreSplits = false;
  private int totalSplitNum = 0;
  private int skipFirstLineNums = 0;
  private OssSourceSplit currentSplit;
  FileSystem fs;

  public OssSourceReader(BitSailConfiguration jobConf, Context context) {
    this.ossConfig = new OssConfig(jobConf);
    this.context = context;
    this.deserializationSchema = DeserializationSchemaFactory.createDeserializationSchema(jobConf, context, ossConfig);
    this.splits = new LinkedList<>();
    this.ossConf = OssConf.buildWithConfig(jobConf);
    LOG.info("OssSourceReader is initialized.");
  }

  @Override
  public void start() {
    if (this.ossConfig.getSkipFirstLine()) {
      skipFirstLine = true;
      this.skipFirstLineNums = 1;
    }
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    if (currentSplit == null && splits.isEmpty()) {
      LOG.info("pollnext no splits");
      Thread.sleep(OssConstants.OSS_SOURCE_SLEEP_MILL_SECS);
      return;
    }
    LOG.info("pollnext split size {}", this.splits.size());
    Configuration conf = getConfiguration();
    fs = FileSystem.get(conf);
    this.currentSplit = this.splits.poll();
    LOG.info("split {} path {}", currentSplit, currentSplit.getPath());
    Path filePath = new Path(currentSplit.getPath());
    try (BufferedReader reader =
             new BufferedReader(
                 new InputStreamReader(fs.open(filePath), StandardCharsets.UTF_8))) {
      reader.lines()
          .skip(skipFirstLineNums)
          .forEach(
              line -> {
                try {
                  if (line != null) {
                    Row row = deserializationSchema.deserialize(line.getBytes());
                    pipeline.output(row);
                  }
                } catch (IOException e) {
                  String errorMsg =
                      String.format(
                          "Read data from this file [%s] failed",
                          filePath);
                  throw BitSailException.asBitSailException(
                      OssConnectorErrorCode.FILE_OPERATION_FAILED, errorMsg, e);
                }
              });
    }
  }

  Configuration getConfiguration() {
    return getConfiguration(this.ossConf);
  }

  public Configuration getConfiguration(OssConf ossConf) {
    Configuration configuration = new Configuration();
    configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, ossConf.getHdfsNameKey());
    configuration.set(
        String.format("fs.%s.impl", ossConf.getSchema()), ossConf.getHdfsImpl());
    ossConf.setExtraOptionsForConfiguration(configuration);
    return configuration;
  }

  @Override
  public void addSplits(List<OssSourceSplit> splitList) {
    totalSplitNum += splitList.size();
    this.splits.addAll(splitList);
  }

  @Override
  public boolean hasMoreElements() {
    if (hasNoMoreSplits && splits.isEmpty()) {
      LOG.info("Finish reading all {} splits.", totalSplitNum);
      return false;
    }
    return true;
  }

  @Override
  public void notifyNoMoreSplits() {
    this.hasNoMoreSplits = true;
    LOG.info("No more splits will be assigned.");
  }

  @Override
  public List<OssSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    SourceReader.super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void close() throws Exception {
    if (fs != null) {
      fs.close();
    }
  }
}
